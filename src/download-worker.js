import { parentPort, workerData } from 'node:worker_threads';
import {
  openSync,
  writevSync,
  writev as writevCb,
  write as writeCb,
  writeSync,
  unlinkSync,
  closeSync,
  writeFileSync,
  constants as FS,
} from 'node:fs';
import path from 'node:path';
import { promisify } from 'node:util';
import { Readable } from 'node:stream';
import { GetObjectCommand } from '@aws-sdk/client-s3';

// Async positional vectored write (libuv threadpool) — used when fileAsync is on,
// so the disk write doesn't block this worker's event loop / socket draining.
const writevAsync = promisify(writevCb);
import { makeClient } from './s3.js';
import { IpThroughputTracker } from './ip-throughput.js';
import { installNativeCrc32 } from './crc32-native.mjs';
import { bumpProgress, progressView } from './progress.js';

/**
 * Worker thread: downloads parts by PartNumber. Each part carries its own object
 * `key`, so a single pool can span many objects (the "X files per size" workload).
 *
 * Two operating modes:
 *   - discard / file  : SLICE mode. The worker owns a fixed slice of parts and
 *                       processes them with up to `concurrency` lanes on 'start'.
 *   - ordered-stream  : DISPATCH mode. The main thread assigns parts one at a time
 *                       (lowest-needed first, budget-bounded) via 'assign'; the
 *                       worker downloads and transfers each to main for in-order
 *                       delivery. This keeps the reorder buffer bounded and never
 *                       starves the next-to-deliver part.
 *
 * Protocol (slice):    main->worker 'start'; worker->main 'done'
 * Protocol (dispatch): main->worker 'assign' {part}; worker->main 'part' (transfer);
 *                      main->worker 'stop'; worker->main 'worker-done'
 */

const {
  bucket,
  region,
  parts, // slice mode: [{ key, partNumber, offset, size }]
  concurrency,
  keep,
  maxSockets,
  validateChecksum,
  deliveryMode,
  filePaths, // { key -> local file path } for deliveryMode === 'file'
  logConnections,
  spreadConnections,
  tls,
  ipThroughput,
  httpHandler,
  ciphers, // OpenSSL cipher string to pin the TLS suite (null = defaults)
  stallTimeoutMs, // abort+retry a part that reads no bytes for this long (0 = off)
  partRetries, // max stall-retries per part before giving up
  partTimes, // when true, record per-part download wall time (+ vip/conn id) and report it
  workerId = 0, // index of this worker, used to make connection ids globally unique
  bufferPool, // ordered-drop: copy chunks into reused contiguous part buffers
  bufferReturn = true, // ordered-stream: reuse dedicated buffers handed back by main
  fileAsync, // legacy (--no-api slice) file mode: write each part asynchronously
  // file (dispatch/API) mode — SIMPLE inline O_DIRECT writer. This worker downloads
  // each range into a reusable (block-aligned) buffer and writes it straight to the
  // destination file at its offset, preferring O_DIRECT (bypass the page cache).
  // There is no shared queue and no separate writer pool: backpressure is just the
  // number of in-flight ranges (workers x concurrency) the scheduler dispatches.
  rangeSize, // max range length (bytes) — sizes the reusable write buffers
  fileDirect, // request O_DIRECT (opt out => false => buffered writes)
  fileChunk, // O_DIRECT pwrite granularity within a range
  fileDiscard, // drain each range WITHOUT writing (network/ingest ceiling A/B)
  deliveryPath, // dir used for the one-time O_DIRECT support/alignment probe
  profile, // when true, CPU-profile this worker and write a .cpuprofile
  profileDir, // directory for the per-worker .cpuprofile files
  nativeCrc32, // patch @aws-crypto/crc32 to use native zlib.crc32
  progressBuf, // shared byte counter for the live progress indicator (or null)
} = workerData;

const progress = progressView(progressBuf);

// Confirm (or, for older SDKs, force) native zlib.crc32 for CRC32 before any
// request creates a checksum instance. The routine "already native" case is
// silent; only worker 0 surfaces an actual patch or a failure to ensure native
// (so this doesn't print once per worker).
if (nativeCrc32) {
  const r = await installNativeCrc32();
  if (workerId === 0) {
    if (r.patched) console.error(`[native-crc32] patched: ${r.reason}`);
    else if (!r.alreadyNative) console.error(`[native-crc32] not applied: ${r.reason}`);
  }
}

// Optional per-worker CPU profiler (find where a worker spends its time — e.g. to
// diff a node-version regression). Started before 'ready' so setup is included but
// timing isn't affected; stopped + written when the worker finishes.
let profSession = null;
if (profile) {
  const inspector = await import('node:inspector/promises');
  profSession = new inspector.Session();
  profSession.connect();
  await profSession.post('Profiler.enable');
  await profSession.post('Profiler.setSamplingInterval', { interval: 250 });
  await profSession.post('Profiler.start');
}
async function stopProfile() {
  if (!profSession) return;
  const s = profSession;
  profSession = null;
  try {
    const { profile: p } = await s.post('Profiler.stop');
    s.disconnect();
    writeFileSync(`${profileDir}/dl-worker-${workerId}.cpuprofile`, JSON.stringify(p));
  } catch {
    /* ignore profiling errors */
  }
}

const STALL_MS = Number(stallTimeoutMs) > 0 ? Number(stallTimeoutMs) : 0;
const MAX_PART_RETRIES = Number.isFinite(Number(partRetries)) ? Number(partRetries) : 3;

const ipCounts = logConnections ? new Map() : null;
const tracker = ipThroughput ? new IpThroughputTracker((s) => s.bytesRead) : null;
const onConnect =
  logConnections || ipThroughput
    ? (ip, socket) => {
        if (ipCounts) ipCounts.set(ip, (ipCounts.get(ip) || 0) + 1);
        if (tracker) tracker.register(socket, ip);
      }
    : null;

// Record the negotiated TLS protocol/cipher of the first secure socket, so the
// benchmark can confirm which cipher was actually used (e.g. an --cipher pin).
let tlsInfo = null;
const onTls = (info) => {
  if (!tlsInfo) tlsInfo = info;
};

const client = makeClient({
  region, maxSockets, validateChecksum, onConnect, spreadConnections, tls, httpHandler,
  captureSocket: Boolean(partTimes), connIdPrefix: `w${workerId}`, ciphers, onTls,
});

// Lazily-opened file descriptors, one per key (file mode only).
const fds = deliveryMode === 'file' ? new Map() : null;
function fdFor(key) {
  let fd = fds.get(key);
  if (fd === undefined) {
    fd = openSync(filePaths[key], 'r+');
    fds.set(key, fd);
  }
  return fd;
}

// Test hook: artificially delay a specific part number to simulate a slow
// low-numbered part (exercises ordered-stream backpressure/head-of-line). Inert
// unless BENCH_SLOW_PART is set.
const SLOW_PART = Number(process.env.BENCH_SLOW_PART || 0);
const SLOW_MS = Number(process.env.BENCH_SLOW_MS || 0);
// When set, only delay the FIRST fetch of the slow part (exercises stall-retry
// recovery: the retry re-fetches with no delay and succeeds).
const SLOW_ONCE = process.env.BENCH_SLOW_ONCE === '1';
const slowedOnce = new Set();
// Test hook: first fetch of this part returns a body that never yields any bytes,
// simulating a true mid-stream stall. The watchdog must abort + re-fetch; the
// retry gets the real body. Inert unless BENCH_STALL_PART is set.
const STALL_PART = Number(process.env.BENCH_STALL_PART || 0);
const stalledOnce = new Set();
async function maybeDelay(part) {
  if (SLOW_PART && SLOW_MS && part.partNumber === SLOW_PART) {
    if (SLOW_ONCE && slowedOnce.has(part.partNumber)) return;
    slowedOnce.add(part.partNumber);
    await new Promise((r) => setTimeout(r, SLOW_MS));
  }
}

/**
 * Force the stalled read to end when our watchdog aborts. Relying on the SDK to
 * tear down an already-returned streaming body on abort is unreliable, so we
 * destroy the body ourselves — that makes the consuming `for await` throw
 * promptly so the retry loop can advance instead of hanging on a dead socket.
 */
function killOnAbort(body, signal) {
  if (!signal || !body) return;
  const kill = () => {
    try {
      if (typeof body.destroy === 'function') body.destroy(new Error('stalled: aborted by watchdog'));
      else if (typeof body.cancel === 'function') body.cancel();
    } catch {
      /* ignore */
    }
  };
  if (signal.aborted) kill();
  else signal.addEventListener('abort', kill, { once: true });
}

async function getPart(part, signal) {
  await maybeDelay(part);

  // Test hook: first fetch of the designated part returns a body that never
  // yields, simulating a true mid-stream stall (no real request is issued, so
  // there's no abandoned socket). The watchdog must abort + destroy it; the retry
  // takes the real path below.
  if (STALL_PART && part.partNumber === STALL_PART && !stalledOnce.has(part.partNumber)) {
    stalledOnce.add(part.partNumber);
    const body = new Readable({ read() {} });
    killOnAbort(body, signal);
    return { body, hasChecksum: false, vip: null, connId: null };
  }

  const res = await client.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: part.key,
      // Fetch a fixed-size byte RANGE (planned on main from download.rangeSize),
      // not a whole part. Arbitrary ranges carry no per-part checksum, so
      // ChecksumMode is a no-op here unless a range happens to cover the object.
      Range: `bytes=${part.offset}-${part.offset + part.size - 1}`,
      ...(validateChecksum ? { ChecksumMode: 'ENABLED' } : {}),
    }),
    signal ? { abortSignal: signal } : {},
  );
  const hasChecksum = Boolean(
    res.ChecksumCRC32C || res.ChecksumCRC32 || res.ChecksumSHA1 || res.ChecksumSHA256,
  );
  // The serving socket's IP + connection id, captured by the deserialize-step
  // middleware in makeClient (null when captureSocket is off or on the undici path).
  const conn = res.$benchConn ?? {};
  killOnAbort(res.Body, signal);
  return { body: res.Body, hasChecksum, vip: conn.vip ?? null, connId: conn.connId ?? null };
}

/**
 * Run one GET+consume attempt under a stall watchdog. `attempt(signal, bump)`
 * must issue the request with the given abortSignal and call bump() as bytes
 * arrive. If no bytes are read for STALL_MS, the request is aborted and the whole
 * part is re-fetched from scratch (ranged GET by PartNumber is idempotent), up to
 * MAX_PART_RETRIES times. Only our own stall-abort triggers a retry here; other
 * errors propagate (the SDK already retries transient network faults internally).
 */
async function withStallRetry(label, attempt) {
  for (let tryNo = 0; ; tryNo++) {
    const ac = new AbortController();
    let lastProgress = Date.now();
    let timer = null;
    if (STALL_MS > 0) {
      timer = setInterval(() => {
        if (Date.now() - lastProgress >= STALL_MS) {
          ac.abort(new Error(`stalled: no bytes for ${STALL_MS}ms`));
        }
      }, Math.min(STALL_MS, 1000));
      timer.unref?.();
    }
    try {
      return await attempt(ac.signal, () => {
        lastProgress = Date.now();
      });
    } catch (err) {
      // Only retry parts we aborted for stalling; let everything else bubble up.
      if (!ac.signal.aborted || tryNo >= MAX_PART_RETRIES) throw err;
      console.error(`[worker] ${label} stalled, re-fetching (retry ${tryNo + 1}/${MAX_PART_RETRIES})`);
    } finally {
      if (timer) clearInterval(timer);
    }
  }
}

// SLICE mode part handler (discard / file): drain + discard or positional-write.
async function downloadPart(part) {
  let vip = null;
  let connId = null;
  const r = await withStallRetry(`${part.key}#${part.partNumber}`, async (signal, bump) => {
    const got = await getPart(part, signal);
    vip = got.vip;
    connId = got.connId;
    const { body, hasChecksum } = got;

    if (deliveryMode === 'file') {
      // Collect the part's chunks, then write them all in ONE positional syscall
      // (writevSync) instead of one write per chunk. Fewer syscalls = less time
      // the (blocking) write stalls this worker's event loop. Holds one part's
      // chunks transiently (bounded by concurrency × partSize per worker).
      const fd = fdFor(part.key);
      const chunks = [];
      let bytes = 0;
      for await (const chunk of body) {
        chunks.push(chunk);
        bytes += chunk.length;
        bump();
      }
      if (chunks.length) {
        // fileAsync: write on the threadpool so the worker keeps draining sockets;
        // per-worker in-flight writes are naturally bounded by `concurrency` (each
        // lane awaits its own write). Otherwise a single blocking writevSync.
        if (fileAsync) await writevAsync(fd, chunks, part.offset);
        else writevSync(fd, chunks, part.offset);
      }
      return { bytes, hasChecksum };
    }

    // discard
    let bytes = 0;
    const kept = keep ? [] : null;
    for await (const chunk of body) {
      bytes += chunk.length;
      if (kept) kept.push(chunk);
      bump();
    }
    return { bytes, hasChecksum };
  });
  return { ...r, vip, connId };
}

function closeFds() {
  if (fds) {
    for (const fd of fds.values()) {
      try {
        closeSync(fd);
      } catch {
        /* ignore */
      }
    }
  }
  // O_DIRECT + buffered fds from the inline file (dispatch/API) writer.
  closeFileRecs();
}

// ---------------------------------------------------------------------------
// SLICE mode (discard / file): own a fixed slice, run with `concurrency` lanes.
// ---------------------------------------------------------------------------
async function runSlice() {
  let cursor = 0;
  let totalBytes = 0;
  let partsDone = 0;
  let checksummed = 0;
  const partTimings = partTimes ? [] : null;

  async function lane() {
    while (cursor < parts.length) {
      const part = parts[cursor++];
      const t0 = performance.now();
      const { bytes, hasChecksum, vip, connId } = await downloadPart(part);
      const ms = performance.now() - t0;
      bumpProgress(progress, bytes);
      totalBytes += bytes;
      partsDone += 1;
      if (hasChecksum) checksummed += 1;
      if (partTimings) partTimings.push({ key: part.key, partNumber: part.partNumber, bytes, ms, vip, connId });
    }
  }

  const lanes = Math.max(1, Math.min(concurrency, parts.length || 1));
  const start = performance.now();
  await Promise.all(Array.from({ length: lanes }, lane));
  const elapsedMs = performance.now() - start;
  return { totalBytes, partsDone, checksummed, elapsedMs, partTimings };
}

// ---------------------------------------------------------------------------
// DISPATCH mode (ordered-stream): the main thread assigns parts one at a time and
// coordinates ordering with TINY metadata messages only. The downloaded bytes are
// HELD HERE in the worker (never transferred to main), and freed when main says
// the part has been delivered in order ('release'). This removes the main-thread
// data funnel and avoids a reassembly memcpy (we retain the raw chunks).
// ---------------------------------------------------------------------------
// Held completed-but-undelivered parts. Two representations:
//   default    -> `${key}#${partNumber}` -> chunk[] (raw chunks retained, zero-copy)
//   bufferPool -> `${key}#${partNumber}` -> Buffer  (one reused contiguous buffer;
//                 chunks are copied in on arrival, so the incoming chunks die young
//                 and the long-lived footprint is a small set of recycled buffers)
const heldChunks = new Map();

// Per-worker free list of reusable part-sized buffers (bufferPool mode only).
const bufPool = [];
function acquireBuf(size) {
  for (let i = 0; i < bufPool.length; i++) {
    if (bufPool[i].length >= size) return bufPool.splice(i, 1)[0];
  }
  return Buffer.allocUnsafe(size); // no zero-fill; we overwrite every byte we use
}
function releaseBuf(buf) {
  if (buf) bufPool.push(buf);
}

// Stream sink: the worker assembles each part into a DEDICATED ArrayBuffer and
// transfers it (zero-copy) to main. With bufferReturn, main transfers consumed
// ArrayBuffers back here, so a bounded set of buffers ping-pongs across the
// thread boundary instead of allocating one per part.
const streamMode = deliveryMode === 'ordered-stream';
const fileMode = deliveryMode === 'file';

// ---------------------------------------------------------------------------
// file (dispatch/API) mode: SIMPLE inline O_DIRECT writer.
//
// Each range is downloaded into a reusable, block-aligned buffer and written
// straight to the destination file at its byte offset — preferring O_DIRECT to
// bypass the page cache (so we measure real disk throughput), with a buffered
// fallback for the final, non-block-aligned range. No shared queue, no separate
// writer pool: the only backpressure is the number of in-flight ranges the
// scheduler dispatches (workers x concurrency).
// ---------------------------------------------------------------------------
const ALIGN = 4096; // device logical-block superset; O_DIRECT offset/len/addr unit
const O_DIRECT = FS.O_DIRECT || 0; // 0 on platforms without it (macOS/Windows)
// Positional writes always go through the libuv threadpool (promisified fs.write),
// so a disk write never blocks the worker's event loop / socket draining. The pool
// is process-global and shared across all workers, so its size (UV_THREADPOOL_SIZE)
// bounds how many range writes run at once — raise it for high worker counts.
const writeAsync = promisify(writeCb);
const RANGE_CAP = rangeSize > 0 ? rangeSize : 16 * 1024 * 1024; // reusable buffer size
const WRITE_CHUNK = fileChunk > 0 ? fileChunk : RANGE_CAP; // O_DIRECT pwrite granularity
const wantDirect = fileMode && fileDirect && !fileDiscard && O_DIRECT > 0 && Boolean(deliveryPath);
let directProbed = false;
let directUsable = false; // set by the first probe: does O_DIRECT work on this fs?

// O_DIRECT needs the write buffer's ADDRESS block-aligned, but V8 buffer memory
// isn't. For a given buffer we find the byte offset `pad` (< ALIGN) whose address
// is aligned by probing a real O_DIRECT write to a temp file on the destination fs.
// Returns the pad, or -1 if O_DIRECT can't be used on this filesystem at all.
function probePad(buf) {
  const probe = path.join(deliveryPath, `.odirect-probe-w${workerId}`);
  let found = -1;
  for (let pad = 0; pad < ALIGN; pad += 8) {
    let fd;
    try {
      fd = openSync(probe, FS.O_RDWR | FS.O_CREAT | FS.O_TRUNC | O_DIRECT, 0o600);
      const n = writeSync(fd, buf, pad, ALIGN, 0);
      closeSync(fd);
      fd = undefined;
      if (n === ALIGN) {
        found = pad;
        break;
      }
    } catch {
      try {
        if (fd !== undefined) closeSync(fd);
      } catch {
        /* ignore */
      }
    }
  }
  try {
    unlinkSync(probe);
  } catch {
    /* ignore */
  }
  return found;
}

// Per-worker pool of reusable range buffers. Each holds one range while it
// downloads, then is written and returned. With O_DIRECT a buffer is over-sized by
// ALIGN and its aligned start is `pad`; otherwise pad is 0 (buffered writes).
const bufSlots = [];
function acquireSlot() {
  const s = bufSlots.pop();
  if (s) return s;
  const buf = Buffer.allocUnsafeSlow(RANGE_CAP + (wantDirect ? ALIGN : 0));
  let pad = 0;
  let direct = false;
  if (wantDirect) {
    const p = directUsable || !directProbed ? probePad(buf) : -1;
    if (!directProbed) {
      directProbed = true;
      directUsable = p >= 0;
      if (workerId === 0) {
        console.error(
          directUsable
            ? `[file] O_DIRECT enabled on ${deliveryPath} (align pad=${p}B)`
            : `[file] O_DIRECT not usable on ${deliveryPath}; using buffered writes`,
        );
      }
    }
    if (p >= 0) {
      pad = p;
      direct = true;
    }
  }
  return { buf, pad, direct };
}
function releaseSlot(s) {
  bufSlots.push(s);
}

// One { directFd, bufferedFd } per destination path. directFd is opened O_DIRECT;
// bufferedFd is opened lazily only when an unaligned (final) range needs it. The
// output file is pre-created and pre-sized by the benchmark, so O_RDWR (no create).
const fileRecs = new Map();
function recForPath(p) {
  let rec = fileRecs.get(p);
  if (!rec) {
    rec = { directFd: null, bufferedFd: null };
    fileRecs.set(p, rec);
  }
  return rec;
}
async function pwrite(fd, buf, off, len, pos) {
  return (await writeAsync(fd, buf, off, len, pos)).bytesWritten;
}
async function writeRangeToFile(dest, slot, byteLength, fileOffset) {
  const rec = recForPath(dest);
  const { buf, pad, direct } = slot;
  let w = 0;
  while (w < byteLength) {
    const n = Math.min(WRITE_CHUNK, byteLength - w);
    const off = fileOffset + w;
    const soff = pad + w;
    if (direct && n % ALIGN === 0 && off % ALIGN === 0) {
      if (rec.directFd === null) rec.directFd = openSync(dest, FS.O_RDWR | O_DIRECT);
      const wrote = await pwrite(rec.directFd, buf, soff, n, off);
      if (wrote !== n) throw new Error(`short O_DIRECT write ${wrote}/${n} @${off}`);
    } else {
      if (rec.bufferedFd === null) rec.bufferedFd = openSync(dest, 'r+');
      let x = 0;
      while (x < n) x += await pwrite(rec.bufferedFd, buf, soff + x, n - x, off + x);
    }
    w += n;
  }
}
function closeFileRecs() {
  for (const rec of fileRecs.values()) {
    try {
      if (rec.directFd !== null) closeSync(rec.directFd);
    } catch {
      /* ignore */
    }
    try {
      if (rec.bufferedFd !== null) closeSync(rec.bufferedFd);
    } catch {
      /* ignore */
    }
  }
  fileRecs.clear();
}
const returnedBufs = []; // ArrayBuffers handed back by main, available for reuse
function acquireArrayBuffer(size) {
  if (bufferReturn) {
    for (let i = 0; i < returnedBufs.length; i++) {
      if (returnedBufs[i].byteLength >= size) return returnedBufs.splice(i, 1)[0];
    }
  }
  // allocUnsafeSlow -> its own (non-pooled) ArrayBuffer, safe to transfer wholesale.
  return Buffer.allocUnsafeSlow(size).buffer;
}

let dispatchInFlight = 0;
let dispatchStopped = false;
let dPartsDone = 0;
let dChecksummed = 0;

let doneEmitted = false;
async function dispatchMaybeDone() {
  if (dispatchStopped && dispatchInFlight === 0 && !doneEmitted) {
    doneEmitted = true;
    await stopProfile();
    const ipTput = tracker ? tracker.snapshot() : null;
    parentPort.postMessage({
      type: 'worker-done',
      parts: dPartsDone,
      checksummed: dChecksummed,
      ipCounts: ipCounts ? [...ipCounts] : null,
      ipThroughput: ipTput,
      tlsInfo,
    });
    heldChunks.clear();
    bufPool.length = 0;
    returnedBufs.length = 0;
    closeFds();
    client.destroy();
  }
}

if (deliveryMode === 'ordered-stream' || deliveryMode === 'ordered-drop' || deliveryMode === 'file') {
  parentPort.on('message', async (msg) => {
    if (msg?.type === 'assign') {
      dispatchInFlight += 1;
      try {
        const label = `${msg.part.key}#${msg.part.partNumber}`;
        const t0 = performance.now();
        let byteLength = 0;
        let vip = null;
        let connId = null;

        if (fileMode) {
          // Download the whole range into a reusable (block-aligned) buffer, then
          // write it straight to the destination file at its offset with O_DIRECT
          // (buffered fallback for the final, unaligned range). In fileDiscard mode
          // we drain the range but don't write it (network/ingest ceiling A/B). No
          // shared queue, no writer pool — just this worker.
          const slot = acquireSlot();
          const base = slot.pad; // aligned start within the reusable buffer
          const hasChecksum = await withStallRetry(label, async (signal, bump) => {
            const got = await getPart(msg.part, signal);
            vip = got.vip;
            connId = got.connId;
            // Refill from byte 0 each attempt: a stall-retry re-downloads the whole
            // range into the SAME buffer, so a range is never written twice/partially.
            byteLength = 0;
            for await (const chunk of got.body) {
              chunk.copy(slot.buf, base + byteLength, 0, chunk.length);
              byteLength += chunk.length;
              bump();
            }
            return got.hasChecksum;
          });
          if (!fileDiscard) await writeRangeToFile(msg.part.file, slot, byteLength, msg.part.offset);
          releaseSlot(slot);
          const downloadMs = performance.now() - t0;
          dPartsDone += 1;
          if (hasChecksum) dChecksummed += 1;
          dispatchInFlight -= 1;
          bumpProgress(progress, byteLength); // bytes are on disk now (or drained in discard)
          parentPort.postMessage({
            type: 'part-downloaded',
            key: msg.part.key,
            partNumber: msg.part.partNumber,
            byteLength,
            hasChecksum,
            downloadMs: partTimes ? downloadMs : undefined,
            vip: partTimes ? vip : undefined,
            connId: partTimes ? connId : undefined,
          });
          dispatchMaybeDone();
          return;
        }

        if (streamMode) {
          // Assemble the part into a dedicated ArrayBuffer, then TRANSFER it to
          // main (zero-copy) for in-order delivery into a per-object Readable.
          const ab = acquireArrayBuffer(msg.part.size);
          const view = Buffer.from(ab, 0, msg.part.size);
          const hasChecksum = await withStallRetry(label, async (signal, bump) => {
            const got = await getPart(msg.part, signal);
            vip = got.vip;
            connId = got.connId;
            byteLength = 0; // reset per attempt so a stalled partial read isn't kept
            for await (const chunk of got.body) {
              chunk.copy(view, byteLength); // one assembly copy (as in bufferPool)
              byteLength += chunk.length;
              bump();
            }
            return got.hasChecksum;
          });
          const downloadMs = performance.now() - t0;
          dPartsDone += 1;
          if (hasChecksum) dChecksummed += 1;
          dispatchInFlight -= 1;
          bumpProgress(progress, byteLength);
          parentPort.postMessage(
            {
              type: 'part',
              key: msg.part.key,
              partNumber: msg.part.partNumber,
              buffer: ab,
              byteLength,
              hasChecksum,
              downloadMs: partTimes ? downloadMs : undefined,
              vip: partTimes ? vip : undefined,
              connId: partTimes ? connId : undefined,
            },
            [ab], // transfer list: hand ownership of the bytes to main, no copy
          );
          dispatchMaybeDone();
          return;
        }

        // discard sink: hold bytes here; main only accounts + 'release's them.
        let held = null; // chunk[] (default) or a pooled Buffer (bufferPool)
        // bufferPool: acquire one contiguous buffer up front (reused across retries).
        const poolBuf = bufferPool ? acquireBuf(msg.part.size) : null;
        const hasChecksum = await withStallRetry(label, async (signal, bump) => {
          const got = await getPart(msg.part, signal);
          vip = got.vip;
          connId = got.connId;
          // Reset accumulation each attempt so a stalled partial read isn't kept.
          byteLength = 0;
          const chunks = bufferPool ? null : [];
          for await (const chunk of got.body) {
            if (bufferPool) {
              chunk.copy(poolBuf, byteLength); // copy in; the chunk then dies young
            } else {
              chunks.push(chunk); // retain (no memcpy, no transfer) to incur real memory
            }
            byteLength += chunk.length;
            bump();
          }
          held = bufferPool ? poolBuf : chunks;
          return got.hasChecksum;
        });
        const downloadMs = performance.now() - t0;
        heldChunks.set(label, held);
        dPartsDone += 1;
        if (hasChecksum) dChecksummed += 1;
        dispatchInFlight -= 1;
        bumpProgress(progress, byteLength);
        // Tiny metadata only — no bytes cross to main.
        parentPort.postMessage({
          type: 'part-ready',
          key: msg.part.key,
          partNumber: msg.part.partNumber,
          byteLength,
          hasChecksum,
          downloadMs: partTimes ? downloadMs : undefined,
          vip: partTimes ? vip : undefined,
          connId: partTimes ? connId : undefined,
        });
        dispatchMaybeDone();
      } catch (err) {
        parentPort.postMessage({ type: 'error', message: err?.message ?? String(err) });
      }
    } else if (msg?.type === 'return') {
      // Stream sink: main handed back a consumed part's ArrayBuffer for reuse.
      if (bufferReturn && msg.buffer) returnedBufs.push(msg.buffer);
    } else if (msg?.type === 'release') {
      // Delivered in order — free the held bytes (or recycle the pooled buffer).
      const rkey = `${msg.key}#${msg.partNumber}`;
      if (bufferPool) releaseBuf(heldChunks.get(rkey));
      heldChunks.delete(rkey);
    } else if (msg?.type === 'stop') {
      dispatchStopped = true;
      dispatchMaybeDone();
    }
  });
} else {
  parentPort.on('message', async (msg) => {
    if (msg?.type !== 'start') return;
    try {
      const { totalBytes, partsDone, checksummed, elapsedMs, partTimings } = await runSlice();
      await stopProfile();
      const ipTput = tracker ? tracker.snapshot() : null;
      parentPort.postMessage({
        type: 'done',
        bytes: totalBytes,
        parts: partsDone,
        checksummed,
        elapsedMs,
        ipCounts: ipCounts ? [...ipCounts] : null,
        ipThroughput: ipTput,
        partTimes: partTimings,
        tlsInfo,
      });
    } catch (err) {
      parentPort.postMessage({ type: 'error', message: err?.message ?? String(err) });
    } finally {
      closeFds();
      client.destroy();
    }
  });
}

parentPort.postMessage({ type: 'ready' });
