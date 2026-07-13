import { parentPort, workerData } from 'node:worker_threads';
import { openSync, writevSync, writev as writevCb, closeSync } from 'node:fs';
import { promisify } from 'node:util';
import { Readable } from 'node:stream';
import { GetObjectCommand } from '@aws-sdk/client-s3';

// Async positional vectored write (libuv threadpool) — used when fileAsync is on,
// so the disk write doesn't block this worker's event loop / socket draining.
const writevAsync = promisify(writevCb);
import { makeClient } from './s3.js';
import { IpThroughputTracker } from './ip-throughput.js';

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
  bufferPool, // ordered-stream: copy chunks into reused contiguous part buffers
  fileAsync, // file mode: write each part asynchronously (don't block the event loop)
} = workerData;

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
      PartNumber: part.partNumber,
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
  if (!fds) return;
  for (const fd of fds.values()) {
    try {
      closeSync(fd);
    } catch {
      /* ignore */
    }
  }
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

let dispatchInFlight = 0;
let dispatchStopped = false;
let dPartsDone = 0;
let dChecksummed = 0;

function dispatchMaybeDone() {
  if (dispatchStopped && dispatchInFlight === 0) {
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
    closeFds();
    client.destroy();
  }
}

if (deliveryMode === 'ordered-stream') {
  parentPort.on('message', async (msg) => {
    if (msg?.type === 'assign') {
      dispatchInFlight += 1;
      try {
        const label = `${msg.part.key}#${msg.part.partNumber}`;
        const t0 = performance.now();
        let held = null; // chunk[] (default) or a pooled Buffer (bufferPool)
        let byteLength = 0;
        let vip = null;
        let connId = null;
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
