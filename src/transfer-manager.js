// S3TransferManager — a Transfer-Manager-shaped wrapper over the ranged-GET-by-
// PartNumber worker pool, exposing a clean, stream-returning download API.
//
// Lifecycle mirrors a real transfer manager:
//   const tm = new S3TransferManager(cfg);   // spawns the worker pool ONCE
//   await tm.ready();                          // one-time spawn + client init (tm.spawnMs)
//   const { body } = await tm.download({ bucket, key });   // per-call: HEAD -> plan ->
//                                                          // dispatch -> ordered Readable
//   await pipeline(body, sink);                // caller drains "usable bytes"
//   await tm.close();                          // stop + terminate the pool
//
// Design (see README "Delivery modes"): all objects share ONE worker pool and ONE
// global reorder budget (maxBufferedBytes). Parts are NOT pinned to objects — the
// scheduler round-robins across active objects so every object's frontier advances
// and the pool stays full (what actually saturates the NIC). Each object gets its
// own ordered Readable (`body`); a part returned by any worker is routed to its
// object's stream and pushed in PartNumber order. Backpressure is two-tier: the
// global budget bounds the cross-object reorder backlog, and each Readable's
// highWaterMark (streamHwm) throttles a slow consumer per object.
//
// Buffers are handed to the caller by ownership transfer (worker -> main -> the
// Readable). A consumer that is DONE with a chunk may return it to the owning worker
// via recycle() so a bounded set of buffers ping-pongs across the thread boundary
// (opt-in, gated by bufferReturn) instead of a fresh allocation per part. Real callers
// that retain chunks simply never call recycle() — nothing is returned, buffers GC.

import { Worker } from 'node:worker_threads';
import { Readable } from 'node:stream';
import { statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import {
  HeadObjectCommand,
  CreateMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
} from '@aws-sdk/client-s3';
import { makeClient } from './s3.js';
import { bumpProgress, progressView } from './progress.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WORKER_PATH = path.join(__dirname, 'download-worker.js');
const UPLOAD_WORKER_PATH = path.join(__dirname, 'upload-worker.js');

/**
 * Build the read work list for one object as fixed-size byte RANGES of `rangeSize`
 * (last range is the remainder), independent of the object's multipart layout.
 * `partNumber` here is just the 1-based sequence index used for in-order delivery.
 */
function buildRanges(key, totalSize, rangeSize) {
  const rs = rangeSize > 0 ? rangeSize : totalSize || 1;
  const parts = [];
  const n = Math.max(1, Math.ceil(totalSize / rs));
  for (let i = 0; i < n; i++) {
    const offset = i * rs;
    const size = Math.min(rs, totalSize - offset);
    parts.push({ key, partNumber: i + 1, offset, size });
  }
  return parts;
}

/** HEAD the object for its total size (reads are planned as byte ranges, not parts). */
async function describeObject(client, bucket, key) {
  const whole = await client.send(
    new HeadObjectCommand({ Bucket: bucket, Key: key, ChecksumMode: 'ENABLED' }),
  );
  return { totalSize: Number(whole.ContentLength) };
}

export class S3TransferManager {
  /**
   * @param {object} cfg bucket, region, workers, concurrency, maxBufferedBytes,
   *   streamHwm, validateChecksum, httpHandler, spreadConnections, tls, ciphers,
   *   stallTimeoutMs, partRetries, partTimes, nativeCrc32, progressBuf
   */
  constructor(cfg) {
    this.cfg = cfg;
    this.mode = cfg.mode ?? 'download'; // 'download' | 'upload'
    this.nWorkers = Math.max(1, cfg.workers);
    this.concurrency = Math.max(1, cfg.concurrency);
    this.cap = cfg.maxBufferedBytes > 0 ? cfg.maxBufferedBytes : Infinity;
    // Buffer recycling: when enabled, a consumer that is DONE with a chunk can hand
    // its ArrayBuffer back to the owning worker (via recycle()) for reuse, so a
    // bounded set of buffers ping-pongs across the thread boundary instead of a fresh
    // allocation per part. Only safe when the consumer no longer references the bytes
    // — real API callers that keep chunks must leave this off (they just never call
    // recycle(), so nothing is returned and buffers are simply GC'd).
    this.bufferReturn = Boolean(cfg.bufferReturn);
    // download 'file' mode: each download worker writes the ranges it fetches
    // straight to disk with O_DIRECT (no separate writer pool, no shared queue, no
    // reorder buffer, no main funnel). Anything else = ordered-stream (Readable out).
    this.downloadFile = this.mode === 'download' && cfg.deliveryMode === 'file';
    // Download read granularity: each object is fetched as fixed-size byte RANGES of
    // this size (independent of how it was uploaded / its part layout).
    this.rangeSize = cfg.rangeSize > 0 ? cfg.rangeSize : 16 * 1024 * 1024;
    if (this.downloadFile) {
      // download 'file' mode is the SIMPLE inline O_DIRECT writer: each download
      // worker writes the ranges it fetches straight to disk (no separate writer
      // pool, no shared queue). These knobs are forwarded to the workers; the only
      // backpressure is the in-flight range count (workers x concurrency).
      this.fileChunk = cfg.fileChunk > 0 ? cfg.fileChunk : 8 * 1024 * 1024; // O_DIRECT pwrite granularity
      this.fileDirect = cfg.fileDirect !== false; // O_DIRECT unless explicitly opted out
      this.fileDiscard = Boolean(cfg.fileDiscard); // drain ranges without writing (network A/B)
    }
    this.progress = progressView(cfg.progressBuf);

    // Pool + scheduler state.
    this.threads = [];
    this.freeLanes = new Array(this.nWorkers).fill(this.concurrency);
    this.active = []; // objects with parts still to DISPATCH (round-robin source)
    this.rr = 0;
    this.objects = new Map(); // key -> object record
    this.heldBuffers = new Map(); // `${key}#${n}` -> { buf, byteLength, hasChecksum }
    this.bufferedBytes = 0; // reorder backlog only (held, not yet delivered)
    this.totalInFlight = 0;

    // Aggregate stats (filled as parts flow / at close()).
    this.deliveredBytes = 0;
    this.deliveredCount = 0;
    this.deliveredChecksummed = 0;
    this.partTimes = cfg.partTimes ? [] : null;
    this.ipCounts = new Map();
    this.ipThroughput = new Map();
    this.tlsInfo = null;

    // Readiness + close plumbing.
    this._readyCount = 0;
    this._readyResolve = null;
    this._readyPromise = new Promise((res) => (this._readyResolve = res));
    this._spawnStart = performance.now();
    this.spawnMs = 0;
    this._workerDone = 0;
    // One pool now (the download workers write their own ranges in file mode).
    this._readyNeed = this.nWorkers;
    this._workerDoneNeed = this.nWorkers;
    this._closeResolve = null;
    this._closePromise = null;
    this._failed = null;

    // --- Upload state (mode 'upload') --------------------------------------
    // upload() runs CreateMPU -> parallel UploadPart -> CompleteMPU from one of four
    // per-object sources:
    //   parts  - pre-filled part-sized Buffers (filled untimed; transferred to workers)
    //   sab    - pre-filled SharedArrayBuffer (workers read zero-copy slices)
    //   file   - a file path (read + carved on main into pooled buffers, transferred)
    //   stream - a customer Readable from main (carved on main, transferred)
    // parts/sab carry no in-window fill; file/stream carve on the fly (read/memcpy in
    // the timed window) using a lazily-allocated, bounded recycled buffer pool.
    this.partSize = cfg.partSize > 0 ? cfg.partSize : 32 * 1024 * 1024;
    this.checksum = cfg.checksum || null;
    this.uploadMaxBuffered = cfg.uploadMaxBuffered > 0 ? cfg.uploadMaxBuffered : 0;
    this.uploadObjects = new Map(); // key -> upload record
    this.uploadActive = []; // objects with parts still to dispatch (round-robin source)
    this.urr = 0; // round-robin pointer across active upload objects
    this.carvePool = null; // Buffer[] for stream carving (lazy)
    this.bufWaiters = []; // carvers awaiting a free pool buffer (backpressure)

    this.control = makeClient({ region: cfg.region });
    if (this.mode === 'upload') {
      this._spawnUploadPool();
    } else {
      this._spawnPool();
    }
  }

  _spawnPool() {
    const c = this.cfg;
    const maxSockets = Math.max(64, this.concurrency * 2);
    for (let wi = 0; wi < this.nWorkers; wi++) {
      const worker = new Worker(WORKER_PATH, {
        workerData: {
          bucket: c.bucket,
          region: c.region,
          parts: [],
          concurrency: this.concurrency,
          maxSockets,
          validateChecksum: c.validateChecksum,
          deliveryMode: this.downloadFile ? 'file' : 'ordered-stream',
          // file mode: this worker writes each range it downloads straight to disk.
          rangeSize: this.rangeSize, // sizes the reusable (aligned) write buffers
          fileDirect: this.downloadFile ? this.fileDirect : undefined,
          fileChunk: this.downloadFile ? this.fileChunk : undefined,
          fileDiscard: this.downloadFile ? this.fileDiscard : undefined,
          deliveryPath: this.downloadFile ? c.deliveryPath : undefined,
          logConnections: c.logConnections,
          spreadConnections: c.spreadConnections,
          tls: c.tls,
          ipThroughput: c.ipThroughput,
          httpHandler: c.httpHandler,
          ciphers: c.ciphers,
          stallTimeoutMs: c.stallTimeoutMs,
          partRetries: c.partRetries,
          partTimes: c.partTimes,
          workerId: wi,
          bufferPool: false,
          bufferReturn: this.bufferReturn, // enable worker-side reuse pool for returned buffers
          profile: c.profile,
          profileDir: c.profileDir,
          nativeCrc32: c.nativeCrc32,
          progressBuf: c.progressBuf,
        },
      });
      this.threads.push(worker);
      worker.on('message', (msg) => this._onMessage(wi, msg));
      worker.on('error', (err) => this._fail(err));
      worker.on('exit', (code) => {
        if (code !== 0 && !this._failed) this._fail(new Error(`worker exited with code ${code}`));
      });
    }
  }

  _spawnUploadPool() {
    const c = this.cfg;
    const maxSockets = Math.max(64, this.concurrency * 2);
    for (let wi = 0; wi < this.nWorkers; wi++) {
      const worker = new Worker(UPLOAD_WORKER_PATH, {
        workerData: {
          bucket: c.bucket,
          region: c.region,
          role: 'uploader',
          uploadSource: 'stream',
          concurrency: this.concurrency,
          checksum: this.checksum,
          maxSockets,
          spreadConnections: c.spreadConnections,
          tls: c.tls,
          ipThroughput: c.ipThroughput,
          httpHandler: c.httpHandler,
          ciphers: c.ciphers,
          nativeCrc32: c.nativeCrc32,
          workerId: wi,
          progressBuf: c.progressBuf,
        },
      });
      this.threads.push(worker);
      worker.on('message', (msg) => this._onUploadMessage(wi, msg));
      worker.on('error', (err) => this._fail(err));
      worker.on('exit', (code) => {
        if (code !== 0 && !this._failed) this._fail(new Error(`worker exited with code ${code}`));
      });
    }
  }

  /** Resolve when every worker has initialized its client (records spawnMs). */
  ready() {
    return this._readyPromise;
  }
}

// --- Prototype methods (kept off the constructor for readability) ------------

/** Worker -> main message pump. */
S3TransferManager.prototype._onMessage = function (wi, msg) {
  if (msg.type === 'ready') {
    if (++this._readyCount === this._readyNeed) {
      this.spawnMs = performance.now() - this._spawnStart;
      this._readyResolve();
    }
  } else if (msg.type === 'part') {
    // Zero-copy transfer: the worker handed us ownership of this part's bytes.
    const buf = Buffer.from(msg.buffer, 0, msg.byteLength);
    this.heldBuffers.set(`${msg.key}#${msg.partNumber}`, {
      buf,
      byteLength: msg.byteLength,
      hasChecksum: msg.hasChecksum,
      wi, // owning worker, for return-credit recycling (bufferReturn)
    });
    if (this.partTimes && msg.downloadMs !== undefined) {
      this.partTimes.push({
        key: msg.key,
        partNumber: msg.partNumber,
        bytes: msg.byteLength,
        ms: msg.downloadMs,
        vip: msg.vip ?? null,
        connId: msg.connId ?? null,
      });
    }
    this.bufferedBytes += msg.byteLength;
    this.freeLanes[wi] += 1;
    this.totalInFlight -= 1;
    this._drainKey(msg.key);
    this._dispatchMore();
  } else if (msg.type === 'part-downloaded') {
    // file mode: the worker downloaded this range AND wrote it to disk (or drained
    // it in fileDiscard). Count the bytes, free the lane, and resolve the object
    // once every one of its bytes has landed.
    this.freeLanes[wi] += 1;
    this.totalInFlight -= 1;
    this.deliveredBytes += msg.byteLength;
    this.deliveredCount += 1;
    if (msg.hasChecksum) this.deliveredChecksummed += 1;
    if (this.partTimes && msg.downloadMs !== undefined) {
      this.partTimes.push({
        key: msg.key,
        partNumber: msg.partNumber,
        bytes: msg.byteLength,
        ms: msg.downloadMs,
        vip: msg.vip ?? null,
        connId: msg.connId ?? null,
      });
    }
    const obj = this.objects.get(msg.key);
    if (obj) {
      obj.written = (obj.written || 0) + msg.byteLength;
      if (obj.written >= obj.contentLength && !obj.done) {
        obj.done = true;
        this.objects.delete(obj.key);
        obj.resolve({ key: obj.key, bytes: obj.contentLength });
      }
    }
    this._dispatchMore();
  } else if (msg.type === 'worker-done') {
    if (msg.ipCounts) for (const [ip, c] of msg.ipCounts) this.ipCounts.set(ip, (this.ipCounts.get(ip) || 0) + c);
    if (msg.ipThroughput) for (const [ip, v] of msg.ipThroughput) this.ipThroughput.set(ip, (this.ipThroughput.get(ip) || 0) + v);
    if (!this.tlsInfo && msg.tlsInfo) this.tlsInfo = msg.tlsInfo;
    if (++this._workerDone === this._workerDoneNeed && this._closeResolve) this._closeResolve();
  } else if (msg.type === 'error') {
    this._fail(new Error(`worker: ${msg.message}`));
  }
};

/** Round-robin the next undispatched part across active objects (fair fan-out). */
S3TransferManager.prototype._nextPart = function () {
  const n = this.active.length;
  for (let i = 0; i < n; i++) {
    const obj = this.active[this.rr % this.active.length];
    this.rr += 1;
    if (obj.cursor < obj.parts.length) return obj.parts[obj.cursor++];
  }
  return null; // every active object has dispatched all its parts
};

/** Fill free lanes with parts, honoring the global reorder-backlog budget. */
S3TransferManager.prototype._dispatchMore = function () {
  for (let wi = 0; wi < this.nWorkers; wi++) {
    while (this.freeLanes[wi] > 0) {
      // Throttle only when the backlog is full AND something is in flight (so a
      // stalled frontier can't deadlock: with nothing in flight we always fetch).
      if (this.totalInFlight > 0 && this.bufferedBytes >= this.cap) return;
      const part = this._nextPart();
      if (!part) return;
      this.threads[wi].postMessage({ type: 'assign', part });
      this.freeLanes[wi] -= 1;
      this.totalInFlight += 1;
    }
  }
};

/** Push an object's held parts into its Readable in PartNumber order. */
S3TransferManager.prototype._drainKey = function (key) {
  const obj = this.objects.get(key);
  if (!obj || obj.paused) return;
  let n = obj.nextN;
  let id = `${key}#${n}`;
  while (this.heldBuffers.has(id)) {
    const info = this.heldBuffers.get(id);
    const buf = info.buf;
    if (this.bufferReturn) buf.__wi = info.wi; // remember owner so recycle() can return it
    this.heldBuffers.delete(id);
    this.bufferedBytes -= info.byteLength;
    this.deliveredBytes += info.byteLength;
    if (info.hasChecksum) this.deliveredChecksummed += 1;
    this.deliveredCount += 1;
    n += 1;
    id = `${key}#${n}`;
    const ok = obj.readable.push(buf);
    if (n > obj.partsCount) {
      obj.readable.push(null); // object complete -> EOF for the caller
      obj.eofPushed = true;
    }
    if (!ok) {
      obj.paused = true; // consumer HWM hit; resume on the Readable's read()
      break;
    }
  }
  obj.nextN = n;
};

/**
 * Return a fully-consumed chunk's buffer to its owning worker for reuse. Call this
 * ONLY once you no longer reference the bytes (e.g. from a sink's write callback
 * after copying/discarding). No-op unless bufferReturn is enabled and the chunk was
 * produced by this manager (carries __wi). Transferring detaches the ArrayBuffer, so
 * the chunk must not be touched afterwards.
 */
S3TransferManager.prototype.recycle = function (chunk) {
  if (!this.bufferReturn || !chunk || chunk.__wi === undefined) return;
  const w = this.threads[chunk.__wi];
  if (!w) return;
  try {
    w.postMessage({ type: 'return', buffer: chunk.buffer }, [chunk.buffer]);
  } catch {
    /* worker already stopped — buffer will just be GC'd */
  }
};

/**
 * Download one object. Resolves once planning (HEAD + part list) is done and the
 * ordered Readable is created; bytes flow into `body` as parts arrive. The caller
 * MUST drain `body` (its highWaterMark exerts backpressure).
 * @returns {Promise<{ key: string, body: Readable, contentLength: number }>}
 */
S3TransferManager.prototype.download = async function ({ bucket, key, file }) {
  if (this._failed) throw this._failed;
  const b = bucket ?? this.cfg.bucket;
  const info = await describeObject(this.control, b, key);
  const parts = buildRanges(key, info.totalSize, this.rangeSize);

  // file mode: each range is dispatched to a download worker, which writes it
  // straight to disk at its offset with O_DIRECT (buffered tail fallback), out of
  // order and with no reorder buffer. Resolves once every byte of this object has
  // landed (driven by 'part-downloaded' byte counts). The file must already be sized.
  if (this.downloadFile || file) {
    const dest = file ?? this.cfg.deliveryPath;
    for (const p of parts) p.file = dest; // worker opens/writes this destination
    const obj = {
      key,
      parts,
      cursor: 0,
      partsCount: parts.length,
      contentLength: info.totalSize,
      written: 0, // BYTES written to disk so far (drives completion)
      done: false,
      resolve: null,
      reject: null,
    };
    const done = new Promise((res, rej) => {
      obj.resolve = res;
      obj.reject = rej;
    });
    this.objects.set(key, obj);
    this.active.push(obj);
    this._dispatchMore();
    return done;
  }

  const streamHwm =
    this.cfg.streamHwm > 0 ? this.cfg.streamHwm : Math.max(1 << 20, 2 * (parts[0]?.size ?? 1 << 20));

  const self = this;
  const obj = {
    key,
    parts,
    cursor: 0,
    partsCount: parts.length,
    contentLength: info.totalSize,
    nextN: 1,
    paused: false,
    eofPushed: false,
    readable: null,
  };
  obj.readable = new Readable({
    highWaterMark: streamHwm,
    read() {
      // Consumer wants more: lift backpressure, deliver held parts, fetch more.
      if (obj.paused) {
        obj.paused = false;
        self._drainKey(key);
        self._dispatchMore();
      }
    },
  });
  this.objects.set(key, obj);
  this.active.push(obj);
  this._dispatchMore();
  return { key, body: obj.readable, contentLength: obj.contentLength };
};

/**
 * Download many objects through the same shared pool + budget. Convenience over
 * calling download() N times: returns the per-object handles up front (wire all
 * sinks, then everything flows) plus a `done` promise. Also async-iterable.
 * @returns {{ objects: Array, done: Promise<void>, [Symbol.asyncIterator]: Function }}
 */
S3TransferManager.prototype.downloadMany = async function ({ bucket, keys }) {
  const handles = await Promise.all(keys.map((key) => this.download({ bucket, key })));
  const done = Promise.all(
    handles.map(
      (h) =>
        new Promise((res, rej) => {
          h.body.once('end', res);
          h.body.once('error', rej);
        }),
    ),
  ).then(() => {});
  return {
    objects: handles,
    done,
    [Symbol.asyncIterator]() {
      let i = 0;
      return { next: () => Promise.resolve(i < handles.length ? { value: handles[i++], done: false } : { value: undefined, done: true }) };
    },
  };
};

// --- Upload (mode 'upload') --------------------------------------------------
//
// upload() runs CreateMPU -> parallel UploadPart -> CompleteMPU from one of four
// per-object sources:
//   parts: [Buffer, ...]   - PRE-FILLED part-sized standalone Buffers (filled untimed);
//                            each is TRANSFERRED (zero-copy) to a worker, then back so
//                            the array is reusable. No in-window fill.
//   { buffer, size }       - PRE-FILLED SharedArrayBuffer; workers read zero-copy SLICES
//                            (shared, not transferred, reusable). No in-window fill.
//   { file }               - a file path; each WORKER positionally reads its own part
//                            range from the file (distributed ingress, no main funnel).
//   { body }               - a customer Readable from main; main carves it into pooled
//                            buffers (ingress memcpy is in the timed window).

/** Uploader worker -> main pump. */
S3TransferManager.prototype._onUploadMessage = function (wi, msg) {
  if (msg.type === 'ready') {
    if (++this._readyCount === this.nWorkers) {
      this.spawnMs = performance.now() - this._spawnStart;
      this._readyResolve();
    }
  } else if (msg.type === 'uploaded') {
    this.freeLanes[wi] += 1;
    if (!this.tlsInfo && msg.tlsInfo) this.tlsInfo = msg.tlsInfo;
    const obj = this.uploadObjects.get(msg.key);
    if (obj) {
      obj.completed.push({
        PartNumber: msg.partNumber,
        ETag: msg.ETag,
        ChecksumCRC32C: msg.ChecksumCRC32C,
        ChecksumCRC32: msg.ChecksumCRC32,
        ChecksumSHA1: msg.ChecksumSHA1,
        ChecksumSHA256: msg.ChecksumSHA256,
      });
      obj.partsUploaded += 1;
      obj.totalBytes += msg.size;
      this.deliveredBytes += msg.size;
      this.deliveredCount += 1;
      // Worker transferred the buffer back. parts mode: restore it into the caller's
      // array (reusable next iteration). file/stream: recycle it to the carve pool.
      if (msg.buffer) {
        if (obj.mode === 'parts' && obj.parts) obj.parts[msg.partNumber - 1] = Buffer.from(msg.buffer);
        else this._releaseBuf(msg.buffer);
      }
    }
    bumpProgress(this.progress, msg.size);
    if (obj) this._maybeCompleteUpload(obj);
    this._dispatchUpload();
  } else if (msg.type === 'error') {
    this._fail(new Error(`worker: ${msg.message}`));
  }
};

/** Queue a part for an object and register the object as an active dispatch source. */
S3TransferManager.prototype._enqueueUpload = function (obj, item) {
  obj.pending.push(item);
  if (!obj._inActive) {
    obj._inActive = true;
    this.uploadActive.push(obj);
  }
};

/**
 * Pick the next part to dispatch, ROUND-ROBIN across active objects, so consecutive
 * parts come from DIFFERENT objects/files — each worker's in-flight lanes then span
 * many files (maximally distributed reads), instead of draining one file at a time.
 */
S3TransferManager.prototype._nextUploadPart = function () {
  const n = this.uploadActive.length;
  for (let i = 0; i < n; i++) {
    const obj = this.uploadActive[this.urr % this.uploadActive.length];
    this.urr += 1;
    if (obj.pending.length) return obj.pending.shift();
  }
  return null; // every active object has dispatched all currently-queued parts
};

/** Fill free uploader lanes, round-robin across objects/files. */
S3TransferManager.prototype._dispatchUpload = function () {
  for (let wi = 0; wi < this.nWorkers; wi++) {
    while (this.freeLanes[wi] > 0) {
      const it = this._nextUploadPart();
      if (!it) return;
      this.freeLanes[wi] -= 1;
      if (it.sab) {
        // SAB mode: hand the worker the shared buffer + this part's slice range.
        // SharedArrayBuffer is shared by reference (NOT transferred), so it stays
        // valid on main and is reusable across iterations.
        this.threads[wi].postMessage({
          type: 'upload-sab',
          key: it.key,
          uploadId: it.uploadId,
          partNumber: it.partNumber,
          start: it.start,
          size: it.size,
          sab: it.sab,
        });
      } else if (it.file) {
        // file mode: the worker positionally reads its OWN part range from the file
        // (distributed ingress — no main-thread read/carve). Just metadata crosses.
        this.threads[wi].postMessage({
          type: 'upload-file',
          key: it.key,
          uploadId: it.uploadId,
          partNumber: it.partNumber,
          start: it.start,
          size: it.size,
          file: it.file,
        });
      } else {
        // parts mode: transfer ownership of this part's standalone ArrayBuffer.
        this.threads[wi].postMessage(
          { type: 'upload', key: it.key, uploadId: it.uploadId, partNumber: it.partNumber, size: it.size, buffer: it.buf.buffer },
          [it.buf.buffer],
        );
      }
    }
  }
};

/** When an object's parts are fully known and all uploaded, CompleteMPU and resolve. */
S3TransferManager.prototype._maybeCompleteUpload = function (obj) {
  if (obj.completing || obj.partsTotal == null || obj.partsUploaded !== obj.partsTotal) return;
  obj.completing = true;
  (async () => {
    try {
      const Parts = obj.completed
        .slice()
        .sort((a, b) => a.PartNumber - b.PartNumber)
        .map((r) => {
          const p = { PartNumber: r.PartNumber, ETag: r.ETag };
          if (r.ChecksumCRC32C) p.ChecksumCRC32C = r.ChecksumCRC32C;
          if (r.ChecksumCRC32) p.ChecksumCRC32 = r.ChecksumCRC32;
          if (r.ChecksumSHA1) p.ChecksumSHA1 = r.ChecksumSHA1;
          if (r.ChecksumSHA256) p.ChecksumSHA256 = r.ChecksumSHA256;
          return p;
        });
      await this.control.send(
        new CompleteMultipartUploadCommand({
          Bucket: obj.bucket,
          Key: obj.key,
          UploadId: obj.uploadId,
          MultipartUpload: { Parts },
        }),
      );
      this.uploadObjects.delete(obj.key);
      obj.resolve({ key: obj.key, bytes: obj.totalBytes, parts: obj.partsTotal });
    } catch (err) {
      obj.reject(err);
    }
  })();
};

/** Lazily allocate the bounded carve buffer pool (file/stream sources only). */
S3TransferManager.prototype._ensureCarvePool = function () {
  if (this.carvePool) return;
  const lanes = this.nWorkers * this.concurrency;
  const budget = this.uploadMaxBuffered > 0 ? this.uploadMaxBuffered : (lanes + 1) * this.partSize;
  const n = Math.max(lanes + 1, Math.floor(budget / this.partSize) || 1);
  this.carvePool = [];
  for (let i = 0; i < n; i++) this.carvePool.push(Buffer.allocUnsafeSlow(this.partSize));
};

/** Acquire a carve buffer, awaiting one when the pool is empty (backpressure). */
S3TransferManager.prototype._acquireBuf = async function () {
  let b = this.carvePool.pop();
  while (!b) {
    await new Promise((r) => this.bufWaiters.push(r));
    b = this.carvePool.pop();
  }
  return b;
};

/** Return a carve buffer to the pool and wake one waiting carver. */
S3TransferManager.prototype._releaseBuf = function (ab) {
  this.carvePool.push(Buffer.from(ab)); // ArrayBuffer transferred back from a worker
  this.bufWaiters.shift()?.();
};

/** Carve a Readable (file/stream source) into partSize buffers and enqueue each. */
S3TransferManager.prototype._carve = async function (obj, body) {
  let buf = null;
  let off = 0;
  let partNumber = 1;
  let carved = 0;
  for await (const chunk of body) {
    let cpos = 0;
    while (cpos < chunk.length) {
      if (!buf) {
        buf = await this._acquireBuf(); // backpressure: blocks when the pool is empty
        off = 0;
      }
      const n = Math.min(chunk.length - cpos, this.partSize - off);
      chunk.copy(buf, off, cpos, cpos + n); // INGRESS memcpy: source bytes -> part buffer
      off += n;
      cpos += n;
      if (off === this.partSize) {
        carved += 1;
        this._enqueueUpload(obj, { key: obj.key, uploadId: obj.uploadId, partNumber: partNumber++, size: this.partSize, buf });
        this._dispatchUpload();
        buf = null;
      }
    }
  }
  if (buf && off > 0) {
    carved += 1;
    this._enqueueUpload(obj, { key: obj.key, uploadId: obj.uploadId, partNumber: partNumber++, size: off, buf });
    this._dispatchUpload();
  } else if (buf) {
    this.carvePool.push(buf); // unused tail buffer
    this.bufWaiters.shift()?.();
  }
  if (carved === 0) throw new Error(`upload ${obj.key}: source produced 0 bytes (multipart needs >=1 part)`);
  obj.partsTotal = carved; // now known — allows completion once all uploaded
  this._maybeCompleteUpload(obj);
};

/**
 * Upload one object. Provide exactly one source:
 *   - parts:  Buffer[]                    pre-filled part-sized buffers (transferred)
 *   - buffer: SharedArrayBuffer + size    pre-filled shared buffer (zero-copy slices)
 *   - file:   string path (+ size)        workers read their own part ranges (disk)
 *   - body:   Readable (from main)        carve a customer stream (in-window memcpy)
 * Runs CreateMPU -> parallel UploadPart -> CompleteMPU. Resolves after CompleteMPU
 * with { key, bytes, parts }. Fire per object concurrently for parallelism.
 * @returns {Promise<{ key: string, bytes: number, parts: number }>}
 */
S3TransferManager.prototype.upload = async function ({ bucket, key, parts, buffer, size, file, body }) {
  if (this._failed) throw this._failed;
  const b = bucket ?? this.cfg.bucket;
  const create = await this.control.send(
    new CreateMultipartUploadCommand({
      Bucket: b,
      Key: key,
      ...(this.checksum ? { ChecksumAlgorithm: this.checksum } : {}),
    }),
  );
  const obj = {
    key,
    bucket: b,
    uploadId: create.UploadId,
    completed: [],
    partsTotal: null, // known upfront for parts/sab/file; set at carve end for stream
    partsUploaded: 0,
    completing: false,
    totalBytes: 0,
    mode: null,
    parts: null, // parts mode: caller's array (repopulated as buffers return)
    pending: [], // parts queued for dispatch (round-robin source)
    _inActive: false,
    resolve: null,
    reject: null,
  };
  const done = new Promise((res, rej) => {
    obj.resolve = res;
    obj.reject = rej;
  });

  const abort = (err) => {
    this.control
      .send(new AbortMultipartUploadCommand({ Bucket: b, Key: key, UploadId: obj.uploadId }))
      .catch(() => {});
    obj.reject(err instanceof Error ? err : new Error(String(err)));
  };

  try {
    if (Array.isArray(parts)) {
      if (parts.length === 0) throw new Error(`upload ${key}: parts array is empty (need >=1)`);
      obj.mode = 'parts';
      obj.parts = parts;
      obj.partsTotal = parts.length;
      this.uploadObjects.set(key, obj);
      for (let i = 0; i < parts.length; i++) {
        this._enqueueUpload(obj, { key, uploadId: obj.uploadId, partNumber: i + 1, size: parts[i].length, buf: parts[i] });
      }
      this._dispatchUpload();
    } else if (buffer && size > 0) {
      obj.mode = 'sab';
      obj.partsTotal = Math.max(1, Math.ceil(size / this.partSize));
      this.uploadObjects.set(key, obj);
      let partNumber = 1;
      for (let start = 0; start < size; start += this.partSize) {
        const psize = Math.min(this.partSize, size - start);
        this._enqueueUpload(obj, { key, uploadId: obj.uploadId, partNumber: partNumber++, start, size: psize, sab: buffer });
      }
      this._dispatchUpload();
    } else if (file) {
      // file mode: workers positionally read their OWN part ranges from the file
      // (distributed ingress). Main only plans the byte ranges + dispatches metadata.
      obj.mode = 'file';
      const fsize = size > 0 ? size : statSync(file).size;
      obj.partsTotal = Math.max(1, Math.ceil(fsize / this.partSize));
      this.uploadObjects.set(key, obj);
      let partNumber = 1;
      for (let start = 0; start < fsize; start += this.partSize) {
        const psize = Math.min(this.partSize, fsize - start);
        this._enqueueUpload(obj, { key, uploadId: obj.uploadId, partNumber: partNumber++, start, size: psize, file });
      }
      this._dispatchUpload();
    } else if (body) {
      // stream: carve the customer Readable on main into pooled buffers (partsTotal
      // set at carve end). A Readable can't cross the worker boundary, so ingress is
      // single-threaded on main here — unlike file, which fans reads across workers.
      obj.mode = 'stream';
      this._ensureCarvePool();
      this.uploadObjects.set(key, obj);
      this._carve(obj, body).catch(abort);
    } else {
      throw new Error(`upload ${key}: provide one of parts | { buffer, size } | file | body`);
    }
  } catch (err) {
    abort(err);
  }
  return done;
};

/**
 * Upload many objects through the same uploader pool. `sources` is an array of
 * per-object descriptors: { key, parts } | { key, buffer, size } | { key, file } |
 * { key, body } (optional per-item bucket). Resolves when every MPU completes.
 * @returns {Promise<Array<{ key: string, bytes: number, parts: number }>>}
 */
S3TransferManager.prototype.uploadMany = async function ({ bucket, sources }) {
  return Promise.all(
    sources.map((s) =>
      this.upload({ bucket: s.bucket ?? bucket, key: s.key, parts: s.parts, buffer: s.buffer, size: s.size, file: s.file, body: s.body }),
    ),
  );
};

/**
 * Reset per-run scheduler state so the WARM pool can serve another batch (e.g. the
 * next benchmark iteration). Safe only when idle — call between fully-drained runs.
 * The worker threads and their clients/connections are left untouched.
 */
S3TransferManager.prototype.resetScheduler = function () {
  this.active = [];
  this.objects.clear();
  this.heldBuffers.clear();
  this.bufferedBytes = 0;
  this.totalInFlight = 0;
  this.rr = 0;
  this.freeLanes.fill(this.concurrency);
  this.deliveredBytes = 0;
  this.deliveredCount = 0;
  this.deliveredChecksummed = 0;
  if (this.partTimes) this.partTimes = [];
  // Upload-mode collections (idle between runs; buffers already all released).
  this.uploadObjects.clear();
  this.uploadActive = [];
  this.urr = 0;
};

/** Stop the pool, collect per-worker stats, and terminate the threads. */
S3TransferManager.prototype.close = async function () {
  if (this._closePromise) return this._closePromise;
  // Uploader workers don't emit a 'worker-done' handshake — just stop + terminate.
  if (this.mode === 'upload') {
    this._closePromise = Promise.resolve();
    for (const w of this.threads) {
      try {
        w.postMessage({ type: 'stop' });
      } catch {
        /* already gone */
      }
    }
    await Promise.all(this.threads.map((w) => w.terminate()));
    this.control.destroy();
    return {
      deliveredBytes: this.deliveredBytes,
      partsDone: this.deliveredCount,
      checksummed: this.deliveredCount,
      ipCounts: [],
      ipThroughput: [],
      partTimes: [],
      tlsInfo: this.tlsInfo,
      spawnMs: this.spawnMs,
    };
  }
  this._closePromise = new Promise((res) => (this._closeResolve = res));
  for (const w of this.threads) w.postMessage({ type: 'stop' });
  await this._closePromise;
  await Promise.all(this.threads.map((w) => w.terminate()));
  this.control.destroy();
  return {
    deliveredBytes: this.deliveredBytes,
    partsDone: this.deliveredCount,
    checksummed: this.deliveredChecksummed,
    ipCounts: [...this.ipCounts],
    ipThroughput: [...this.ipThroughput],
    partTimes: this.partTimes ?? [],
    tlsInfo: this.tlsInfo,
    spawnMs: this.spawnMs,
  };
};

S3TransferManager.prototype._fail = function (err) {
  if (this._failed) return;
  this._failed = err instanceof Error ? err : new Error(String(err));
  // Surface on any open download streams so callers draining them see the error.
  for (const obj of this.objects.values()) {
    if (!obj.eofPushed) obj.readable.destroy(this._failed);
  }
  // Reject any in-flight uploads.
  for (const obj of this.uploadObjects.values()) {
    if (obj.reject && !obj.completing) obj.reject(this._failed);
  }
  if (this._readyResolve) this._readyResolve(); // unblock ready() waiters
  if (this._closeResolve) this._closeResolve();
};
