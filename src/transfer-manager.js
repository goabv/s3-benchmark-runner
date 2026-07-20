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

/** Build the PartNumber work list for one object (uniform parts, last is the tail). */
function buildParts(key, partsCount, firstPartSize, totalSize) {
  const parts = [];
  for (let p = 1; p <= partsCount; p++) {
    const offset = (p - 1) * firstPartSize;
    const size = p < partsCount ? firstPartSize : totalSize - offset;
    parts.push({ key, partNumber: p, offset, size });
  }
  return parts;
}

/** HEAD the object (whole + PartNumber=1) to learn size, part count, part size. */
async function describeObject(client, bucket, key) {
  const whole = await client.send(
    new HeadObjectCommand({ Bucket: bucket, Key: key, ChecksumMode: 'ENABLED' }),
  );
  const totalSize = Number(whole.ContentLength);
  const part1 = await client.send(
    new HeadObjectCommand({ Bucket: bucket, Key: key, PartNumber: 1, ChecksumMode: 'ENABLED' }),
  );
  const partsCount = part1.PartsCount ? Number(part1.PartsCount) : 1;
  const firstPartSize = Number(part1.ContentLength);
  return { totalSize, partsCount, firstPartSize };
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
    this._closeResolve = null;
    this._closePromise = null;
    this._failed = null;

    // --- Upload state (mode 'upload') --------------------------------------
    // Customer hands one Readable per object from MAIN; main carves it into
    // partSize buffers (single-thread ingress + memcpy), transfers each part
    // (zero-copy) to the uploader pool, then CompleteMPU. A bounded, pre-allocated
    // buffer pool (uploadMaxBuffered) provides backpressure to the customer stream.
    this.partSize = cfg.partSize > 0 ? cfg.partSize : 32 * 1024 * 1024;
    this.checksum = cfg.checksum || null;
    const lanes = this.nWorkers * this.concurrency;
    this.uploadBudget = cfg.uploadMaxBuffered > 0 ? cfg.uploadMaxBuffered : (lanes + 1) * this.partSize;
    this.maxBuffers = Math.max(lanes + 1, Math.floor(this.uploadBudget / this.partSize) || 1);
    this.freeBufs = [];
    this.bufWaiters = [];
    this.readyQueue = []; // carved parts awaiting a free uploader lane
    this.uploadObjects = new Map(); // key -> upload record

    this.control = makeClient({ region: cfg.region });
    if (this.mode === 'upload') this._spawnUploadPool();
    else this._spawnPool();
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
          deliveryMode: 'ordered-stream',
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
    // Pre-allocate the recycled part-buffer pool (bounds resident memory + gives
    // backpressure: carving blocks on acquireBuf() when the pool is empty).
    for (let i = 0; i < this.maxBuffers; i++) this.freeBufs.push(Buffer.allocUnsafeSlow(this.partSize));
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
    if (++this._readyCount === this.nWorkers) {
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
  } else if (msg.type === 'worker-done') {
    if (msg.ipCounts) for (const [ip, c] of msg.ipCounts) this.ipCounts.set(ip, (this.ipCounts.get(ip) || 0) + c);
    if (msg.ipThroughput) for (const [ip, v] of msg.ipThroughput) this.ipThroughput.set(ip, (this.ipThroughput.get(ip) || 0) + v);
    if (!this.tlsInfo && msg.tlsInfo) this.tlsInfo = msg.tlsInfo;
    if (++this._workerDone === this.nWorkers && this._closeResolve) this._closeResolve();
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
S3TransferManager.prototype.download = async function ({ bucket, key }) {
  if (this._failed) throw this._failed;
  const b = bucket ?? this.cfg.bucket;
  const info = await describeObject(this.control, b, key);
  const parts = buildParts(key, info.partsCount, info.firstPartSize, info.totalSize);
  const streamHwm =
    this.cfg.streamHwm > 0 ? this.cfg.streamHwm : Math.max(1 << 20, 2 * (parts[0]?.size ?? 1 << 20));

  const self = this;
  const obj = {
    key,
    parts,
    cursor: 0,
    partsCount: info.partsCount,
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
    }
    bumpProgress(this.progress, msg.size);
    this._releaseBuf(msg.buffer); // recycle the (now sent) buffer -> unblocks carving
    if (obj) this._maybeCompleteUpload(obj);
    this._dispatchUpload();
  } else if (msg.type === 'error') {
    this._fail(new Error(`worker: ${msg.message}`));
  }
};

/** Acquire a free part buffer, awaiting one if the pool is exhausted (backpressure). */
S3TransferManager.prototype._acquireBuf = async function () {
  let b = this.freeBufs.pop();
  while (!b) {
    await new Promise((r) => this.bufWaiters.push(r));
    b = this.freeBufs.pop();
  }
  return b;
};

/** Return a sent part's ArrayBuffer to the pool and wake one waiting carver. */
S3TransferManager.prototype._releaseBuf = function (ab) {
  this.freeBufs.push(Buffer.from(ab)); // ArrayBuffer transferred back from a worker
  this.bufWaiters.shift()?.();
};

/** Assign carved parts to free uploader lanes (round-robin), zero-copy transfer. */
S3TransferManager.prototype._dispatchUpload = function () {
  for (let wi = 0; wi < this.nWorkers && this.readyQueue.length; wi++) {
    while (this.freeLanes[wi] > 0 && this.readyQueue.length) {
      const it = this.readyQueue.shift();
      this.freeLanes[wi] -= 1;
      this.threads[wi].postMessage(
        { type: 'upload', key: it.key, uploadId: it.uploadId, partNumber: it.partNumber, size: it.size, buffer: it.buf.buffer },
        [it.buf.buffer],
      );
    }
  }
};

/** Carve the customer Readable into partSize buffers and enqueue each for upload. */
S3TransferManager.prototype._carve = async function (obj, body) {
  let buf = null;
  let off = 0;
  let partNumber = 1;
  for await (const chunk of body) {
    let cpos = 0;
    while (cpos < chunk.length) {
      if (!buf) {
        buf = await this._acquireBuf(); // blocks (backpressure) when pool is empty
        off = 0;
      }
      const n = Math.min(chunk.length - cpos, this.partSize - off);
      chunk.copy(buf, off, cpos, cpos + n); // INGRESS memcpy: customer bytes -> part buffer
      off += n;
      cpos += n;
      if (off === this.partSize) {
        obj.partsCarved += 1;
        this.readyQueue.push({ key: obj.key, uploadId: obj.uploadId, partNumber: partNumber++, size: this.partSize, buf });
        this._dispatchUpload();
        buf = null;
      }
    }
  }
  if (buf && off > 0) {
    obj.partsCarved += 1;
    this.readyQueue.push({ key: obj.key, uploadId: obj.uploadId, partNumber: partNumber++, size: off, buf });
    this._dispatchUpload();
  } else if (buf) {
    this.freeBufs.push(buf); // unused tail buffer
    this.bufWaiters.shift()?.();
  }
  if (obj.partsCarved === 0) {
    throw new Error(`upload ${obj.key}: source stream produced 0 bytes (multipart requires >=1 part)`);
  }
  obj.streamEnded = true;
  this._maybeCompleteUpload(obj);
};

/** When an object's stream has ended and all its parts are uploaded, CompleteMPU. */
S3TransferManager.prototype._maybeCompleteUpload = function (obj) {
  if (obj.completing || !obj.streamEnded || obj.partsUploaded !== obj.partsCarved) return;
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
      obj.resolve({ key: obj.key, bytes: obj.totalBytes, parts: obj.partsCarved });
    } catch (err) {
      obj.reject(err);
    }
  })();
};

/**
 * Upload one object from a caller-provided Readable (from MAIN). Runs the full
 * multipart lifecycle: CreateMPU -> carve+parallel UploadPart -> CompleteMPU.
 * Resolves after CompleteMPU with { key, bytes, parts }. The caller drives many
 * uploads concurrently by calling upload() per object without awaiting in series.
 * @returns {Promise<{ key: string, bytes: number, parts: number }>}
 */
S3TransferManager.prototype.upload = async function ({ bucket, key, body }) {
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
    partsCarved: 0,
    partsUploaded: 0,
    streamEnded: false,
    completing: false,
    totalBytes: 0,
    resolve: null,
    reject: null,
  };
  const done = new Promise((res, rej) => {
    obj.resolve = res;
    obj.reject = rej;
  });
  this.uploadObjects.set(key, obj);
  this._carve(obj, body).catch((err) => {
    // Abort the MPU so a failed/partial upload doesn't leave dangling parts.
    this.control
      .send(new AbortMultipartUploadCommand({ Bucket: b, Key: key, UploadId: obj.uploadId }))
      .catch(() => {});
    obj.reject(err instanceof Error ? err : new Error(String(err)));
  });
  return done;
};

/**
 * Upload many objects through the same pool + budget. `sources` is an array of
 * { key, body } (and optional per-item bucket). Returns when every object's MPU
 * completes. The uploads share one uploader pool and one backpressure budget.
 * @returns {Promise<Array<{ key: string, bytes: number, parts: number }>>}
 */
S3TransferManager.prototype.uploadMany = async function ({ bucket, sources }) {
  return Promise.all(sources.map((s) => this.upload({ bucket: s.bucket ?? bucket, key: s.key, body: s.body })));
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
  this.readyQueue = [];
  this.uploadObjects.clear();
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
