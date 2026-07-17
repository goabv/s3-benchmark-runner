#!/usr/bin/env node
import { Worker } from 'node:worker_threads';
import { Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';
import { writeFileSync, mkdirSync, createWriteStream, rmSync } from 'node:fs';
import { randomFillSync } from 'node:crypto';
import { createRequire } from 'node:module';
import os from 'node:os';
import path from 'node:path';
import {
  CreateMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  HeadObjectCommand,
} from '@aws-sdk/client-s3';
import { makeClient } from './s3.js';
import { parseUploadArgs, parseSize, computeParts, formatBytes, throughput } from './config.js';
import { ResourceMonitor } from './resource-monitor.js';
import { newProgressBuffer, ProgressReporter } from './progress.js';
import {
  mergeIpThroughput,
  ipIterationGbps,
  accumulateIpSamples,
  summarizeIpHistory,
  printIpThroughput,
  appendIpRecord,
} from './ip-throughput.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WORKER_PATH = path.join(__dirname, 'upload-worker.js');

const require = createRequire(import.meta.url);
const pkgVersion = (name) => {
  try {
    return require(`${name}/package.json`).version;
  } catch {
    return 'unknown';
  }
};
const SDK_VERSION = pkgVersion('@aws-sdk/client-s3');
const SMITHY_CORE_VERSION = pkgVersion('@smithy/core');

/** Round-robin parts across N buckets. */
function assignParts(parts, n) {
  const buckets = Array.from({ length: n }, () => []);
  parts.forEach((p, i) => buckets[i % n].push(p));
  return buckets;
}

/** Existing object's total + part layout, or null. Mirrors the seeder's check. */
async function describeExisting(client, bucket, key) {
  let size;
  try {
    const head = await client.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    size = Number(head.ContentLength);
  } catch (err) {
    if (err?.$metadata?.httpStatusCode === 404 || err?.name === 'NotFound') return null;
    throw err;
  }
  let firstPartSize = size;
  try {
    const p1 = await client.send(new HeadObjectCommand({ Bucket: bucket, Key: key, PartNumber: 1 }));
    firstPartSize = Number(p1.ContentLength);
  } catch {
    /* not multipart */
  }
  return { size, firstPartSize };
}

/** Build a CompletedPart entry, carrying whichever checksum the part returned. */
function completedPart(r) {
  const p = { PartNumber: r.PartNumber, ETag: r.ETag };
  if (r.ChecksumCRC32C) p.ChecksumCRC32C = r.ChecksumCRC32C;
  if (r.ChecksumCRC32) p.ChecksumCRC32 = r.ChecksumCRC32;
  if (r.ChecksumSHA1) p.ChecksumSHA1 = r.ChecksumSHA1;
  if (r.ChecksumSHA256) p.ChecksumSHA256 = r.ChecksumSHA256;
  return p;
}

/**
 * Time one parallel upload of the parts of an already-created multipart upload.
 * Excludes worker spawn + client init + data generation (ready/start handshake),
 * so it measures only the parallel UploadPart data transfer.
 */
function runOnce({ bucket, region, baseParts, keys, workers, concurrency, checksum, maxSockets, uploadSource, sourceFilePath, openDesc, objectBuffers, spreadConnections, tls, ipThroughput, httpHandler, ciphers, nativeCrc32, progressBuf, reporter, onCreate, onComplete }) {
  const totalParts = baseParts.length * keys.length;
  const nWorkers = Math.max(1, Math.min(workers, totalParts));
  const partSizeMax = baseParts.reduce((m, p) => Math.max(m, p.size), 0);

  return new Promise((resolve, reject) => {
    const threads = [];
    let readyCount = 0;
    let doneCount = 0;
    let t0 = 0;
    const results = [];
    let settled = false;
    let tlsInfo = null;
    const runIpTput = new Map();

    const cleanup = () => {
      reporter?.stop();
      threads.forEach((w) => w.terminate());
    };
    const fail = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err instanceof Error ? err : new Error(String(err)));
    };

    // All workers ready (spawn + data-gen done, untimed). Now start the clock and
    // run the full multipart lifecycle: CreateMPU -> parallel UploadPart -> CompleteMPU.
    const beginTimedRun = async () => {
      try {
        t0 = performance.now();
        reporter?.start();
        const { parts } = await onCreate();
        const buckets = assignParts(parts, nWorkers); // nWorkers <= totalParts -> all non-empty
        for (let wi = 0; wi < nWorkers; wi++) threads[wi].postMessage({ type: 'start', parts: buckets[wi] });
      } catch (err) {
        fail(err);
      }
    };

    for (let wi = 0; wi < nWorkers; wi++) {
      const worker = new Worker(WORKER_PATH, {
        workerData: {
          bucket, region, concurrency, checksum, maxSockets, maxPartSize: partSizeMax,
          uploadSource, sourceFilePath, openDesc, objectBuffers, spreadConnections, tls, ipThroughput, httpHandler, ciphers, nativeCrc32,
          workerId: wi, progressBuf,
        },
      });
      threads.push(worker);

      worker.on('message', (msg) => {
        if (msg.type === 'ready') {
          if (++readyCount === nWorkers) beginTimedRun();
        } else if (msg.type === 'done') {
          results.push(msg);
          mergeIpThroughput(runIpTput, msg.ipThroughput);
          if (!tlsInfo && msg.tlsInfo) tlsInfo = msg.tlsInfo;
          if (++doneCount === nWorkers && !settled) {
            // All parts uploaded; complete the MPUs (inside the timed window), then resolve.
            (async () => {
              try {
                const completed = results.flatMap((r) => r.completed);
                await onComplete(completed);
                const wallMs = performance.now() - t0;
                settled = true;
                cleanup();
                const bytes = results.reduce((s, r) => s + r.bytes, 0);
                resolve({ bytes, completed, wallMs, ipThroughput: [...runIpTput], tlsInfo });
              } catch (err) {
                fail(err);
              }
            })();
          }
        } else if (msg.type === 'error') {
          fail(new Error(`worker: ${msg.message}`));
        }
      });
      worker.on('error', fail);
      worker.on('exit', (code) => {
        if (code !== 0 && !settled) fail(new Error(`worker exited with code ${code}`));
      });
    }
  });
}

/**
 * Synthetic customer stream: a real Readable that emits `size` bytes as a client
 * would push them — in chunks of `template.length`, optionally throttled to
 * `clientRate` bytes/sec. Reuses the pre-filled template (content is irrelevant to
 * S3, and the consumer copies each chunk out immediately, so sharing is safe).
 */
function makeCustomerStream(size, template, clientRate) {
  let sent = 0;
  return new Readable({
    highWaterMark: template.length,
    read() {
      if (sent >= size) {
        this.push(null);
        return;
      }
      const n = Math.min(template.length, size - sent);
      const chunk = n === template.length ? template : template.subarray(0, n);
      const emit = () => {
        sent += n;
        this.push(chunk);
      };
      if (clientRate > 0) setTimeout(emit, (n / clientRate) * 1000);
      else emit();
    },
  });
}

/**
 * STREAM upload: the customer hands one Readable per object to the main thread.
 * Main reads each stream, carves + fills part buffers (single-thread ingress),
 * then TRANSFERS each part (zero-copy) to a pool of uploader worker threads that
 * UploadPart in parallel, out of order. A bounded pool of recycled buffers (cap =
 * uploadMaxBuffered) is the backpressure: when main can't get a free buffer it
 * stops reading the customer stream. Completion is by part count.
 *
 * Returns the same shape as runOnce: { bytes, completed, wallMs, ipThroughput, tlsInfo }.
 */
function runStreamUpload({
  bucket, region, keys, baseParts, workers, concurrency, checksum, maxSockets,
  spreadConnections, tls, ipThroughput, httpHandler, ciphers, nativeCrc32, uploadMaxBuffered, clientRate, clientChunk,
  progressBuf, reporter, onCreate, onComplete,
}) {
  const perFileBytes = baseParts.reduce((s, p) => s + p.size, 0);
  const partSizeMax = baseParts.reduce((m, p) => Math.max(m, p.size), 0) || 1;
  const totalParts = baseParts.length * keys.length;
  const nWorkers = Math.max(1, Math.min(workers, totalParts));
  const lanesPerWorker = Math.max(1, concurrency);
  const budget = uploadMaxBuffered > 0 ? uploadMaxBuffered : (nWorkers * lanesPerWorker + 1) * partSizeMax;
  const maxBuffers = Math.max(nWorkers * lanesPerWorker + 1, Math.floor(budget / partSizeMax) || 1);

  return new Promise((resolve, reject) => {
    const threads = [];
    let readyCount = 0;
    let settled = false;
    let t0 = 0;
    let uploadIds = {};
    let tlsInfo = null;

    // Recycled buffer pool on main: carve -> transfer to worker -> transferred back.
    const freeBufs = [];
    for (let i = 0; i < maxBuffers; i++) freeBufs.push(Buffer.allocUnsafeSlow(partSizeMax));
    const bufWaiters = [];
    const acquireBuf = async () => {
      let b = freeBufs.pop();
      while (!b) {
        await new Promise((r) => bufWaiters.push(r));
        b = freeBufs.pop();
      }
      return b;
    };
    const releaseBuf = (ab) => {
      freeBufs.push(Buffer.from(ab)); // ArrayBuffer transferred back from a worker
      bufWaiters.shift()?.();
    };

    const freeLanes = new Array(nWorkers).fill(lanesPerWorker);
    const ready = []; // carved parts awaiting a free lane: { key, uploadId, partNumber, size, buf }
    const completed = [];
    let uploadedCount = 0;
    let totalBytes = 0;

    const cleanup = () => {
      reporter?.stop();
      threads.forEach((w) => w.terminate());
    };
    const fail = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err instanceof Error ? err : new Error(String(err)));
    };

    const dispatch = () => {
      for (let wi = 0; wi < nWorkers && ready.length; wi++) {
        while (freeLanes[wi] > 0 && ready.length) {
          const it = ready.shift();
          freeLanes[wi] -= 1;
          threads[wi].postMessage(
            { type: 'upload', key: it.key, uploadId: it.uploadId, partNumber: it.partNumber, size: it.size, buffer: it.buf.buffer },
            [it.buf.buffer],
          );
        }
      }
    };
    const enqueue = (item) => {
      ready.push(item);
      dispatch();
    };

    // Pre-filled template (untimed) sized to the client chunk: the customer stream
    // emits it repeatedly. Larger chunk = fewer stream cycles on the ingress thread.
    const template = Buffer.allocUnsafe(Math.max(1, clientChunk || 1 << 20));
    randomFillSync(template);

    // One producer per object: read its customer stream, carve + fill part buffers.
    async function produceKey(key) {
      const uploadId = uploadIds[key];
      const stream = makeCustomerStream(perFileBytes, template, clientRate);
      let partIdx = 0;
      let buf = null;
      let off = 0;
      for await (const chunk of stream) {
        let cpos = 0;
        while (cpos < chunk.length) {
          if (!buf) {
            buf = await acquireBuf();
            off = 0;
          }
          const target = baseParts[partIdx].size;
          const n = Math.min(chunk.length - cpos, target - off);
          chunk.copy(buf, off, cpos, cpos + n); // INGRESS fill: customer bytes -> part buffer
          off += n;
          cpos += n;
          if (off === target) {
            enqueue({ key, uploadId, partNumber: baseParts[partIdx].partNumber, size: target, buf });
            buf = null;
            partIdx += 1;
          }
        }
      }
    }

    const onUploaded = (wi, msg) => {
      freeLanes[wi] += 1;
      if (!tlsInfo && msg.tlsInfo) tlsInfo = msg.tlsInfo;
      completed.push({
        key: msg.key,
        PartNumber: msg.partNumber,
        ETag: msg.ETag,
        ChecksumCRC32C: msg.ChecksumCRC32C,
        ChecksumCRC32: msg.ChecksumCRC32,
        ChecksumSHA1: msg.ChecksumSHA1,
        ChecksumSHA256: msg.ChecksumSHA256,
      });
      totalBytes += msg.size;
      uploadedCount += 1;
      releaseBuf(msg.buffer);
      if (uploadedCount === totalParts) {
        if (!settled) {
          // All parts uploaded; complete the MPUs (inside the timed window), then resolve.
          (async () => {
            try {
              await onComplete(completed);
              const wallMs = performance.now() - t0;
              settled = true;
              threads.forEach((w) => w.postMessage({ type: 'stop' }));
              cleanup();
              resolve({ bytes: totalBytes, completed, wallMs, ipThroughput: [], tlsInfo });
            } catch (err) {
              fail(err);
            }
          })();
        }
        return;
      }
      dispatch();
    };

    for (let wi = 0; wi < nWorkers; wi++) {
      const worker = new Worker(WORKER_PATH, {
        workerData: {
          bucket, region, uploadSource: 'stream', concurrency, checksum, maxSockets,
          spreadConnections, tls, ipThroughput, httpHandler, ciphers, nativeCrc32, workerId: wi, progressBuf,
        },
      });
      threads.push(worker);
      worker.on('message', (msg) => {
        if (msg.type === 'ready') {
          if (++readyCount === nWorkers) {
            // All uploaders ready. Start the clock, create the MPUs, then stream.
            (async () => {
              try {
                t0 = performance.now();
                reporter?.start();
                ({ uploadIds } = await onCreate());
                Promise.all(keys.map((k) => produceKey(k))).catch(fail); // completion driven by uploadedCount
              } catch (err) {
                fail(err);
              }
            })();
          }
        } else if (msg.type === 'uploaded') {
          onUploaded(wi, msg);
        } else if (msg.type === 'error') {
          fail(new Error(`worker: ${msg.message}`));
        }
      });
      worker.on('error', fail);
      worker.on('exit', (code) => {
        if (code !== 0 && !settled) fail(new Error(`worker exited with code ${code}`));
      });
    }
  });
}

/** One full upload iteration: create -> (timed) parts -> complete. */
/** Stream `size` bytes of random data to a file (untimed setup for file source). */
function writeRandomFile(filePath, size, chunkSize = 8 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const ws = createWriteStream(filePath);
    const buf = Buffer.allocUnsafe(Math.min(chunkSize, size || chunkSize));
    let remaining = size;
    ws.on('error', reject);
    ws.on('finish', resolve);
    const pump = () => {
      let ok = true;
      while (remaining > 0 && ok) {
        const n = Math.min(buf.length, remaining);
        randomFillSync(buf, 0, n);
        remaining -= n;
        ok = ws.write(Buffer.from(buf.subarray(0, n)));
      }
      if (remaining > 0) ws.once('drain', pump);
      else ws.end();
    };
    pump();
  });
}

/**
 * OPEN-STREAM upload (two tiers). CARVER workers each open a whole-object stream
 * (via the opener, one object per stream, on their own thread) and carve part
 * buffers; a separate UPLOADER pool does the UploadParts. Parts flow
 * carver -> main -> uploader by zero-copy transfer; each uploaded buffer is
 * transferred back to its carver (recycled) with an 'ack' that also frees a credit,
 * so a carver never has more than `carverLimit` parts outstanding (backpressure).
 *
 * Returns the runOnce shape: { bytes, completed, wallMs, ipThroughput, tlsInfo }.
 */
function runOpenStreamUpload({
  bucket, region, keys, baseParts, workers, concurrency, checksum, maxSockets,
  spreadConnections, tls, ipThroughput, httpHandler, ciphers, nativeCrc32,
  progressBuf, reporter, onCreate, onComplete, openDesc, uploadMaxBuffered, carvers: nCarversCfg,
}) {
  const partSizeMax = baseParts.reduce((m, p) => Math.max(m, p.size), 0) || 1;
  const perFileBytes = baseParts.reduce((s, p) => s + p.size, 0);
  const totalParts = baseParts.length * keys.length;
  const nUploaders = Math.max(1, Math.min(workers, totalParts));
  const nCarvers = Math.max(1, Math.min(nCarversCfg > 0 ? nCarversCfg : keys.length, keys.length));
  const lanesPerUploader = Math.max(1, concurrency);
  const budget = uploadMaxBuffered > 0 ? uploadMaxBuffered : (nUploaders * lanesPerUploader + nCarvers) * partSizeMax;
  const maxBuffers = Math.max(nUploaders * lanesPerUploader + nCarvers, Math.floor(budget / partSizeMax));
  const carverLimit = Math.max(1, Math.floor(maxBuffers / nCarvers));

  // Round-robin objects across carvers (a stream is sequential -> one carver/object).
  const carverObjects = Array.from({ length: nCarvers }, () => []);
  keys.forEach((key, i) => carverObjects[i % nCarvers].push(key));

  return new Promise((resolve, reject) => {
    const uploaders = [];
    const carvers = [];
    let uReady = 0;
    let cReady = 0;
    let started = false;
    let settled = false;
    let t0 = 0;
    let tlsInfo = null;
    let uploadedCount = 0;
    let totalBytes = 0;
    let uploadIds = {};
    const completed = [];
    const freeLanes = new Array(nUploaders).fill(lanesPerUploader);
    const ready = []; // carved parts awaiting an uploader lane: { ..., carverId, buf }

    const cleanup = () => {
      reporter?.stop();
      for (const w of uploaders) { try { w.terminate(); } catch { /* ignore */ } }
      for (const w of carvers) { try { w.terminate(); } catch { /* ignore */ } }
    };
    const fail = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err instanceof Error ? err : new Error(String(err)));
    };

    const dispatch = () => {
      for (let ui = 0; ui < nUploaders && ready.length; ui++) {
        while (freeLanes[ui] > 0 && ready.length) {
          const it = ready.shift();
          freeLanes[ui] -= 1;
          uploaders[ui].postMessage(
            { type: 'upload', key: it.key, uploadId: it.uploadId, partNumber: it.partNumber, size: it.size, carverId: it.carverId, buffer: it.buf.buffer },
            [it.buf.buffer],
          );
        }
      }
    };

    const maybeBegin = () => {
      if (started || uReady !== nUploaders || cReady !== nCarvers) return;
      started = true;
      (async () => {
        try {
          t0 = performance.now();
          reporter?.start();
          ({ uploadIds } = await onCreate());
          for (let ci = 0; ci < nCarvers; ci++) {
            const objects = carverObjects[ci].map((key) => ({ key, uploadId: uploadIds[key], size: perFileBytes, baseParts }));
            carvers[ci].postMessage({ type: 'carve', objects });
          }
        } catch (err) {
          fail(err);
        }
      })();
    };

    // Uploader tier (reuses the pool worker role).
    for (let ui = 0; ui < nUploaders; ui++) {
      const worker = new Worker(WORKER_PATH, {
        workerData: {
          role: 'uploader', bucket, region, concurrency, checksum, maxSockets,
          spreadConnections, tls, ipThroughput, httpHandler, ciphers, nativeCrc32, workerId: ui, progressBuf,
        },
      });
      uploaders.push(worker);
      worker.on('message', (msg) => {
        if (msg.type === 'ready') {
          uReady += 1;
          maybeBegin();
        } else if (msg.type === 'uploaded') {
          freeLanes[ui] += 1;
          if (!tlsInfo && msg.tlsInfo) tlsInfo = msg.tlsInfo;
          completed.push({ key: msg.key, PartNumber: msg.partNumber, ETag: msg.ETag, ChecksumCRC32C: msg.ChecksumCRC32C, ChecksumCRC32: msg.ChecksumCRC32, ChecksumSHA1: msg.ChecksumSHA1, ChecksumSHA256: msg.ChecksumSHA256 });
          totalBytes += msg.size;
          uploadedCount += 1;
          // Ack the carver + hand the freed buffer back for reuse (credit + recycle).
          carvers[msg.carverId]?.postMessage({ type: 'ack', buffer: msg.buffer }, [msg.buffer]);
          if (uploadedCount === totalParts) {
            if (!settled) {
              (async () => {
                try {
                  await onComplete(completed);
                  const wallMs = performance.now() - t0;
                  settled = true;
                  cleanup();
                  resolve({ bytes: totalBytes, completed, wallMs, ipThroughput: [], tlsInfo });
                } catch (err) {
                  fail(err);
                }
              })();
            }
            return;
          }
          dispatch();
        } else if (msg.type === 'error') {
          fail(new Error(`uploader: ${msg.message}`));
        }
      });
      worker.on('error', fail);
      worker.on('exit', (code) => { if (code !== 0 && !settled) fail(new Error(`uploader exited with code ${code}`)); });
    }

    // Carver tier (one stream per object, carves parts, transfers to main).
    for (let ci = 0; ci < nCarvers; ci++) {
      const cid = ci;
      const worker = new Worker(WORKER_PATH, {
        workerData: {
          role: 'carver', bucket, region, concurrency, checksum, maxSockets,
          spreadConnections, tls, ipThroughput, httpHandler, ciphers, nativeCrc32, workerId: ci,
          openDesc, maxPartSize: partSizeMax, carverLimit,
        },
      });
      carvers.push(worker);
      worker.on('message', (msg) => {
        if (msg.type === 'ready') {
          cReady += 1;
          maybeBegin();
        } else if (msg.type === 'part') {
          ready.push({ key: msg.key, uploadId: msg.uploadId, partNumber: msg.partNumber, size: msg.size, carverId: cid, buf: Buffer.from(msg.buffer) });
          dispatch();
        } else if (msg.type === 'carver-done') {
          /* informational; completion is driven by uploadedCount */
        } else if (msg.type === 'error') {
          fail(new Error(`carver: ${msg.message}`));
        }
      });
      worker.on('error', fail);
      worker.on('exit', (code) => { if (code !== 0 && !settled) fail(new Error(`carver exited with code ${code}`)); });
    }
  });
}

/**
 * One iteration for a whole group. Worker spawn + data generation happen first,
 * UNTIMED (main starts the clock only once every worker is ready). The measured
 * window then spans the whole multipart lifecycle: CreateMultipartUpload -> parallel
 * UploadPart -> CompleteMultipartUpload, so the reported throughput is end-to-end.
 */
async function uploadIterationGroup(control, cfg, keys, baseParts, maxSockets, sourceFilePath, ipTputEnabled, progressBuf = null, reporter = null, objectBuffers = null) {
  const uploadIds = {};

  // Create one MPU per key and build the pooled part list (key + uploadId tagged).
  const onCreate = async () => {
    for (const key of keys) {
      const create = await control.send(
        new CreateMultipartUploadCommand({
          Bucket: cfg.bucket,
          Key: key,
          ...(cfg.checksum ? { ChecksumAlgorithm: cfg.checksum } : {}),
        }),
      );
      uploadIds[key] = create.UploadId;
    }
    const parts = [];
    for (const key of keys) {
      for (const p of baseParts) parts.push({ ...p, key, uploadId: uploadIds[key] });
    }
    return { parts, uploadIds };
  };

  // Complete each MPU from the gathered part results.
  const onComplete = async (completed) => {
    const byKey = new Map(keys.map((k) => [k, []]));
    for (const c of completed) byKey.get(c.key).push(c);
    for (const key of keys) {
      const orderedParts = byKey
        .get(key)
        .map(completedPart)
        .sort((a, b) => a.PartNumber - b.PartNumber);
      await control.send(
        new CompleteMultipartUploadCommand({
          Bucket: cfg.bucket,
          Key: key,
          UploadId: uploadIds[key],
          MultipartUpload: { Parts: orderedParts },
        }),
      );
    }
  };

  const abortAll = async () => {
    for (const key of keys) {
      if (!uploadIds[key]) continue;
      await control
        .send(new AbortMultipartUploadCommand({ Bucket: cfg.bucket, Key: key, UploadId: uploadIds[key] }))
        .catch(() => {});
    }
  };

  // 'open'/'open-stream' source: the re-openable descriptor each worker opens from.
  // Built-in file opener gets the shared source file path; a custom module opener's
  // path is resolved to an absolute file URL on MAIN (so the worker's import() is
  // unambiguous — a relative specifier would otherwise resolve against the worker
  // file, not the customer's cwd), with the source path passed through in params.
  let openDesc = null;
  if (cfg.uploadSource === 'open' || cfg.uploadSource === 'open-stream') {
    openDesc =
      cfg.uploadOpen?.type === 'memory'
        ? { type: 'memory', chunk: cfg.uploadClientChunk } // generate in-memory (no disk)
        : { type: 'file', path: sourceFilePath }; // read the shared source file
  }

  const common = {
    bucket: cfg.bucket,
    region: cfg.region,
    keys,
    baseParts,
    workers: cfg.workers,
    concurrency: cfg.concurrency,
    checksum: cfg.checksum,
    maxSockets,
    spreadConnections: cfg.spreadConnections,
    tls: cfg.tls,
    ipThroughput: ipTputEnabled,
    httpHandler: cfg.httpHandler,
    ciphers: cfg.ciphers,
    nativeCrc32: cfg.nativeCrc32,
    progressBuf,
    reporter,
    onCreate,
    onComplete,
    objectBuffers,
  };

  try {
    if (cfg.uploadSource === 'stream') {
      // Customer streams each object into main; main carves + transfers parts to
      // a pool of uploader workers that UploadPart in parallel, out of order.
      return await runStreamUpload({ ...common, uploadMaxBuffered: cfg.uploadMaxBuffered, clientRate: cfg.uploadClientRate, clientChunk: cfg.uploadClientChunk });
    }
    if (cfg.uploadSource === 'open-stream') {
      // Carver threads open whole-object streams and carve parts; a separate uploader
      // pool does the UploadParts. Parts flow carver -> main -> uploader (zero-copy).
      return await runOpenStreamUpload({ ...common, openDesc, uploadMaxBuffered: cfg.uploadMaxBuffered, carvers: cfg.uploadCarvers });
    }
    return await runOnce({ ...common, uploadSource: cfg.uploadSource, sourceFilePath, openDesc });
  } catch (err) {
    await abortAll();
    throw err;
  }
}

/**
 * 'memory' source: allocate ONE object-sized SharedArrayBuffer per object (shared
 * by reference across the whole worker pool — no copy on transfer) and random-fill
 * each once. Parts become zero-copy views into their object's buffer.
 *
 * Resident memory = sum of all object sizes, so we preflight against box RAM and
 * fail fast (rather than OOM mid-run). Allocation + fill is UNTIMED.
 */
function allocObjectBuffers(keys, size, { json } = {}) {
  const total = size * keys.length;
  const totalMem = os.totalmem();
  // Leave headroom for the SDK/undici socket buffers, V8 heap, and the OS.
  const budget = Math.floor(totalMem * 0.8);
  if (total > budget) {
    throw new Error(
      `memory source needs ${formatBytes(total)} resident (${keys.length} x ${formatBytes(size)}), ` +
        `but only ~${formatBytes(budget)} of ${formatBytes(totalMem)} RAM is available for buffers. ` +
        `Reduce object size or count (e.g. fewer files, or a smaller size), or use uploadSource ` +
        `"open"/"stream" (no full-object residency).`,
    );
  }
  if (!json) {
    console.error(
      `[setup] allocating ${keys.length} x ${formatBytes(size)} = ${formatBytes(total)} object buffer(s), ` +
        `random-filling (untimed) ...`,
    );
  }
  const buffers = {};
  // randomFillSync bounds BOTH its offset and size args by 2^31-1, so fill through a
  // zero-based sub-view per chunk (Buffer.from accepts large byte offsets) rather
  // than passing an absolute offset into a >2 GiB buffer.
  const FILL_CHUNK = 1 << 30; // 1 GiB, safely under the 2^31-1 size limit
  for (const key of keys) {
    const sab = new SharedArrayBuffer(size);
    for (let off = 0; off < size; off += FILL_CHUNK) {
      randomFillSync(Buffer.from(sab, off, Math.min(FILL_CHUNK, size - off)));
    }
    buffers[key] = sab;
  }
  return buffers;
}

async function benchmarkGroup(cfg, group) {
  const bytes = parseSize(group.label);
  const baseParts = computeParts(bytes, cfg.partSize);
  const maxSockets = Math.max(64, cfg.concurrency * 2);
  const control = makeClient({ region: cfg.region });

  // Decide which of the group's keys to upload (skip matching existing unless force).
  const keysToUpload = [];
  if (cfg.forceUpload) {
    keysToUpload.push(...group.keys);
  } else {
    const expectedFirstPart = Math.min(cfg.partSize, bytes);
    for (const key of group.keys) {
      const have = await describeExisting(control, cfg.bucket, key);
      if (have && have.size === bytes && have.firstPartSize === expectedFirstPart) continue;
      keysToUpload.push(key);
    }
  }

  if (!keysToUpload.length) {
    control.destroy();
    if (!cfg.json) {
      console.error(
        `[skip] ${group.label}: all ${group.count} file(s) already exist. ` +
          `Set forceUpload/--force to benchmark upload anyway.`,
      );
    }
    return null;
  }

  const totalBytes = bytes * keysToUpload.length;
  if (!cfg.json) {
    console.error(
      `[info] ${group.label}: ${keysToUpload.length} file(s) x ${formatBytes(bytes)} ` +
        `= ${formatBytes(totalBytes)}, ${baseParts.length} parts/file, ` +
        `checksum ${cfg.checksum || 'none'}, source ${cfg.uploadSource}`,
    );
  }

  // For 'file' source, one shared random source file of this size (untimed).
  let sourceFilePath = null;
  const ipTputEnabled = cfg.ipThroughput || cfg.ipThroughputSizes.includes(group.label);
  const ipHistory = new Map();
  const progressBuf = cfg.progress && !cfg.json ? newProgressBuffer() : null;
  const progressLabel = keysToUpload.length > 1 ? `${group.label} x${keysToUpload.length}` : group.label;
  const makeReporter = () =>
    progressBuf ? new ProgressReporter(progressBuf, totalBytes, { label: progressLabel }) : null;

  try {
    // 'file' and the 'file' opener of 'open'/'open-stream' need a source file;
    // the 'memory' opener generates bytes in-memory, so it does not.
    const needsSourceFile =
      cfg.uploadSource === 'file' ||
      ((cfg.uploadSource === 'open' || cfg.uploadSource === 'open-stream') &&
        (cfg.uploadOpen?.type ?? 'file') === 'file');
    if (needsSourceFile) {
      mkdirSync(cfg.sourcePath, { recursive: true }); // createWriteStream won't create parents
      const safe = group.label.replace(/[^\w.-]/g, '_');
      sourceFilePath = path.join(cfg.sourcePath, `s3ulbench-src-${safe}`);
      if (!cfg.json) console.error(`[setup] writing ${formatBytes(bytes)} source file (untimed) ...`);
      await writeRandomFile(sourceFilePath, bytes);
    }

    // 'memory': one object-sized SharedArrayBuffer per object, filled once and
    // reused across warmup + timed iterations (allocation is untimed).
    const objectBuffers =
      cfg.uploadSource === 'memory' ? allocObjectBuffers(keysToUpload, bytes, { json: cfg.json }) : null;

    for (let i = 0; i < cfg.warmup; i++) {
      await uploadIterationGroup(control, cfg, keysToUpload, baseParts, maxSockets, sourceFilePath, ipTputEnabled, progressBuf, makeReporter(), objectBuffers);
    }

    const monitor = new ResourceMonitor();
    monitor.start();
    const samples = [];
    let negotiatedTls = null;
    for (let i = 0; i < cfg.iterations; i++) {
      const r = await uploadIterationGroup(control, cfg, keysToUpload, baseParts, maxSockets, sourceFilePath, ipTputEnabled, progressBuf, makeReporter(), objectBuffers);
      const secs = r.wallMs / 1000;
      samples.push({ secs, ...throughput(r.bytes, secs) });
      if (!negotiatedTls && r.tlsInfo) negotiatedTls = r.tlsInfo;
      if (ipTputEnabled) accumulateIpSamples(ipHistory, ipIterationGbps(new Map(r.ipThroughput)));
    }
    const resources = monitor.stop();

    let ipThroughputRows = null;
    if (ipTputEnabled) {
      ipThroughputRows = summarizeIpHistory(ipHistory);
      appendIpRecord(cfg.ipThroughputFile, {
        ts: new Date().toISOString(),
        mode: 'upload',
        node: process.version,
        sdk: SDK_VERSION,
        bucket: cfg.bucket,
        region: cfg.region,
        size: group.label,
        files: keysToUpload.length,
        workers: Math.min(cfg.workers, baseParts.length * keysToUpload.length),
        concurrency: cfg.concurrency,
        spreadConnections: cfg.spreadConnections,
        tls: cfg.tls,
        iterations: cfg.iterations,
        perIp: ipThroughputRows,
      });
    }

    const byThroughput = [...samples].sort((a, b) => a.mibps - b.mibps);
    const median = byThroughput[Math.floor(byThroughput.length / 2)];
    const best = byThroughput[byThroughput.length - 1];
    const totalParts = baseParts.length * keysToUpload.length;
    const workers = Math.min(cfg.workers, totalParts);

    return {
      label: group.label,
      files: keysToUpload.length,
      perFileSize: bytes,
      size: totalBytes,
      parts: totalParts,
      partSize: cfg.partSize,
      checksum: cfg.checksum,
      uploadSource: cfg.uploadSource,
      workers,
      concurrency: cfg.concurrency,
      totalInFlight: workers * cfg.concurrency,
      iterations: cfg.iterations,
      samples,
      median: { secs: median.secs, mibps: median.mibps, gbps: median.gbps },
      best: { secs: best.secs, mibps: best.mibps, gbps: best.gbps },
      resources,
      ipThroughput: ipThroughputRows,
      tlsInfo: negotiatedTls,
    };
  } finally {
    if (sourceFilePath) rmSync(sourceFilePath, { force: true });
    control.destroy();
  }
}

/** transport= label, including the negotiated TLS protocol/cipher when available. */
function uploadTlsNote(cfg, all) {
  if (!cfg.tls) return 'HTTP (no TLS)';
  const neg = all.find((r) => r.tlsInfo)?.tlsInfo;
  const pin = cfg.cipher && cfg.cipher !== 'default' ? ` (pin ${cfg.cipher})` : '';
  return neg ? `HTTPS ${neg.protocol}/${neg.cipher}${pin}` : `HTTPS${pin}`;
}

function printHuman(cfg, all) {
  console.log('\n=== S3 multipart UPLOAD benchmark (AWS SDK JS v3) ===');
  console.log(`node=${process.version}  sdk=@aws-sdk/client-s3@${SDK_VERSION}  @smithy/core@${SMITHY_CORE_VERSION}`);
  console.log(`region=${cfg.region ?? '(default)'}  bucket=${cfg.bucket}`);
  console.log(
    `source=${cfg.uploadSource}${
      cfg.uploadSource === 'stream'
        ? ` (main-carve->worker-pool, max-buffered ${cfg.uploadMaxBuffered > 0 ? formatBytes(cfg.uploadMaxBuffered) : 'auto'}, client-chunk ${formatBytes(cfg.uploadClientChunk)}${cfg.uploadClientRate > 0 ? `, client ${formatBytes(cfg.uploadClientRate)}/s` : ''})`
        : cfg.uploadSource === 'open'
          ? ` (worker-open ${cfg.uploadOpen?.type ?? 'file'})`
          : cfg.uploadSource === 'open-stream'
            ? ` (carvers${cfg.uploadCarvers > 0 ? ` x${cfg.uploadCarvers}` : ' auto'} -> uploaders x${cfg.workers}, ${cfg.uploadOpen?.type ?? 'file'})`
            : cfg.uploadSource === 'memory'
              ? ` (one SharedArrayBuffer per object, zero-copy part views)`
              : ''
    }  ` +
      `handler=${cfg.httpHandler}  transport=${uploadTlsNote(cfg, all)}  ` +
      `part-size=${formatBytes(cfg.partSize)}  checksum=${cfg.checksum || 'off'}  ` +
      `spread-conns=${cfg.spreadConnections ? 'ON' : 'OFF'}  workers=${cfg.workers}  ` +
      `concurrency/worker=${cfg.concurrency}  iterations=${cfg.iterations} (warmup=${cfg.warmup})\n`,
  );

  const pad = (s, n) => String(s).padEnd(n);
  const padS = (s, n) => String(s).padStart(n);
  console.log(
    pad('size', 12) + padS('files', 6) + padS('total', 11) + padS('parts', 7) +
      padS('inflight', 10) + padS('med MiB/s', 12) + padS('med Gbps', 11) + padS('best Gbps', 11),
  );
  console.log('-'.repeat(81));
  for (const r of all) {
    console.log(
      pad(r.label, 12) +
        padS(r.files, 6) +
        padS(formatBytes(r.size), 11) +
        padS(r.parts, 7) +
        padS(r.totalInFlight, 10) +
        padS(r.median.mibps.toFixed(1), 12) +
        padS(r.median.gbps.toFixed(3), 11) +
        padS(r.best.gbps.toFixed(3), 11),
    );
  }
  console.log('');
  printResources(all);
  for (const r of all) if (r.ipThroughput) printIpThroughput(r.label, r.ipThroughput);
}

/** Per-group resource usage during the measured iterations. */
function printResources(all) {
  if (!all.length || !all[0].resources) return;
  const pad = (s, n) => String(s).padEnd(n);
  const padS = (s, n) => String(s).padStart(n);
  console.log('resource usage (whole process, during measured iterations):');
  console.log(
    pad('size', 14) + padS('peak RSS', 12) + padS('avg RSS', 12) +
      padS('peak CPU', 10) + padS('avg CPU', 10) +
      padS('main pk', 9) + padS('main avg', 9) + padS('peak MEM', 10),
  );
  console.log('-'.repeat(86));
  for (const r of all) {
    const rs = r.resources;
    console.log(
      pad(r.label, 14) +
        padS(formatBytes(rs.peakRssBytes), 12) +
        padS(formatBytes(rs.avgRssBytes), 12) +
        padS(`${rs.peakCpuPercent.toFixed(0)}%`, 10) +
        padS(`${rs.avgCpuPercent.toFixed(0)}%`, 10) +
        padS(`${rs.peakMainPercent.toFixed(0)}%`, 9) +
        padS(`${rs.avgMainPercent.toFixed(0)}%`, 9) +
        padS(`${rs.peakMemUtilPercent.toFixed(1)}%`, 10),
    );
  }
  console.log(
    `(CPU% is of all ${all[0].resources.cpuCount} cores; ` +
      `main pk/avg = main-thread event-loop utilization, share of ONE core; ` +
      `MEM% is of ${formatBytes(all[0].resources.totalMemBytes)} total RAM)\n`,
  );
}

async function main() {
  const cfg = parseUploadArgs();
  const all = [];
  for (const group of cfg.groups) {
    try {
      const r = await benchmarkGroup(cfg, group);
      if (!r) continue;
      all.push(r);
      if (!cfg.json) {
        console.error(
          `[done] ${r.label} (${r.files} file(s)): ${r.median.mibps.toFixed(1)} MiB/s ` +
            `(${r.median.gbps.toFixed(3)} Gbps) median over ${r.iterations} runs`,
        );
      }
    } catch (err) {
      console.error(`[error] ${group.label}: ${err.message}`);
    }
  }

  const payload = JSON.stringify(
    { mode: 'upload', nodeVersion: process.version, sdkVersion: SDK_VERSION, smithyCoreVersion: SMITHY_CORE_VERSION, config: { ...cfg }, results: all },
    null,
    2,
  );

  if (cfg.out) {
    mkdirSync(path.dirname(path.resolve(cfg.out)), { recursive: true });
    writeFileSync(cfg.out, payload + '\n');
    console.error(`[out] wrote JSON results to ${cfg.out}`);
  }

  if (cfg.json) {
    process.stdout.write(payload + '\n');
  } else {
    printHuman(cfg, all);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
