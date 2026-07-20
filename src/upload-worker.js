import { parentPort, workerData } from 'node:worker_threads';
import { randomFillSync } from 'node:crypto';
import { openSync, readSync, closeSync, createReadStream } from 'node:fs';
import { Readable } from 'node:stream';
import { UploadPartCommand } from '@aws-sdk/client-s3';
import { makeClient } from './s3.js';
import { IpThroughputTracker } from './ip-throughput.js';
import { installNativeCrc32 } from './crc32-native.mjs';
import { bumpProgress, progressView } from './progress.js';

/**
 * Worker thread for the S3 multipart UPLOAD benchmark. Two roles:
 *
 *  - SLICE mode (uploadSource 'memory' | 'file' | 'open'): the worker owns a fixed
 *    slice of parts and uploads them with up to `concurrency` parallel UploadPart
 *    calls, sourcing bytes from a per-object SharedArrayBuffer shared from main
 *    (memory, zero-copy view), a shared source file (file), or a per-worker opener
 *    (open). Protocol: main->worker 'start'; worker->main 'done'.
 *
 *  - POOL mode (uploadSource 'stream'): the worker is a generic uploader. The main
 *    thread carves the customer stream into part buffers and TRANSFERS them here
 *    one at a time; this worker uploads each and transfers the (now free) buffer
 *    back for reuse. Protocol: main->worker 'upload' {part, buffer}; worker->main
 *    'uploaded' {result, buffer}; main->worker 'stop'.
 *
 * Common: worker->main 'ready' once the client is initialized; 'error' on failure.
 */

const {
  bucket,
  region,
  parts, // slice mode: [{ key, uploadId, partNumber, start, size }]. Absent in pool mode.
  concurrency,
  checksum, // algorithm name or falsy to skip
  maxSockets,
  uploadSource, // 'memory' | 'file' | 'stream' | 'open' | 'open-stream'
  role, // 'carver' | 'uploader' (open-stream two-tier); undefined = single-tier
  sourceFilePath, // for uploadSource === 'file'
  openDesc, // for uploadSource === 'open'(-stream): { type:'file', path } | { type:'memory', chunk }
  objectBuffers, // for uploadSource === 'memory': { key -> SharedArrayBuffer } (shared, zero-copy)
  carverObjects, // carver role: [{ key, uploadId(set on carve), size, baseParts }]
  carverLimit = 1, // carver role: max outstanding (un-acked) parts before pausing
  spreadConnections,
  tls,
  ipThroughput,
  httpHandler,
  ciphers, // OpenSSL cipher string to pin the TLS suite (null = defaults)
  nativeCrc32, // patch @aws-crypto/crc32 to use native zlib.crc32
  workerId = 0, // only worker 0 logs the native-crc32 status (avoid N-way spam)
  maxPartSize = 0, // carver/uploader pool: size the recycled part buffers
  progressBuf, // shared byte counter for the live progress indicator (or null)
} = workerData;

const progress = progressView(progressBuf);

if (nativeCrc32) {
  const r = await installNativeCrc32();
  if (workerId === 0) {
    if (r.patched) console.error(`[native-crc32] patched: ${r.reason}`);
    else if (!r.alreadyNative) console.error(`[native-crc32] not applied: ${r.reason}`);
  }
}

const tracker = ipThroughput ? new IpThroughputTracker((s) => s.bytesWritten) : null;
const onConnect = tracker ? (ip, socket) => tracker.register(socket, ip) : null;

// Record the negotiated TLS protocol/cipher of the first secure socket.
let tlsInfo = null;
const onTls = (info) => {
  if (!tlsInfo) tlsInfo = info;
};

const client = makeClient({ region, maxSockets, spreadConnections, tls, onConnect, httpHandler, ciphers, onTls });

// In-memory synthetic stream: emits `total` bytes from a reused random template
// (no disk I/O). Backs the 'memory' opener for 'open' / 'open-stream', so ingress
// is generated on the worker at memcpy speed rather than read from a file.
function memoryStream(total, template) {
  let sent = 0;
  return new Readable({
    highWaterMark: template.length,
    read() {
      if (sent >= total) {
        this.push(null);
        return;
      }
      const n = Math.min(template.length, total - sent);
      sent += n;
      this.push(n === template.length ? template : template.subarray(0, n));
    },
  });
}

// Send one part's body (already materialized) and normalize the result.
async function sendPartBody({ key, uploadId, partNumber, size }, body) {
  const res = await client.send(
    new UploadPartCommand({
      Bucket: bucket,
      Key: key,
      UploadId: uploadId,
      PartNumber: partNumber,
      Body: body,
      ...(checksum ? { ChecksumAlgorithm: checksum } : {}),
    }),
  );
  return {
    key,
    PartNumber: partNumber,
    ETag: res.ETag,
    size,
    ChecksumCRC32C: res.ChecksumCRC32C,
    ChecksumCRC32: res.ChecksumCRC32,
    ChecksumSHA1: res.ChecksumSHA1,
    ChecksumSHA256: res.ChecksumSHA256,
  };
}

// ---------------------------------------------------------------------------
// CARVER role (open-stream): open each assigned object's whole stream, carve parts
// into dedicated buffers, and TRANSFER them to main for the uploader pool. Bounded
// by `carverLimit` outstanding (un-acked) parts — pause reading when at the cap;
// main returns each buffer (for reuse) with the 'ack' after its upload completes.
// ---------------------------------------------------------------------------
if (role === 'carver') {
  const partSize = maxPartSize;
  const freePool = [];
  let outstanding = 0;
  let resume = null;
  const acquire = () => freePool.pop() || Buffer.allocUnsafeSlow(partSize);

  // Resolve the whole-object opener: open(params, { key, size }) -> Readable.
  let openObject;
  if (openDesc?.type === 'memory') {
    // Generate bytes in-memory (no disk): the benchmark "pushes" the object into
    // the stream from a reused template — ingress is memcpy speed, per carver.
    const tmpl = Buffer.allocUnsafe(Math.max(1, openDesc.chunk || 1 << 20));
    randomFillSync(tmpl);
    openObject = (key, size) => memoryStream(size, tmpl);
  } else {
    openObject = () => createReadStream(openDesc.path); // whole file = one object's bytes
  }

  parentPort.on('message', async (msg) => {
    if (msg?.type === 'ack') {
      if (msg.buffer) freePool.push(Buffer.from(msg.buffer)); // recycle the freed buffer
      outstanding -= 1;
      if (resume && outstanding < carverLimit) {
        const r = resume;
        resume = null;
        r();
      }
      return;
    }
    if (msg?.type !== 'carve') return;
    try {
      for (const obj of msg.objects) {
        const stream = await openObject(obj.key, obj.size);
        const bp = obj.baseParts;
        let partIdx = 0;
        let buf = acquire();
        let off = 0;
        for await (const chunk of stream) {
          let cpos = 0;
          while (cpos < chunk.length && partIdx < bp.length) {
            const target = bp[partIdx].size;
            const n = Math.min(chunk.length - cpos, target - off);
            chunk.copy(buf, off, cpos, cpos + n);
            off += n;
            cpos += n;
            if (off === target) {
              parentPort.postMessage(
                { type: 'part', key: obj.key, uploadId: obj.uploadId, partNumber: bp[partIdx].partNumber, size: target, buffer: buf.buffer },
                [buf.buffer],
              );
              outstanding += 1;
              partIdx += 1;
              off = 0;
              if (outstanding >= carverLimit) await new Promise((r) => (resume = r));
              if (partIdx < bp.length) buf = acquire();
            }
          }
        }
      }
      parentPort.postMessage({ type: 'carver-done' });
    } catch (err) {
      parentPort.postMessage({ type: 'error', message: err?.message ?? String(err) });
    }
  });
  parentPort.postMessage({ type: 'ready' });
}
// ---------------------------------------------------------------------------
// UPLOADER / POOL role: upload part bytes on demand. The bytes arrive one of three
// ways: a TRANSFERRED buffer ('upload', from parts/stream carve), a shared-buffer
// SLICE ('upload-sab'), or a positional FILE range this worker reads itself
// ('upload-file' — distributed ingress).
// ---------------------------------------------------------------------------
else if (role === 'uploader' || uploadSource === 'stream') {
  // Cache one fd per source file for positional reads (upload-file).
  const uploaderFds = new Map();
  const fdForFile = (p) => {
    let fd = uploaderFds.get(p);
    if (fd === undefined) {
      fd = openSync(p, 'r');
      uploaderFds.set(p, fd);
    }
    return fd;
  };
  parentPort.on('message', async (msg) => {
    if (msg?.type === 'upload') {
      try {
        const body = Buffer.from(msg.buffer, 0, msg.size);
        const r = await sendPartBody(
          { key: msg.key, uploadId: msg.uploadId, partNumber: msg.partNumber, size: msg.size },
          body,
        );
        bumpProgress(progress, msg.size);
        // Transfer the (now fully sent) buffer back to main for recycling.
        parentPort.postMessage(
          {
            type: 'uploaded',
            key: r.key,
            partNumber: r.PartNumber,
            size: r.size,
            carverId: msg.carverId, // echo so main can ack the right carver (open-stream)
            ETag: r.ETag,
            ChecksumCRC32C: r.ChecksumCRC32C,
            ChecksumCRC32: r.ChecksumCRC32,
            ChecksumSHA1: r.ChecksumSHA1,
            ChecksumSHA256: r.ChecksumSHA256,
            tlsInfo,
            buffer: msg.buffer,
          },
          [msg.buffer],
        );
      } catch (err) {
        parentPort.postMessage({ type: 'error', message: err?.message ?? String(err) });
      }
    } else if (msg?.type === 'upload-sab') {
      // SAB mode: bytes live in a shared buffer (not transferred). Read this part's
      // zero-copy slice and upload it; the SAB stays valid on main for reuse.
      try {
        const body = Buffer.from(msg.sab, msg.start, msg.size);
        const r = await sendPartBody(
          { key: msg.key, uploadId: msg.uploadId, partNumber: msg.partNumber, size: msg.size },
          body,
        );
        bumpProgress(progress, msg.size);
        parentPort.postMessage({
          type: 'uploaded',
          key: r.key,
          partNumber: r.PartNumber,
          size: r.size,
          ETag: r.ETag,
          ChecksumCRC32C: r.ChecksumCRC32C,
          ChecksumCRC32: r.ChecksumCRC32,
          ChecksumSHA1: r.ChecksumSHA1,
          ChecksumSHA256: r.ChecksumSHA256,
          tlsInfo,
          // no buffer field: SAB is shared, nothing to transfer back
        });
      } catch (err) {
        parentPort.postMessage({ type: 'error', message: err?.message ?? String(err) });
      }
    } else if (msg?.type === 'upload-file') {
      // file mode: read THIS part's byte range from the file (positional, this worker
      // does its own I/O — distributed ingress), then upload it.
      try {
        const fd = fdForFile(msg.file);
        const body = Buffer.allocUnsafeSlow(msg.size);
        let read = 0;
        while (read < msg.size) {
          const n = readSync(fd, body, read, msg.size - read, msg.start + read);
          if (n === 0) break;
          read += n;
        }
        const r = await sendPartBody(
          { key: msg.key, uploadId: msg.uploadId, partNumber: msg.partNumber, size: msg.size },
          body,
        );
        bumpProgress(progress, msg.size);
        parentPort.postMessage({
          type: 'uploaded',
          key: r.key,
          partNumber: r.PartNumber,
          size: r.size,
          ETag: r.ETag,
          ChecksumCRC32C: r.ChecksumCRC32C,
          ChecksumCRC32: r.ChecksumCRC32,
          ChecksumSHA1: r.ChecksumSHA1,
          ChecksumSHA256: r.ChecksumSHA256,
          tlsInfo,
          // no buffer field: the worker allocated its own read buffer (GC'd here)
        });
      } catch (err) {
        parentPort.postMessage({ type: 'error', message: err?.message ?? String(err) });
      }
    } else if (msg?.type === 'stop') {
      for (const fd of uploaderFds.values()) {
        try {
          closeSync(fd);
        } catch {
          /* ignore */
        }
      }
      uploaderFds.clear();
      try {
        client.destroy();
      } catch {
        /* ignore */
      }
    }
  });
  parentPort.postMessage({ type: 'ready' });
} else {
  // -------------------------------------------------------------------------
  // SLICE mode (uploadSource 'memory' | 'file' | 'open').
  // -------------------------------------------------------------------------
  // memory: one object-sized SharedArrayBuffer per object is allocated + filled on
  //   main (untimed) and shared here by reference; each part is a zero-copy view.
  // file:   open the source file; part bytes are read on demand during the timed run.
  // open:   resolve a per-worker opener; each part opens its own byte-range stream.
  let fd = null;
  let openStream = null; // (part) => Readable, for uploadSource === 'open'
  if (uploadSource === 'file') {
    fd = openSync(sourceFilePath, 'r');
  } else if (uploadSource === 'open') {
    // Factory pattern: resolve the opener ON THIS WORKER from the descriptor, so
    // each worker opens its own stream for its part ranges (distributed ingress).
    if (openDesc?.type === 'memory') {
      const tmpl = Buffer.allocUnsafe(Math.max(1, openDesc.chunk || 1 << 20));
      randomFillSync(tmpl);
      openStream = (part) => memoryStream(part.size, tmpl); // generate the part in-memory
    } else {
      openStream = (part) =>
        createReadStream(openDesc.path, { start: part.start, end: part.start + part.size - 1 }); // end inclusive
    }
  }
  // memory: object-sized SharedArrayBuffers were allocated + filled on main and are
  // shared here by reference (no copy, no per-worker allocation). Each part is a
  // zero-copy Buffer view into its object's buffer — nothing to set up per worker.

  // materialize the body, then send.
  async function uploadPart(part) {
    const { start, size } = part;
    let body;
    if (uploadSource === 'file') {
      // Read this part's bytes from disk (timed — part of the file-upload cost).
      body = Buffer.allocUnsafeSlow(size);
      let read = 0;
      while (read < size) {
        const n = readSync(fd, body, read, size - read, start + read);
        if (n === 0) break;
        read += n;
      }
    } else if (uploadSource === 'open') {
      // Open our own stream for this part's byte range and drain it (ingress here).
      // openStream may return a Readable OR a Promise<Readable> (e.g. an HTTP opener
      // that must await the connection), so await it either way.
      const rs = await openStream(part);
      const chunks = [];
      for await (const c of rs) chunks.push(c);
      body = chunks.length === 1 ? chunks[0] : Buffer.concat(chunks, size);
    } else {
      // memory: zero-copy view into this object's shared buffer at its part range.
      body = Buffer.from(objectBuffers[part.key], start, size);
    }
    return sendPartBody(part, body);
  }

  async function run(sliceParts) {
    let cursor = 0;
    let totalBytes = 0;
    const completed = [];

    async function nextInQueue() {
      while (cursor < sliceParts.length) {
        const part = sliceParts[cursor++];
        const r = await uploadPart(part);
        bumpProgress(progress, part.size);
        totalBytes += part.size;
        completed.push(r);
      }
    }

    const lanes = Math.max(1, Math.min(concurrency, sliceParts.length || 1));
    await Promise.all(Array.from({ length: lanes }, nextInQueue));
    return { totalBytes, partsDone: completed.length, completed };
  }

  parentPort.on('message', async (msg) => {
    if (msg?.type !== 'start') return; // carries this worker's part slice
    try {
      const { totalBytes, partsDone, completed } = await run(msg.parts || []);
      const ipTput = tracker ? tracker.snapshot() : null;
      parentPort.postMessage({
        type: 'done',
        bytes: totalBytes,
        parts: partsDone,
        completed,
        ipThroughput: ipTput,
        tlsInfo,
      });
    } catch (err) {
      parentPort.postMessage({ type: 'error', message: err?.message ?? String(err) });
    } finally {
      if (fd !== null) {
        try {
          closeSync(fd);
        } catch {
          /* ignore */
        }
      }
      client.destroy();
    }
  });

  parentPort.postMessage({ type: 'ready' });
}
