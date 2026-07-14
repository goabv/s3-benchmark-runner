import { parentPort, workerData } from 'node:worker_threads';
import { randomFillSync } from 'node:crypto';
import { openSync, readSync, closeSync } from 'node:fs';
import { UploadPartCommand } from '@aws-sdk/client-s3';
import { makeClient } from './s3.js';
import { IpThroughputTracker } from './ip-throughput.js';
import { installNativeCrc32 } from './crc32-native.mjs';

/**
 * Worker thread: uploads an assigned set of parts of a single in-progress
 * multipart upload, running up to `concurrency` UploadPart calls in parallel.
 * Mirrors download-worker.js.
 *
 * Data is pseudo-random and generated ONCE at init (before `ready`) into a single
 * reusable buffer sized to this worker's largest part, so RNG/allocation cost is
 * excluded from the measured window (like client init on the download side).
 * S3 doesn't care that parts share content; the SDK still computes each part's
 * checksum independently.
 *
 * Returns per-part { PartNumber, ETag, Checksum* } so the main thread can call
 * CompleteMultipartUpload.
 *
 * Protocol:
 *   worker -> main: { type: 'ready' }
 *   main -> worker: { type: 'start' }
 *   worker -> main: { type: 'done', bytes, parts, completed:[...], elapsedMs }
 *   worker -> main: { type: 'error', message }
 */

const {
  bucket,
  region,
  parts, // [{ key, uploadId, partNumber, start, size }] — may span multiple objects
  concurrency,
  checksum, // algorithm name or falsy to skip
  maxSockets,
  uploadSource, // 'memory' | 'file'
  sourceFilePath, // for uploadSource === 'file'
  spreadConnections,
  tls,
  ipThroughput,
  httpHandler,
  ciphers, // OpenSSL cipher string to pin the TLS suite (null = defaults)
  nativeCrc32, // patch @aws-crypto/crc32 to use native zlib.crc32
} = workerData;

if (nativeCrc32) {
  const r = await installNativeCrc32();
  if (r.patched) console.error(`[native-crc32] patched: ${r.reason}`);
  else if (r.alreadyNative) console.error(`[native-crc32] no patch needed: ${r.reason}`);
  else console.error(`[native-crc32] not applied: ${r.reason}`);
}

const tracker = ipThroughput ? new IpThroughputTracker((s) => s.bytesWritten) : null;
const onConnect = tracker ? (ip, socket) => tracker.register(socket, ip) : null;

// Record the negotiated TLS protocol/cipher of the first secure socket.
let tlsInfo = null;
const onTls = (info) => {
  if (!tlsInfo) tlsInfo = info;
};

const client = makeClient({ region, maxSockets, spreadConnections, tls, onConnect, httpHandler, ciphers, onTls });

// memory: pre-generate one random buffer sized to the largest part this worker
// handles, BEFORE signalling ready, so buffer creation is excluded from timing.
// file: open the source file; part bytes are read on demand during the timed run.
let buffer = null;
let fd = null;
if (uploadSource === 'file') {
  fd = openSync(sourceFilePath, 'r');
} else {
  const maxPartSize = parts.reduce((m, p) => Math.max(m, p.size), 0);
  buffer = Buffer.allocUnsafe(maxPartSize);
  if (maxPartSize > 0) randomFillSync(buffer);
}

async function uploadPart({ key, uploadId, partNumber, start, size }) {
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
  } else {
    body = size === buffer.length ? buffer : buffer.subarray(0, size);
  }
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

async function run() {
  let cursor = 0;
  let totalBytes = 0;
  const completed = [];

  async function nextInQueue() {
    while (cursor < parts.length) {
      const part = parts[cursor++];
      const r = await uploadPart(part);
      totalBytes += part.size;
      completed.push(r);
    }
  }

  const lanes = Math.max(1, Math.min(concurrency, parts.length || 1));
  const start = performance.now();
  await Promise.all(Array.from({ length: lanes }, nextInQueue));
  const elapsedMs = performance.now() - start;

  return { totalBytes, partsDone: completed.length, completed, elapsedMs };
}

parentPort.on('message', async (msg) => {
  if (msg?.type !== 'start') return;
  try {
    const { totalBytes, partsDone, completed, elapsedMs } = await run();
    const ipTput = tracker ? tracker.snapshot() : null;
    parentPort.postMessage({
      type: 'done',
      bytes: totalBytes,
      parts: partsDone,
      completed,
      elapsedMs,
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
