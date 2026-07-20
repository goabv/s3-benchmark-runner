#!/usr/bin/env node
/**
 * S3 upload benchmark runner following the aws-crt-s3-benchmarks runner protocol.
 *
 * Usage:
 *   node minimal-upload.mjs <S3_CLIENT> <WORKLOAD> <BUCKET> <REGION> <TARGET_THROUGHPUT>
 *
 * Prints "Run:<N> Secs:<f> Gb/s:<f>" per iteration (parsed by metrics.py).
 * Exits 123 to skip unsupported workloads.
 */
import { Worker } from 'node:worker_threads';
import { randomFillSync } from 'node:crypto';
import { readFileSync, appendFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import {
  CreateMultipartUploadCommand,
  CompleteMultipartUploadCommand,
} from '@aws-sdk/client-s3';
import { makeClient } from './src/s3.js';
import { computeParts } from './src/config.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WORKER_PATH = path.join(__dirname, 'src', 'upload-worker.js');

const [,, s3Client, workloadPath, bucket, region, targetThroughputGbps] = process.argv;

if (!workloadPath || !bucket || !region) {
  console.error('Usage: node minimal-upload.mjs <S3_CLIENT> <WORKLOAD> <BUCKET> <REGION> <TARGET_THROUGHPUT>');
  process.exit(1);
}

// Load workload config
const workload = JSON.parse(readFileSync(workloadPath, 'utf8'));
if (workload.version !== 2) {
  console.error(`Skipping benchmark - workload version not supported: ${workload.version}`);
  process.exit(123);
}

const tasks = workload.tasks.filter(t => t.action === 'upload');
if (!tasks.length) {
  console.error('Skipping benchmark - no upload tasks');
  process.exit(123);
}

const maxRepeatCount = workload.maxRepeatCount || 10;
const maxRepeatSecs = workload.maxRepeatSecs || 600;
const bytesPerRun = tasks.reduce((sum, t) => sum + t.size, 0);
const gigabitsPerRun = (bytesPerRun * 8) / 1e9;

const PART_SIZE = 16 * 1024 * 1024; // 16 MiB
const nWorkers = Math.max(1, (await import('node:os')).default.cpus().length);
const concurrency = 4;
const maxSockets = Math.max(64, nWorkers * concurrency);

const client = makeClient({ region });

// Pre-generate random source data per task (untimed)
const objectBuffers = {};
for (const task of tasks) {
  const sab = new SharedArrayBuffer(task.size);
  const CHUNK = 1 << 26;
  for (let off = 0; off < task.size; off += CHUNK) {
    randomFillSync(Buffer.from(sab, off, Math.min(CHUNK, task.size - off)));
  }
  objectBuffers[task.key] = sab;
}

function assignParts(parts, n) {
  const buckets = Array.from({ length: n }, () => []);
  parts.forEach((p, i) => buckets[i % n].push(p));
  return buckets;
}

async function runOnce() {
  // CreateMultipartUpload for each task (each gets its own uploadId even if same key)
  const uploads = []; // [{ key, uploadId, size }]
  for (const task of tasks) {
    const cmd = { Bucket: bucket, Key: task.key };
    if (workload.checksum) cmd.ChecksumAlgorithm = workload.checksum;
    const res = await client.send(new CreateMultipartUploadCommand(cmd));
    uploads.push({ key: task.key, uploadId: res.UploadId, size: task.size });
  }

  // Build part list — tag each part with a unique taskIdx so completions don't collide
  const allParts = [];
  for (let ti = 0; ti < uploads.length; ti++) {
    const u = uploads[ti];
    for (const p of computeParts(u.size, PART_SIZE)) {
      allParts.push({ ...p, key: u.key, uploadId: u.uploadId, taskIdx: ti });
    }
  }
  const buckets = assignParts(allParts, nWorkers);

  // Spawn workers, upload parts, collect results
  const { completed } = await new Promise((resolve, reject) => {
    const threads = [];
    let readyCount = 0, doneCount = 0;
    const results = [];
    let t0 = 0;

    for (let wi = 0; wi < nWorkers; wi++) {
      const worker = new Worker(WORKER_PATH, {
        workerData: {
          bucket, region, concurrency, maxSockets,
          checksum: workload.checksum || null, uploadSource: 'memory',
          objectBuffers, sourceFilePath: null,
          spreadConnections: false, tls: true,
          ipThroughput: false, httpHandler: 'node',
          ciphers: null, nativeCrc32: false,
          workerId: wi, maxPartSize: PART_SIZE,
        },
      });
      threads.push(worker);
      worker.on('message', (msg) => {
        if (msg.type === 'ready') {
          if (++readyCount === nWorkers) {
            t0 = performance.now();
            for (let i = 0; i < nWorkers; i++) threads[i].postMessage({ type: 'start', parts: buckets[i] });
          }
        } else if (msg.type === 'done') {
          results.push(msg);
          if (++doneCount === nWorkers) {
            const completed = results.flatMap(r => r.completed);
            threads.forEach(w => w.terminate());
            resolve({ completed });
          }
        } else if (msg.type === 'error') {
          threads.forEach(w => w.terminate());
          reject(new Error(`worker: ${msg.message}`));
        }
      });
      worker.on('error', reject);
    }
  });

  // CompleteMultipartUpload per task
  for (let ti = 0; ti < uploads.length; ti++) {
    const u = uploads[ti];
    const parts = completed
      .filter(c => c.key === u.key && c.uploadId === u.uploadId)
      .map(c => {
        const p = { PartNumber: c.PartNumber, ETag: c.ETag };
        if (c.ChecksumCRC32) p.ChecksumCRC32 = c.ChecksumCRC32;
        if (c.ChecksumCRC32C) p.ChecksumCRC32C = c.ChecksumCRC32C;
        if (c.ChecksumSHA1) p.ChecksumSHA1 = c.ChecksumSHA1;
        if (c.ChecksumSHA256) p.ChecksumSHA256 = c.ChecksumSHA256;
        return p;
      })
      .sort((a, b) => a.PartNumber - b.PartNumber);
    await client.send(new CompleteMultipartUploadCommand({
      Bucket: bucket, Key: u.key, UploadId: u.uploadId,
      MultipartUpload: { Parts: parts },
    }));
  }
}

// Run loop
const header = `\n=== Workload: ${workloadPath} ===
Workers: ${nWorkers} | Concurrency/worker: ${concurrency} | Part size: ${PART_SIZE / (1024 * 1024)} MiB | Checksum: ${workload.checksum || 'none'}
`;
console.log(header);
const resultsFile = path.join(__dirname, 'smilkuri_upload_results.txt');
appendFileSync(resultsFile, header + '\n');
const appStart = performance.now();
for (let run = 1; run <= maxRepeatCount; run++) {
  const runStart = performance.now();
  await runOnce();
  const runSecs = (performance.now() - runStart) / 1000;
  const gbps = gigabitsPerRun / runSecs;

  const line = `Run:${run} Secs:${runSecs.toFixed(6)} Gb/s:${gbps.toFixed(6)}`;
  console.log(line);
  appendFileSync(resultsFile, line + '\n');

  if ((performance.now() - appStart) / 1000 >= maxRepeatSecs) break;
}

client.destroy();
