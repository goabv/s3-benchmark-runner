#!/usr/bin/env node
/**
 * S3 download benchmark runner for s3-benchmark-runner's S3TransferManager,
 * following the aws-crt-s3-benchmarks runner protocol.
 *
 * Usage:
 *   node minimal-download.mjs <S3_CLIENT> <WORKLOAD> <BUCKET> <REGION> <TARGET_THROUGHPUT>
 *
 * Prints "Run:<N> Secs:<f> Gb/s:<f>" per iteration (parsed by metrics.py).
 * Exits 123 to skip unsupported workloads.
 */
import { S3TransferManager } from './src/transfer-manager.js';
import { parseArgs } from './src/config.js';
import { Writable } from 'node:stream';
import { readFileSync, appendFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const [,, s3Client, workloadPath, bucket, region, targetThroughputGbps] = process.argv;

if (!workloadPath || !bucket || !region) {
  console.error('Usage: node minimal-download.mjs <S3_CLIENT> <WORKLOAD> <BUCKET> <REGION> <TARGET_THROUGHPUT>');
  process.exit(1);
}

// Load workload config
const workload = JSON.parse(readFileSync(workloadPath, 'utf8'));
if (workload.version !== 2) {
  console.error(`Skipping benchmark - workload version not supported: ${workload.version}`);
  process.exit(123);
}

const tasks = workload.tasks.filter(t => t.action === 'download');
if (!tasks.length) {
  console.error('Skipping benchmark - no download tasks');
  process.exit(123);
}

const maxRepeatCount = workload.maxRepeatCount || 10;
const maxRepeatSecs = workload.maxRepeatSecs || 600;
const bytesPerRun = tasks.reduce((sum, t) => sum + t.size, 0);
const gigabitsPerRun = (bytesPerRun * 8) / 1e9;
const keys = tasks.map(t => t.key);

// Init transfer manager (workers/concurrency from bench.config.json)
const cfg = parseArgs(['--bucket', bucket, '--region', region, '--keys', ...keys]);
const tm = new S3TransferManager(cfg);
await tm.ready();

// Run loop: repeat until maxRepeatCount or maxRepeatSecs
const resultsFile = path.join(__dirname, 'smilkuri_download_results.txt');
const header = `\n=== Workload: ${workloadPath} ===
Workers: ${cfg.workers} | Concurrency/worker: ${cfg.concurrency} | Tasks: ${tasks.length}
`;
console.log(header);
appendFileSync(resultsFile, header + '\n');

const appStart = performance.now();
for (let run = 1; run <= maxRepeatCount; run++) {
  tm.resetScheduler();

  const runStart = performance.now();
  const job = await tm.downloadMany({ bucket, keys });
  await Promise.all(job.objects.map(({ body }) =>
    new Promise((res, rej) => {
      body.pipe(new Writable({
        write(chunk, _enc, cb) { tm.recycle(chunk); cb(); },
      })).on('finish', res).on('error', rej);
    })
  ));
  const runSecs = (performance.now() - runStart) / 1000;
  const gbps = gigabitsPerRun / runSecs;

  const line = `Run:${run} Secs:${runSecs.toFixed(6)} Gb/s:${gbps.toFixed(6)}`;
  console.log(line);
  appendFileSync(resultsFile, line + '\n');

  if ((performance.now() - appStart) / 1000 >= maxRepeatSecs) break;
}

await tm.close();
