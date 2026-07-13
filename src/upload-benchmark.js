#!/usr/bin/env node
import { Worker } from 'node:worker_threads';
import { fileURLToPath } from 'node:url';
import { writeFileSync, mkdirSync, createWriteStream, rmSync } from 'node:fs';
import { randomFillSync } from 'node:crypto';
import { createRequire } from 'node:module';
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
function runOnce({ bucket, region, parts, workers, concurrency, checksum, maxSockets, uploadSource, sourceFilePath, spreadConnections, tls, ipThroughput, httpHandler, ciphers }) {
  const buckets = assignParts(parts, workers).filter((b) => b.length > 0);
  const active = buckets.length;

  return new Promise((resolve, reject) => {
    const threads = [];
    let readyCount = 0;
    let doneCount = 0;
    let startTime = 0;
    const results = [];
    let settled = false;
    let tlsInfo = null;
    const runIpTput = new Map();

    const cleanup = () => threads.forEach((w) => w.terminate());
    const fail = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err instanceof Error ? err : new Error(String(err)));
    };

    for (const slice of buckets) {
      const worker = new Worker(WORKER_PATH, {
        workerData: {
          bucket, region, parts: slice, concurrency, checksum, maxSockets,
          uploadSource, sourceFilePath, spreadConnections, tls, ipThroughput, httpHandler, ciphers,
        },
      });
      threads.push(worker);

      worker.on('message', (msg) => {
        if (msg.type === 'ready') {
          if (++readyCount === active) {
            startTime = performance.now();
            threads.forEach((w) => w.postMessage({ type: 'start' }));
          }
        } else if (msg.type === 'done') {
          results.push(msg);
          mergeIpThroughput(runIpTput, msg.ipThroughput);
          if (!tlsInfo && msg.tlsInfo) tlsInfo = msg.tlsInfo;
          if (++doneCount === active && !settled) {
            settled = true;
            const wallMs = performance.now() - startTime;
            cleanup();
            const bytes = results.reduce((s, r) => s + r.bytes, 0);
            const completed = results.flatMap((r) => r.completed);
            resolve({ bytes, completed, wallMs, ipThroughput: [...runIpTput], tlsInfo });
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
 * One iteration for a whole group: create a multipart upload per key (untimed),
 * upload all parts pooled across keys (timed), then complete each upload (untimed).
 */
async function uploadIterationGroup(control, cfg, keys, baseParts, maxSockets, sourceFilePath, ipTputEnabled) {
  // Create one multipart upload per key.
  const uploadIds = {};
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

  // Pool every key's parts into one work list, tagged with key + uploadId.
  const parts = [];
  for (const key of keys) {
    for (const p of baseParts) parts.push({ ...p, key, uploadId: uploadIds[key] });
  }

  const abortAll = async () => {
    for (const key of keys) {
      await control
        .send(new AbortMultipartUploadCommand({ Bucket: cfg.bucket, Key: key, UploadId: uploadIds[key] }))
        .catch(() => {});
    }
  };

  let timed;
  try {
    timed = await runOnce({
      bucket: cfg.bucket,
      region: cfg.region,
      parts,
      workers: Math.min(cfg.workers, parts.length),
      concurrency: cfg.concurrency,
      checksum: cfg.checksum,
      maxSockets,
      uploadSource: cfg.uploadSource,
      sourceFilePath,
      spreadConnections: cfg.spreadConnections,
      tls: cfg.tls,
      ipThroughput: ipTputEnabled,
      httpHandler: cfg.httpHandler,
      ciphers: cfg.ciphers,
    });
  } catch (err) {
    await abortAll();
    throw err;
  }

  // Group completed parts by key and complete each multipart upload.
  const byKey = new Map(keys.map((k) => [k, []]));
  for (const c of timed.completed) byKey.get(c.key).push(c);
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

  return timed;
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

  try {
    if (cfg.uploadSource === 'file') {
      mkdirSync(cfg.sourcePath, { recursive: true }); // createWriteStream won't create parents
      const safe = group.label.replace(/[^\w.-]/g, '_');
      sourceFilePath = path.join(cfg.sourcePath, `s3ulbench-src-${safe}`);
      if (!cfg.json) console.error(`[setup] writing ${formatBytes(bytes)} source file (untimed) ...`);
      await writeRandomFile(sourceFilePath, bytes);
    }

    for (let i = 0; i < cfg.warmup; i++) {
      await uploadIterationGroup(control, cfg, keysToUpload, baseParts, maxSockets, sourceFilePath, ipTputEnabled);
    }

    const monitor = new ResourceMonitor();
    monitor.start();
    const samples = [];
    let negotiatedTls = null;
    for (let i = 0; i < cfg.iterations; i++) {
      const r = await uploadIterationGroup(control, cfg, keysToUpload, baseParts, maxSockets, sourceFilePath, ipTputEnabled);
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
    `source=${cfg.uploadSource}  handler=${cfg.httpHandler}  transport=${uploadTlsNote(cfg, all)}  ` +
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
      padS('peak CPU', 10) + padS('avg CPU', 10) + padS('peak MEM', 10),
  );
  console.log('-'.repeat(68));
  for (const r of all) {
    const rs = r.resources;
    console.log(
      pad(r.label, 14) +
        padS(formatBytes(rs.peakRssBytes), 12) +
        padS(formatBytes(rs.avgRssBytes), 12) +
        padS(`${rs.peakCpuPercent.toFixed(0)}%`, 10) +
        padS(`${rs.avgCpuPercent.toFixed(0)}%`, 10) +
        padS(`${rs.peakMemUtilPercent.toFixed(1)}%`, 10),
    );
  }
  console.log(
    `(CPU% is of all ${all[0].resources.cpuCount} cores; ` +
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
