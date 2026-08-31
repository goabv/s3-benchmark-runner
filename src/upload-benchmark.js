#!/usr/bin/env node
import { Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';
import { writeFileSync, mkdirSync, createWriteStream, statSync, statfsSync } from 'node:fs';
import { randomFillSync } from 'node:crypto';
import { createRequire } from 'node:module';
import os from 'node:os';
import path from 'node:path';
import { HeadObjectCommand } from '@aws-sdk/client-s3';
import { makeClient } from './s3.js';
import { parseUploadArgs, parseSize, formatBytes, throughput } from './config.js';
import { ResourceMonitor } from './resource-monitor.js';
import { newProgressBuffer, ProgressReporter } from './progress.js';
import { S3TransferManager } from './transfer-manager.js';
import { accumulateIpSamples, ipIterationGbps, summarizeIpHistory, printIpThroughput, appendIpRecord } from './ip-throughput.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

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

/**
 * Synthetic customer stream (deliveryMode 'stream'): a real Readable that emits `size`
 * bytes as a client would push them — in chunks of `template.length`, optionally
 * throttled to `clientRate` bytes/sec. Reuses the pre-filled template (content is
 * irrelevant to S3, and the manager copies each chunk out immediately).
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

/** Write `size` bytes of random data to a file (untimed setup for deliveryMode 'file'). */
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
 * Build PRE-FILLED upload sources per object (filled UNTIMED, before the run) for
 * deliveryMode 'memory'. memoryInputType picks the shape:
 *   parts - a collection of part-sized standalone Buffers per object (transferred to
 *           workers; the manager restores them after each iteration for reuse)
 *   sab   - one object-sized SharedArrayBuffer per object (workers read zero-copy
 *           slices; shared, trivially reusable)
 * Resident memory = sum of object sizes, so it's preflight-guarded against box RAM.
 */
function buildUploadSources(keys, bytes, partSize, memoryInputType, { json } = {}) {
  const total = bytes * keys.length;
  const budget = Math.floor(os.totalmem() * 0.8);
  if (total > budget) {
    throw new Error(
      `upload memory/${memoryInputType} needs ${formatBytes(total)} of pre-filled buffers ` +
        `(${keys.length} x ${formatBytes(bytes)}), but only ~${formatBytes(budget)} of ` +
        `${formatBytes(os.totalmem())} RAM is available. Reduce object size/count.`,
    );
  }
  if (!json) {
    console.error(
      `[setup] pre-filling ${keys.length} x ${formatBytes(bytes)} = ${formatBytes(total)} ` +
        `as memory/${memoryInputType} buffer(s), random-filling (untimed) ...`,
    );
  }
  const FILL = 1 << 30; // randomFillSync offset/size cap is 2^31-1; fill in <=1GiB windows
  const sources = [];
  for (const key of keys) {
    if (memoryInputType === 'sab') {
      const sab = new SharedArrayBuffer(bytes);
      for (let off = 0; off < bytes; off += FILL) {
        randomFillSync(Buffer.from(sab, off, Math.min(FILL, bytes - off)));
      }
      sources.push({ key, buffer: sab, size: bytes });
    } else {
      const parts = [];
      for (let start = 0; start < bytes; start += partSize) {
        const psize = Math.min(partSize, bytes - start);
        const buf = Buffer.allocUnsafeSlow(psize); // standalone ArrayBuffer (transferable)
        for (let off = 0; off < psize; off += FILL) {
          randomFillSync(buf, off, Math.min(FILL, psize - off));
        }
        parts.push(buf);
      }
      sources.push({ key, parts });
    }
  }
  return sources;
}

/**
 * API-mode upload run: fire one upload() per object CONCURRENTLY (x calls for x
 * objects) through the warm S3TransferManager uploader pool. Measured window =
 * first upload() (CreateMPU) -> last CompleteMPU. Pool spawn + any pre-fill are
 * one-time (outside this window).
 */
async function runUploadViaManager(manager, cfg, sources, reporter) {
  manager.resetScheduler();
  const t0 = performance.now();
  reporter?.start();
  const results = await manager.uploadMany({ bucket: cfg.bucket, sources });
  const wallMs = performance.now() - t0;
  reporter?.stop();
  const totalBytes = results.reduce((s, r) => s + r.bytes, 0);
  return { bytes: totalBytes, wallMs, tlsInfo: manager.tlsInfo };
}

async function benchmarkGroup(cfg, group) {
  const bytes = parseSize(group.label);
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
  const perObjectParts = Math.max(1, Math.ceil(bytes / cfg.partSize));
  const totalParts = perObjectParts * keysToUpload.length;
  if (!cfg.json) {
    console.error(
      `[info] ${group.label}: ${keysToUpload.length} file(s) x ${formatBytes(bytes)} ` +
        `= ${formatBytes(totalBytes)}, ${perObjectParts} parts/file, ` +
        `checksum ${cfg.checksum || 'none'}, delivery ${cfg.deliveryMode}` +
        `${cfg.deliveryMode === 'memory' ? `/${cfg.memoryInputType}` : ''}`,
    );
  }

  const ipTputEnabled = cfg.ipThroughput || cfg.ipThroughputSizes.includes(group.label);
  const ipHistory = new Map();
  const progressBuf = cfg.progress && !cfg.json ? newProgressBuffer() : null;
  const progressLabel = keysToUpload.length > 1 ? `${group.label} x${keysToUpload.length}` : group.label;
  const makeReporter = () =>
    progressBuf ? new ProgressReporter(progressBuf, totalBytes, { label: progressLabel }) : null;

  let sourceFilePath = null;
  let manager = null;
  let spawnMs = 0;

  try {
    // Prepare the per-object sources (UNTIMED). parts/sab pre-fill buffers; file writes
    // one shared source file; stream builds a template (streams are made per iteration).
    let staticSources = null;
    let template = null;
    if (cfg.deliveryMode === 'memory') {
      // memoryInputType picks the buffer shape (parts | sab); pre-filled untimed.
      staticSources = buildUploadSources(keysToUpload, bytes, cfg.partSize, cfg.memoryInputType, { json: cfg.json });
    } else if (cfg.deliveryMode === 'file') {
      mkdirSync(cfg.sourcePath, { recursive: true });
      const safe = group.label.replace(/[^\w.-]/g, '_');
      sourceFilePath = path.join(cfg.sourcePath, `s3ulbench-src-${safe}`);
      // Reuse an already-staged source file of the exact size (staging a large file
      // is slow, untimed setup). Delete the file to force a regenerate.
      let existing = null;
      try { existing = statSync(sourceFilePath); } catch { /* missing */ }
      if (existing && existing.size === bytes) {
        if (!cfg.json) console.error(`[setup] reusing existing ${formatBytes(bytes)} source file: ${sourceFilePath}`);
      } else {
        // Preflight: never stage a source file bigger than the target filesystem's
        // free space. This turns "fill the fs (e.g. tmpfs /tmp) and freeze the box"
        // into a fast, clear error. Account for space reclaimed by overwriting an
        // existing (wrong-size) file at this path.
        const fss = statfsSync(cfg.sourcePath);
        const freeBytes = fss.bavail * fss.bsize + (existing ? existing.size : 0);
        if (freeBytes < bytes) {
          throw new Error(
            `staging ${formatBytes(bytes)} source file needs more than the ${formatBytes(freeBytes)} free at ${cfg.sourcePath}. ` +
              `Set upload.sourcePath to a volume with room (e.g. a large mount), or reduce the size. ` +
              `The default os.tmpdir() is often tmpfs (RAM-backed) — do not stage large files there.`,
          );
        }
        if (!cfg.json) {
          console.error(
            `[setup] writing ${formatBytes(bytes)} source file (untimed)${existing ? ` (size ${formatBytes(existing.size)} != ${formatBytes(bytes)}, regenerating)` : ''} ...`,
          );
        }
        await writeRandomFile(sourceFilePath, bytes);
      }
      staticSources = keysToUpload.map((key) => ({ key, file: sourceFilePath, size: bytes }));
    } else {
      // stream: pre-fill a small template (untimed); a fresh Readable per object per run.
      template = Buffer.allocUnsafe(Math.max(1, cfg.uploadClientChunk || 1 << 20));
      randomFillSync(template);
    }
    // memory/file sources are reusable across iterations; stream needs fresh Readables.
    const makeSources = () =>
      cfg.deliveryMode === 'stream'
        ? keysToUpload.map((key) => ({ key, body: makeCustomerStream(bytes, template, cfg.uploadClientRate) }))
        : staticSources;

    // Construct the persistent uploader pool ONCE (spawn + client init untimed).
    manager = new S3TransferManager({
      bucket: cfg.bucket,
      region: cfg.region,
      mode: 'upload',
      workers: cfg.workers,
      concurrency: cfg.concurrency,
      partSize: cfg.partSize,
      checksum: cfg.checksum,
      uploadMaxBuffered: cfg.uploadMaxBuffered,
      spreadConnections: cfg.spreadConnections,
      tls: cfg.tls,
      ciphers: cfg.ciphers,
      httpHandler: cfg.httpHandler,
      nativeCrc32: cfg.nativeCrc32,
      progressBuf,
      ipThroughput: ipTputEnabled,
    });
    await manager.ready();

    for (let i = 0; i < cfg.warmup; i++) {
      await runUploadViaManager(manager, cfg, makeSources(), makeReporter());
    }

    const monitor = new ResourceMonitor();
    monitor.start();
    const samples = [];
    let negotiatedTls = null;
    for (let i = 0; i < cfg.iterations; i++) {
      const r = await runUploadViaManager(manager, cfg, makeSources(), makeReporter());
      const secs = r.wallMs / 1000;
      samples.push({ secs, ...throughput(r.bytes, secs) });
      if (!negotiatedTls && r.tlsInfo) negotiatedTls = r.tlsInfo;
    }
    const resources = monitor.stop();
    spawnMs = (await manager.close()).spawnMs;

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
        workers: cfg.workers,
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
    const workers = Math.min(cfg.workers, totalParts);

    return {
      label: group.label,
      files: keysToUpload.length,
      perFileSize: bytes,
      size: totalBytes,
      parts: totalParts,
      partSize: cfg.partSize,
      checksum: cfg.checksum,
      deliveryMode: cfg.deliveryMode,
      memoryInputType: cfg.deliveryMode === 'memory' ? cfg.memoryInputType : null,
      workers,
      concurrency: cfg.concurrency,
      totalInFlight: workers * cfg.concurrency,
      iterations: cfg.iterations,
      api: true,
      spawnMs, // one-time uploader-pool spawn + client init (NOT in any Gbps)
      samples,
      median: { secs: median.secs, mibps: median.mibps, gbps: median.gbps },
      best: { secs: best.secs, mibps: best.mibps, gbps: best.gbps },
      resources,
      ipThroughput: ipThroughputRows,
      tlsInfo: negotiatedTls,
    };
  } finally {
    if (manager && !manager._closePromise) await manager.close().catch(() => {});
    // Keep the staged source file so it can be reused next run (staging a large file
    // is expensive). Delete it manually (or it's overwritten if the size changes).
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

/** Human-readable label for the active deliveryMode. */
function sourceNote(cfg) {
  switch (cfg.deliveryMode) {
    case 'memory':
      return cfg.memoryInputType === 'sab'
        ? 'memory/sab (pre-filled SharedArrayBuffer, zero-copy slices)'
        : 'memory/parts (pre-filled part buffers, transferred)';
    case 'file':
      return `file (workers read own part ranges from disk, max-buffered ${cfg.uploadMaxBuffered > 0 ? formatBytes(cfg.uploadMaxBuffered) : 'auto'})`;
    case 'stream':
      return (
        `stream (customer Readable from main -> carve, max-buffered ` +
        `${cfg.uploadMaxBuffered > 0 ? formatBytes(cfg.uploadMaxBuffered) : 'auto'}, ` +
        `client-chunk ${formatBytes(cfg.uploadClientChunk)}` +
        `${cfg.uploadClientRate > 0 ? `, client ${formatBytes(cfg.uploadClientRate)}/s` : ''})`
      );
    default:
      return cfg.deliveryMode;
  }
}

function printHuman(cfg, all) {
  console.log('\n=== S3 multipart UPLOAD benchmark (AWS SDK JS v3) ===');
  console.log(`node=${process.version}  sdk=@aws-sdk/client-s3@${SDK_VERSION}  @smithy/core@${SMITHY_CORE_VERSION}`);
  console.log(`region=${cfg.region ?? '(default)'}  bucket=${cfg.bucket}`);
  console.log(
    `api=S3TransferManager (x concurrent upload() + parallel UploadPart)  ` +
      `delivery=${sourceNote(cfg)}  ` +
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
  console.log(
    `(med/best Gbps = full upload() call CreateMPU -> UploadPart -> CompleteMPU. ` +
      `One-time uploader-pool spawn + client init (excluded): ` +
      `${all.map((r) => `${r.label} ${(r.spawnMs ?? 0).toFixed(0)}ms`).join(', ')})`,
  );
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
