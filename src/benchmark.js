#!/usr/bin/env node
import { Worker } from 'node:worker_threads';
import { fileURLToPath } from 'node:url';
import { writeFileSync, mkdirSync, openSync, ftruncateSync, closeSync, rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import os from 'node:os';
import path from 'node:path';
import { HeadObjectCommand } from '@aws-sdk/client-s3';
import { makeClient } from './s3.js';
import { parseArgs, formatBytes, throughput } from './config.js';
import { ResourceMonitor } from './resource-monitor.js';
import { renderSvg } from './plot.js';
import {
  mergeIpThroughput,
  ipIterationGbps,
  accumulateIpSamples,
  summarizeIpHistory,
  printIpThroughput,
  appendIpRecord,
} from './ip-throughput.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WORKER_PATH = path.join(__dirname, 'download-worker.js');

// Record which SDK version produced these numbers — essential when comparing
// runs across different @aws-sdk/client-s3 versions.
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

/** Round-robin items across N buckets so each worker gets an interleaved share. */
function assignParts(items, n) {
  const buckets = Array.from({ length: n }, () => []);
  items.forEach((it, i) => buckets[i % n].push(it));
  return buckets;
}

/**
 * Build the part list with absolute byte offsets. Multipart objects have uniform
 * part sizes except the last, so offset = (partNumber-1) * firstPartSize.
 */
function buildParts(key, partsCount, firstPartSize, totalSize) {
  const parts = [];
  for (let p = 1; p <= partsCount; p++) {
    const offset = (p - 1) * firstPartSize;
    const size = p < partsCount ? firstPartSize : totalSize - offset;
    parts.push({ key, partNumber: p, offset, size });
  }
  return parts;
}

/**
 * Discover how the object was uploaded. A HEAD with PartNumber=1 returns the
 * object's PartsCount (from the multipart upload) and part 1's size; a plain HEAD
 * returns the total size. ChecksumMode: ENABLED surfaces the stored checksum.
 */
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
  const hasPartChecksum = Boolean(
    part1.ChecksumCRC32C || part1.ChecksumCRC32 || part1.ChecksumSHA1 || part1.ChecksumSHA256,
  );
  const checksumAlgo = part1.ChecksumCRC32C
    ? 'CRC32C'
    : part1.ChecksumCRC32
      ? 'CRC32'
      : part1.ChecksumSHA256
        ? 'SHA256'
        : part1.ChecksumSHA1
          ? 'SHA1'
          : null;

  return { totalSize, partsCount, firstPartSize, hasPartChecksum, checksumAlgo };
}

/**
 * Run one measured download of a single object across `workers` threads.
 * Timing excludes worker spawn + client init: we wait for all workers to report
 * `ready`, then broadcast `start` and time until the last `done`.
 */
// SLICE mode (discard / file): each worker owns a fixed slice and runs freely.
function runOnce({ bucket, region, parts, workers, concurrency, keep, maxSockets, validateChecksum, deliveryMode, filePaths, logConnections, spreadConnections, tls, ipThroughput, httpHandler, ciphers, stallTimeoutMs, partRetries, partTimes, fileAsync, profile, profileDir }) {
  const buckets = assignParts(parts, workers).filter((b) => b.length > 0);
  const active = buckets.length;

  return new Promise((resolve, reject) => {
    const threads = [];
    let readyCount = 0;
    let doneCount = 0;
    let startTime = 0;
    let settled = false;
    const results = [];
    const runPartTimes = [];
    let tlsInfo = null;
    const runIp = new Map();
    const runIpTput = new Map();
    const mergeIp = (arr) => {
      if (arr) for (const [ip, c] of arr) runIp.set(ip, (runIp.get(ip) || 0) + c);
    };

    const cleanup = () => threads.forEach((w) => w.terminate());
    const fail = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err instanceof Error ? err : new Error(String(err)));
    };

    let sliceIdx = 0;
    for (const slice of buckets) {
      const worker = new Worker(WORKER_PATH, {
        workerData: {
          bucket, region, parts: slice, concurrency, keep, maxSockets,
          validateChecksum, deliveryMode, filePaths, logConnections, spreadConnections, tls, ipThroughput, httpHandler,
          ciphers, stallTimeoutMs, partRetries, partTimes, workerId: sliceIdx++, fileAsync,
          profile, profileDir,
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
          if (msg.partTimes) runPartTimes.push(...msg.partTimes);
          if (!tlsInfo && msg.tlsInfo) tlsInfo = msg.tlsInfo;
          mergeIp(msg.ipCounts);
          mergeIpThroughput(runIpTput, msg.ipThroughput);
          if (++doneCount === active && !settled) {
            settled = true;
            const wallMs = performance.now() - startTime;
            cleanup();
            resolve({
              bytes: results.reduce((s, r) => s + r.bytes, 0),
              partsDone: results.reduce((s, r) => s + r.parts, 0),
              checksummed: results.reduce((s, r) => s + r.checksummed, 0),
              wallMs,
              ipCounts: [...runIp],
              ipThroughput: [...runIpTput],
              partTimes: runPartTimes,
              tlsInfo,
            });
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
 * DISPATCH mode (ordered-stream): the MAIN thread hands out parts in ascending
 * order (frontier-first), keeping in-flight + buffered bytes under
 * `maxBufferedBytes`. Proactive — it always fetches the lowest-needed parts to
 * complete the deliverable chain — and can't hang: when nothing is in flight it
 * dispatches the next part regardless of the cap (bounded one-part overshoot),
 * so the part delivery is waiting on is always fetched.
 */
function runOrdered({ bucket, region, parts, workers, concurrency, maxSockets, validateChecksum, logConnections, spreadConnections, tls, ipThroughput, maxBufferedBytes, httpHandler, ciphers, timeseries, stallTimeoutMs, partRetries, partTimes, bufferPool, profile, profileDir }) {
  const queue = [...parts].sort(
    (a, b) => a.partNumber - b.partNumber || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0),
  );
  const totalParts = queue.length;
  const nWorkers = Math.max(1, Math.min(workers, totalParts));
  const cap = maxBufferedBytes > 0 ? maxBufferedBytes : Infinity;

  return new Promise((resolve, reject) => {
    const threads = [];
    let readyCount = 0;
    let startTime = 0;
    let settled = false;

    // 500ms time-series sampler (ordered-stream only). Captures memory, buffered
    // part count, in-flight part count and process CPU% against elapsed time.
    const tsSamples = [];
    let tsTimer = null;
    let lastCpu = null;
    let lastCpuAt = 0n;
    const cpuCount = os.cpus().length;
    const sampleOnce = () => {
      const nowNs = process.hrtime.bigint();
      const cpu = process.cpuUsage();
      let cpuPct = 0;
      if (lastCpu) {
        const usedMicros = cpu.user + cpu.system - (lastCpu.user + lastCpu.system);
        const wallMicros = Number(nowNs - lastCpuAt) / 1000;
        if (wallMicros > 0) cpuPct = (usedMicros / wallMicros / cpuCount) * 100;
      }
      lastCpu = cpu;
      lastCpuAt = nowNs;
      tsSamples.push({
        tMs: performance.now() - startTime,
        rss: process.memoryUsage().rss,
        bufferedParts: heldMeta.size,
        bufferedBytes,
        inFlight: totalInFlight,
        cpuPct,
      });
    };

    let queueIndex = 0;
    let bufferedBytes = 0; // reorder buffer only (completed, not yet delivered)
    let totalInFlight = 0;
    const freeLanes = new Array(nWorkers).fill(concurrency);

    // Metadata only — the bytes stay in the workers. id -> { byteLength, hasChecksum, wi }
    const heldMeta = new Map();
    const nextByKey = new Map();
    let deliveredCount = 0;
    let deliveredBytes = 0;
    let deliveredChecksummed = 0;
    let deliveryWallMs = 0;
    let deliveryDone = false;

    const runIp = new Map();
    const runIpTput = new Map();
    const runPartTimes = [];
    let tlsInfo = null;
    let workerDone = 0;

    const cleanup = () => {
      if (tsTimer) clearInterval(tsTimer);
      threads.forEach((w) => w.terminate());
    };
    const fail = (err) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(err instanceof Error ? err : new Error(String(err)));
    };

    const dispatchMore = () => {
      for (let wi = 0; wi < nWorkers; wi++) {
        while (freeLanes[wi] > 0 && queueIndex < totalParts) {
          // Bound only the reorder backlog (completed-but-undelivered) by the cap;
          // in-flight is bounded separately by lane count, so full network
          // concurrency is preserved. Throttle only when the backlog is full —
          // except always dispatch when nothing is in flight (liveness: that part
          // is the lowest-needed one, so delivery can advance).
          if (totalInFlight > 0 && bufferedBytes >= cap) return;
          threads[wi].postMessage({ type: 'assign', part: queue[queueIndex] });
          freeLanes[wi] -= 1;
          totalInFlight += 1;
          queueIndex += 1;
        }
      }
    };

    const drainKey = (key) => {
      let n = nextByKey.get(key) ?? 1;
      let id = `${key}#${n}`;
      while (heldMeta.has(id)) {
        const info = heldMeta.get(id);
        deliveredBytes += info.byteLength;
        if (info.hasChecksum) deliveredChecksummed += 1;
        bufferedBytes -= info.byteLength;
        heldMeta.delete(id);
        // Tell the owning worker it may free this part (delivered in order).
        threads[info.wi].postMessage({ type: 'release', key, partNumber: n });
        deliveredCount += 1;
        n += 1;
        id = `${key}#${n}`;
      }
      nextByKey.set(key, n);
      if (deliveredCount === totalParts && !deliveryDone) {
        deliveryDone = true;
        deliveryWallMs = performance.now() - startTime;
        threads.forEach((w) => w.postMessage({ type: 'stop' }));
      }
    };

    const maybeResolve = () => {
      if (!settled && deliveryDone && workerDone === threads.length) {
        settled = true;
        cleanup();
        resolve({
          bytes: deliveredBytes,
          partsDone: deliveredCount,
          checksummed: deliveredChecksummed,
          wallMs: deliveryWallMs,
          ipCounts: [...runIp],
          ipThroughput: [...runIpTput],
          timeseries: tsSamples,
          partTimes: runPartTimes,
          tlsInfo,
        });
      }
    };

    for (let wi = 0; wi < nWorkers; wi++) {
      const worker = new Worker(WORKER_PATH, {
        workerData: {
          bucket, region, parts: [], concurrency, maxSockets, validateChecksum,
          deliveryMode: 'ordered-stream', logConnections, spreadConnections, tls, ipThroughput, httpHandler,
          ciphers, stallTimeoutMs, partRetries, partTimes, workerId: wi, bufferPool,
          profile, profileDir,
        },
      });
      threads.push(worker);
      worker.on('message', (msg) => {
        if (msg.type === 'ready') {
          if (++readyCount === nWorkers) {
            startTime = performance.now();
            if (timeseries) {
              lastCpu = process.cpuUsage();
              lastCpuAt = process.hrtime.bigint();
              sampleOnce();
              tsTimer = setInterval(sampleOnce, 500);
              tsTimer.unref();
            }
            dispatchMore();
          }
        } else if (msg.type === 'part-ready') {
          // Metadata only; the worker holds the bytes until we 'release' them.
          heldMeta.set(`${msg.key}#${msg.partNumber}`, {
            byteLength: msg.byteLength,
            hasChecksum: msg.hasChecksum,
            wi,
          });
          if (partTimes && msg.downloadMs !== undefined) {
            runPartTimes.push({
              key: msg.key,
              partNumber: msg.partNumber,
              bytes: msg.byteLength,
              ms: msg.downloadMs,
              vip: msg.vip ?? null,
              connId: msg.connId ?? null,
            });
          }
          bufferedBytes += msg.byteLength;
          freeLanes[wi] += 1;
          totalInFlight -= 1;
          drainKey(msg.key);
          dispatchMore();
        } else if (msg.type === 'worker-done') {
          if (msg.ipCounts) for (const [ip, c] of msg.ipCounts) runIp.set(ip, (runIp.get(ip) || 0) + c);
          mergeIpThroughput(runIpTput, msg.ipThroughput);
          if (!tlsInfo && msg.tlsInfo) tlsInfo = msg.tlsInfo;
          workerDone += 1;
          maybeResolve();
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

/** Summarize how connections spread across remote S3 IPs. */
function summarizeConnections(ipCounts) {
  if (!ipCounts || !ipCounts.length) return null;
  const counts = ipCounts.map(([, c]) => c).sort((a, b) => a - b);
  const total = counts.reduce((s, c) => s + c, 0);
  const median = counts[Math.floor(counts.length / 2)];
  const top = [...ipCounts]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([ip, c]) => ({ ip, count: c }));
  return {
    distinctIps: ipCounts.length,
    totalConnections: total,
    minPerIp: counts[0],
    medianPerIp: median,
    maxPerIp: counts[counts.length - 1],
    top,
  };
}

const MIB = 1024 * 1024;

/** Nearest-rank percentile (p in 0..100) over an already-sorted ascending array. */
function percentile(sorted, p) {
  if (!sorted.length) return 0;
  const rank = Math.ceil((p / 100) * sorted.length);
  return sorted[Math.min(sorted.length - 1, Math.max(0, rank - 1))];
}

/**
 * Write per-part download times to a CSV (one row per part per iteration) and
 * return latency stats (count, min, p50, p90, p99, p999, max, mean in ms). Times
 * are the wall duration of each part's fetch (incl. any stall-retry).
 */
function writePartTimes(cfg, label, ptByIter) {
  const ts = new Date().toISOString().replace(/[:.]/g, '').slice(0, 15);
  const safeLabel = String(label).replace(/[^\w.-]/g, '_');
  const base = cfg.partTimesFile
    ? cfg.partTimesFile.replace(/\.csv$/i, '')
    : path.join('results', `parttimes-${safeLabel}-${ts}`);
  mkdirSync(path.dirname(path.resolve(base)), { recursive: true });

  const rows = ['iter,key,part_number,bytes,download_ms,vip,conn_id'];
  const allMs = [];
  for (const { iter, parts } of ptByIter) {
    const sorted = [...parts].sort(
      (a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0) || a.partNumber - b.partNumber,
    );
    for (const p of sorted) {
      rows.push(
        `${iter},${p.key},${p.partNumber},${p.bytes},${p.ms.toFixed(2)},${p.vip ?? ''},${p.connId ?? ''}`,
      );
      allMs.push(p.ms);
    }
  }
  const csvPath = `${base}.csv`;
  writeFileSync(csvPath, rows.join('\n') + '\n');

  allMs.sort((a, b) => a - b);
  const stats = {
    count: allMs.length,
    min: allMs[0] ?? 0,
    p50: percentile(allMs, 50),
    p90: percentile(allMs, 90),
    p99: percentile(allMs, 99),
    p999: percentile(allMs, 99.9),
    max: allMs[allMs.length - 1] ?? 0,
    mean: allMs.length ? allMs.reduce((s, v) => s + v, 0) / allMs.length : 0,
  };
  console.error(`[part-times] wrote ${csvPath} (${stats.count} parts)`);
  return stats;
}

/**
 * Write the ordered-stream time series to a CSV (all iterations, one row per
 * sample) and an SVG plot (from the last iteration). Base path comes from
 * cfg.timeseriesFile or an auto-generated results/timeseries-<label>-<ts> name.
 */
function writeTimeseries(cfg, label, tsByIter) {
  const ts = new Date().toISOString().replace(/[:.]/g, '').replace('T', 'T').slice(0, 15);
  const safeLabel = String(label).replace(/[^\w.-]/g, '_');
  const base = cfg.timeseriesFile
    ? cfg.timeseriesFile.replace(/\.(csv|svg)$/i, '')
    : path.join('results', `timeseries-${safeLabel}-${ts}`);
  mkdirSync(path.dirname(path.resolve(base)), { recursive: true });

  // CSV: every iteration, one row per 500ms sample.
  const header = 'iter,t_ms,rss_mib,buffered_parts,buffered_mib,inflight_parts,cpu_pct';
  const rows = [header];
  for (const { iter, samples } of tsByIter) {
    for (const s of samples) {
      rows.push(
        `${iter},${s.tMs.toFixed(0)},${(s.rss / MIB).toFixed(1)},${s.bufferedParts},` +
          `${(s.bufferedBytes / MIB).toFixed(1)},${s.inFlight},${s.cpuPct.toFixed(1)}`,
      );
    }
  }
  const csvPath = `${base}.csv`;
  writeFileSync(csvPath, rows.join('\n') + '\n');

  // SVG: plot the last measured iteration (most representative of steady state).
  const last = tsByIter[tsByIter.length - 1];
  const plotSamples = last.samples.map((s) => ({
    tMs: s.tMs,
    rssMiB: s.rss / MIB,
    bufferedParts: s.bufferedParts,
    inFlight: s.inFlight,
    cpuPct: s.cpuPct,
  }));
  const svgPath = `${base}.svg`;
  writeFileSync(svgPath, renderSvg(plotSamples, `ordered-stream ${label} (iter ${last.iter})`));

  console.error(`[timeseries] wrote ${csvPath} and ${svgPath}`);
}

async function benchmarkGroup(cfg, group) {
  const control = makeClient({ region: cfg.region });
  // Describe every file in the group. A missing file throws (download "fails if
  // it does not find enough files of that size").
  const infos = [];
  for (const key of group.keys) {
    try {
      infos.push({ key, info: await describeObject(control, cfg.bucket, key) });
    } catch (err) {
      control.destroy();
      const missing = err?.$metadata?.httpStatusCode === 404 || err?.name === 'NotFound';
      throw new Error(
        missing
          ? `missing object ${key} (need ${group.count} file(s) of ${group.label}; seed them first)`
          : `describe ${key}: ${err.message}`,
      );
    }
  }
  control.destroy();

  // Pool parts across all files in the group into one work list.
  let parts = [];
  let totalBytes = 0;
  for (const { key, info } of infos) {
    parts = parts.concat(buildParts(key, info.partsCount, info.firstPartSize, info.totalSize));
    totalBytes += info.totalSize;
  }
  const first = infos[0].info;
  const perFileSize = first.totalSize;
  const groupLabel = group.label ?? group.keys[0];

  if (!cfg.json) {
    const cs = !cfg.validateChecksum
      ? `${first.checksumAlgo ?? 'none'} stored, validation DISABLED`
      : first.hasPartChecksum
        ? `per-part ${first.checksumAlgo} (SDK-validated)`
        : 'NONE';
    console.error(
      `[info] ${groupLabel}: ${group.count} file(s) x ${formatBytes(perFileSize)} ` +
        `= ${formatBytes(totalBytes)}, ${parts.length} parts total, checksums: ${cs}`,
    );
  }

  const maxSockets = Math.max(64, cfg.concurrency * 2);

  // 'file' mode: create + pre-size one output file per key.
  let filePaths = null;
  if (cfg.deliveryMode === 'file') {
    mkdirSync(cfg.deliveryPath, { recursive: true }); // openSync won't create parents
    filePaths = {};
    for (const { key, info } of infos) {
      const safe = key.replace(/[^\w.-]/g, '_');
      const fp = path.join(cfg.deliveryPath, `s3dlbench-${safe}`);
      const fd = openSync(fp, 'w');
      ftruncateSync(fd, info.totalSize);
      closeSync(fd);
      filePaths[key] = fp;
    }
  }

  const runCfg = {
    bucket: cfg.bucket,
    region: cfg.region,
    parts,
    workers: Math.min(cfg.workers, parts.length),
    concurrency: cfg.concurrency,
    keep: cfg.keep,
    maxSockets,
    validateChecksum: cfg.validateChecksum,
    deliveryMode: cfg.deliveryMode,
    filePaths,
    logConnections: cfg.logConnections,
    spreadConnections: cfg.spreadConnections,
    tls: cfg.tls,
    // Record per-IP throughput for this size if enabled globally or listed.
    ipThroughput: cfg.ipThroughput || cfg.ipThroughputSizes.includes(groupLabel),
    maxBufferedBytes: cfg.maxBufferedBytes,
    httpHandler: cfg.httpHandler,
    ciphers: cfg.ciphers,
    stallTimeoutMs: cfg.stallTimeoutMs,
    partRetries: cfg.partRetries,
    partTimes: cfg.partTimes,
    fileAsync: cfg.fileAsync,
    // Time series only makes sense for ordered-stream (buffered/in-flight counts
    // are centrally tracked there); ignored by runOnce.
    timeseries: cfg.timeseries && cfg.deliveryMode === 'ordered-stream',
    // Buffer pool is an ordered-stream-only memory strategy.
    bufferPool: cfg.bufferPool && cfg.deliveryMode === 'ordered-stream',
    profile: cfg.profile,
    profileDir: cfg.profileDir,
  };
  if (cfg.profile) mkdirSync(cfg.profileDir, { recursive: true });

  const doRun = () => (cfg.deliveryMode === 'ordered-stream' ? runOrdered(runCfg) : runOnce(runCfg));

  let samples;
  let resources;
  let negotiatedTls = null;
  let lastIpCounts = null;
  const ipHistory = new Map();
  const tsByIter = [];
  const ptByIter = [];
  try {
    for (let i = 0; i < cfg.warmup; i++) {
      await doRun();
    }
    const monitor = new ResourceMonitor();
    monitor.start();
    samples = [];
    for (let i = 0; i < cfg.iterations; i++) {
      const r = await doRun();
      const secs = r.wallMs / 1000;
      samples.push({ ...r, secs, ...throughput(r.bytes, secs) });
      if (!negotiatedTls && r.tlsInfo) negotiatedTls = r.tlsInfo;
      lastIpCounts = r.ipCounts;
      if (runCfg.ipThroughput) {
        accumulateIpSamples(ipHistory, ipIterationGbps(new Map(r.ipThroughput)));
      }
      if (runCfg.timeseries && r.timeseries?.length) {
        tsByIter.push({ iter: i, samples: r.timeseries });
      }
      if (runCfg.partTimes && r.partTimes?.length) {
        ptByIter.push({ iter: i, parts: r.partTimes });
      }
    }
    resources = monitor.stop();
  } finally {
    if (filePaths) for (const fp of Object.values(filePaths)) rmSync(fp, { force: true });
  }

  if (runCfg.timeseries && tsByIter.length) {
    writeTimeseries(cfg, groupLabel, tsByIter);
  }
  let partTimeStats = null;
  if (runCfg.partTimes && ptByIter.length) {
    partTimeStats = writePartTimes(cfg, groupLabel, ptByIter);
  }

  const connectionSpread = cfg.logConnections ? summarizeConnections(lastIpCounts) : null;

  // Per-IP throughput summary + ongoing JSONL record.
  let ipThroughputRows = null;
  if (runCfg.ipThroughput) {
    ipThroughputRows = summarizeIpHistory(ipHistory);
    appendIpRecord(cfg.ipThroughputFile, {
      ts: new Date().toISOString(),
      mode: 'download',
      node: process.version,
      sdk: SDK_VERSION,
      bucket: cfg.bucket,
      region: cfg.region,
      size: groupLabel,
      files: group.count,
      workers: runCfg.workers,
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
  const lastChecksummed = samples[samples.length - 1].checksummed;

  return {
    label: groupLabel,
    files: group.count,
    perFileSize,
    size: totalBytes,
    parts: parts.length,
    checksumAlgo: first.checksumAlgo,
    checksumValidated: cfg.validateChecksum,
    deliveryMode: cfg.deliveryMode,
    partsChecksummedPerRun: lastChecksummed,
    workers: runCfg.workers,
    concurrency: cfg.concurrency,
    totalInFlight: runCfg.workers * cfg.concurrency,
    iterations: cfg.iterations,
    samples: samples.map((s) => ({ secs: s.secs, mibps: s.mibps, gbps: s.gbps })),
    median: { secs: median.secs, mibps: median.mibps, gbps: median.gbps },
    best: { secs: best.secs, mibps: best.mibps, gbps: best.gbps },
    resources,
    connectionSpread,
    ipThroughput: ipThroughputRows,
    partTimeStats,
    tlsInfo: negotiatedTls,
  };
}

function printHuman(cfg, all) {
  console.log('\n=== S3 part-boundary download benchmark (AWS SDK JS v3) ===');
  console.log(`node=${process.version}  sdk=@aws-sdk/client-s3@${SDK_VERSION}  @smithy/core@${SMITHY_CORE_VERSION}`);
  console.log(`region=${cfg.region ?? '(default)'}  bucket=${cfg.bucket}`);
  const tlsNote = cfg.tls
    ? (() => {
        const neg = all.find((r) => r.tlsInfo)?.tlsInfo;
        const pin = cfg.cipher && cfg.cipher !== 'default' ? ` (pin ${cfg.cipher})` : '';
        return neg ? `HTTPS ${neg.protocol}/${neg.cipher}${pin}` : `HTTPS${pin}`;
      })()
    : 'HTTP (no TLS)';
  console.log(
    `download unit=PartNumber  handler=${cfg.httpHandler}  transport=${tlsNote}  ` +
      `delivery=${cfg.deliveryMode}${
        cfg.deliveryMode === 'ordered-stream'
          ? ` (max-buffered ${formatBytes(cfg.maxBufferedBytes)}, buffer-pool ${cfg.bufferPool ? 'ON' : 'OFF'})`
          : cfg.deliveryMode === 'file'
            ? ` (writes ${cfg.fileAsync ? `async, UV_THREADPOOL_SIZE=${process.env.UV_THREADPOOL_SIZE ?? '4 (default)'}` : 'sync/blocking'})`
            : ''
      }  ` +
      `checksum validation=${cfg.validateChecksum ? 'ON' : 'OFF'}  ` +
      `spread-conns=${cfg.spreadConnections ? 'ON' : 'OFF'}  ` +
      `workers=${cfg.workers}  concurrency/worker=${cfg.concurrency}  ` +
      `iterations=${cfg.iterations} (warmup=${cfg.warmup})\n`,
  );

  const pad = (s, n) => String(s).padEnd(n);
  const padS = (s, n) => String(s).padStart(n);
  console.log(
    pad('size', 12) + padS('files', 6) + padS('total', 11) + padS('parts', 7) + padS('cksum', 8) +
      padS('inflight', 10) + padS('med MiB/s', 12) + padS('med Gbps', 11) + padS('best Gbps', 11),
  );
  console.log('-'.repeat(88));
  for (const r of all) {
    console.log(
      pad(r.label, 12) +
        padS(r.files, 6) +
        padS(formatBytes(r.size), 11) +
        padS(r.parts, 7) +
        padS(r.checksumValidated ? (r.checksumAlgo ?? '-') : 'off', 8) +
        padS(r.totalInFlight, 10) +
        padS(r.median.mibps.toFixed(1), 12) +
        padS(r.median.gbps.toFixed(3), 11) +
        padS(r.best.gbps.toFixed(3), 11),
    );
  }
  console.log('');
  printResources(all);
  printPartTimes(all);
  printConnectionsAndIps(all);
}

/** Per-part download-time latency percentiles (ms), when --part-times is set. */
function printPartTimes(all) {
  const withPt = all.filter((r) => r.partTimeStats);
  if (!withPt.length) return;
  const pad = (s, n) => String(s).padEnd(n);
  const padS = (s, n) => String(s).padStart(n);
  console.log('per-part download time (ms), across all measured iterations:');
  console.log(
    pad('size', 14) + padS('parts', 8) + padS('min', 9) + padS('p50', 9) +
      padS('p90', 9) + padS('p99', 9) + padS('p99.9', 9) + padS('max', 9) + padS('mean', 9),
  );
  console.log('-'.repeat(85));
  for (const r of withPt) {
    const s = r.partTimeStats;
    console.log(
      pad(r.label, 14) +
        padS(s.count, 8) +
        padS(s.min.toFixed(1), 9) +
        padS(s.p50.toFixed(1), 9) +
        padS(s.p90.toFixed(1), 9) +
        padS(s.p99.toFixed(1), 9) +
        padS(s.p999.toFixed(1), 9) +
        padS(s.max.toFixed(1), 9) +
        padS(s.mean.toFixed(1), 9),
    );
  }
  console.log('');
}

/** Print connection spread (if any) and per-IP throughput (if any). */
function printConnectionsAndIps(all) {
  printConnections(all);
  for (const r of all) if (r.ipThroughput) printIpThroughput(r.label, r.ipThroughput);
}

/** Per-object resource usage during the measured iterations. */
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

/** Per-object connection-to-S3-IP distribution (one representative run). */
function printConnections(all) {
  const withConn = all.filter((r) => r.connectionSpread);
  if (!withConn.length) return;
  console.log('connection spread across S3 IPs (last measured run):');
  for (const r of withConn) {
    const c = r.connectionSpread;
    console.log(
      `  ${r.label}: ${c.distinctIps} distinct IPs, ${c.totalConnections} connections ` +
        `(per-IP min ${c.minPerIp} / median ${c.medianPerIp} / max ${c.maxPerIp})`,
    );
    const top = c.top.map((t) => `${t.ip}=${t.count}`).join(', ');
    console.log(`    top: ${top}`);
  }
  console.log(
    '  (few distinct IPs = connections concentrated on a few S3 front-ends, ' +
      'which caps throughput)\n',
  );
}

async function main() {
  const cfg = parseArgs();
  const all = [];
  for (const group of cfg.groups) {
    const label = group.label ?? group.keys[0];
    try {
      const r = await benchmarkGroup(cfg, group);
      all.push(r);
      if (!cfg.json) {
        const cksumNote = r.checksumValidated
          ? `${r.partsChecksummedPerRun}/${r.parts} parts checksum-validated`
          : 'checksum validation disabled';
        console.error(
          `[done] ${label} (${r.files} file(s)): ${r.median.mibps.toFixed(1)} MiB/s ` +
            `(${r.median.gbps.toFixed(3)} Gbps) median over ${r.iterations} runs; ${cksumNote}`,
        );
      }
    } catch (err) {
      console.error(`[error] ${label}: ${err.message}`);
    }
  }

  const payload = JSON.stringify(
    {
      nodeVersion: process.version,
      sdkVersion: SDK_VERSION,
      smithyCoreVersion: SMITHY_CORE_VERSION,
      config: { ...cfg },
      results: all,
    },
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

  if (cfg.profile) {
    console.error(
      `[profile] wrote per-worker .cpuprofile files to ${cfg.profileDir}\n` +
        `  analyze:  node scripts/prof-top.mjs ${cfg.profileDir}/dl-worker-0.cpuprofile\n` +
        `  (run under each node version — dirs are per-version — then diff the top tables)`,
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
