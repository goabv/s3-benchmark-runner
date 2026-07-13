#!/usr/bin/env node
// Render a saved sweep JSON (download-sweep.json / upload-sweep.json) into the
// same style of formatted table the benchmark prints to stdout. Works offline on
// any committed run.
//   node scripts/show-results.mjs results/runs/<run>/download-sweep.json
import { readFileSync } from 'node:fs';

const file = process.argv[2];
if (!file) {
  console.error('usage: node scripts/show-results.mjs <sweep.json>');
  process.exit(1);
}
const j = JSON.parse(readFileSync(file, 'utf8'));
const isUpload = j.mode === 'upload';
const cfg = j.config || {};

const MIB = 1048576, GIB = 1024 * MIB;
const fmtBytes = (b) =>
  b >= GIB ? (b / GIB).toFixed(2) + ' GiB' : b >= MIB ? (b / MIB).toFixed(2) + ' MiB' : b + ' B';
const pad = (s, n) => String(s).padEnd(n);
const padS = (s, n) => String(s).padStart(n);

console.log(`\n=== S3 ${isUpload ? 'multipart UPLOAD' : 'part-boundary download'} benchmark (AWS SDK JS v3) ===`);
console.log(`node=${j.nodeVersion ?? '(not recorded)'}  sdk=@aws-sdk/client-s3@${j.sdkVersion}  @smithy/core@${j.smithyCoreVersion}`);
console.log(`region=${cfg.region ?? '(default)'}  bucket=${cfg.bucket}`);
if (isUpload) {
  console.log(
    `source=${cfg.uploadSource}  handler=${cfg.httpHandler}  transport=${cfg.tls === false ? 'HTTP' : 'HTTPS'}  ` +
      `part-size=${fmtBytes(cfg.partSize)}  checksum=${cfg.checksum}  ` +
      `workers=${cfg.workers}  concurrency=${cfg.concurrency}  iterations=${cfg.iterations} (warmup=${cfg.warmup})`,
  );
} else {
  console.log(
    `delivery=${cfg.deliveryMode}  handler=${cfg.httpHandler}  transport=${cfg.tls === false ? 'HTTP' : 'HTTPS'}  ` +
      `checksum=${cfg.validateChecksum ? 'ON' : 'OFF'}  spread-conns=${cfg.spreadConnections ? 'ON' : 'OFF'}  ` +
      `workers=${cfg.workers}  concurrency=${cfg.concurrency}  iterations=${cfg.iterations} (warmup=${cfg.warmup})`,
  );
}
console.log('');

console.log(
  pad('size', 12) + padS('files', 6) + padS('total', 12) + padS('parts', 8) +
    padS('inflight', 10) + padS('med MiB/s', 12) + padS('med Gbps', 11) + padS('best Gbps', 11),
);
console.log('-'.repeat(82));
for (const r of j.results || []) {
  console.log(
    pad(r.label, 12) + padS(r.files, 6) + padS(fmtBytes(r.size), 12) + padS(r.parts, 8) +
      padS(r.totalInFlight, 10) + padS(r.median.mibps.toFixed(1), 12) +
      padS(r.median.gbps.toFixed(3), 11) + padS(r.best.gbps.toFixed(3), 11),
  );
}
console.log('');

const withRes = (j.results || []).filter((r) => r.resources);
if (withRes.length) {
  console.log('resource usage (whole process, during measured iterations):');
  console.log(
    pad('size', 12) + padS('peak RSS', 12) + padS('avg RSS', 12) +
      padS('peak CPU', 10) + padS('avg CPU', 10) + padS('peak MEM', 10),
  );
  console.log('-'.repeat(66));
  for (const r of withRes) {
    const rs = r.resources;
    console.log(
      pad(r.label, 12) + padS(fmtBytes(rs.peakRssBytes), 12) + padS(fmtBytes(rs.avgRssBytes), 12) +
        padS(rs.peakCpuPercent.toFixed(0) + '%', 10) + padS(rs.avgCpuPercent.toFixed(0) + '%', 10) +
        padS(rs.peakMemUtilPercent.toFixed(1) + '%', 10),
    );
  }
  console.log(
    `(CPU% is of all ${withRes[0].resources.cpuCount} cores; ` +
      `MEM% is of ${fmtBytes(withRes[0].resources.totalMemBytes)} total RAM)`,
  );
}

for (const r of j.results || []) {
  if (r.partTimeStats) {
    const s = r.partTimeStats;
    console.log(
      `\nper-part download time ${r.label} (ms): p50=${s.p50.toFixed(1)}  p90=${s.p90.toFixed(1)}  ` +
        `p99=${s.p99.toFixed(1)}  p99.9=${s.p999.toFixed(1)}  max=${s.max.toFixed(1)}  (n=${s.count})`,
    );
  }
}
console.log('');
