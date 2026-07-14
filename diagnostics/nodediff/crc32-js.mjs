// Isolated microbench of the pure-JS CRC32 loop the SDK uses for checksum
// validation (@aws-crypto/crc32) — the function that regressed on Node 24. No
// network, no streams: just the hot byte loop, so you can attribute a version
// difference to V8 codegen and bisect it with V8 flags.
//
//   node crc32-js.mjs [--seconds S] [--size BYTES]
//   node --no-maglev crc32-js.mjs        # does disabling Maglev recover Node 24?
//   node --trace-deopt crc32-js.mjs      # any deopt churn in the loop?
//   node --trace-opt   crc32-js.mjs      # which tier does the loop reach?
//
// Run under Node 22 and Node 24 and compare MB/s; then try --no-maglev on 24.
import { randomFillSync } from 'node:crypto';

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i >= 0 ? process.argv[i + 1] : d; };
const SECONDS = Number(arg('seconds', 4));
const SIZE = Number(arg('size', 256 * 1024 * 1024));
const MB = 1 << 20;

const buf = Buffer.allocUnsafe(SIZE);
randomFillSync(buf.subarray(0, Math.min(SIZE, 8 * MB))); // partial fill is fine; CRC cost is data-independent

// The real SDK implementation, if resolvable (transitive dep of @aws-sdk/client-s3;
// present on a full install). If not, the local table impl below is the same
// algorithm class and gives the same V8-codegen signal.
let Crc32 = null;
for (const spec of ['@aws-crypto/crc32', '@aws-crypto/crc32/build/index.js']) {
  try { ({ Crc32 } = await import(spec)); if (Crc32) break; } catch { /* try next */ }
}

// Local table-based CRC32 (same algorithm class as @aws-crypto/crc32) as a
// dependency-free reference / comparison.
const TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c >>> 0;
  }
  return t;
})();
function localCrc32(b) {
  let crc = 0xffffffff;
  for (let i = 0; i < b.length; i++) crc = (TABLE[(crc ^ b[i]) & 0xff] ^ (crc >>> 8)) >>> 0;
  return (crc ^ 0xffffffff) >>> 0;
}

function bench(fn, seconds) {
  // warm up so V8 tiers the loop up before measuring
  fn();
  fn();
  let bytes = 0;
  const t0 = performance.now();
  const end = t0 + seconds * 1000;
  while (performance.now() < end) { fn(); bytes += buf.length; }
  return bytes / MB / ((performance.now() - t0) / 1000);
}

console.log(`node ${process.version}  flags=[${process.execArgv.join(' ') || '(none)'}]  size=${(SIZE / MB).toFixed(0)}MiB`);
if (Crc32) {
  const mbps = bench(() => { const c = new Crc32(); c.update(buf); c.digest(); }, SECONDS);
  console.log(`@aws-crypto/crc32 (SDK impl)\t${mbps.toFixed(0)} MB/s`);
} else {
  console.log('@aws-crypto/crc32 (SDK impl)\tnot resolvable here — using local reference only');
}
{
  const mbps = bench(() => localCrc32(buf), SECONDS);
  console.log(`local table CRC32 (reference)\t${mbps.toFixed(0)} MB/s`);
}
