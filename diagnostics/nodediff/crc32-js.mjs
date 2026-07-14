// Pinpoint WHY the SDK's JS CRC32 regressed on Node 24 by comparing coding
// PATTERNS, not just the algorithm. @aws-crypto/crc32's real loop is:
//     for (const byte of data) this.checksum = table[(this.checksum ^ byte) & 0xff] ^ (this.checksum >>> 8)
// i.e. for..of over a typed array + instance-field mutation — both are patterns
// sensitive to V8 codegen. A plain indexed loop over a local var is the fast path.
//
// Run under both node versions and diff. If the for..of / instance-field variants
// regress on 24 but "indexed + local" doesn't, the culprit is V8 13.6's codegen
// for those patterns — a specific, upstream-reportable finding.
//
//   node crc32-js.mjs [--seconds S] [--size BYTES]
//   node --no-maglev crc32-js.mjs
import { randomFillSync } from 'node:crypto';

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i >= 0 ? process.argv[i + 1] : d; };
const SECONDS = Number(arg('seconds', 3));
const SIZE = Number(arg('size', 128 * 1024 * 1024));
const MB = 1 << 20;

const buf = Buffer.allocUnsafe(SIZE);
randomFillSync(buf.subarray(0, Math.min(SIZE, 8 * MB)));

const TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; t[n] = c >>> 0; }
  return t;
})();

// (1) indexed loop, LOCAL accumulator — the V8-friendly pattern
function indexedLocal(b) {
  let crc = 0xffffffff;
  for (let i = 0; i < b.length; i++) crc = (TABLE[(crc ^ b[i]) & 0xff] ^ (crc >>> 8)) >>> 0;
  return (crc ^ 0xffffffff) >>> 0;
}
// (2) indexed loop, INSTANCE FIELD accumulator
class IndexedField {
  constructor() { this.crc = 0xffffffff; }
  update(b) { for (let i = 0; i < b.length; i++) this.crc = (TABLE[(this.crc ^ b[i]) & 0xff] ^ (this.crc >>> 8)) >>> 0; return this; }
}
// (3) for..of over bytes, INSTANCE FIELD — the actual @aws-crypto/crc32 pattern
class ForOfField {
  constructor() { this.crc = 0xffffffff; }
  update(b) { for (const byte of b) this.crc = (TABLE[(this.crc ^ byte) & 0xff] ^ (this.crc >>> 8)) >>> 0; return this; }
}

function bench(fn, seconds) {
  fn(); fn(); // warm up / tier up
  let bytes = 0;
  const t0 = performance.now();
  const end = t0 + seconds * 1000;
  while (performance.now() < end) { fn(); bytes += buf.length; }
  return bytes / MB / ((performance.now() - t0) / 1000);
}

// The real SDK impl, if resolvable (present on a full install).
let Crc32 = null;
for (const spec of ['@aws-crypto/crc32', '@aws-crypto/crc32/build/index.js']) {
  try { ({ Crc32 } = await import(spec)); if (Crc32) break; } catch { /* next */ }
}

console.log(`node ${process.version}  flags=[${process.execArgv.join(' ') || '(none)'}]  size=${(SIZE / MB).toFixed(0)}MiB`);
console.log(`(1) indexed + local var          \t${bench(() => indexedLocal(buf), SECONDS).toFixed(0)} MB/s`);
console.log(`(2) indexed + instance field     \t${bench(() => { new IndexedField().update(buf); }, SECONDS).toFixed(0)} MB/s`);
console.log(`(3) for..of + instance field     \t${bench(() => { new ForOfField().update(buf); }, SECONDS).toFixed(0)} MB/s  <- @aws-crypto/crc32 pattern`);
if (Crc32) {
  console.log(`(4) @aws-crypto/crc32 (real SDK) \t${bench(() => { const c = new Crc32(); c.update(buf); c.digest(); }, SECONDS).toFixed(0)} MB/s`);
} else {
  console.log('(4) @aws-crypto/crc32 (real SDK) \tnot resolvable here');
}
