// Reproduce the PER-CHUNK stream receive overhead that regressed on Node 24 — the
// real download does: body Readable -> (SDK ChecksumStream: update+push per chunk)
// -> `for await` consumer. This isolates that pattern (no network) so you can
// measure it across node versions and bisect with V8 flags.
//
//   node stream-chunks.mjs [--chunk BYTES] [--total BYTES] [--stages N] [--checksum]
//   node stream-chunks.mjs --chunk 65536  --stages 1 --checksum   # ~ real path
//   node stream-chunks.mjs --chunk 1048576 --stages 1 --checksum  # larger reads (mitigation test)
//   node --no-maglev stream-chunks.mjs --chunk 65536 --stages 1 --checksum
//
// --chunk     bytes per chunk (models the socket read size; default 64 KiB)
// --stages    number of pass-through Transform stages (models the ChecksumStream
//             wrapper; 1 ~= checksum on, 0 = raw body straight to consumer)
// --checksum  run a CRC32 update per chunk inside each stage (adds the checksum work)
import { Readable, Transform } from 'node:stream';

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i >= 0 ? process.argv[i + 1] : d; };
const has = (n) => process.argv.includes('--' + n);

const CHUNK = Number(arg('chunk', 65536));
const TOTAL = Number(arg('total', 2 * 1024 * 1024 * 1024));
const STAGES = Number(arg('stages', 1));
const CHECKSUM = has('checksum');
const MB = 1 << 20;

const buf = Buffer.allocUnsafe(CHUNK);

const TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; t[n] = c >>> 0; }
  return t;
})();

function makeSource(nChunks) {
  let sent = 0;
  return new Readable({
    highWaterMark: CHUNK,
    read() {
      while (sent < nChunks) { sent++; if (!this.push(buf)) return; }
      this.push(null);
    },
  });
}

function makeStage() {
  let crc = 0xffffffff;
  return new Transform({
    highWaterMark: CHUNK,
    transform(chunk, _enc, cb) {
      if (CHECKSUM) { for (let i = 0; i < chunk.length; i++) crc = (TABLE[(crc ^ chunk[i]) & 0xff] ^ (crc >>> 8)) >>> 0; }
      cb(null, chunk); // push downstream (this is the per-chunk onSourceData-style hop)
    },
  });
}

async function run(totalBytes) {
  const nChunks = Math.floor(totalBytes / CHUNK);
  let s = makeSource(nChunks);
  for (let i = 0; i < STAGES; i++) s = s.pipe(makeStage());
  let bytes = 0;
  const t0 = performance.now();
  for await (const chunk of s) bytes += chunk.length; // consumer, like the worker's for-await
  const secs = (performance.now() - t0) / 1000;
  return { secs, bytes, nChunks };
}

await run(256 * MB); // warm up (let V8 tier the pipeline up)
const { secs, bytes, nChunks } = await run(TOTAL);

console.log(`node ${process.version}  flags=[${process.execArgv.join(' ') || '(none)'}]  chunk=${(CHUNK / 1024).toFixed(0)}KiB  stages=${STAGES}  checksum=${CHECKSUM}`);
console.log(`${(bytes / MB / secs).toFixed(0)} MB/s\t${(nChunks / secs / 1000).toFixed(0)}K chunks/s\t(${(bytes / (1 << 30)).toFixed(2)} GiB in ${secs.toFixed(1)}s)`);
