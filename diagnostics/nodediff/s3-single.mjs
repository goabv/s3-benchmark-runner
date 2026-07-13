// Layer 4: real S3 GET of ONE part over a single connection — SDK on, but no
// worker threads, no ordered-stream, no pooling. Confirms whether the regression
// reproduces on the actual S3 transport with minimal machinery, and reports
// time-to-first-byte (handshake/latency) separately from streaming throughput.
//
//   node s3-single.mjs [--key K] [--part N] [--iters M] [--handler node|undici]
//                      [--cipher aes128|aes256|...] [--no-checksum]
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { makeClient } from '../../src/s3.js';
import { loadFileConfig, sectionValue, parseSizeSpec, keyForSize, resolveCiphers } from '../../src/config.js';

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i >= 0 ? process.argv[i + 1] : d; };
const has = (n) => process.argv.includes('--' + n);

const cfg = loadFileConfig();
const region = arg('region', cfg.region);
const bucket = arg('bucket', cfg.bucket);
const sizes = sectionValue(cfg, 'download', 'sizes') || cfg.sizes || ['30GiB'];
const first = parseSizeSpec(sizes[0]);
const key = arg('key', keyForSize(cfg.dataPrefix || '', first.label, 0, first.count));
const part = Number(arg('part', 1));
const iters = Number(arg('iters', 5));
const handler = arg('handler', sectionValue(cfg, 'download', 'httpHandler') || 'node');
const cipher = arg('cipher', null);
const validateChecksum = !has('no-checksum');

const MIB = 1 << 20;
const client = makeClient({
  region, httpHandler: handler, validateChecksum,
  ciphers: resolveCiphers(cipher), onTls: (i) => { negotiated = `${i.protocol}/${i.cipher}`; },
});
let negotiated = null;

const mibps = [];
const ttfbs = [];
for (let i = 0; i < iters; i++) {
  const t0 = performance.now();
  const res = await client.send(new GetObjectCommand({
    Bucket: bucket, Key: key, PartNumber: part,
    ...(validateChecksum ? { ChecksumMode: 'ENABLED' } : {}),
  }));
  let bytes = 0;
  let ttfb = null;
  for await (const chunk of res.Body) {
    if (ttfb === null) ttfb = performance.now() - t0;
    bytes += chunk.length;
  }
  const secs = (performance.now() - t0) / 1000;
  mibps.push(bytes / MIB / secs);
  ttfbs.push(ttfb);
}
client.destroy();

const med = (a) => [...a].sort((x, y) => x - y)[Math.floor(a.length / 2)];
console.log(
  `s3-single key=${key} part=${part} handler=${handler}\t` +
    `median ${med(mibps).toFixed(0)} MiB/s (${(med(mibps) * 8 / 1024).toFixed(2)} Gbps)\t` +
    `TTFB median ${med(ttfbs).toFixed(0)} ms\t${negotiated ?? ''}`,
);
