// Layer 1: pure OpenSSL bulk AEAD throughput — NO network, NO streams.
// Isolates the cipher speed that TLS record decryption depends on. If this
// regresses between node versions, the bundled OpenSSL is the cause.
import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';

const MB = 1 << 20;
const CHUNK = 1 * MB;
const SECONDS = Number(processArg('seconds', 3));

function processArg(name, def) {
  const i = process.argv.indexOf('--' + name);
  return i >= 0 ? process.argv[i + 1] : def;
}

// Bulk update throughput on a single cipher object (models TLS record crypto).
function bulk(make, seconds) {
  const buf = randomBytes(CHUNK);
  const c = make();
  let bytes = 0;
  const t0 = performance.now();
  const end = t0 + seconds * 1000;
  while (performance.now() < end) {
    c.update(buf);
    bytes += CHUNK;
  }
  try { c.final(); } catch { /* GCM final may need tag; ignore for bulk timing */ }
  const secs = (performance.now() - t0) / 1000;
  return bytes / MB / secs; // MB/s
}

for (const [algo, keyLen] of [['aes-128-gcm', 16], ['aes-256-gcm', 32], ['chacha20-poly1305', 32]]) {
  let enc = 0, dec = 0;
  try {
    const key = randomBytes(keyLen);
    const iv = randomBytes(12);
    enc = bulk(() => createCipheriv(algo, key, iv, { authTagLength: 16 }), SECONDS);
    dec = bulk(() => createDecipheriv(algo, key, iv, { authTagLength: 16 }), SECONDS);
  } catch (e) {
    console.log(`${algo}\tunsupported (${e.message})`);
    continue;
  }
  console.log(`${algo}\tencrypt ${enc.toFixed(0)} MB/s\tdecrypt ${dec.toFixed(0)} MB/s`);
}
