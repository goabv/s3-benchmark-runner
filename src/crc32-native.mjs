// SDK-layer patch: replace @aws-crypto/crc32's pure-JS CRC32 (a `for..of` +
// instance-field loop, several× slower than it needs to be, and heavier still on
// Node 24) with Node's native, hardware-accelerated `zlib.crc32`.
//
// This monkey-patches the `Crc32` class the SDK actually uses (via `AwsCrc32` in
// @aws-sdk/middleware-flexible-checksums), so every SDK GET/PUT that validates or
// computes a CRC32 benefits — no change to call sites. Only applies when
// `zlib.crc32` exists (Node >= 18) AND a runtime self-test confirms it produces
// identical results to the implementation it's replacing. Idempotent.
//
// Note: covers CRC32 only (zlib has no CRC32C). If the object uses CRC32C, this is
// a no-op and the SDK's JS crc32c path is used unchanged.
import zlib from 'node:zlib';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
let done = false;

export function installNativeCrc32() {
  if (done) return { patched: false, reason: 'already run' };
  done = true;

  if (typeof zlib.crc32 !== 'function') return { patched: false, reason: 'zlib.crc32 unavailable (Node < 18)' };

  let mod;
  try { mod = require('@aws-crypto/crc32'); } catch { return { patched: false, reason: '@aws-crypto/crc32 not resolvable' }; }
  const Crc32 = mod?.Crc32;
  if (!Crc32 || !Crc32.prototype || typeof Crc32.prototype.update !== 'function') {
    return { patched: false, reason: 'Crc32 not exported as expected' };
  }

  // Self-test against the ORIGINAL implementation (single-shot + streaming) before
  // replacing it — never apply a patch that would change checksum results.
  const a = Buffer.from('The quick brown fox');
  const b = Buffer.from(' jumps 0123456789\x00\xff');
  const origWhole = new Crc32().update(Buffer.concat([a, b])).digest() >>> 0;
  const origStream = new Crc32().update(a).update(b).digest() >>> 0;
  const nativeWhole = zlib.crc32(Buffer.concat([a, b]), 0) >>> 0;
  const nativeStream = zlib.crc32(b, zlib.crc32(a, 0)) >>> 0;
  if (origWhole !== nativeWhole || origStream !== nativeStream || origWhole !== origStream) {
    return { patched: false, reason: `self-test mismatch (orig=${origWhole} native=${nativeWhole})` };
  }

  // Replace the hot methods. zlib.crc32 seeds at 0 (crc of empty) and returns the
  // standard CRC32 directly, so accumulate with the running value.
  Crc32.prototype.update = function update(data) {
    if (!this.__nativeInit) { this.checksum = 0; this.__nativeInit = true; }
    this.checksum = zlib.crc32(data, this.checksum >>> 0);
    return this;
  };
  Crc32.prototype.digest = function digest() {
    return this.__nativeInit ? this.checksum >>> 0 : 0; // crc32 of empty === 0
  };

  return { patched: true };
}
