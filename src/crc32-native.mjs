// SDK-layer CRC32 native check/patch.
//
// Goal: make the SDK compute CRC32 with Node's native, hardware-accelerated
// `zlib.crc32` instead of a pure-JS byte loop.
//
// Reality for the SDK version this project pins (@smithy/core 3.29.2, which the
// AWS SDK v3 client uses via @aws-sdk/checksums): the SDK ALREADY does this. The
// CRC32 the checksum middleware uses resolves like so:
//
//   selectChecksumAlgorithmFunction
//     -> Crc32 from "@aws-sdk/checksums/crc"
//       -> Crc32 (= Crc32Node) from "@smithy/core/checksum"
//         -> Crc32Node = (typeof zlib.crc32 === 'function')
//              ? buildNativeClass(zlib.crc32)   // native, hardware-accelerated
//              : Crc32Js                         // pure-JS table loop fallback
//
// `zlib.crc32` exists on Node >= 22.2 (and >= 18 lines that backported it), so on
// the EC2 node versions here (22.23 / 24.18) the SDK is native out of the box.
//
// So this module's job is now mostly to CONFIRM that (and say so), and only fall
// back to monkey-patching for older SDKs that still ship the slow pure-JS
// `@aws-crypto/crc32` (a `for..of` + instance-field loop). Resolution is done with
// ESM `import()` so we inspect/patch the SAME module copy the ESM workers use
// (CJS `require` would resolve a different dist-cjs copy).
//
// CRC32 only. Returns:
//   { patched: true }                              -> legacy JS impl replaced
//   { patched: false, alreadyNative: true, ... }   -> SDK already native (the norm)
//   { patched: false, reason }                     -> nothing applicable
import zlib from 'node:zlib';

let done = false;
let cached = null;

export async function installNativeCrc32() {
  if (done) return cached;
  done = true;

  if (typeof zlib.crc32 !== 'function') {
    return (cached = { patched: false, reason: 'zlib.crc32 unavailable (Node < 18/22.2)' });
  }

  // 1) Modern SDK: inspect the checksum module the middleware actually imports.
  //    These export Crc32 (the active class), Crc32Node (native) and Crc32Js
  //    (pure-JS), so we can tell exactly which one is live by identity.
  const tried = [];
  for (const spec of ['@aws-sdk/checksums/crc', '@smithy/core/checksum']) {
    let mod;
    try {
      mod = await import(spec);
    } catch (e) {
      tried.push(`${spec}: ${e.code || e.message}`);
      continue;
    }
    const { Crc32, Crc32Node, Crc32Js } = mod;
    if (!Crc32) {
      tried.push(`${spec}: no Crc32 export`);
      continue;
    }
    const hasNative = Crc32Node && Crc32Js && Crc32Node !== Crc32Js;
    if (hasNative && Crc32 === Crc32Node) {
      return (cached = {
        patched: false,
        alreadyNative: true,
        reason: `SDK already uses native zlib.crc32 (Crc32Node via ${spec})`,
      });
    }
    // Native exists in the package but the active Crc32 isn't it (unusual). Nothing
    // to do — reassigning an ESM binding isn't possible and the JS fallback here is
    // already the fast indexed-table loop, not the slow @aws-crypto for..of.
    return (cached = {
      patched: false,
      reason: `active Crc32 is not the native class (via ${spec}); JS fallback in use`,
    });
  }

  // 2) Legacy SDK: still on the slow pure-JS @aws-crypto/crc32. Monkey-patch its
  //    prototype to use zlib.crc32, but only if a self-test proves byte-identical.
  let legacy;
  try {
    legacy = await import('@aws-crypto/crc32');
  } catch (e) {
    tried.push(`@aws-crypto/crc32: ${e.code || e.message}`);
    return (cached = { patched: false, reason: `no CRC32 module resolvable (${tried.join('; ')})` });
  }
  const Crc32 = legacy?.Crc32 ?? legacy?.default?.Crc32;
  if (!Crc32?.prototype || typeof Crc32.prototype.update !== 'function') {
    return (cached = { patched: false, reason: '@aws-crypto/crc32 Crc32 not exported as expected' });
  }

  const a = Buffer.from('The quick brown fox');
  const b = Buffer.from(' jumps 0123456789\x00\xff');
  const origWhole = new Crc32().update(Buffer.concat([a, b])).digest() >>> 0;
  const origStream = new Crc32().update(a).update(b).digest() >>> 0;
  const nativeWhole = zlib.crc32(Buffer.concat([a, b]), 0) >>> 0;
  const nativeStream = zlib.crc32(b, zlib.crc32(a, 0)) >>> 0;
  if (origWhole !== nativeWhole || origStream !== nativeStream || origWhole !== origStream) {
    return (cached = { patched: false, reason: `self-test mismatch (orig=${origWhole} native=${nativeWhole})` });
  }

  Crc32.prototype.update = function update(data) {
    if (!this.__nativeInit) { this.checksum = 0; this.__nativeInit = true; }
    this.checksum = zlib.crc32(data, this.checksum >>> 0);
    return this;
  };
  Crc32.prototype.digest = function digest() {
    return this.__nativeInit ? this.checksum >>> 0 : 0; // crc32 of empty === 0
  };
  return (cached = { patched: true, reason: 'patched legacy @aws-crypto/crc32 -> zlib.crc32' });
}
