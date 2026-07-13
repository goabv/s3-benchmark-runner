# Node-version download regression rig

Isolates *why* one node version downloads slower than another by decomposing the
download hot path into independent layers and running each under both node
binaries — with no external-network noise for the decisive probes.

The download path, bottom to top, and the probe that isolates each:

| Layer | Probe | What it measures |
|-------|-------|------------------|
| Bulk AEAD crypto (OpenSSL) | `crypto-aesgcm.mjs` | AES-128/256-GCM + ChaCha20 encrypt/decrypt MB/s — no network, no streams |
| TLS record crypto + stream | `loopback.mjs --tls` | HTTPS receive throughput over loopback (TLS decrypt + HTTP parse + stream drain) |
| Plain stream / http / GC | `loopback.mjs` | Same over plain HTTP (no TLS) — isolates V8 / stream / parser / Buffer / GC |
| TLS handshake / connect | `loopback.mjs --tls --fresh` | Per-request new connection — isolates handshake cost |
| Real S3 transport (SDK) | `s3-single.mjs` | One part, single connection, no workers — TTFB + streaming MiB/s |
| Version fingerprint | `fingerprint.mjs` | node / v8 / openssl / uv / undici / llhttp versions |

## Run it

On the box, with both node versions available (e.g. via nvm):

```bash
cd diagnostics/nodediff
chmod +x run.sh
# offline layers only (recommended first):
./run.sh "$(nvm which 22)" "$(nvm which 24)"
# add the real single-connection S3 probe (needs a seeded object + network):
./run.sh "$(nvm which 22)" "$(nvm which 24)" --s3
```

`run.sh` pins the HTTPS probe to `TLS_AES_128_GCM_SHA256` so both versions negotiate
the same cipher (a different default would confound the result).

## Interpreting the diff

Compare the two version blocks line by line; the layer with the largest gap is the
culprit:

- **`crypto-aesgcm` regressed** → bundled **OpenSSL** bulk cipher (compare the
  `openssl` line in the fingerprints).
- **only HTTPS loopback regressed** (plain HTTP is equal) → **TLS layer** (OpenSSL
  record crypto or handshake), not JS.
- **plain HTTP loopback also regressed** → **V8 / stream / http-parser / GC /
  Buffer** path (a JS-side regression that affects all downloads).
- **only `https fresh` regressed** → **handshake/connect** cost (OpenSSL) — matters
  most for short-lived connections.
- **loopback fine but `s3-single` regressed** → something on the **S3 network path**
  (DNS, handshake to S3, congestion) or the **SDK** path, not node core.

## Extra knobs

- **Client path** — `loopback.mjs --client fetch` runs the receive over **global
  fetch = the bundled undici** (whose version changes across node releases), vs the
  default `--client node` (core `http`/`https`). If node http regresses but fetch
  doesn't, the regression is in **node core streams** and switching to the undici
  handler may sidestep it; if fetch regresses too, it's a shared/undici layer.
- **GC accounting** — add `--gc-stats` to any loopback run to print GC event count,
  total pause, and % of wall time. If the slower version shows markedly higher GC%,
  the regression is **GC-driven** (a V8 change); try `--max-semi-space-size=<MB>`
  (e.g. `NODE_OPTIONS=--max-semi-space-size=128`) to test.

The runner exercises both node and fetch clients (with `--gc-stats`) automatically.

## Notes

- The loopback server and client run in the same process (single event loop), so
  absolute numbers aren't line-rate — but the *ratio* between node versions is what
  matters, and both versions pay the same overhead.
- Bump `--duration`, `--conns`, `--size` for steadier numbers on a big box.
- `s3-single.mjs` reads bucket/region/sizes from `bench.config.json`; override with
  `--key`, `--part`, `--handler`, `--cipher`, `--no-checksum`.
- `--client fetch` disables TLS cert verification for the loopback self-signed cert
  (`NODE_TLS_REJECT_UNAUTHORIZED=0`) — diagnostic use only.
