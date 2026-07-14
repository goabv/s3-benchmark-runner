# Node-version download regression rig

Isolates *why* one node version downloads slower than another by decomposing the
download hot path into independent layers and running each under both node
binaries — with no external-network noise for the decisive probes.

The download path, bottom to top, and the probe that isolates each:

| Layer | Probe | What it measures |
|-------|-------|------------------|
| Bulk AEAD crypto (OpenSSL) | `crypto-aesgcm.mjs` | AES-128/256-GCM + ChaCha20 encrypt/decrypt MB/s — no network, no streams |
| **Pure-JS CRC32 loop** | `crc32-js.mjs` | The SDK's checksum-validation hot loop in isolation — MB/s; bisect with V8 flags |
| TLS record crypto + stream | `loopback.mjs --tls` | HTTPS receive throughput over loopback (TLS decrypt + HTTP parse + stream drain) |
| Plain stream / http / GC | `loopback.mjs` | Same over plain HTTP (no TLS) — isolates V8 / stream / parser / Buffer / GC |
| TLS handshake / connect | `loopback.mjs --tls --fresh` | Per-request new connection — isolates handshake cost |
| Real S3 transport (SDK) | `s3-single.mjs` | One part, single connection, no workers — TTFB + streaming MiB/s |
| Version fingerprint | `fingerprint.mjs` | node / v8 / openssl / uv / undici / llhttp versions |
| **Where the CPU goes** | `loopback.mjs --prof` | CPU-profiles the measured window and prints the **top self-time functions** — diff two node versions to name the regressed function |

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

## Why is checksum validation slower on Node 24? (`crc32-js.mjs`)

The download regression is dominated by the SDK's **pure-JS CRC32** checksum loop
(no hardware/native CRC — the SDK computes it in JavaScript). `crc32-js.mjs`
isolates that loop (no network/streams) so you can attribute the slowdown to V8
codegen and bisect it:

```bash
nvm use 22 && node crc32-js.mjs                 # baseline
nvm use 24 && node crc32-js.mjs                 # is it slower?
nvm use 24 && node --no-maglev crc32-js.mjs     # if this recovers -> Maglev codegen
nvm use 24 && node --trace-deopt crc32-js.mjs   # any deopt churn?
nvm use 24 && node --trace-opt   crc32-js.mjs   # which tier the loop reaches
```

If `--no-maglev` recovers Node 22-like MB/s, the culprit is V8 13.6's Maglev
mid-tier generating slower code for this loop — a specific, upstream-reportable
finding.

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

- **CPU profiling** — `loopback.mjs --prof [--prof-top N]` profiles the measured
  window and prints the top-N functions by self time. Run it under each node
  version and diff the tables — the function whose self-time share grew is where
  the regression lives:

  ```bash
  node loopback.mjs --conns 8 --duration 5 --prof            # node22
  node loopback.mjs --conns 8 --duration 5 --prof            # node24, then diff
  ```

## Profiling the real benchmark

The download hot path runs in the worker threads, so the benchmark profiles
*there*: `node src/benchmark.js --profile` (or `"profile": true`) writes one
`.cpuprofile` per worker to `results/profile-<nodeVersion>/` (per-version so runs
don't clobber). Summarize/diff with:

```bash
node scripts/prof-top.mjs results/profile-v22.23.1/dl-worker-0.cpuprofile
node scripts/prof-top.mjs results/profile-v24.18.0/dl-worker-0.cpuprofile
```

Use a **large** object (e.g. 30GiB) so the receive hot path dominates over
worker-startup/module-load noise (the worker profile includes startup).

### Profiling a full sweep under both node versions

`sweep-download.sh` takes a `PROFILE=1` env passthrough, so you can profile the
real download sweep end-to-end under each node without editing config:

```bash
nvm use 22 && PROFILE=1 ./scripts/sweep-download.sh    # -> results/profile-v22.x/
nvm use 24 && PROFILE=1 ./scripts/sweep-download.sh    # -> results/profile-v24.x/

# diff the hottest worker between versions:
node scripts/prof-top.mjs results/profile-v22.23.1/dl-worker-0.cpuprofile 30
node scripts/prof-top.mjs results/profile-v24.18.0/dl-worker-0.cpuprofile 30
```

The profile is taken in the **worker threads** (where the download runs); the seed
step isn't profiled. Profiles go to `results/profile-<nodeVersion>/` by default
(per-version, no clobber); set `PROFILE_DIR` to override. Windows: `$env:PROFILE=1`.

## Notes

- The loopback server and client run in the same process (single event loop), so
  absolute numbers aren't line-rate — but the *ratio* between node versions is what
  matters, and both versions pay the same overhead.
- Bump `--duration`, `--conns`, `--size` for steadier numbers on a big box.
- `s3-single.mjs` reads bucket/region/sizes from `bench.config.json`; override with
  `--key`, `--part`, `--handler`, `--cipher`, `--no-checksum`.
- `--client fetch` disables TLS cert verification for the loopback self-signed cert
  (`NODE_TLS_REJECT_UNAUTHORIZED=0`) — diagnostic use only.
