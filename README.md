# S3 Benchmark Runner — AWS SDK for JavaScript v3

Measures parallel download throughput from S3 using **worker threads**, where each
worker downloads objects **by `PartNumber`** on top of `@aws-sdk/client-s3`.
Requesting a part by number (rather than an arbitrary byte range) aligns every
request to the object's original multipart-upload boundaries and makes S3 return
that part's stored **CRC32** checksum — which the SDK validates automatically as
the body is read. Reports throughput (MiB/s and Gbps) per object.

## What it does

- HEADs the object to discover its `PartsCount` (from the multipart upload).
- Distributes part numbers `1..N` round-robin across `workers` worker threads.
- Each worker runs up to `concurrency` `PartNumber` GETs in parallel with
  `ChecksumMode: ENABLED`, draining (or optionally keeping) the bodies.
- The `med`/`best Gbps` window is the part transfer only (workers ready → all parts
  fetched and drained; for `ordered-stream`, until the consumer finishes the stream).
  Worker spawn / client init are excluded. An extra `e2e Gbps` column reports the
  same transfer plus **every recurring per-call planning cost**: `HeadObject`
  (`describeMs`), `buildParts` (part-boundary computation), and `assignParts`/queue-sort
  (dispatch planning) — i.e. "download call → drained," to line up with an SDK public
  API that does all of this inside one call. Genuinely one-time costs (worker-thread
  spawn, client init, data generation) stay excluded, since a warm SDK transfer manager
  does not repay them per call. The footer prints the breakdown. On in-region hosts these
  helpers are a few ms so `e2e ≈ transfer`; they matter most for small objects.
- Runs `warmup` unmeasured iterations, then `iterations` measured ones, and
  reports median + best, plus how many parts were checksum-validated.

(All of these — `workers`, `concurrency`, `warmup`, `iterations`, … — are keys in
`bench.config.json`; see [Configuration](#configuration-single-source-of-truth).)

Connections are tuned for throughput: keep-alive agents with a high `maxSockets`,
and one `S3Client` per worker (clients are never shared across threads).

## Layout

```
bench.config.json       # single source of truth for all settings
src/
  benchmark.js          # download orchestrator (worker_threads + timing/reporting)
  download-worker.js    # download worker: PartNumber GETs, delivery modes, stall-retry
  upload-benchmark.js   # upload orchestrator (multipart, worker_threads)
  upload-worker.js      # upload worker: parallel UploadPart
  upload-test-data.js   # seed a bucket with multipart objects (CRC32 per part)
  s3.js                 # S3Client factory (agents, DNS spread, cipher, socket capture)
  config.js             # config-file + CLI parsing, size/throughput helpers
  resource-monitor.js   # peak/avg RSS + CPU% + MEM% sampling
  ip-throughput.js      # per-IP throughput tracking + JSONL history
  plot.js               # dependency-free SVG plot for the ordered-stream time series
scripts/
  sweep-download.sh     # EC2: seed + benchmark download across the size curve
  sweep-upload.sh       # EC2: benchmark multipart upload across the size curve
  sweep-download.ps1    # Windows equivalents
  sweep-upload.ps1
  push.ps1              # Windows: aws s3 sync project -> S3 staging prefix
  pull.sh               # EC2:     aws s3 sync S3 staging prefix -> box + npm install
  tune-network.sh       # EC2: TCP/network kernel tuning (+ --revert)
test/
  ordered-backpressure.test.mjs   # ordered-stream backpressure regression test
```

## Prerequisites

- Node.js >= 18 (tested on 24).
- AWS credentials. Locally use a profile/env; on EC2 prefer an **instance role**
  with `s3:GetObject` (and `s3:PutObject` if you seed data from the box).

## Configuration (single source of truth)

All settings live in **`bench.config.json`** at the project root. Both Node tools
(`benchmark.js`, `upload-benchmark.js`, `upload-test-data.js`) and every sweep
script read it, so you edit one file to change everything. You normally run the
benchmarks with **no arguments** — the config is the interface. CLI flags and
sweep env vars exist only for ad-hoc overrides (see the [CLI appendix](#appendix-cli-reference)).

The file has three parts:
- **shared top-level keys** — apply to both directions (bucket, region, sizes, …);
- a **`download`** section and an **`upload`** section — per-direction knobs.

Resolution precedence for any single setting:

```
CLI flag  >  matching section (download/upload)  >  shared top-level  >  built-in default
```

So a key set inside `download` overrides the same key at the top level, and a CLI
flag overrides both. This is how you give, say, upload a different `concurrency`
than download, or share `region` once at the top.

A representative config (this repo's current one):

```json
{
  "bucket": "s3dl-bench-usw2-801400661003",
  "region": "us-west-2",
  "dataPrefix": "bench/",
  "codePrefix": "code/",
  "sizes": [{ "size": "30GiB", "count": 10 }],
  "partSize": "32MiB",
  "checksum": "CRC32",

  "download": {
    "workers": 64,
    "concurrency": 4,
    "iterations": 1,
    "warmup": 1,
    "keep": false,
    "validateChecksum": true,
    "deliveryMode": "ordered-drop",
    "maxBufferedBytes": "64GiB",
    "bufferPool": true,
    "timeseries": false,
    "partTimes": true,
    "stallTimeoutMs": 10000,
    "partRetries": 8,
    "httpHandler": "undici",
    "spreadConnections": true,
    "tls": true,
    "ipThroughput": false,
    "ipThroughputSizes": [],
    "ipThroughputFile": "results/ip-throughput.jsonl"
  },

  "upload": {
    "workers": 64,
    "concurrency": 4,
    "iterations": 1,
    "warmup": 1,
    "forceUpload": false,
    "uploadSource": "memory",
    "spreadConnections": false,
    "ipThroughputSizes": []
  }
}
```

### Shared keys (top-level)

| Key | Example | What it controls and why |
|-----|---------|--------------------------|
| `bucket` | `s3dl-bench-usw2-…` | Target S3 bucket for both seeding and benchmarking. Set once so no tool needs `--bucket`. |
| `region` | `us-west-2` | AWS region — **must** match the bucket. Always benchmark in-region; there's no implicit default region. |
| `dataPrefix` | `bench/` | Key prefix for test objects. `sizes` expand to `<dataPrefix><size>.bin` (or `-<i>.bin` for multi-file), so the seeder and both benchmarks agree on keys. |
| `codePrefix` | `code/` | S3 staging prefix used by `push.ps1`/`pull.sh` for the code-sync loop. Not read by the benchmarks themselves. |
| `sizes` | `[{ "size":"30GiB","count":10 }]` | The workload. Each entry is an object size and how many **distinct files** of it to use; all files of a size are pooled into one timed run. More files = more objects/connections = easier NIC saturation. Also accepts strings: `"30GiB"` or `"30GiB:10"`. |
| `partSize` | `32MiB` | Multipart part size **at upload time** — this defines the boundaries the download reads back by `PartNumber`. Smaller parts → more parts → finer parallelism, smaller per-part memory, and (ordered-stream) smaller overshoot. Change it and re-seed to experiment with boundaries. |
| `checksum` | `CRC32` | Per-part checksum algorithm used when seeding/uploading. `CRC32` uses Node's native `zlib.crc32` (fast); `CRC32C` is pure-JS and much slower at high throughput; `SHA256`/`SHA1` also available. |

### `download` section

| Key | Example | What it controls and why |
|-----|---------|--------------------------|
| `workers` | `64` | Worker **threads** — real CPU parallelism (one per vCPU is a good start). TLS decrypt, checksum, and buffer copies are CPU-bound and only parallelize across threads. |
| `concurrency` | `4` | Async in-flight `PartNumber` GETs **per worker** (event-loop I/O overlap). Total in-flight connections = `workers × concurrency`. Raise to hide network latency; too high oversubscribes one core and self-throttles. |
| `iterations` | `1` | Measured runs per size. More iterations tighten the median (a 300 GiB run is long, hence 1 here). |
| `warmup` | `1` | Unmeasured runs before timing. Primes JIT/describe; note workers/connections are recreated each iteration, so it does not pre-warm the measured sockets. |
| `keep` | `false` | If `true`, retain downloaded bytes in memory instead of discarding. Leave `false` to measure the pure network/CPU ceiling without buffering cost. |
| `validateChecksum` | `true` | Whether the SDK validates each part's stored checksum as it streams. Set `false` to isolate raw transfer throughput from the checksum CPU cost. |
| `deliveryMode` | `ordered-drop` | What happens to bytes: `discard` (drain + drop on arrival — pure ceiling, no ordering), `ordered-drop` (reorder buffer, delivered in part order then dropped at the frontier — models the ordering machinery with no consumer), `ordered-stream` (reorder, then transfer each part zero-copy into a per-object `Readable` a consumer drains — models a real user reading a stream per object), or `file` (positional write to disk). See [Delivery modes](#delivery-modes-what-happens-to-downloaded-bytes). |
| `api` | `true` (default) | Route the download through the `S3TransferManager` API — **this is the default and recommended path**. The pool is constructed **once** (spawn + client init = one-time, reported separately as "pool spawn"); each iteration fires **x concurrent `download()` calls** (one per object) and drains the returned per-object `Readable`s **concurrently**. The measured window is the full "`download()` call → streams drained", so `med`/`best`/`e2e` all include HeadObject + planning — the cleanest apples-to-apples with a warm transfer manager. Delivery is always per-object streams; `deliveryMode` is ignored. Set `api:false` (or `--no-api`) to fall back to the legacy `deliveryMode` run loop (`discard`/`ordered-drop`/`ordered-stream`/`file`). See [Download API](#download-api-s3transfermanager). |
| `maxBufferedBytes` | `64GiB` | **ordered modes only.** Cap on the completed-but-undelivered reorder backlog; dispatch throttles above it (with one-part liveness so it can't hang). Bigger = keeps full concurrency at higher memory; smaller = tighter RSS but can throttle behind a slow low part. |
| `bufferPool` | `true` | **ordered-drop only.** Copy each part into a reused contiguous buffer instead of retaining raw chunk arrays. Trades a memcpy for far less GC pressure and flat RSS. (`ordered-stream` transfers dedicated buffers instead, so this is a no-op there.) See [Buffer pool](#buffer-pool-ordered-drop-memory-strategy-bufferpool). |
| `consumerRate` | `0` | **ordered-stream only.** Throttle the consumer to this many bytes/sec per object (e.g. `500MiB`), to model a slow reader and exercise backpressure end-to-end. `0` = unlimited. |
| `bufferReturn` | `true` | **ordered-stream only.** After the consumer reads a part, transfer its buffer back to the owning worker for reuse, so a bounded set of buffers ping-pongs across the thread boundary (zero-copy both ways) instead of allocating one per part. |
| `streamHwm` | `2 × partSize` | **ordered-stream only.** Per-object stream highWaterMark, applied to both the object `Readable` and its consumer sink. It's a soft backpressure threshold (not a hard cap): bigger lets more in-order parts flush before the object pauses, at the cost of more resident memory per active object. Omit (or `0`) for the `2 × partSize` auto-default. |
| `timeseries` | `false` | **ordered modes only.** Sample RSS / buffered parts / in-flight / CPU every 500 ms → CSV + SVG plot. Off for normal runs. |
| `partTimes` | `true` | Record every part's download time (+ serving `vip`/`conn_id`) to CSV and print p50/p90/p99/p99.9 latency. Also enables the socket-capture middleware. All delivery modes. |
| `stallTimeoutMs` | `10000` | Abort + re-fetch a part that reads **no bytes** for this long (a stuck connection blocking in-order delivery). `0` disables the watchdog. |
| `partRetries` | `8` | Max stall-retries per part before the run fails. Raise if your network produces frequent stalls (e.g. the plaintext-HTTP path). |
| `httpHandler` | `undici` | HTTP handler: `node` (`@smithy/node-http-handler`) or `undici` (`@smithy/undici-http-handler`). undici has a leaner per-request path and often drains sockets faster. Note: `vip`/`conn_id` capture only works on `node`. |
| `spreadConnections` | `true` | Custom DNS round-robin across all resolved S3 front-end IPs instead of Node's single-IP lookup. Prevents connections piling onto one front-end and capping throughput. |
| `tls` | `true` | `true` = HTTPS; `false` = S3's plaintext HTTP endpoint (measures TLS overhead; test buckets only). |
| `cipher` | *(unset)* | Pin the TLS suite: `aes128` / `aes256` / `chacha20` / a raw OpenSSL string. Unset = let S3 choose. Graviton3 does AES-128-GCM ~20% cheaper than AES-256. See [Pinning the TLS cipher](#pinning-the-tls-cipher-cipher). |
| `ipThroughput` | `false` | Record per-IP throughput for **every** size this run. |
| `ipThroughputSizes` | `[]` | Record per-IP throughput only for these size labels, e.g. `["30GiB"]`. |
| `ipThroughputFile` | `results/ip-throughput.jsonl` | Append-only JSONL history of per-IP throughput across runs. |

### `upload` section

| Key | Example | What it controls and why |
|-----|---------|--------------------------|
| `workers` | `64` | Worker threads running parallel `UploadPart` calls. |
| `concurrency` | `4` | Concurrent `UploadPart`s per worker. Total in-flight = `workers × concurrency`. |
| `iterations` | `1` | Measured upload runs per size. |
| `warmup` | `1` | Unmeasured priming run. |
| `forceUpload` | `false` | If `true`, upload even when a matching object already exists; otherwise skip objects whose size **and** part size already match. |
| `api` | `true` (default) | Route the upload through the `S3TransferManager` API — **the default and recommended path**. The uploader pool is constructed **once** (spawn + client init = one-time, reported separately); each iteration fires **x concurrent `upload()` calls** (one per object), each fed a customer `Readable` from **main** (a synthetic stream here). The manager carves each stream into parts and fans them out to the pool, running the full `CreateMPU → UploadPart → CompleteMPU` per object. `uploadSource` is ignored on this path. Set `api:false` / `--no-api` for the legacy `uploadSource` loop. |
| `uploadSource` | `memory` | Where part bytes come from: `memory` (one object-sized `SharedArrayBuffer` per object, filled once; parts are zero-copy views — resident memory = sum of object sizes, preflight-guarded), `file` (read each part from disk inline — measures disk + upload), `stream` (the customer hands one `Readable` per object to main; main carves + transfers parts to a pool of uploader threads), `open` (the customer passes a re-openable source descriptor; each worker opens its own stream for its part range — distributes ingress across cores), or `open-stream` (carver threads open whole-object streams, a separate uploader pool sends). See the Data source section below. |
| `uploadOpen` | `{ "type": "file" }` | **open / open-stream only.** The source descriptor: `{ "type": "file" }` (read the shared source file), `{ "type": "memory" }` (generate bytes in-memory on each worker — no disk, fastest ingress). Fixed built-in openers only. |
| `uploadCarvers` | `0` (auto) | **open-stream only.** Number of carver threads. `0` = one per object; set lower to put multiple objects on each carver (they're carved sequentially). Carvers + uploaders (`workers`) are separate pools, so total threads ≈ `carvers + workers`. |
| `uploadMaxBuffered` | `0` (auto) | **stream source only.** Cap (bytes) on carved-but-not-yet-uploaded parts held on main (the dispatch queue). Bounds memory and gives the uploader pool a surplus to pull from; when full, main pauses reading the customer stream (backpressure). `0` = auto (`workers × concurrency + 1` parts). |
| `uploadClientRate` | `0` | **stream source only.** Throttle the simulated customer stream to this many bytes/sec per object (models client send rate). `0` = as fast as possible. |
| `uploadClientChunk` | `1MiB` | **stream source only.** Chunk size the simulated customer stream pushes. Smaller = more realistic client behavior but more per-part event-loop churn on the single-threaded main ingress; larger reduces that overhead. Useful for measuring how much of the ingress ceiling is stream machinery vs. the raw per-byte memcpy. |
| `spreadConnections` | `false` | Same DNS-spreading as download, per-direction. |
| `ipThroughputSizes` | `[]` | Per-IP throughput recording for upload, gated by size label. |

The `upload` section can also carry `partSize`, `checksum`, `tls`, `httpHandler`,
and `cipher`; if omitted it inherits `partSize`/`checksum` from the shared keys and
falls back to built-in defaults for the rest (notably `httpHandler` defaults to
`node`, so set it explicitly if you want upload on undici too).

> Note: `scripts/pull.sh` is the one exception to "config is the source of truth" —
> it bootstraps the very first sync before the config exists on the box, so it keeps
> its own baked-in bucket/prefix/region defaults (edit the block at the top of that
> script if they change).

## Running the benchmarks

The primary way to run is the **sweep scripts** — arg-free wrappers that read
`bench.config.json` and drive a full size curve, writing timestamped JSON to
`results/`. Run them **on the EC2 instance, in-region** (a 30 GiB transfer from a
laptop is meaningless).

```bash
npm install               # once, on the box (pull.sh does this for you)
```

**Download sweep** — seeds the configured sizes (skipping objects that already
exist at the right size + part size), then benchmarks download:

```bash
./scripts/sweep-download.sh                          # -> results/download-sweep-<ts>.json
WORKERS=16 CONCURRENCY=8 ./scripts/sweep-download.sh  # ad-hoc tunable overrides
```

**Upload sweep** — benchmarks multipart upload across the sizes. It **forces**
re-upload by default (re-uploading is the point of an upload sweep):

```bash
./scripts/sweep-upload.sh                             # -> results/upload-sweep-<ts>.json
FORCE=0 ./scripts/sweep-upload.sh                     # respect config forceUpload instead
```

> WARNING: the upload sweep re-uploads every configured size each iteration
> (e.g. 30 GiB × count × iterations). Trim `sizes` for a quick check first.

Both scripts accept these override env vars: `WORKERS`, `CONCURRENCY`,
`ITERATIONS`, `WARMUP`, `PART_SIZE` — everything else comes from the config. They
also export `UV_THREADPOOL_SIZE=64` by default (raise libuv's 4-thread pool so
async file writes / DNS lookups don't serialize); override with
`UV_THREADPOOL_SIZE=128 ./scripts/sweep-download.sh`. The download run header echoes
the active file-write strategy, e.g. `delivery=file (writes async,
UV_THREADPOOL_SIZE=64)` or `(writes sync/blocking)`, so you can confirm at a glance.

Windows equivalents (for local smoke tests; real numbers need the EC2 box):

```powershell
.\scripts\sweep-download.ps1
.\scripts\sweep-upload.ps1 -Workers 16 -Concurrency 8   # add -NoForce to respect config
```

You can also invoke the tools directly (they're arg-free once the config is
populated) if you want to bypass the sweep wrapper:

```bash
node src/benchmark.js         # download, entirely from bench.config.json
node src/upload-benchmark.js  # upload,   entirely from bench.config.json
```

Any setting can be overridden per-run with a CLI flag, but that's purely for
ad-hoc experiments — see the [CLI appendix](#appendix-cli-reference). Everything in
the sections below is described in terms of the `bench.config.json` key you set.

## Multiple files per size

Each `sizes` entry can specify how many distinct files of that size to
upload/benchmark. Set it in `bench.config.json` as an object with a `count`, or as
a `"<size>:<count>"` string:

```json
"sizes": [
  { "size": "1GiB", "count": 4 },
  { "size": "30GiB", "count": 2 }
]
// equivalently: "sizes": ["1GiB:4", "30GiB:2"]
```

All files of a size are handled **together** — their parts are pooled across the
worker pool and timed as one run, with aggregate throughput reported. This is useful
for saturating the NIC (more objects = more connections, spread across S3 keys).
The sweep scripts seed and benchmark exactly what `sizes` describes.

Keys are `<dataPrefix><size>.bin` for count 1, or `<dataPrefix><size>-<i>.bin` for
count >1 (e.g. `bench/1gib-0.bin` … `bench/1gib-3.bin`). The download sweep seeds
the matching files automatically; the **download benchmark fails** if a required
file is missing:

```
[error] 30GiB: missing object bench/30gib-1.bin (need 2 file(s) of 30GiB; seed them first)
```

## Seeding & re-seeding

`./scripts/sweep-download.sh` seeds automatically before benchmarking (there's also
a standalone `node src/upload-test-data.js`, arg-free, that reads the config).
Seeding **skips** an object only when it already exists at **both** the expected
total size and the configured `partSize`. So if you change `partSize` in
`bench.config.json`, the next seed **re-uploads** the affected objects with the new
boundaries — you can experiment with part sizes just by editing the config and
re-running the sweep. Set `"forceUpload": true` (in the `upload` section) to always
re-upload regardless.

## Upload benchmark

`src/upload-benchmark.js` (driven by `./scripts/sweep-upload.sh`) measures **upload**
throughput with the same worker-thread design as the download side: it creates a
multipart upload, uploads all parts in parallel across workers (each doing up to
`concurrency` `UploadPart` calls), then completes it. It reads the `upload` section
plus shared keys (`sizes`, `partSize`, `checksum`, …) from `bench.config.json`.

Worker spawn + data generation happen first, **untimed** (the clock starts only
once every worker is ready). The measured window then spans the whole multipart
lifecycle: `CreateMultipartUpload` → parallel `UploadPart` → `CompleteMultipartUpload`,
so the reported throughput is **end-to-end** for the upload (create + all parts +
complete), not just the part transfer.

**Data source** — set `uploadSource` in the `upload` section:

```json
"upload": { "uploadSource": "memory" }   // or "file" | "stream" | "open" | "open-stream"
```

| `uploadSource` | What it does | Measures |
|--------|--------------|----------|
| `memory` (default) | Allocate one **object-sized `SharedArrayBuffer` per object** up front (untimed), random-filled once and shared across the whole worker pool. Every part is a **zero-copy view** into its object's buffer | Pure network + CPU (checksum) upload cost. **Resident memory = sum of all object sizes**, so it only fits when that sum is under box RAM (preflight-guarded; fails fast otherwise) |
| `file` | Read each part from a local file during upload, inline with the send loop (a blocking `readSync`, so the worker's event loop stalls during each read) | Disk read + upload, serialized |
| `stream` | The customer hands one `Readable` per object to **main**; main reads it, carves + fills part buffers, and **transfers** each part (zero-copy) to a pool of **uploader worker threads** that `UploadPart` in parallel, out of order | Transfer-Manager-style streaming upload: single-thread ingress + fanned-out parallel upload |
| `open` | The customer passes a re-openable source **descriptor** (factory pattern), not a live stream; **each worker opens its own stream** for its part's byte range and uploads it | Distributed ingress — reading is fanned across worker threads (no single-thread main funnel), for range-addressable sources |
| `open-stream` | Two tiers: **carver** threads each open a *whole-object* stream (via the opener callback, one object per stream) and carve parts; a separate **uploader** pool does the `UploadPart`s. Parts flow carver → main → uploader by zero-copy transfer, with credit-based backpressure | Carving (ingress) and uploading run on separate thread pools; ingress parallelizes across objects without needing a range-addressable source |

**Stream source (`stream`) — a Transfer-Manager-style API where the customer only touches the main thread.** The customer hands one `Readable` per object to main; internally we fan the upload across worker threads without the customer ever seeing them:

- **Main carves.** A producer per object reads the (here, synthetic) customer stream, copies each part's bytes into a recycled buffer (the ingress fill — bytes flow through main as a client would send them), numbers it, and enqueues it. A `Readable` can't cross the worker boundary, so this ingress is necessarily single-threaded on main.
- **Fan-out to an uploader pool.** Main transfers each carved part (zero-copy `postMessage`) to an idle lane across the uploader worker pool; any worker uploads any object's parts, out of order (S3 assembles by `PartNumber`). Workers transfer the freed buffer back for reuse (return-credit).
- **Backpressure.** A bounded pool of recycled buffers (cap = `uploadMaxBuffered`) gates everything: when main can't get a free buffer it stops reading the customer stream, which throttles the client. Bounded memory end-to-end.
- **`CompleteMultipartUpload`** per object once its stream ends and all its parts are acked.
- Optional `uploadClientRate` throttles the simulated client's send rate.

Honest ceiling: the ingress (read + fill) is single-threaded on main (~one core's memcpy), so a *single* stream is capped there regardless of how many uploader workers there are — you can hide the fan-out behind main, but not beat single-thread ingress for one stream. Parallelism comes from multiple concurrent object streams. Each part is still sent as a materialized `Buffer` (header checksum, same wire format as `memory`/`file`).

**Open source (`open`) — the factory pattern that distributes ingress.** The `stream` ceiling exists because a live `Readable` can't cross the worker boundary, so main is the sole ingress thread. The `open` source removes that limit: instead of a live stream, the customer passes a **re-openable descriptor** (data, not a closure), and **each worker opens its own stream** for its assigned part ranges — so reading is fanned across worker threads, no main funnel.

```json
"upload": { "uploadSource": "open", "uploadOpen": { "type": "file" } }
```

- **file opener** (`{ "type": "file" }`, the default): a shared source file is written up front (untimed), and each worker opens `fs.createReadStream(path, { start, end })` for each of its parts' byte ranges, drains it, and uploads. Because a file is range-addressable, one object's ingress spans all workers.
- **memory opener** (`{ "type": "memory" }`, or `--open-type memory`): each worker generates its part's bytes in-memory (no disk) from a reused template — fastest ingress.

(These are the only two built-in openers — fixed params, no custom callback module for now.)

The requirement is that the source be **re-openable from a reference on the worker's thread** (a path, URL, key + byte range). That's what lets ingress parallelize — unlike a live push stream, which is stuck on main (`stream` mode). Use `open` to lift the single-thread ingress ceiling when the source is seekable; use `stream` when the customer genuinely hands you one live stream.

**Open-stream source (`open-stream`) — two tiers: carvers + uploaders.** Where `open` opens a *ranged* read per part (needs a seekable source), `open-stream` handles a source that's only *sequential per object*: **carver** threads each open one whole-object `Readable` (the built-in `file` or `memory` opener), read it front-to-back, and carve part buffers; a separate **uploader** pool does the `UploadPart`s.

```json
"upload": { "uploadSource": "open-stream", "uploadCarvers": 4, "uploadOpen": { "type": "memory" } }
```

- Parts flow **carver → main → uploader** by zero-copy transfer; main brokers and, when a part finishes uploading, transfers the freed buffer **back to its carver** with an `ack`. That ack is also a **credit**: a carver never has more than `uploadMaxBuffered / carvers` parts outstanding, so it pauses reading its stream when the uploader pool falls behind (end-to-end backpressure, bounded memory).
- **Carving and uploading are separate thread pools**, so per-object sequential ingress (carving) doesn't block the parallel `UploadPart`s. Ingress parallelizes **across objects** (one carver per object by default).
- `uploadCarvers` consolidates objects onto fewer carver threads (each carves its objects sequentially), so you don't spawn a thread per object when you have many. Total threads ≈ `carvers + workers`.

This is the model for "the customer hands us a stream we can re-open per object but not range-address" — e.g. a per-object generator or a non-seekable-but-reopenable source. A genuinely live single push stream still belongs in `stream` mode (main-carve).

- **memory**: the random buffer is allocated + filled once per worker before the
  worker signals ready, so buffer-creation time is excluded from the measured
  window. Memory ≈ `workers × partSize`.
- **file**: a random source file of the object size is written to `sourcePath`
  (default OS temp) before timing (reported as `[setup] ...`), then each part is
  read from disk during the timed upload and the file is deleted afterward. Point
  `sourcePath` at a fast disk so storage doesn't cap the result.
- Parts are uploaded with the configured `checksum` algorithm, so the completed
  object has per-part checksums and doubles as **seed data** for the download
  benchmark.
- By default it **skips** a size whose object already exists at the matching total
  size and part size (so you don't re-push 30 GiB just to run downloads). Set
  `"forceUpload": true` in the `upload` section to benchmark upload anyway (the
  upload sweep forces this by default).

Both sweeps write timestamped JSON to `results/`, so to compare upload vs download
for a size, just run `sweep-upload.sh` and `sweep-download.sh` and diff the files.

## Getting the code onto EC2 (seamless loop)

Since the EC2 instance already has AWS access, the simplest transfer loop is
`aws s3 sync` through a staging prefix — no SSH keys or git auth to manage.

**On your Windows machine**, push after each change (arg-free — `push.ps1` reads
`bench.config.json` for bucket/prefix/region):

```powershell
.\scripts\push.ps1
```

**On the EC2 instance**, pull and install (`pull.sh` uses its baked-in
bucket/prefix/region defaults — it runs before the config exists on the box):

```bash
./scripts/pull.sh
# lands in ~/s3-bench (override dir with BENCH_DIR=/path ./scripts/pull.sh)
```

Both use `aws s3 sync --delete`, so only changed files move and deletions
propagate. `node_modules/` and `.git/` are excluded; the pull step runs
`npm install` on the box.

## Saving & committing benchmark runs

Code is authored on your dev box and synced to EC2; **results are generated on
EC2**. To get a run into git cleanly, use the capture + pull loop — nothing gets
committed by hand and each run is self-describing.

**1. On EC2 — run and capture** with `scripts/run.sh`. It bundles a self-contained,
committable run under `results/runs/<timestamp>[-label]/` (config snapshot +
environment + results + any CSV/SVG artifacts) and uploads it to S3:

```bash
./scripts/run.sh                          # DEFAULT: seed + download sweep, then upload sweep
./scripts/run.sh both aes128-spread       # same, with a label
./scripts/run.sh download aes128-spread   # download sweep only (seeds first)
./scripts/run.sh bench    quick           # download sweep, skip seeding
./scripts/run.sh upload   file-source     # forced upload sweep only
```

With the default `both` mode you get one run folder containing both
`download-sweep.json` and `upload-sweep.json` (and a combined `summary.txt`). The
2nd arg is an optional label to tell runs apart. See
[`results/runs/README.md`](results/runs/README.md) for the directory layout.

**2. On your dev box — pull and commit.** `pull-results.ps1` syncs the captured
runs from S3 into the repo; then commit code and benchmarks together:

```powershell
.\scripts\pull-results.ps1                          # -> results\runs\
git add -A
git commit -m "benchmarks: 30GiB x10 aes128 + spread on c7gn.16xlarge"
git push
```

Git tracking: only `results/runs/` is committed. Loose scratch output under
`results/` (raw sweep JSON, ad-hoc CSV/SVG, the per-IP JSONL history) and all
`*.bin` data are git-ignored, so the repo stays clean and you commit exactly the
curated runs. Runs **accumulate** — every captured run stays as its own timestamped
folder, so the repo keeps your full history side by side.

**Pruning runs you don't want** — `scripts/prune-runs.ps1` lists runs and removes
selected ones from both the local repo and S3, then stages the deletion:

```powershell
.\scripts\prune-runs.ps1                       # list all runs
.\scripts\prune-runs.ps1 20260713T101500-exp1  # remove one (by name)
.\scripts\prune-runs.ps1 *exp1* 20260714T*     # globs / multiple patterns
.\scripts\prune-runs.ps1 *old* -Force -Push    # skip prompt, commit + push
```

It keeps `README.md`/`.gitkeep` (only run directories match). Without `-Push` it
just stages the removals so you can review, then `git commit && git push`. (Bash
equivalent: `scripts/prune-runs.sh <glob> [--force] [--push]`.) Note this removes
runs going forward; they remain in git *history* unless you rewrite it.

First-time git setup:

```powershell
git init
git add -A
git commit -m "S3 SDK v3 download/upload benchmark"
git branch -M main
git remote add origin https://github.com/<you>/<repo>.git
git push -u origin main
```

(If you'd rather commit straight from EC2, you can instead `git clone` the repo on
the box, run `scripts/run.sh`, and `git add results/runs && git commit && git push`
there — that just means putting GitHub credentials on the instance. The S3
round-trip above keeps auth on your dev box only.)

## Delivery modes (what happens to downloaded bytes)

The download benchmark can handle the bytes four ways, set via `deliveryMode` in
the `download` section:

| `deliveryMode` | What it does | Models | Cost |
|------|--------------|--------|------|
| `discard` (default) | Drain and throw away each part on arrival | Pure network/CPU throughput ceiling | Minimal memory |
| `ordered-drop` | Buffer parts, deliver strictly in part order, then drop (free) at the frontier — no consumer | The reorder/backpressure machinery in isolation (infinitely fast sink) | Holds out-of-order parts in memory (head-of-line blocking) |
| `ordered-stream` | Buffer parts, then **transfer** each (zero-copy) into a per-object `Readable` a consumer drains | A real user reading a stream per object | Reorder buffer + cross-thread hand-off + consumer-driven backpressure |
| `file` | Positional-write each part to its byte offset in a local file | Downloading to disk | Disk write; no reorder buffer (out-of-order writes are fine) |

```json
"download": {
  "deliveryMode": "ordered-drop",   // discard | ordered-drop | ordered-stream | file
  "deliveryPath": "/mnt/nvme"       // output dir for "file" mode (default OS temp)
}
```

### Download API (`S3TransferManager`)

By default (`download.api: true`) the benchmark runs through a Transfer-Manager-shaped
wrapper, `S3TransferManager` (the same class serves uploads via `upload()`/`uploadMany()`
— see below). This makes the measurement boundary match a real transfer manager's public
API: construct once, then call `download()` per object. It always delivers to per-object
streams; `deliveryMode` selects only the **destination** the harness drains each stream to:
`file` pipes the ordered stream to a local file (download-to-disk), and anything else
(`discard`/`ordered-drop`/`ordered-stream`) drains to a discard sink (pure throughput; a
slow reader can be modeled with `consumerRate`). Set `api:false` / `--no-api` to use the
legacy `deliveryMode` run loop instead.

**Uploads use the same manager.** `upload({ bucket, key, body })` takes a customer
`Readable` from main; the manager runs `CreateMPU`, carves the stream into `partSize`
buffers on main (single-thread ingress + memcpy — a `Readable` can't cross the worker
boundary), transfers each part zero-copy to a warm uploader pool that does parallel
`UploadPart`, then `CompleteMPU`. `uploadMany({ sources })` runs many through one pool +
one `uploadMaxBuffered` budget. The upload benchmark uses this by default (`upload.api`),
firing x concurrent `upload()` calls fed by synthetic customer streams; window =
`CreateMPU → CompleteMPU`, pool spawn one-time.

```js
const tm = new S3TransferManager(cfg);      // spawns the worker pool ONCE (one-time cost)
await tm.ready();

// single object -> a Readable of the object's bytes, reassembled in PartNumber order
const { body, contentLength } = await tm.download({ bucket, key });
await pipeline(body, sink);

// many objects through the SAME pool + global budget (convenience over N download()s)
const job = await tm.downloadMany({ bucket, keys });
for (const { key, body } of job.objects) body.pipe(mySink(key));
await job.done();

await tm.close();
```

- **One shared pool, objects not pinned.** All objects' parts feed one worker pool; the
  scheduler round-robins across active objects so every object's frontier advances and
  the pool stays full. Each object gets its own ordered `Readable`; a part from any
  worker is routed to its object's stream and pushed in order.
- **Two-tier backpressure.** A global `maxBufferedBytes` budget bounds the cross-object
  reorder backlog; each `Readable`'s `streamHwm` throttles a slow consumer per object
  (a slow reader on one object pauses only that object's fetches).
- **Buffers are handed to the caller** by ownership transfer (worker → main → the
  `Readable`) and **not recycled** — once bytes cross the API boundary the caller owns
  them, so the internal buffer ping-pong (`bufferReturn`) is disabled here.
- **`download()` vs `downloadMany()`:** identical throughput when `download()` is
  invoked concurrently and drained concurrently (both feed the one shared scheduler +
  budget). `downloadMany()` is convenience: hands back all per-object handles up front,
  plus a `done` promise. Draining **sequentially** is memory-safe but serializes delivery
  and tanks throughput — always attach all sinks and drain concurrently.

**Harness behavior (default):** each measured iteration fires `x` concurrent
`download()` calls (`x` = number of objects) and drains their streams concurrently. The
timed window is "first `download()` → last stream `finish`", so `med`/`best`/`e2e` all
include HeadObject + planning; only the one-time pool spawn is reported separately.

Notes:
- **ordered-drop** and **ordered-stream** share the same engine: the main thread
  drives dispatch (ascending, frontier-first) and enforces per-object in-order
  delivery, holding out-of-order parts until earlier ones arrive. The reorder
  backlog (held-but-undelivered bytes, across all workers) is bounded by
  `maxBufferedBytes` (default 2 GiB); in-flight is bounded separately by
  `workers × concurrency`, so full network concurrency is preserved. It cannot hang:
  when nothing is in flight it dispatches the next (lowest-needed) part regardless
  of the cap.
- **ordered-drop** coordinates ordering with **tiny metadata messages only** — the
  downloaded **bytes stay in the workers** (held as raw chunks, or reused contiguous
  buffers with `bufferPool`) and are freed when main signals a part was delivered in
  order. Nothing is consumed. This isolates the ordering/backpressure cost against an
  infinitely fast sink. Compare its peak RSS/throughput to `discard` to see the
  memory and head-of-line-blocking cost of in-order delivery.
- **ordered-stream** actually delivers the bytes to a consumer — see
  [Stream delivery](#stream-delivery-ordered-stream) below.
- **file** writes to `deliveryPath` (default OS temp dir) and removes the file
  after the run. Byte-integrity of the offset-based assembly is verified. Point
  `deliveryPath` at a fast disk (NVMe) so storage doesn't become the bottleneck.
  A leading `~` is expanded to your home directory, and the directory is created
  automatically if missing. Each part's chunks are written in a single positional
  `writev` (one syscall per part, not per chunk).
- **file writes and the event loop:** by default the per-part write is a **blocking**
  `writevSync`, so disk-write latency stalls that worker's socket draining. Set
  `"fileAsync": true` in the `download` section to write on libuv's threadpool
  instead, keeping the event loop free to drain sockets while writes are in flight
  (per-worker writes stay bounded by `concurrency`). At high worker counts, raise
  `UV_THREADPOOL_SIZE` (env var) so writes aren't queued behind the default 4-thread
  pool. Use `fileAsync` when the disk is stalling your network numbers; leave it off
  to see the honest blocking-write cost.

  ```json
  "download": { "deliveryMode": "file", "deliveryPath": "/mnt/nvme", "fileAsync": true }
  ```
- **discard** is the right mode for finding the NIC/CPU ceiling.

### Buffer pool (ordered-drop memory strategy, `bufferPool`)

By default ordered-drop retains each completed part as its **raw chunk array**
(zero-copy) until delivery. That's cheap on CPU but holds hundreds of thousands of
small `Buffer`s alive under head-of-line blocking — heavy old-gen GC and inflated,
fragmented RSS. Setting `"bufferPool": true` in the `download` section switches to
copying each part into a **reused, contiguous part-sized buffer** from a per-worker
free list:

```json
"download": { "deliveryMode": "ordered-drop", "bufferPool": true }
```

The tradeoff (the run header shows `buffer-pool ON/OFF`):
- **Adds** one memcpy per byte as chunks arrive.
- **Removes** long-lived retention of many small buffers — incoming chunks die
  young, and the long-lived footprint collapses to a small set of recycled buffers,
  so GC pressure drops and RSS stays flat and bounded.

It's a net win only when GC/fragmentation is the limiter (large reorder backlog,
not network-bound). Pair it with `timeseries` and `partTimes` to compare RSS, CPU,
and the latency tail against the default path. `ordered-drop` only; `ordered-stream`
transfers dedicated buffers instead, so it's ignored there.

### Stream delivery (`ordered-stream`)

`ordered-drop` is a *virtual* consumer: once a part reaches the delivery frontier
the main thread only accounts its bytes and tells the worker to free them — **the
bytes never leave the worker**. That isolates the reorder/backpressure machinery
against an infinitely fast sink, but it isn't what a real user sees. A real
Transfer-Manager consumer wants to *read an ordered stream per object*.

`deliveryMode: "ordered-stream"` models that faithfully:

```json
"download": { "deliveryMode": "ordered-stream", "consumerRate": "0" }
```

- The worker assembles each part into a **dedicated** `ArrayBuffer` and **transfers**
  it to the main thread via the `postMessage` transfer list — ownership of the
  memory moves, so it's **zero-copy** (O(1) regardless of part size), not a funnel
  copy. The one assembly copy (socket → contiguous buffer) is the same one
  `bufferPool` already pays.
- The main thread pushes transferred parts, in order, into a **per-object
  `Readable`**, which a consumer drains. Delivery for an object cannot advance past a
  missing part (per-object head-of-line ordering).
- **Consumer backpressure is real:** if the `Readable`'s highWaterMark fills (e.g. a
  slow consumer, set via `consumerRate`), delivery for that object pauses, the
  reorder backlog rises to `maxBufferedBytes`, and dispatch throttles — end to end.
- With `bufferReturn` (default), the consumed buffer is transferred **back** to the
  owning worker for reuse, so a bounded set of buffers ping-pongs across the thread
  boundary (zero-copy both ways) rather than allocating one per part.

Use `ordered-drop` to find the reorder ceiling; use `ordered-stream` to measure the
true cost of delivering ordered bytes to a consumer — the cross-thread hand-off plus
consumer-driven backpressure — which is exactly what a worker-thread Transfer
Manager pays. Compare the two modes' throughput/RSS/latency to quantify it.

### Time series (ordered-stream)

To see *how* the reorder buffer behaves over the course of a run — and where
backpressure kicks in — set `"timeseries": true` in the `download` section. It
samples every 500ms during each measured ordered-stream iteration and captures:

- RSS (process memory, MiB)
- buffered parts (completed-but-undelivered, held in workers)
- buffered bytes (MiB)
- in-flight parts (downloading right now)
- CPU% (whole process, across all cores)

```json
"download": { "deliveryMode": "ordered-stream", "timeseries": true }
```

It writes two files per size:
- a **CSV** (`iter,t_ms,rss_mib,buffered_parts,buffered_mib,inflight_parts,cpu_pct`,
  one row per sample across all iterations), and
- a self-contained **SVG plot** (dependency-free; opens in any browser) with four
  stacked panels — RSS, buffered parts, in-flight parts, CPU% — sharing a time axis.

Files land at `results/timeseries-<size>-<timestamp>.{csv,svg}` by default, or set
`"timeseriesFile"` to choose the base path. Only applies to `ordered-stream` (the
other modes don't centrally track buffered/in-flight parts); ignored otherwise.

### Per-part download times (`partTimes`)

Set `"partTimes": true` in the `download` section to record the wall time of every
individual part fetch and get a latency profile. Works in **all** delivery modes.

```json
"download": { "partTimes": true }
```

It writes a **CSV** (`iter,key,part_number,bytes,download_ms,vip,conn_id`, one row
per part per measured iteration) to `results/parttimes-<size>-<timestamp>.csv`
(set `"partTimesFile"` to choose the base path), and prints a percentile summary
across all measured iterations:

```
per-part download time (ms), across all measured iterations:
size             parts      min      p50      p90      p99    p99.9      max     mean
-------------------------------------------------------------------------------------
30GiB             4800     41.2    118.5    263.7    511.9    980.3   3204.1    142.8
```

The per-part time includes any stall-retry (below), so a stalled-then-refetched
part shows up as a fat tail (p99.9 / max).

Each row also records the connection it was served on:
- `vip` — the S3 front-end IP that served the part (the "virtual IP" the DNS
  round-robin landed on). Lets you check whether slow parts cluster on specific
  front-ends.
- `conn_id` — a stable id for the socket (`w<worker>c<n>`), so you can see which
  parts shared a keep-alive connection and whether a particular connection ran
  slow throughout.

Both are populated on the default `node` handler; the `undici` handler doesn't
expose the serving socket, so they're blank there.

### Stall detection + retry

A single slow or stuck connection on a low-numbered part can stall in-order
delivery and let the reorder buffer balloon (see the ordered-stream notes above).
To guard against this, each part fetch runs under a **stall watchdog**: if a part
reads no bytes for `stallTimeoutMs` (default 10000; set `0` to disable), the request
is aborted and the whole part is re-fetched from scratch (a ranged GET by PartNumber
is idempotent, and the retry typically lands on a fresh S3 front-end). Up to
`partRetries` (default 3) attempts before the run fails.

```json
"download": { "stallTimeoutMs": 10000, "partRetries": 8 }
```

When the watchdog fires it **destroys the response stream directly** (not just the
SDK abort signal), so the consuming read throws promptly and the re-fetch actually
proceeds — otherwise an abort that doesn't tear down an already-streaming body
would leave the part hanging past the timeout. Only genuine zero-progress stalls
trigger this retry — transient network errors are still handled by the SDK's own
internal retry. Applies to all delivery modes.

Note: the watchdog detects **zero bytes** for the window, not merely *slow*
transfer. A connection that trickles a few bytes each interval keeps resetting the
timer and won't be aborted; for that you'd want a hard per-part deadline (not
currently implemented — ask if you want it).

## Resource usage

Both benchmarks sample whole-process resource usage during the measured iterations
and print a per-object table (also in the JSON results the sweep writes, under
`resources`):

```
resource usage (whole process, during measured iterations):
object                    peak RSS     avg RSS  peak CPU   avg CPU  peak MEM
---------------------------------------------------------------------------
bench/100mib.bin        212.84 MiB  156.25 MiB       28%       19%      0.7%
(CPU% is of all 12 cores; MEM% is of 31.64 GiB total RAM)
```

- **peak RSS / avg RSS** — resident memory of the whole Node process (main thread
  plus all worker threads share one OS process).
- **peak CPU / avg CPU** — percentage of the entire machine (`coresUsed /
  totalCores`), so 28% on a 12-core box ≈ 3.4 cores busy. Peak catches transient
  spikes; avg (total CPU time over the measured window) reflects sustained load —
  the better signal for whether TLS + checksum work is the ceiling before the NIC.
- **peak MEM** — peak RSS as a percentage of total system RAM.

Watch peak CPU when chasing NIC saturation: if it approaches 100%, you're
CPU-bound (raise `workers`, or set `validateChecksum: false`), not network-bound.

## HTTP handler: node vs undici (`httpHandler`)

The SDK's request handler is pluggable, chosen with `httpHandler` in the
`download`/`upload` section: `node` (default) uses `@smithy/node-http-handler`
(Node's core `http`/`https` + keep-alive agents); `undici` uses
`@smithy/undici-http-handler`. A/B them to see whether undici's leaner HTTP/1.1
stack helps your workload:

```json
"download": { "httpHandler": "undici" }   // vs "node"
```

The header shows `handler=…`. Both handlers keep full feature parity here:
`spreadConnections` (DNS round-robin), connection logging, and per-IP throughput
all work on the undici path (via a custom undici connector that resolves/pins the
IP and captures the socket). undici's per-origin connection limit is set to the
same `maxSockets` the node agents use.

Expectation: undici has lower per-request overhead, which helps most with many
small requests; for large streaming parts the TLS-decrypt/byte-moving cost is
shared, so gains may be modest — measure it.

## TLS overhead (`tls`)

At high throughput, TLS decryption is a big chunk of per-byte CPU. Setting
`"tls": false` in a section points the client at S3's HTTP endpoint
(`http://<bucket>.s3.<region>.amazonaws.com`), so you can A/B the cost of TLS:

```json
"download": { "tls": false }   // HTTP; default true = HTTPS
```

If throughput jumps (and avg CPU drops) with `tls: false`, TLS was your ceiling.
Available for both benchmarks; the header shows `transport=HTTPS | HTTP (no TLS)`.
Caveats: sends data in the clear (test buckets/data only), and fails if the bucket
policy enforces `aws:SecureTransport`. Checksum validation still works over HTTP.

## Pinning the TLS cipher (`cipher`)

At high throughput, the bulk AEAD cipher is a real slice of per-byte CPU, and on
Graviton3 **AES-128-GCM is ~20% cheaper than AES-256-GCM**. By default S3 picks the
suite, so runs aren't directly comparable. Set `cipher` in a section to pin it:

```json
"download": { "cipher": "aes128" }   // aes128 | aes256 | chacha20 | default | raw OpenSSL string
```

The pin is applied across both **TLS 1.3** (the `TLS_*` suite name) and **TLS 1.2**
(the `ECDHE-*` names), so it holds regardless of which protocol S3 negotiates — and
because it works on TLS 1.3, you keep the 1-RTT handshake.

The run header echoes the **negotiated** protocol/cipher so you can confirm the pin
took, e.g. `transport=HTTPS TLSv1.3/TLS_AES_128_GCM_SHA256 (pin aes128)`. Available
for both download and upload; works on the `node` and `undici` handlers. To measure
the cipher's effect, A/B `cipher: "aes128"` vs `cipher: "aes256"` and compare
throughput and avg CPU.

## Native CRC32 checksum (`nativeCrc32`)

The SDK validates CRC32 with `@aws-crypto/crc32`, a **pure-JS** loop (`for..of` over
the bytes + `this.checksum` field mutation) — measured ~5–9× slower than an
optimal loop, and a big share of download CPU when checksum validation is on.
`nativeCrc32` (or `--native-crc32`) monkey-patches that class at the **SDK layer**
(`src/crc32-native.mjs`) to use Node's native, hardware-accelerated `zlib.crc32`
instead, so every SDK GET/PUT that computes a CRC32 benefits — no call-site changes.

```json
"nativeCrc32": true
```

Safety: it only applies when `zlib.crc32` exists (Node ≥ 18) **and** a runtime
self-test confirms it produces byte-identical results (single-shot + streaming) to
the implementation it replaces — otherwise it's a no-op and logs why. It covers
**CRC32 only** (zlib has no CRC32C); with CRC32C it's a no-op. Keeps full checksum
validation, just computed natively — the recommended way to keep integrity checks
without the slow JS loop (and it sidesteps the Node 24 per-chunk amplification).

## Network tuning on the EC2 box (`scripts/tune-network.sh`)

Per-connection throughput ramps over the first few round-trips (TCP slow-start),
and bursty part fetching keeps re-triggering that ramp. `scripts/tune-network.sh`
applies host-wide kernel settings that shorten the ramp and raise the ceiling. Run
it **on the EC2 instance** (it edits kernel/network settings, so it needs root and
assumes a dedicated benchmark host):

```bash
sudo ./scripts/tune-network.sh          # apply (runtime + persisted to /etc/sysctl.d)
sudo ./scripts/tune-network.sh --revert # restore the exact prior values
```

Before changing anything, apply snapshots the current values to
`/etc/sysctl.d/.s3bench-backup.env`, so `--revert` restores precisely what was
there (not guessed defaults), removes the persisted config, and resets the route
`initcwnd`. A reboot also clears the route change on its own.

What it sets and why:

| Setting | Effect |
|---------|--------|
| `tcp_slow_start_after_idle=0` | Stop resetting the congestion window after a brief idle — the biggest ramp win for bursty part fetching. |
| `tcp_congestion_control=bbr` + `default_qdisc=fq` | Ramp to available bandwidth faster than CUBIC; tolerate loss better. |
| `rmem/wmem` + `tcp_rmem/tcp_wmem` (up to 128 MiB) | Let the TCP window open enough to fill the bandwidth-delay product. |
| `netdev_max_backlog`, `tcp_mtu_probing` | Headroom for high packet rates / PMTU. |
| `initcwnd/initrwnd=30` (default route) | Send ~30 segments in the first RTT instead of 10, shortening cold-start. |

Sysctl values persist across reboot (`/etc/sysctl.d/99-s3bench.conf`); the
`initcwnd` route edit does not (re-run after reboot if needed). Verify BBR took
effect with `sysctl net.ipv4.tcp_congestion_control` — if it still says `cubic`,
the kernel lacks the `tcp_bbr` module. Intra-region RTT is sub-millisecond, so
these help most on small parts, short runs, and the ordered-stream frontier;
they're less visible on long large-object transfers that spend most of their time
at full window.

## Connection spread (`logConnections`)

Set `"logConnections": true` in the `download` section to report how many distinct
S3 front-end IPs your connections landed on:

```
connection spread across S3 IPs (last measured run):
  bench/30gib.bin: 1 distinct IPs, 256 connections (per-IP min 256 / median 256 / max 256)
    top: 3.5.81.62=256
```

This is a common throughput killer: Node's default `dns.lookup` returns a single
address, so every connection concentrates on one S3 front-end and you can't get
past that IP's share of bandwidth — no matter how high you set concurrency. A
healthy run shows connections spread across many distinct IPs. If you see 1–2
distinct IPs, that's your bottleneck.

## Per-IP throughput (`ipThroughput` / `ipThroughputSizes`)

Records the throughput of connections to each S3 front-end IP, so you can see
whether some IPs are consistently faster than others. Works for both benchmarks.

- **Metric:** per connection, the socket's wire bytes (`bytesRead` for download,
  `bytesWritten` for upload) ÷ its active time → each IP's value is the average
  per-connection Gbps to that IP, aggregated across workers and sampled once per
  iteration (so you get min/median/max per IP within a run).
- **Gating by size:** record only for chosen sizes via `"ipThroughputSizes":
  ["30GiB"]` in the `download`/`upload` section. Set `"ipThroughput": true` to
  record for every size in the run.
- **Ongoing record:** each matching group appends one JSON line to the file named
  by `"ipThroughputFile"` (default `results/ip-throughput.jsonl`), with timestamp,
  mode, node/sdk versions, size, settings, and the per-IP samples. The file
  **accumulates across runs**, so you can track whether a given IP is reliably
  fast/slow over time.

```json
"download": { "ipThroughputSizes": ["30GiB"], "spreadConnections": true }
```

Analyze the history later, e.g. average Gbps per IP across all runs:
```bash
cat results/ip-throughput.jsonl | jq -r '.perIp[] | "\(.ip) \(.medianGbps)"' | sort | ...
```

Console output (sorted fastest median first):
```
per-IP throughput (30GiB, per-connection avg over 5 iter):
  16.15.38.237     med 1.940 Gbps  (min 1.810 / max 2.020, 12 conn)
  52.92.249.242    med 0.860 Gbps  (min 0.790 / max 0.910, 9 conn)
```

Note: with `spreadConnections` on, each iteration tends to hit different IPs, so a
given IP often has one sample per run — the JSONL history across many runs is
where consistency for a recurring IP shows up.

## Connection spreading (`spreadConnections`)

The mitigation for single-IP concentration: a custom DNS resolver that resolves
all of the endpoint's A-records and round-robins each new connection across them,
so N concurrent connections fan out over many S3 front-ends instead of piling onto
one. Enable with `"spreadConnections": true` in the `download`/`upload` section
(available for both benchmarks):

```json
"download": { "spreadConnections": true }
```

To A/B its effect, also turn on `"logConnections": true` and compare the
connection-spread report with `spreadConnections` off vs on.

Verified locally (7-part object): spreading OFF → all 7 connections to 1 IP;
spreading ON → 7 connections across 7 distinct IPs. On a high-bandwidth instance
(e.g. c7gn), this is often the difference between stalling at a fraction of the
NIC and scaling toward line rate. Implementation: short-TTL cached `dns.resolve4`
+ per-host round-robin, falling back to the default resolver on error or IPv6.

## Tests

`npm run test:ordered` is a regression test for ordered-stream backpressure: it
seeds a 10-part object, artificially slows part 1 (via the `BENCH_SLOW_PART` /
`BENCH_SLOW_MS` worker hook), and runs an ordered-stream download with a tiny cap.
It asserts the run completes in order without hanging — proving the proactive
dispatch handles the "buffer full while waiting on a slow low part" case. Requires
AWS credentials + the configured bucket; cleans up after itself.

## Benchmarking notes

- Run the benchmark **in the same region** as the bucket, on an instance type with
  enough network bandwidth (e.g. a `*n` / large instance) so the SDK, not the NIC,
  is what you're measuring.
- Sweep one variable at a time (part size at upload, then concurrency, then
  workers) to see where throughput saturates.
- Draining bodies (default) measures transfer throughput without disk/memory
  bottlenecks. Set `"keep": true` only if you specifically want to include
  buffering cost.
- The `[done]` line reports `N/M parts checksum-validated`; expect `M/M` for a
  CRC32-seeded object. `0/M` means the object wasn't uploaded with per-part
  checksums (re-seed with `"checksum": "CRC32"`).

## Verified

The `PartNumber` + CRC32 path was smoke-tested end-to-end in us-west-2 against a
20 MiB / 4-part object: `4/4 parts checksum-validated`. Throughput numbers are
only meaningful from an in-region EC2 instance; a laptop over the public internet
will be latency/bandwidth bound.

## Appendix: CLI reference

The config file is the primary interface; CLI flags exist only for **ad-hoc,
per-run overrides** and follow the precedence `CLI > section > shared > default`.
Nearly every config key has a matching flag (kebab-case of the camelCase key), and
each tool prints its full flag list with `--help`:

```bash
node src/benchmark.js --help          # download
node src/upload-benchmark.js --help   # upload
node src/upload-test-data.js --help   # seed
```

### Download (`src/benchmark.js`)

| Flag | Config key | Meaning |
|------|-----------|---------|
| `--bucket <name>` | `bucket` | S3 bucket |
| `--region <r>` | `region` | AWS region |
| `--keys <k1,k2>` | — | Explicit object keys (one run per key), bypassing `sizes` |
| `--sizes <s1,s2>` | `sizes` | Size labels, each optionally `<size>:<count>` |
| `--prefix <p>` | `dataPrefix` | Key prefix used with `--sizes` |
| `--workers <n>` | `workers` | Worker threads |
| `--concurrency <n>` | `concurrency` | Concurrent `PartNumber` GETs per worker |
| `--iterations <n>` | `iterations` | Measured iterations |
| `--warmup <n>` | `warmup` | Unmeasured warmup iterations |
| `--keep` | `keep` | Keep bodies in memory instead of draining |
| `--no-checksum` | `validateChecksum:false` | Disable per-part checksum validation |
| `--delivery <mode>` | `deliveryMode` | `discard` \| `ordered-drop` \| `ordered-stream` \| `file` |
| `--no-api` | `api:false` | Disable the default `S3TransferManager` API path and use the legacy `deliveryMode` run loop instead |
| `--delivery-path <dir>` | `deliveryPath` | Output dir for `file` mode |
| `--max-buffered <size>` | `maxBufferedBytes` | ordered-stream reorder-buffer cap |
| `--buffer-pool` | `bufferPool` | ordered-stream: copy into reused contiguous buffers |
| `--consumer-rate <size>` | `consumerRate` | ordered-stream: throttle consumer bytes/sec (0 = unlimited) |
| `--no-buffer-return` | `bufferReturn:false` | ordered-stream: don't recycle buffers back to workers |
| `--stream-hwm <size>` | `streamHwm` | ordered-stream: per-object stream highWaterMark (default 2 × partSize) |
| `--timeseries` | `timeseries` | ordered-stream: 500 ms RSS/buffer/CPU CSV + SVG |
| `--timeseries-file <base>` | `timeseriesFile` | Base path for the time-series files |
| `--part-times` | `partTimes` | Per-part download-time CSV + latency percentiles |
| `--part-times-file <base>` | `partTimesFile` | Base path for the part-times CSV |
| `--stall-timeout <ms>` | `stallTimeoutMs` | Abort + re-fetch a part idle this long (0 = off) |
| `--part-retries <n>` | `partRetries` | Max stall-retries per part |
| `--handler <h>` | `httpHandler` | `node` \| `undici` |
| `--cipher <name>` | `cipher` | `aes128` \| `aes256` \| `chacha20` \| raw OpenSSL string |
| `--spread-connections` | `spreadConnections` | DNS round-robin across S3 front-end IPs |
| `--no-tls` | `tls:false` | Use S3's plaintext HTTP endpoint |
| `--log-connections` | — | Report connection spread across S3 IPs |
| `--ip-throughput` | `ipThroughput` | Record per-IP throughput for every size |
| `--ip-throughput-sizes <s1,..>` | `ipThroughputSizes` | Record per-IP throughput for these sizes |
| `--ip-throughput-file <f>` | `ipThroughputFile` | JSONL history file |
| `--native-crc32` | `nativeCrc32` | Patch `@aws-crypto/crc32` to use native `zlib.crc32` (CRC32 only) |
| `--profile` | `profile` | CPU-profile each worker → one `.cpuprofile` per worker |
| `--profile-dir <dir>` | `profileDir` | Dir for the profiles (default `results/profile-<node>/`) |
| `--json` | — | Emit JSON results to stdout |
| `--out <file>` | — | Also write JSON results to a file |

Total in-flight requests = `workers × concurrency`. Part **count and boundaries**
come from the object's upload (`--part-size` at seed time), not a download flag.

### Upload (`src/upload-benchmark.js`)

Shares the tuning flags above (`--workers`, `--concurrency`, `--iterations`,
`--warmup`, `--handler`, `--cipher`, `--no-tls`, `--spread-connections`,
`--ip-throughput*`, `--json`, `--out`) plus:

| Flag | Config key | Meaning |
|------|-----------|---------|
| `--sizes <s1,s2>` | `sizes` | Sizes to upload (each optionally `<size>:<count>`) |
| `--part-size <size>` | `partSize` | Multipart part size |
| `--checksum <algo>` | `checksum` | `CRC32` \| `CRC32C` \| `SHA256` \| `SHA1` |
| `--source <mode>` | `uploadSource` | `memory` \| `file` \| `stream` \| `open` \| `open-stream` |
| `--open-type <type>` | `uploadOpen.type` | `open`/`open-stream` opener: `file` \| `memory` (in-memory, no disk) |
| `--carvers <n>` | `uploadCarvers` | open-stream: carver thread count (0 = one per object) |
| `--max-buffered <size>` | `uploadMaxBuffered` | stream source: main-side dispatch queue cap (0 = auto) |
| `--client-rate <size>` | `uploadClientRate` | stream source: throttle simulated client bytes/sec (0 = unlimited) |
| `--client-chunk <size>` | `uploadClientChunk` | stream source: simulated client push chunk size (default 1MiB) |
| `--source-path <dir>` | `sourcePath` | Dir for the `file` source temp file |
| `--no-api` | `api:false` | Disable the default `S3TransferManager` upload API; use the legacy `uploadSource` loop |
| `--force` | `forceUpload` | Upload even if a matching object exists |

### Seed (`src/upload-test-data.js`)

| Flag | Config key | Meaning |
|------|-----------|---------|
| `--bucket <name>` | `bucket` | S3 bucket |
| `--region <r>` | `region` | AWS region |
| `--sizes <s1,s2>` | `sizes` | Sizes to create (each optionally `<size>:<count>`) |
| `--prefix <p>` | `dataPrefix` | Key prefix |
| `--part-size <size>` | `partSize` | Multipart part size = download part boundary |
| `--checksum <algo>` | `checksum` | Per-part checksum (`CRC32` fast; `CRC32C` pure-JS/slow) |
| `--force` | `forceUpload` | Re-upload even if the object already exists |

### Sweep script env overrides

Both `sweep-download.sh` and `sweep-upload.sh` accept: `WORKERS`, `CONCURRENCY`,
`ITERATIONS`, `WARMUP`, `PART_SIZE`. `sweep-upload.sh` also honors `FORCE` (default
`1`; set `FORCE=0` to respect the config's `forceUpload`). Everything else is read
from `bench.config.json`.
