import os from 'node:os';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const MIB = 1024 * 1024;
const GIB = 1024 * MIB;

/**
 * Expand a leading `~` (or `~/…`) to the user's home directory. Node's path/fs
 * APIs don't understand `~` (it's a shell feature), so a config value like
 * "~/tempfiles" would otherwise be treated as a literal relative path and fail.
 */
export function expandHome(p) {
  if (typeof p !== 'string' || !p.startsWith('~')) return p;
  if (p === '~') return os.homedir();
  if (p.startsWith('~/') || p.startsWith('~\\')) return path.join(os.homedir(), p.slice(2));
  return p; // "~otheruser" — leave as-is (not supported)
}

/** Absolute path to the single central config file at the project root. */
export const CONFIG_PATH = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '..',
  'bench.config.json',
);

/**
 * Load bench.config.json from the project root. Missing file -> {} (all defaults).
 * Values here are overridden by CLI flags at parse time.
 */
export function loadFileConfig() {
  try {
    return JSON.parse(readFileSync(CONFIG_PATH, 'utf8'));
  } catch (err) {
    if (err.code === 'ENOENT') return {};
    throw new Error(`Failed to read ${CONFIG_PATH}: ${err.message}`);
  }
}

/**
 * Resolve a config value with section precedence: a key inside the "download" or
 * "upload" section overrides the same key at the top level (shared). Returns
 * undefined if set in neither, so callers can `?? default`.
 *
 *   sectionValue(file, 'upload', 'workers')
 *     -> file.upload.workers ?? file.workers
 */
export function sectionValue(file, section, key) {
  const sec = file[section] && typeof file[section] === 'object' ? file[section] : {};
  return sec[key] !== undefined ? sec[key] : file[key];
}

/**
 * Resolve per-IP-throughput recording settings (shared by download + upload).
 *   enabledAll: record for every size in this run (--ip-throughput or config)
 *   sizes:      record only for these size labels (e.g. ["30GiB"])
 *   file:       JSONL history file appended to across runs
 */
function ipThroughputOpts(args, pick) {
  const sizes = args['ip-throughput-sizes']
    ? args['ip-throughput-sizes'].split(',').map((s) => s.trim()).filter(Boolean)
    : (pick('ipThroughputSizes') ?? []);
  return {
    ipThroughput: Boolean(args['ip-throughput'] || pick('ipThroughput')),
    ipThroughputSizes: sizes,
    ipThroughputFile: args['ip-throughput-file'] ?? pick('ipThroughputFile') ?? 'results/ip-throughput.jsonl',
  };
}

/**
 * Parse a human size like "8MiB", "1GB", "512kb", or a raw byte count.
 * Uses binary units (KiB/MiB/GiB) and treats KB/MB/GB as binary too, which is
 * the convention most people expect when reasoning about part sizes.
 */
export function parseSize(v) {
  if (typeof v === 'number') return v;
  const m = String(v).trim().match(/^(\d+(?:\.\d+)?)\s*([kmgt]?i?b?)?$/i);
  if (!m) throw new Error(`Invalid size: ${v}`);
  const n = parseFloat(m[1]);
  const unit = (m[2] || '').toLowerCase();
  const mult = {
    '': 1, b: 1,
    k: 1024, kb: 1024, kib: 1024,
    m: MIB, mb: MIB, mib: MIB,
    g: GIB, gb: GIB, gib: GIB,
    t: 1024 * GIB, tb: 1024 * GIB, tib: 1024 * GIB,
  }[unit];
  if (mult == null) throw new Error(`Invalid size unit in: ${v}`);
  return Math.round(n * mult);
}

/**
 * Both ordered delivery modes ('ordered-drop' and 'ordered-stream') use the same
 * frontier-first dispatch + reorder-buffer machinery; they differ only in what the
 * delivery frontier feeds (drop vs. a per-object Readable). This groups them.
 */
export function isOrderedMode(mode) {
  return mode === 'ordered-stream' || mode === 'ordered-drop';
}

/**
 * Canonical object key for a given size label. Single source of truth shared by
 * the seed script and the benchmark so `--sizes` maps to the same keys on both
 * sides (e.g. "30GiB" + prefix "bench/" -> "bench/30gib.bin").
 */
export function keyForSize(prefix, sizeLabel, index = 0, count = 1) {
  const clean = String(sizeLabel).replace(/\s+/g, '').toLowerCase();
  // Single file per size keeps the plain name (backward compatible); multiple
  // files get an index suffix: <prefix><size>-0.bin, <prefix><size>-1.bin, ...
  return count > 1 ? `${prefix}${clean}-${index}.bin` : `${prefix}${clean}.bin`;
}

/**
 * Parse one size entry into { label, count }. Accepts:
 *   "1GiB"              -> { label: "1GiB", count: 1 }
 *   "1GiB:4"            -> { label: "1GiB", count: 4 }
 *   { size, count }     -> { label: size, count }
 * The count is how many distinct files of that size to upload/benchmark.
 */
export function parseSizeSpec(entry) {
  if (entry && typeof entry === 'object') {
    return { label: String(entry.size), count: Math.max(1, Number(entry.count ?? 1)) };
  }
  const s = String(entry).trim();
  const colon = s.lastIndexOf(':');
  if (colon > 0 && /^\d+$/.test(s.slice(colon + 1))) {
    return { label: s.slice(0, colon).trim(), count: Math.max(1, Number(s.slice(colon + 1))) };
  }
  return { label: s, count: 1 };
}

/**
 * Expand size specs into groups: { label, count, keys[] }. Each group is a set of
 * `count` distinct object keys of the same size, benchmarked together.
 */
export function sizeGroups(prefix, rawSizes) {
  return rawSizes
    .map(parseSizeSpec)
    .filter((s) => s.label)
    .map(({ label, count }) => ({
      label,
      count,
      keys: Array.from({ length: count }, (_, i) => keyForSize(prefix, label, i, count)),
    }));
}

/**
 * Split a total size into contiguous parts of `partSize` bytes (last part is the
 * remainder). Shared by the upload benchmark (to know what to PUT) and matches
 * the boundaries the download benchmark reads back via PartNumber.
 * Returns [{ partNumber, start, end, size }] with 1-based partNumber.
 */
export function computeParts(totalSize, partSize) {
  const parts = [];
  let start = 0;
  let partNumber = 1;
  while (start < totalSize) {
    const end = Math.min(start + partSize, totalSize);
    parts.push({ partNumber, start, end, size: end - start });
    start = end;
    partNumber += 1;
  }
  // A zero-byte object still needs one (empty) part.
  if (parts.length === 0) parts.push({ partNumber: 1, start: 0, end: 0, size: 0 });
  return parts;
}

/**
 * Resolve a friendly cipher name to an OpenSSL cipher string that pins the given
 * AEAD suite across TLS 1.3 (the TLS_* name) and TLS 1.2 (the ECDHE-* names), so
 * the pin holds regardless of which protocol S3 negotiates. Node applies the
 * TLS_* entries to 1.3 ciphersuites and the rest to the <=1.2 cipher list.
 *   aes128 / aes256 / chacha20  -> pin that suite
 *   default (or empty)          -> null (use Node's defaults)
 *   anything else               -> treated as a raw OpenSSL cipher string
 */
export function resolveCiphers(name) {
  if (!name || String(name).toLowerCase() === 'default') return null;
  switch (String(name).toLowerCase()) {
    case 'aes128':
      return 'TLS_AES_128_GCM_SHA256:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256';
    case 'aes256':
      return 'TLS_AES_256_GCM_SHA384:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384';
    case 'chacha20':
    case 'chacha':
      return 'TLS_CHACHA20_POLY1305_SHA256:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305';
    default:
      return name;
  }
}

export function formatBytes(bytes) {
  if (bytes >= GIB) return `${(bytes / GIB).toFixed(2)} GiB`;
  if (bytes >= MIB) return `${(bytes / MIB).toFixed(2)} MiB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(2)} KiB`;
  return `${bytes} B`;
}

/**
 * Throughput helpers. We report both MiB/s and Gbps because storage folks think
 * in MiB/s and network folks think in Gbps.
 */
export function throughput(bytes, seconds) {
  const mibps = bytes / MIB / seconds;
  const gbps = (bytes * 8) / 1e9 / seconds;
  return { mibps, gbps };
}

const USAGE = `
S3 part-boundary download benchmark — AWS SDK for JavaScript v3

Downloads each object by PartNumber (aligned to its original multipart upload
boundaries) so the SDK validates each part's stored CRC32C checksum automatically.

All settings default from bench.config.json at the project root; CLI flags below
override it. With a populated config file, "node src/benchmark.js" needs no args.

Usage:
  node src/benchmark.js [options]

Object selection (else taken from bench.config.json "sizes" + "dataPrefix"):
  --keys <k1,k2,...>       Comma-separated object keys to benchmark (one run per key).
  --sizes <s1,s2,...>      Sizes to benchmark; each may carry a per-size file count
                           as <size>:<count> (default 1). Keys derived as
                           <prefix><size>.bin (count 1) or <prefix><size>-<i>.bin.
                           All files of a size are downloaded together (pooled).
                           e.g. --sizes 1MiB,1GiB:4,30GiB:2 --prefix bench/
                           Download fails if a required file is missing.

Options (override bench.config.json):
  --bucket <name>          S3 bucket name.
  --prefix <p>             Key prefix used with --sizes. Default: "".
  --region <region>        AWS region. Default: AWS_REGION env / SDK default.
  --workers <n>            Number of worker threads. Default: CPU count.
  --concurrency <n>        Concurrent part requests PER worker. Default: 4.
                           Total in-flight requests = workers * concurrency.
  --iterations <n>         Measured iterations per key. Default: 3.
  --warmup <n>             Warmup iterations (not counted). Default: 1.
  --keep                   Keep downloaded bytes in memory (default: discard/drain).
  --delivery <mode>        How bytes are handled: discard | ordered-drop |
                           ordered-stream | file.
                           discard        = drain + throw away on arrival (default)
                           ordered-drop   = reorder, drop at frontier (no consumer)
                           ordered-stream = reorder, transfer to a per-object
                                            Readable a consumer drains
                           file           = write each part to its offset in a file
  --delivery-path <dir>    Directory for 'file' mode output (default: OS temp dir).
  --file-async             (file mode) write each part asynchronously (threadpool)
                           so disk writes don't block the worker's event loop.
                           Consider raising UV_THREADPOOL_SIZE at high worker counts.
  --profile                CPU-profile each download worker; writes one .cpuprofile
                           per worker (default results/profile-<nodeVersion>/).
                           Analyze/diff with: node scripts/prof-top.mjs <file>.
  --profile-dir <dir>      Directory for the per-worker .cpuprofile files.
  --native-crc32           Ensure CRC32 uses native zlib.crc32. Modern SDKs
                           (@smithy/core >= ~3.x on Node >= 22.2) already do this,
                           so this just confirms it; on older SDKs it patches the
                           pure-JS @aws-crypto/crc32 loop (verified byte-identical).
  --max-buffered <size>    ordered-stream reorder-buffer cap (default 2GiB). Pauses
                           new part downloads when exceeded; resumes below half.
  --buffer-pool            (ordered-drop) copy each part into a reused contiguous
                           buffer instead of retaining raw chunk arrays. Trades a
                           memcpy for lower GC pressure + flat RSS. A/B against the
                           default zero-copy path.
  --consumer-rate <size>   (ordered-stream) throttle the consumer to size bytes/sec
                           per object to model a slow reader (0 = unlimited).
  --no-buffer-return       (ordered-stream) don't transfer consumed buffers back to
                           workers for reuse (allocate per part instead).
  --stream-hwm <size>      (ordered-stream) per-object stream highWaterMark for the
                           Readable + its consumer sink. Default 2x part size.
  --log-connections        Report how connections spread across S3 front-end IPs.
  --ip-throughput          Record per-IP throughput for every size in this run.
  --ip-throughput-sizes <s1,..>  Record per-IP throughput only for these sizes.
  --ip-throughput-file <f> JSONL history file (default results/ip-throughput.jsonl).
  --spread-connections     Fan connections across all resolved S3 IPs (custom DNS
                           round-robin), instead of Node's default single-IP lookup.
  --no-tls                 Use S3's HTTP endpoint (no TLS) to measure TLS overhead.
                           Sends data in the clear; test buckets only.
  --handler <h>            HTTP handler: node (default) | undici. undici uses
                           @smithy/undici-http-handler to A/B against node's http.
  --cipher <name>          Pin the TLS cipher: aes128 | aes256 | chacha20 | default
                           | <raw OpenSSL string>. Pins across TLS 1.3 and 1.2.
                           (Graviton does AES-128-GCM faster than AES-256-GCM.)
  --stall-timeout <ms>     Abort+re-fetch a part that reads no bytes for this long
                           (default 10000; 0 disables). Prevents one stuck
                           connection from blocking in-order delivery.
  --part-retries <n>       Max stall-retries per part before failing. Default 3.
  --timeseries             (ordered-stream) sample memory / buffered parts /
                           in-flight parts / CPU every 500ms; writes a CSV + SVG plot.
  --timeseries-file <f>    Base path for the CSV/SVG (default results/timeseries-<ts>).
  --part-times             Record each part's download time to a CSV and print
                           p50/p90/p99/p99.9 latency percentiles. All delivery modes.
  --part-times-file <f>    Base path for the CSV (default results/parttimes-<ts>).
  --no-checksum            Disable per-part checksum validation on download
                           (overrides "validateChecksum" in bench.config.json).
  --json                   Emit machine-readable JSON results to stdout.
  --out <file>             Also write JSON results to <file> (works with the table).
  --help                   Show this help.

Note: part count and boundaries come from the object's multipart upload, not a
flag. Control them at upload time via upload-test-data.js --part-size.

Environment:
  AWS_REGION, AWS_PROFILE and standard AWS credential env vars are respected.

Examples:
  node src/benchmark.js --bucket my-bench --keys 30gib.bin \\
    --workers 8 --concurrency 4 --iterations 5
`;

export function parseArgs(argv = process.argv.slice(2)) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    const key = a.slice(2);
    // Boolean flags.
    if (['keep', 'json', 'help', 'no-checksum', 'log-connections', 'spread-connections', 'no-tls', 'ip-throughput', 'timeseries', 'part-times', 'buffer-pool', 'file-async', 'profile', 'native-crc32', 'no-buffer-return', 'no-progress'].includes(key)) {
      args[key] = true;
      continue;
    }
    args[key] = argv[++i];
  }

  if (args.help) {
    process.stdout.write(USAGE);
    process.exit(0);
  }

  // Precedence: CLI flag > "download" section > shared top-level > built-in default.
  const file = loadFileConfig();
  const pick = (key) => sectionValue(file, 'download', key);

  const bucket = args.bucket ?? pick('bucket');
  if (!bucket) throw new Error('No bucket set. Add "bucket" to bench.config.json or pass --bucket.');

  // Resolve object groups: explicit --keys (each its own single-file group), else
  // --sizes / config sizes (each size expands to `count` files via dataPrefix).
  const prefix = args.prefix ?? pick('dataPrefix') ?? '';
  let groups;
  if (args.keys) {
    groups = args.keys
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
      .map((k) => ({ label: k, count: 1, keys: [k] }));
  } else {
    const rawSizes = args.sizes
      ? args.sizes.split(',').map((s) => s.trim()).filter(Boolean)
      : pick('sizes');
    if (!rawSizes || !rawSizes.length) {
      throw new Error('Nothing to benchmark. Set "sizes" in bench.config.json or pass --keys/--sizes.');
    }
    groups = sizeGroups(prefix, rawSizes);
  }

  // How downloaded bytes are handled:
  //   discard        - drain and throw away on arrival (pure network throughput)
  //   ordered-stream - buffer parts, deliver (free) strictly in part order via a
  //                    reorder buffer (models a sequential consumer)
  //   file           - write each part to its byte offset in a local file
  // How downloaded bytes are handled end-to-end:
  //   discard        - drain + throw away on arrival (no ordering; pure ceiling)
  //   ordered-drop   - reorder buffer, delivered strictly in part order but DROPPED
  //                    at the frontier (bytes freed in the worker; no consumer).
  //                    Measures the reorder/backpressure machinery in isolation.
  //   ordered-stream - reorder, then TRANSFER each part (zero-copy) into a
  //                    per-object Readable a consumer drains. Models a real user
  //                    reading a stream per object (cross-thread hand-off +
  //                    consumer-driven backpressure).
  //   file           - write each part to its byte offset in a local file
  const deliveryMode = args.delivery ?? pick('deliveryMode') ?? 'discard';
  if (!['discard', 'ordered-drop', 'ordered-stream', 'file'].includes(deliveryMode)) {
    throw new Error(`Invalid deliveryMode "${deliveryMode}". Use discard | ordered-drop | ordered-stream | file.`);
  }

  return {
    bucket,
    groups,
    region: args.region || pick('region') || process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION,
    workers: Number(args.workers ?? pick('workers') ?? os.cpus().length),
    concurrency: Number(args.concurrency ?? pick('concurrency') ?? 4),
    iterations: Number(args.iterations ?? pick('iterations') ?? 3),
    warmup: Number(args.warmup ?? pick('warmup') ?? 1),
    keep: Boolean(args.keep || pick('keep')),
    // Precedence: --no-checksum (CLI) > "validateChecksum" (section/shared) > true.
    validateChecksum: args['no-checksum'] ? false : (pick('validateChecksum') ?? true),
    deliveryMode,
    deliveryPath: expandHome(args['delivery-path'] ?? pick('deliveryPath') ?? os.tmpdir()),
    // ordered-stream backpressure: pause new part downloads when the reorder
    // buffer exceeds this many bytes; resume when it drains below half.
    maxBufferedBytes: parseSize(args['max-buffered'] ?? pick('maxBufferedBytes') ?? '2GiB'),
    logConnections: Boolean(args['log-connections'] || pick('logConnections')),
    spreadConnections: Boolean(args['spread-connections'] || pick('spreadConnections')),
    // Precedence: --no-tls (CLI) > "tls" (section/shared) > true.
    tls: args['no-tls'] ? false : (pick('tls') ?? true),
    httpHandler: (args.handler ?? pick('httpHandler') ?? 'node').toLowerCase(),
    // Pin the TLS cipher suite (aes128 | aes256 | chacha20 | default | raw string)
    // to measure per-cipher cost. Graviton3 does AES-128-GCM faster than 256.
    cipher: args.cipher ?? pick('cipher') ?? 'default',
    ciphers: resolveCiphers(args.cipher ?? pick('cipher')),
    // Stall guard: if a part reads no bytes for this many ms, abort and re-fetch
    // it (a slow/stuck connection lands on a fresh S3 front-end on retry). 0 = off.
    stallTimeoutMs: Number(args['stall-timeout'] ?? pick('stallTimeoutMs') ?? 10000),
    partRetries: Number(args['part-retries'] ?? pick('partRetries') ?? 3),
    // ordered-stream time series: sample buffer/in-flight/mem/cpu every 500ms and
    // write a CSV + SVG plot.
    timeseries: Boolean(args.timeseries || pick('timeseries')),
    timeseriesFile: args['timeseries-file'] ?? pick('timeseriesFile') ?? null,
    // Per-part download-time CSV + latency percentiles (all delivery modes).
    partTimes: Boolean(args['part-times'] || pick('partTimes')),
    partTimesFile: args['part-times-file'] ?? pick('partTimesFile') ?? null,
    // ordered-stream memory strategy: copy chunks into reused contiguous part
    // buffers instead of retaining raw chunk arrays (A/B GC/RSS vs copy cost).
    bufferPool: Boolean(args['buffer-pool'] || pick('bufferPool')),
    // ordered-stream: throttle the consumer to this many bytes/sec per object (0 =
    // unlimited) to model a slow reader and exercise backpressure end-to-end.
    consumerRate: parseSize(args['consumer-rate'] ?? pick('consumerRate') ?? '0'),
    // ordered-stream: recycle delivered buffers by transferring them back to the
    // owning worker (bounded memory, zero-copy both ways). --no-buffer-return off.
    bufferReturn: args['no-buffer-return'] ? false : (pick('bufferReturn') ?? true),
    // ordered-stream: per-object stream highWaterMark (both the Readable and its
    // consumer sink). 0 = auto (2 x part size). Bigger lets more in-order parts
    // flush before backpressure pauses the object.
    streamHwm: (args['stream-hwm'] ?? pick('streamHwm')) != null
      ? parseSize(args['stream-hwm'] ?? pick('streamHwm'))
      : 0,
    // file mode: write each part asynchronously (libuv threadpool) so disk-write
    // latency doesn't block the worker's event loop / socket draining.
    fileAsync: Boolean(args['file-async'] || pick('fileAsync')),
    // Diagnostics: CPU-profile each download worker and write a .cpuprofile per
    // worker. Default dir is per-node-version so runs under different nodes don't
    // clobber each other. Analyze with scripts/prof-top.mjs.
    profile: Boolean(args.profile || pick('profile')),
    profileDir: args['profile-dir'] ?? pick('profileDir') ?? `results/profile-${process.version}`,
    // SDK-layer patch: use native zlib.crc32 instead of @aws-crypto/crc32's JS loop.
    nativeCrc32: Boolean(args['native-crc32'] || pick('nativeCrc32')),
    // Live progress indicator (bytes/%/Gbps/ETA to stderr). Off in JSON mode.
    progress: args['no-progress'] ? false : (pick('progress') ?? true),
    ...ipThroughputOpts(args, pick),
    json: Boolean(args.json),
    out: args.out ?? null,
  };
}

const UPLOAD_USAGE = `
S3 multipart UPLOAD benchmark — AWS SDK for JavaScript v3

Uploads objects with parallel UploadPart calls across worker threads (mirrors the
download benchmark). Part boundaries come from "partSize"; per-part checksums use
"checksum". Shares bench.config.json with the download benchmark.

Usage:
  node src/upload-benchmark.js [options]

Options (override bench.config.json):
  --sizes <s1,s2,...>      Object sizes to upload; each may carry a per-size file
                           count as <size>:<count> (default 1). All files of a size
                           are uploaded together (pooled). e.g. 100MiB,1GiB:4,30GiB:2
  --bucket <name>          S3 bucket name.
  --prefix <p>             Key prefix (keys are <prefix><size>.bin).
  --region <region>        AWS region.
  --part-size <size>       Multipart part size (default 64MiB).
  --checksum <algo>        Per-part checksum (CRC32C/CRC32/SHA256/SHA1).
  --source <mode>          Upload data source: memory | file | stream. Default: memory.
                           memory = upload from a pre-built in-memory buffer
                                    (buffer creation is excluded from timing)
                           file   = read each part from a local file (measures
                                    disk read + upload); a temp file is created
                                    up front, untimed, and removed afterward.
                           stream = customer hands one Readable per object to main;
                                    main carves+fills parts and transfers them (zero
                                    copy) to a pool of uploader threads that
                                    UploadPart in parallel, out of order.
                           open   = customer passes a re-openable source descriptor
                                    (factory); each worker opens its OWN stream for
                                    its part range (distributes ingress across cores).
                           open-stream = carver threads open whole-object streams and
                                    carve parts; a separate uploader pool uploads them.
  --open-module <path>     ('open'/'open-stream' source) opener module. 'open':
                           open(params,{start,size}); 'open-stream': open(params,
                           {key,size}) -> one whole-object Readable.
  --carvers <n>            (open-stream) number of carver threads (0 = one per object).
  --open-type <type>       ('open'/'open-stream' built-in opener) file | memory.
                           memory = generate bytes in-memory (no disk), fastest ingress.
  --source-path <dir>      Directory for the 'file'/'open' source temp file.
  --max-buffered <size>    (stream) cap on carved-but-unsent parts held on main
                           (dispatch queue). 0 = auto ((workers*concurrency+1) parts).
  --client-rate <size>     (stream) throttle the simulated customer stream to size
                           bytes/sec per object. 0 = as fast as possible.
  --client-chunk <size>    (stream) chunk size the simulated customer stream pushes.
                           Smaller = more realistic, more ingress overhead. Default 1MiB.
  --cipher <name>          Pin the TLS cipher: aes128 | aes256 | chacha20 | default
                           | <raw OpenSSL string>. Pins across TLS 1.3 and 1.2.
  --workers <n>            Worker threads. Default: CPU count.
  --concurrency <n>        Concurrent UploadPart calls PER worker. Default: 4.
  --iterations <n>         Measured iterations per size. Default: 3.
  --warmup <n>             Warmup iterations (not counted). Default: 1.
  --force                  Upload even if an object of matching size+partSize exists
                           (overrides "forceUpload" in bench.config.json).
  --json                   Emit machine-readable JSON results to stdout.
  --out <file>             Also write JSON results to <file>.
  --help                   Show this help.

Note: worker spawn + data generation happen first, UNTIMED. The measured window
then spans the whole multipart lifecycle: CreateMultipartUpload -> parallel
UploadPart -> CompleteMultipartUpload, so throughput is end-to-end. The completed
object is left in the bucket, so it doubles as seed data for the download benchmark.
`;

/**
 * Parse args for the upload benchmark. Precedence: CLI flag > bench.config.json >
 * built-in default. Returns the size *labels* (needed to generate data) plus the
 * shared knobs.
 */
export function parseUploadArgs(argv = process.argv.slice(2)) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    const key = a.slice(2);
    if (['force', 'json', 'help', 'spread-connections', 'no-tls', 'ip-throughput', 'native-crc32', 'no-progress'].includes(key)) {
      args[key] = true;
      continue;
    }
    args[key] = argv[++i];
  }

  if (args.help) {
    process.stdout.write(UPLOAD_USAGE);
    process.exit(0);
  }

  // Precedence: CLI flag > "upload" section > shared top-level > built-in default.
  const file = loadFileConfig();
  const pick = (key) => sectionValue(file, 'upload', key);

  const bucket = args.bucket ?? pick('bucket');
  if (!bucket) throw new Error('No bucket set. Add "bucket" to bench.config.json or pass --bucket.');

  const rawSizes = args.sizes
    ? args.sizes.split(',').map((s) => s.trim()).filter(Boolean)
    : pick('sizes');
  if (!rawSizes || !rawSizes.length) {
    throw new Error('Nothing to upload. Set "sizes" in bench.config.json or pass --sizes.');
  }
  const prefix = args.prefix ?? pick('dataPrefix') ?? '';
  const groups = sizeGroups(prefix, rawSizes);

  // memory - reuse one pre-filled buffer per worker (pure network ceiling)
  // file   - read each part from a shared source file on demand (blocking readSync)
  // stream - the customer hands ONE Readable per object to the main thread; main
  //          reads it, carves + fills part buffers (single-thread ingress), and
  //          TRANSFERS each part (zero-copy) to a pool of uploader worker threads
  //          that UploadPart in parallel, out of order. Models a Transfer-Manager
  //          style API where the customer only ever touches the main thread.
  // open   - the customer passes a re-openable source DESCRIPTOR (factory pattern),
  //          not a live stream; each worker OPENS ITS OWN stream for its part's byte
  //          range and uploads it. Distributes ingress across worker threads (no
  //          single-thread main funnel). Needs a range-addressable source.
  // open-stream - two tiers: CARVER worker threads each open a whole-object stream
  //          (via the opener callback, one object per stream) and carve parts; a
  //          separate UPLOADER worker pool does the UploadParts. Parts flow
  //          carver -> main -> uploader by zero-copy transfer, with credit-based
  //          backpressure. The opener returns one Readable per object (no ranges).
  const uploadSource = args.source ?? pick('uploadSource') ?? 'memory';
  if (!['memory', 'file', 'stream', 'open', 'open-stream'].includes(uploadSource)) {
    throw new Error(`Invalid uploadSource "${uploadSource}". Use memory | file | stream | open | open-stream.`);
  }

  return {
    bucket,
    region: args.region || pick('region') || process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION,
    prefix,
    groups,
    partSize: parseSize(args['part-size'] ?? pick('partSize') ?? '64MiB'),
    checksum: (args.checksum ?? pick('checksum') ?? 'CRC32C').toUpperCase(),
    uploadSource,
    // 'open' source: how each worker opens its own stream. Built-in { type: 'file' }
    // (reads byte ranges of the shared source file) or { module, params } to import a
    // custom opener module. For 'open' the opener is open(params, { start, size });
    // for 'open-stream' it's open(params, { key, size }) returning one whole-object
    // Readable (no ranges — the carver reads it sequentially).
    uploadOpen: pick('uploadOpen') ?? (args['open-module'] ? { module: args['open-module'] } : { type: args['open-type'] ?? 'file' }),
    // open-stream: number of carver threads (each opens whole-object streams and
    // carves parts for the uploader pool). 0 = auto (one per object, capped at that).
    uploadCarvers: Number(args.carvers ?? pick('uploadCarvers') ?? 0),
    // stream source: cap (bytes) on carved-but-not-yet-uploaded parts held on main
    // (the dispatch queue). Bounds memory + gives the uploader pool a surplus to
    // pull from; when full, main pauses reading the customer stream (backpressure).
    // 0 = auto ((workers x concurrency + 1) parts).
    uploadMaxBuffered: (args['max-buffered'] ?? pick('uploadMaxBuffered')) != null
      ? parseSize(args['max-buffered'] ?? pick('uploadMaxBuffered'))
      : 0,
    // stream source: throttle the simulated customer stream to this many bytes/sec
    // per object (models client send rate). 0 = as fast as possible.
    uploadClientRate: (args['client-rate'] ?? pick('uploadClientRate')) != null
      ? parseSize(args['client-rate'] ?? pick('uploadClientRate'))
      : 0,
    // stream source: chunk size the simulated customer stream pushes (models how a
    // client sends). Smaller = more realistic but more per-part event-loop churn on
    // the single-threaded ingress; larger = less overhead. Default 1MiB.
    uploadClientChunk: (args['client-chunk'] ?? pick('uploadClientChunk')) != null
      ? parseSize(args['client-chunk'] ?? pick('uploadClientChunk'))
      : 1 << 20,
    sourcePath: expandHome(args['source-path'] ?? pick('sourcePath') ?? os.tmpdir()),
    spreadConnections: Boolean(args['spread-connections'] || pick('spreadConnections')),
    tls: args['no-tls'] ? false : (pick('tls') ?? true),
    workers: Number(args.workers ?? pick('workers') ?? os.cpus().length),
    concurrency: Number(args.concurrency ?? pick('concurrency') ?? 4),
    iterations: Number(args.iterations ?? pick('iterations') ?? 3),
    warmup: Number(args.warmup ?? pick('warmup') ?? 1),
    // Precedence: --force (CLI) > "forceUpload" (section/shared) > false.
    forceUpload: args.force ? true : Boolean(pick('forceUpload')),
    httpHandler: (args.handler ?? pick('httpHandler') ?? 'node').toLowerCase(),
    // Pin the TLS cipher suite (aes128 | aes256 | chacha20 | default | raw string).
    cipher: args.cipher ?? pick('cipher') ?? 'default',
    ciphers: resolveCiphers(args.cipher ?? pick('cipher')),
    nativeCrc32: Boolean(args['native-crc32'] || pick('nativeCrc32')),
    // Live progress indicator (bytes/%/Gbps/ETA to stderr). Off in JSON mode.
    progress: args['no-progress'] ? false : (pick('progress') ?? true),
    ...ipThroughputOpts(args, pick),
    json: Boolean(args.json),
    out: args.out ?? null,
  };
}
