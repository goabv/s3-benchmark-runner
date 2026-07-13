#!/usr/bin/env node
import { Readable } from 'node:stream';
import { randomFillSync } from 'node:crypto';
import { Upload } from '@aws-sdk/lib-storage';
import { HeadObjectCommand } from '@aws-sdk/client-s3';
import { makeClient } from './s3.js';
import { parseSize, formatBytes, loadFileConfig, sectionValue, sizeGroups } from './config.js';
// NOTE: upload-test-data.js seeds objects (single-threaded, lib-storage). To
// *benchmark* upload throughput with worker threads, use src/upload-benchmark.js.

/**
 * Seed a bucket with test objects of various sizes so the download benchmark has
 * something to pull. Data is generated on the fly and streamed via multipart
 * upload, so it never fully materializes in memory or on disk.
 *
 * Objects are uploaded with a per-part checksum algorithm (CRC32C by default),
 * producing a COMPOSITE checksum with one checksum stored per part. The download
 * benchmark then fetches by PartNumber, and the SDK validates each part's CRC32C.
 *
 * The chosen --part-size defines the part boundaries the benchmark will download
 * against (30GiB / 64MiB = 480 parts, for example).
 *
 * Usage:
 *   node src/upload-test-data.js --bucket <name> [--sizes 30GiB]
 *       [--prefix bench/] [--region <region>] [--part-size 64MiB]
 *       [--checksum CRC32C|CRC32|SHA256|SHA1]
 */

function parseArgs(argv = process.argv.slice(2)) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    if (!argv[i].startsWith('--')) continue;
    const key = argv[i].slice(2);
    if (key === 'force') {
      args.force = true;
      continue;
    }
    args[key] = argv[++i];
  }

  // Seeding is an upload operation: read the "upload" section, falling back to
  // shared top-level. Precedence: CLI flag > "upload" section > shared > default.
  const file = loadFileConfig();
  const pick = (key) => sectionValue(file, 'upload', key);

  const bucket = args.bucket ?? pick('bucket');
  if (!bucket) {
    console.error('No bucket set. Add "bucket" to bench.config.json or pass --bucket.');
    process.exit(1);
  }

  const rawSizes = args.sizes
    ? args.sizes.split(',').map((s) => s.trim()).filter(Boolean)
    : pick('sizes') ?? ['1MiB', '10MiB', '100MiB', '1GiB', '5GiB'];
  const prefix = args.prefix ?? pick('dataPrefix') ?? '';

  return {
    bucket,
    region: args.region || pick('region') || process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION,
    prefix,
    partSize: parseSize(args['part-size'] ?? pick('partSize') ?? '64MiB'),
    checksum: (args.checksum ?? pick('checksum') ?? 'CRC32C').toUpperCase(),
    // Precedence: --force (CLI) > "forceUpload" (section/shared) > false.
    force: args.force ? true : Boolean(pick('forceUpload')),
    // Groups honor per-size counts (e.g. 1GiB:4) so downloads have enough files.
    groups: sizeGroups(prefix, rawSizes),
  };
}

/**
 * Describe an existing object's total size AND its part layout, or null if it
 * doesn't exist. `firstPartSize` (the byte length of part 1) equals the upload
 * part size for any multipart object with >= 2 parts, so it's what we compare
 * against the configured part size to detect a boundary mismatch.
 */
async function describeExisting(client, bucket, key) {
  let size;
  let checksumAlgo = null;
  try {
    // ChecksumMode: ENABLED surfaces which checksum algorithm the object is stored with.
    const head = await client.send(
      new HeadObjectCommand({ Bucket: bucket, Key: key, ChecksumMode: 'ENABLED' }),
    );
    size = Number(head.ContentLength);
    checksumAlgo = head.ChecksumCRC32C
      ? 'CRC32C'
      : head.ChecksumCRC32
        ? 'CRC32'
        : head.ChecksumSHA256
          ? 'SHA256'
          : head.ChecksumSHA1
            ? 'SHA1'
            : null;
  } catch (err) {
    if (err?.$metadata?.httpStatusCode === 404 || err?.name === 'NotFound') return null;
    throw err;
  }

  // A HEAD with PartNumber=1 exposes the multipart layout. For a non-multipart
  // (single PutObject) object this may 416/omit PartsCount — treat as one part.
  let firstPartSize = size;
  let partsCount = 1;
  try {
    const p1 = await client.send(new HeadObjectCommand({ Bucket: bucket, Key: key, PartNumber: 1 }));
    firstPartSize = Number(p1.ContentLength);
    partsCount = p1.PartsCount ? Number(p1.PartsCount) : 1;
  } catch {
    /* not multipart — keep single-part defaults */
  }
  return { size, firstPartSize, partsCount, checksumAlgo };
}

/** A readable stream of `size` bytes of pseudo-random data (reused buffer). */
function randomStream(size, chunkSize = 8 * 1024 * 1024) {
  const buf = Buffer.allocUnsafe(chunkSize);
  let remaining = size;
  return new Readable({
    read() {
      if (remaining <= 0) {
        this.push(null);
        return;
      }
      const n = Math.min(chunkSize, remaining);
      randomFillSync(buf, 0, n);
      remaining -= n;
      this.push(n === chunkSize ? Buffer.from(buf) : Buffer.from(buf.subarray(0, n)));
    },
  });
}

async function main() {
  const cfg = parseArgs();
  const client = makeClient({ region: cfg.region });

  // Flatten groups into (label, bytes, key) work items — `count` files per size.
  const items = [];
  for (const g of cfg.groups) {
    const bytes = parseSize(g.label);
    for (const key of g.keys) items.push({ label: g.label, bytes, key });
  }

  for (const { label, bytes, key } of items) {
    // Skip re-uploading only when the existing object matches BOTH the expected
    // total size AND the configured part size. If the part size differs, re-upload
    // so you can experiment with different part boundaries just by editing config.
    // Use --force to always re-upload.
    if (!cfg.force) {
      const have = await describeExisting(client, cfg.bucket, key);
      if (have) {
        const expectedFirstPart = Math.min(cfg.partSize, bytes);
        const sizeOk = have.size === bytes;
        const partOk = have.firstPartSize === expectedFirstPart;
        const checksumOk = (have.checksumAlgo ?? null) === (cfg.checksum || null);
        if (sizeOk && partOk && checksumOk) {
          console.log(
            `Skipping ${key} (${formatBytes(bytes)}, part ${formatBytes(have.firstPartSize)}, ` +
              `${have.partsCount} parts, ${have.checksumAlgo ?? 'no'} checksum) — matches config. ` +
              `Use --force to re-upload.`,
          );
          continue;
        }
        if (!sizeOk) {
          console.log(`Re-uploading ${key} — size ${formatBytes(have.size)} != expected ${formatBytes(bytes)}.`);
        } else if (!partOk) {
          console.log(
            `Re-uploading ${key} — part size ${formatBytes(have.firstPartSize)} ` +
              `!= configured ${formatBytes(expectedFirstPart)}.`,
          );
        } else {
          console.log(
            `Re-uploading ${key} — checksum ${have.checksumAlgo ?? 'none'} != configured ${cfg.checksum}.`,
          );
        }
      }
    }

    process.stdout.write(`Uploading ${key} (${formatBytes(bytes)}) ... `);
    const started = performance.now();

    const upload = new Upload({
      client,
      params: {
        Bucket: cfg.bucket,
        Key: key,
        Body: randomStream(bytes),
        // Per-part checksums. For a multipart upload this yields a COMPOSITE
        // checksum with one CRC32C stored per part, retrievable via PartNumber GET.
        ChecksumAlgorithm: cfg.checksum,
      },
      partSize: cfg.partSize,
      queueSize: 8,
    });
    await upload.done();

    const secs = (performance.now() - started) / 1000;
    const parts = Math.ceil(bytes / cfg.partSize);
    console.log(
      `ok in ${secs.toFixed(1)}s (${(bytes / 1048576 / secs).toFixed(1)} MiB/s), ` +
        `${parts} parts x ${formatBytes(cfg.partSize)}, ${cfg.checksum} per-part`,
    );
  }

  client.destroy();
  console.log('\nSeed complete. Keys:');
  for (const { key } of items) console.log(`  ${key}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
