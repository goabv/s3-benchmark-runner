#!/usr/bin/env node
/**
 * Regression test for ordered-stream backpressure with a slow low-numbered part.
 *
 * Scenario: a multi-part object where part 1 is artificially delayed. Parts 2..N
 * complete first and fill the (tiny) reorder buffer; new dispatch pauses; part 1
 * (dispatched first, always in-flight) eventually completes; the whole chain
 * drains in order. This must finish correctly and NOT hang.
 *
 * Uses the bucket/region from bench.config.json. Seeds a small object, runs the
 * download benchmark as a child process under a hard timeout, then cleans up.
 *
 * Run: node test/ordered-backpressure.test.mjs   (or: npm run test:ordered)
 */
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { S3Client, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { loadFileConfig } from '../src/config.js';

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const cfg = loadFileConfig();
const region = cfg.region || process.env.AWS_REGION;
const bucket = cfg.bucket;
const SIZE = '80MiB';
const PART = '8MiB'; // 80MiB / 8MiB = 10 parts
const SLOW_MS = 3000;
const CAP = '8MiB'; // tiny cap -> forces backpressure
const TIMEOUT_MS = 90_000;

function run(cmd, args, env) {
  return new Promise((resolve) => {
    const child = spawn(cmd, args, { cwd: root, env: { ...process.env, ...env }, shell: false });
    let out = '';
    const onData = (d) => (out += d.toString());
    child.stdout.on('data', onData);
    child.stderr.on('data', onData);
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      resolve({ timedOut: true, code: null, out });
    }, TIMEOUT_MS);
    child.on('exit', (code) => {
      clearTimeout(timer);
      resolve({ timedOut: false, code, out });
    });
  });
}

function assert(cond, msg) {
  if (!cond) {
    console.error(`FAIL: ${msg}`);
    process.exitCode = 1;
    throw new Error(msg);
  }
  console.log(`  ok: ${msg}`);
}

async function main() {
  if (!bucket) throw new Error('No bucket in bench.config.json');
  console.log(`[test] seeding ordtest/80mib.bin (${SIZE} @ ${PART} parts) ...`);
  const seed = await run('node', [
    'src/upload-test-data.js',
    '--sizes', SIZE, '--part-size', PART, '--prefix', 'ordtest/', '--force',
  ]);
  // upload-test-data names keys <size>.bin; rename expectation: ordtest/80mib.bin
  const seededKey = 'ordtest/80mib.bin';
  assert(!seed.timedOut && seed.code === 0, `seed completed (exit ${seed.code})`);

  console.log(`[test] ordered-stream download with part 1 slowed ${SLOW_MS}ms, cap ${CAP} ...`);
  const res = await run(
    'node',
    [
      'src/benchmark.js',
      '--keys', seededKey,
      '--delivery', 'ordered-stream',
      '--max-buffered', CAP,
      '--iterations', '1',
      '--warmup', '0',
      '--workers', '4',
      '--concurrency', '4',
    ],
    { BENCH_SLOW_PART: '1', BENCH_SLOW_MS: String(SLOW_MS) },
  );

  assert(!res.timedOut, `did not hang (completed within ${TIMEOUT_MS}ms)`);
  assert(res.code === 0, `exited cleanly (exit ${res.code})`);
  assert(/10\/10 parts checksum-validated/.test(res.out), 'all 10 parts delivered in order + validated');

  // cleanup
  const s3 = new S3Client({ region });
  await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: seededKey })).catch(() => {});
  s3.destroy();

  if (process.exitCode) console.error('\n[test] FAILED\n' + res.out);
  else console.log('\n[test] PASSED');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
