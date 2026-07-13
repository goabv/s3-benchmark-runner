// Layer 2/3: receive-path throughput over LOOPBACK (no external network), using
// node core http/https — the same download hot path (TLS decrypt + HTTP parse +
// stream drain) the benchmark uses, minus S3/network variance.
//
//   node loopback.mjs [--tls] [--cipher <suite>] [--conns N] [--size BYTES]
//                     [--duration S] [--fresh] [--client node|fetch] [--gc-stats]
//
// --tls          use HTTPS (exercises OpenSSL record crypto); omit for plain HTTP.
// --fresh        disable keep-alive (new TCP+TLS connection per request) -> isolates
//                connect/handshake cost instead of steady-state streaming.
// --client node  node core http/https receive path (default).
// --client fetch global fetch = the BUNDLED undici receive path (version differs
//                across node releases; --fresh not honored, undici pools).
// --gc-stats     also report GC event count / total pause / % wall — to see whether
//                a version's slowdown is GC-driven.
//
// Interpretation across node versions:
//   plain HTTP regresses      -> V8 / stream / http-parser / GC / Buffer path
//   only HTTPS regresses      -> TLS layer (OpenSSL record crypto or handshake)
//   only --fresh regresses    -> handshake/connect cost (OpenSSL)
import http from 'node:http';
import https from 'node:https';
import { execFileSync } from 'node:child_process';
import { readFileSync, mkdtempSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const arg = (n, d) => { const i = process.argv.indexOf('--' + n); return i >= 0 ? process.argv[i + 1] : d; };
const has = (n) => process.argv.includes('--' + n);

const TLS = has('tls');
const FRESH = has('fresh');
const RESP = Number(arg('size', 64 * 1024 * 1024));
const CONNS = Number(arg('conns', 8));
const DURATION = Number(arg('duration', 5));
const CIPHER = arg('cipher', null);
const CLIENT = arg('client', 'node'); // node (core http/https) | fetch (bundled undici)
const GC = has('gc-stats');

// Bundled undici (global fetch) rejects self-signed certs; for the loopback cert
// disable verification for this diagnostic process only.
if (CLIENT === 'fetch' && TLS) process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

// Optional GC accounting to see whether a version's slowdown is GC-driven.
let gcCount = 0, gcTotal = 0, gcMax = 0;
if (GC) {
  const { PerformanceObserver } = await import('node:perf_hooks');
  const obs = new PerformanceObserver((list) => {
    for (const e of list.getEntries()) { gcCount++; gcTotal += e.duration; if (e.duration > gcMax) gcMax = e.duration; }
  });
  obs.observe({ entryTypes: ['gc'] });
}

const payload = Buffer.allocUnsafe(RESP); // reused; server writes it every response

// --- server (loopback) ---
let serverOpts = {};
if (TLS) {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'nodediff-'));
  const keyP = path.join(dir, 'key.pem');
  const crtP = path.join(dir, 'cert.pem');
  try {
    execFileSync('openssl', [
      'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
      '-keyout', keyP, '-out', crtP, '-days', '1', '-subj', '/CN=localhost',
    ], { stdio: 'ignore' });
  } catch {
    console.error('need openssl on PATH to generate a loopback TLS cert; skipping TLS probe');
    process.exit(0);
  }
  serverOpts.key = readFileSync(keyP);
  serverOpts.cert = readFileSync(crtP);
  if (CIPHER) serverOpts.ciphers = CIPHER;
}

const handler = (req, res) => { res.writeHead(200, { 'content-length': String(RESP) }); res.end(payload); };
const server = TLS ? https.createServer(serverOpts, handler) : http.createServer(handler);
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const port = server.address().port;

// --- client ---
let bytes = 0;
let requests = 0;
let negotiated = null;
let stop = false;
let one;
let agent = null;

if (CLIENT === 'fetch') {
  // Bundled-undici receive path (global fetch). Connection pooling/keep-alive is
  // undici's default; --fresh isn't honored here.
  const url = `${TLS ? 'https' : 'http'}://127.0.0.1:${port}/`;
  one = async () => {
    const res = await fetch(url);
    for await (const chunk of res.body) bytes += chunk.length; // web ReadableStream
    requests += 1;
  };
} else {
  // Node core http/https receive path.
  const mod = TLS ? https : http;
  const AgentCtor = TLS ? https.Agent : http.Agent;
  agent = new AgentCtor({
    keepAlive: !FRESH,
    maxSockets: CONNS,
    ...(TLS ? { rejectUnauthorized: false, ...(CIPHER ? { ciphers: CIPHER } : {}) } : {}),
  });
  const opts = {
    host: '127.0.0.1', port, agent,
    ...(TLS ? { rejectUnauthorized: false, ...(CIPHER ? { ciphers: CIPHER } : {}) } : {}),
  };
  one = () => new Promise((resolve, reject) => {
    const rq = mod.get(opts, (rs) => {
      if (TLS && !negotiated && rs.socket?.getCipher) {
        const c = rs.socket.getCipher();
        negotiated = `${rs.socket.getProtocol?.() ?? ''}/${c?.name ?? ''}`;
      }
      rs.on('data', (c) => { bytes += c.length; });
      rs.on('end', () => { requests += 1; resolve(); });
      rs.on('error', reject);
    });
    rq.on('error', reject);
  });
}
async function lane() { while (!stop) { try { await one(); } catch (e) { if (!stop) throw e; break; } } }

const t0 = performance.now();
const timer = setTimeout(() => { stop = true; }, DURATION * 1000);
timer.unref?.();
await Promise.all(Array.from({ length: CONNS }, lane));
const secs = (performance.now() - t0) / 1000;
server.close();
if (agent) agent.destroy();

const gbps = (bytes * 8) / 1e9 / secs;
const mode = `${TLS ? 'https' : 'http'}${FRESH ? '/fresh' : '/keepalive'}/${CLIENT}`;
let line =
  `${mode}\tconns=${CONNS}\treq=${requests}\t${(bytes / (1 << 30)).toFixed(2)} GiB / ${secs.toFixed(1)}s` +
  `\t${gbps.toFixed(2)} Gbps${negotiated ? `\t${negotiated}` : ''}`;
if (GC) {
  line += `\tGC: ${gcCount} events, ${gcTotal.toFixed(0)} ms total (${((gcTotal / (secs * 1000)) * 100).toFixed(1)}% wall), max ${gcMax.toFixed(1)} ms`;
}
console.log(line);
