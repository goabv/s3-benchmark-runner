/**
 * Tracks per-connection throughput grouped by remote S3 IP, so we can tell whether
 * connections to some front-end IPs are consistently faster than others.
 *
 * For each socket we record its connect time and read its wire-byte counter
 * (`bytesRead` for downloads, `bytesWritten` for uploads). At snapshot time (once
 * per iteration, just before the client is destroyed) we compute, per IP, the
 * summed bytes and summed connection-milliseconds across its sockets. The main
 * thread turns that into an average per-connection throughput for the IP.
 *
 * Using the socket's own byte counter avoids having to correlate individual
 * requests to sockets through the SDK — the socket already knows how much flowed.
 */
export class IpThroughputTracker {
  /** @param {(socket: import('net').Socket) => number} byteCounter */
  constructor(byteCounter) {
    this.byteCounter = byteCounter;
    this.registry = new Map(); // socket -> { ip, connectMs }
  }

  register(socket, ip) {
    this.registry.set(socket, { ip, connectMs: performance.now() });
    socket.on('close', () => this.registry.delete(socket));
  }

  /** Per-IP totals as [[ip, { bytes, ms, conns }], ...]. Call before destroy(). */
  snapshot() {
    const now = performance.now();
    const ips = new Map();
    for (const [socket, { ip, connectMs }] of this.registry) {
      const bytes = this.byteCounter(socket) || 0;
      const ms = now - connectMs;
      const e = ips.get(ip) || { bytes: 0, ms: 0, conns: 0 };
      e.bytes += bytes;
      e.ms += ms;
      e.conns += 1;
      ips.set(ip, e);
    }
    return [...ips];
  }
}

import { appendFileSync, mkdirSync } from 'node:fs';
import path from 'node:path';

/** Merge one worker's [[ip,{bytes,ms,conns}]] into a per-iteration Map. */
export function mergeIpThroughput(target, arr) {
  if (!arr) return;
  for (const [ip, v] of arr) {
    const e = target.get(ip) || { bytes: 0, ms: 0, conns: 0 };
    e.bytes += v.bytes;
    e.ms += v.ms;
    e.conns += v.conns;
    target.set(ip, e);
  }
}

/** Per-IP average per-connection throughput (Gbps) for one iteration. */
export function ipIterationGbps(map) {
  const out = new Map();
  for (const [ip, v] of map) {
    out.set(ip, { gbps: v.ms > 0 ? (v.bytes * 8) / (v.ms * 1e6) : 0, conns: v.conns });
  }
  return out;
}

/** Append this iteration's per-IP Gbps into a history Map (ip -> {samples, conns}). */
export function accumulateIpSamples(history, iterGbps) {
  for (const [ip, { gbps, conns }] of iterGbps) {
    const e = history.get(ip) || { samples: [], conns: 0 };
    e.samples.push(gbps);
    e.conns = Math.max(e.conns, conns);
    history.set(ip, e);
  }
}

/** Reduce the history into sorted per-IP rows (fastest median first). */
export function summarizeIpHistory(history) {
  const rows = [];
  for (const [ip, { samples, conns }] of history) {
    const s = [...samples].sort((a, b) => a - b);
    rows.push({
      ip,
      conns,
      medianGbps: s[Math.floor(s.length / 2)],
      minGbps: s[0],
      maxGbps: s[s.length - 1],
      samples: samples.map((x) => +x.toFixed(3)),
    });
  }
  rows.sort((a, b) => b.medianGbps - a.medianGbps);
  return rows;
}

export function printIpThroughput(label, rows) {
  if (!rows || !rows.length) return;
  console.log(`per-IP throughput (${label}, per-connection avg over ${rows[0].samples.length} iter):`);
  for (const r of rows) {
    console.log(
      `  ${String(r.ip).padEnd(16)} med ${r.medianGbps.toFixed(3)} Gbps  ` +
        `(min ${r.minGbps.toFixed(3)} / max ${r.maxGbps.toFixed(3)}, ${r.conns} conn)`,
    );
  }
  console.log('');
}

/** Append one JSON line to the ongoing per-IP history file. */
export function appendIpRecord(file, record) {
  mkdirSync(path.dirname(path.resolve(file)), { recursive: true });
  appendFileSync(file, JSON.stringify(record) + '\n');
}
