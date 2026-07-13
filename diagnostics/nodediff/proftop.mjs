// Summarize a V8 CPU profile (from the inspector Profiler) into a top-self-time
// table — so you can diff two node versions and see which function regressed
// without loading the profile in a UI.

function shortUrl(url) {
  if (!url) return '';
  if (url.startsWith('node:')) return url; // keep node:internal/... readable
  const i = url.lastIndexOf('/');
  return i >= 0 ? url.slice(i + 1) : url;
}

/** { total(us), rows: [[ "fn\tloc", selfUs ], ...] } sorted desc by self time. */
export function summarizeProfile(profile, topN = 25) {
  const { nodes, samples, timeDeltas } = profile;
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const selfUs = new Map();
  for (let i = 0; i < samples.length; i++) {
    const id = samples[i];
    selfUs.set(id, (selfUs.get(id) || 0) + (timeDeltas[i] || 0));
  }
  const agg = new Map();
  let total = 0;
  for (const [id, us] of selfUs) {
    const n = byId.get(id);
    if (!n) continue;
    const f = n.callFrame;
    const loc = f.url ? `${shortUrl(f.url)}:${(f.lineNumber ?? 0) + 1}` : '';
    const key = `${f.functionName || '(anonymous)'}\t${loc}`;
    agg.set(key, (agg.get(key) || 0) + us);
    total += us;
  }
  const rows = [...agg].sort((a, b) => b[1] - a[1]).slice(0, topN);
  return { total, rows };
}

export function printTop(profile, topN = 25, label = '') {
  const { total, rows } = summarizeProfile(profile, topN);
  console.log(`\n-- CPU profile: top ${topN} by self time${label ? ` [${label}]` : ''} (sampled ${(total / 1000).toFixed(0)} ms) --`);
  console.log('   self ms      %   function @ location');
  for (const [key, us] of rows) {
    const [fn, loc] = key.split('\t');
    console.log(`${(us / 1000).toFixed(1).padStart(10)} ${(us / total * 100).toFixed(1).padStart(6)}%   ${fn}  ${loc}`);
  }
}
