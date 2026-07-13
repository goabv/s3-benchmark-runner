/**
 * Minimal dependency-free SVG line-plot for the ordered-stream time series.
 * Renders stacked small-multiple panels (shared time x-axis, per-panel y-scale)
 * so series with different units (MiB vs counts vs %) are each readable. Output
 * is a self-contained .svg that opens in any browser.
 */

const PANELS = [
  { key: 'rssMiB', label: 'RSS (MiB)', color: '#1f77b4' },
  { key: 'bufferedParts', label: 'buffered parts', color: '#ff7f0e' },
  { key: 'inFlight', label: 'in-flight parts', color: '#2ca02c' },
  { key: 'cpuPct', label: 'CPU (% of all cores)', color: '#d62728' },
];

const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

/**
 * @param {Array<{tMs:number, rssMiB:number, bufferedParts:number, inFlight:number, cpuPct:number}>} samples
 * @param {string} title
 * @returns {string} SVG markup
 */
export function renderSvg(samples, title = '') {
  const W = 900;
  const padL = 70;
  const padR = 20;
  const panelH = 140;
  const panelGap = 30;
  const padT = 40;
  const plotW = W - padL - padR;
  const H = padT + PANELS.length * (panelH + panelGap);

  const tMax = samples.length ? samples[samples.length - 1].tMs : 1;
  const tScale = (t) => padL + (tMax > 0 ? (t / tMax) * plotW : 0);

  const parts = [];
  parts.push(
    `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" font-family="sans-serif" font-size="12">`,
  );
  parts.push(`<rect width="${W}" height="${H}" fill="white"/>`);
  if (title) parts.push(`<text x="${padL}" y="24" font-size="15" font-weight="bold">${esc(title)}</text>`);

  PANELS.forEach((panel, i) => {
    const top = padT + i * (panelH + panelGap);
    const bottom = top + panelH;
    const vals = samples.map((s) => s[panel.key] ?? 0);
    const vMax = Math.max(1e-9, ...vals);
    const yScale = (v) => bottom - (v / vMax) * panelH;

    // axes
    parts.push(`<line x1="${padL}" y1="${top}" x2="${padL}" y2="${bottom}" stroke="#999"/>`);
    parts.push(`<line x1="${padL}" y1="${bottom}" x2="${W - padR}" y2="${bottom}" stroke="#999"/>`);
    // y labels (0 and max)
    parts.push(`<text x="${padL - 6}" y="${bottom}" text-anchor="end">0</text>`);
    parts.push(
      `<text x="${padL - 6}" y="${top + 10}" text-anchor="end">${vMax.toFixed(vMax < 10 ? 1 : 0)}</text>`,
    );
    // panel label
    parts.push(`<text x="${padL}" y="${top - 6}" font-weight="bold" fill="${panel.color}">${esc(panel.label)}</text>`);
    // polyline
    const pts = samples.map((s) => `${tScale(s.tMs).toFixed(1)},${yScale(s[panel.key] ?? 0).toFixed(1)}`).join(' ');
    parts.push(`<polyline fill="none" stroke="${panel.color}" stroke-width="1.5" points="${pts}"/>`);
    // x axis label on last panel
    if (i === PANELS.length - 1) {
      parts.push(`<text x="${padL}" y="${bottom + 20}">0s</text>`);
      parts.push(
        `<text x="${W - padR}" y="${bottom + 20}" text-anchor="end">${(tMax / 1000).toFixed(1)}s</text>`,
      );
      parts.push(
        `<text x="${padL + plotW / 2}" y="${bottom + 20}" text-anchor="middle">time</text>`,
      );
    }
  });

  parts.push('</svg>');
  return parts.join('\n');
}
