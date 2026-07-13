import os from 'node:os';

/**
 * Samples process resource usage on an interval so we can report peak/avg memory
 * and peak CPU during the measured window.
 *
 * Worker threads run inside this same OS process, so `process.memoryUsage().rss`
 * (whole-process RSS) and `process.cpuUsage()` (getrusage RUSAGE_SELF — all
 * threads) both account for the workers' memory and CPU, not just the main thread.
 *
 * CPU utilization is reported as a percentage of the whole machine:
 *   coresUsed = cpuTimeDelta / wallTimeDelta      (e.g. 3.5 cores)
 *   cpu%      = coresUsed / cpuCount * 100         (% of all cores)
 */
export class ResourceMonitor {
  constructor(intervalMs = 100) {
    this.intervalMs = intervalMs;
    this.cpuCount = os.cpus().length;
    this.totalMem = os.totalmem();
    this._reset();
  }

  _reset() {
    this.peakRss = 0;
    this.rssSum = 0;
    this.samples = 0;
    this.peakCpuPercent = 0;
    this._timer = null;
    this._lastCpu = null;
    this._lastHr = null;
    this._startCpu = null;
    this._startHr = null;
  }

  start() {
    this._reset();
    const cpu = process.cpuUsage();
    const hr = process.hrtime.bigint();
    // Baselines for the overall average; last-* for per-interval peak.
    this._startCpu = cpu;
    this._startHr = hr;
    this._lastCpu = cpu;
    this._lastHr = hr;
    this._sampleMem();
    this._timer = setInterval(() => this._tick(), this.intervalMs);
    if (this._timer.unref) this._timer.unref(); // don't keep the event loop alive
  }

  _sampleMem() {
    const rss = process.memoryUsage().rss;
    if (rss > this.peakRss) this.peakRss = rss;
    this.rssSum += rss;
    this.samples += 1;
  }

  _tick() {
    this._sampleMem();

    const cpu = process.cpuUsage();
    const nowHr = process.hrtime.bigint();
    const cpuMicros = cpu.user - this._lastCpu.user + (cpu.system - this._lastCpu.system);
    const wallMicros = Number(nowHr - this._lastHr) / 1000; // ns -> us
    if (wallMicros > 0) {
      const coresUsed = cpuMicros / wallMicros;
      const pct = (coresUsed / this.cpuCount) * 100;
      if (pct > this.peakCpuPercent) this.peakCpuPercent = pct;
    }
    this._lastCpu = cpu;
    this._lastHr = nowHr;
  }

  /** Stop sampling and return the collected stats. */
  stop() {
    // Final tick so short runs (fewer than one interval) still get a CPU delta.
    if (this._timer) {
      this._tick();
      clearInterval(this._timer);
      this._timer = null;
    }

    // Average CPU over the whole measured window: total CPU time / total wall time.
    const cpuNow = process.cpuUsage();
    const totalCpuMicros = cpuNow.user - this._startCpu.user + (cpuNow.system - this._startCpu.system);
    const totalWallMicros = Number(process.hrtime.bigint() - this._startHr) / 1000;
    const avgCpuPercent =
      totalWallMicros > 0 ? (totalCpuMicros / totalWallMicros / this.cpuCount) * 100 : 0;

    return {
      peakRssBytes: this.peakRss,
      avgRssBytes: this.samples ? this.rssSum / this.samples : 0,
      peakCpuPercent: this.peakCpuPercent, // % of all cores (machine utilization)
      avgCpuPercent, // % of all cores, averaged over the measured window
      peakMemUtilPercent: this.totalMem ? (this.peakRss / this.totalMem) * 100 : 0,
      cpuCount: this.cpuCount,
      totalMemBytes: this.totalMem,
      samples: this.samples,
    };
  }
}
