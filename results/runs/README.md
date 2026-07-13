# Benchmark runs

Each subdirectory is one captured benchmark run, produced by `scripts/run.sh` on
the EC2 instance and committed to git. Loose/scratch output elsewhere under
`results/` is git-ignored; only `results/runs/` is tracked.

Layout of a run directory `results/runs/<timestamp>[-label]/`:

| File | What it is |
|------|------------|
| `config.json` | Exact `bench.config.json` used for the run (snapshot) |
| `env.txt` | Instance type, AZ, node version, CPU/mem, kernel, key network sysctls, `UV_THREADPOOL_SIZE` |
| `download-sweep.json` / `upload-sweep.json` | The benchmark results (also contains SDK/@smithy versions and the full resolved config) |
| `*.csv`, `*.svg` | Any per-part-time / ordered-stream time-series artifacts produced during the run |

Naming: `<YYYYMMDDThhmmss>-<label>` — pass a label as the 2nd arg to `run.sh`
(e.g. `./scripts/run.sh download aes128-spread`) to make runs easy to tell apart.

To reproduce a run, copy its `config.json` back to the project root as
`bench.config.json` and re-run on the same instance type.
