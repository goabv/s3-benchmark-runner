# sdk-tm-comparison

Standalone tooling to benchmark and deep-dive the **official** AWS SDK for JavaScript
Transfer Manager (`@aws-sdk/lib-transfer-manager`) alongside this repo's runner — so you
can see why it performs differently. Nothing here touches the main runner code.

## Contents

- `setup-js-tm-runner.sh` — EC2 setup for the `aws-crt-s3-benchmarks` **s3-benchmark-js**
  runner (PR #119, `smilkuri:js-runner-tm`). It clones the harness + PR branch, drops the
  `@aws-sdk/lib-transfer-manager` `.tgz` artifact into the runner before install, extracts
  the shipped build for reference, and sparse-clones the original TypeScript source for reading.

## Quick start (on EC2)

Two ways to get the SDK Transfer Manager into the runner:

**(A) Prebuilt tgz** — fast, just run it:
```sh
# after ./scripts/pull.sh brings this over
./sdk-tm-comparison/setup-js-tm-runner.sh \
  --tgz ~/s3-bench/aws-sdk-lib-transfer-manager-3.1090.0.tgz
```

**(B) Build from source** — clone `aws-sdk-js-v3`, build `@aws-sdk/lib-transfer-manager`
+ its deps, pack a fresh tgz, and wire it in. Use this to **instrument** the SDK (add
timing/logging) and benchmark your modified build:
```sh
./sdk-tm-comparison/setup-js-tm-runner.sh --build-from-source
# pick a ref whose @aws-sdk/client-s3 is published to npm (peer dep), e.g.:
#   --build-from-source --sdk-ref v3.1090.0
```

Then follow the printed NEXT STEPS to build workloads, prep S3/on-disk files, run the SDK
runner, and profile both runners (`scripts/prof-top.mjs` reads either one's `.cpuprofile`).

### Instrument-and-rebuild loop (mode B)

Edit the source under `~/aws-sdk-js-v3/lib/lib-transfer-manager/src/submodules/`, then:
```sh
cd ~/aws-sdk-js-v3
yarn workspace @aws-sdk/lib-transfer-manager build:include:deps
yarn workspace @aws-sdk/lib-transfer-manager pack --out ~/aws-sdk-lib-transfer-manager-source.tgz
cd ~/aws-crt-s3-benchmarks/runners/s3-benchmark-js
cp ~/aws-sdk-lib-transfer-manager-source.tgz . && yarn install --check-files
```

Note: the monorepo is yarn (berry) + turbo; the first `yarn install` and build are slow
(they bootstrap and build the workspace dep subgraph). Source `main` is version `3.1091.0`;
the runner pulls the matching `@aws-sdk/client-s3` as a peer, so pin `--sdk-ref` to a
published version if you hit a peer-resolution warning.

## Where to read the SDK source

After setup, the original TypeScript lands at
`~/aws-sdk-js-v3/lib/lib-transfer-manager/src/submodules/`:

- `transfer-manager/S3TransferManager.ts` — main class; trace `upload()` / `download()`.
- `transfer-manager/worker-http-handler.ts` — dispatches each HTTP request to a worker
  thread (request/response marshalled across the thread boundary).
- `transfer-manager/chunker.ts`, `join-streams.ts` — part splitting / reassembly.
- `worker/http-request-worker.ts` — worker entry that runs one serialized HTTP request.

The extracted tgz under `~/sdk-tm-shipped/dist-cjs/` is the transpiled build that actually
runs — keep it only as a reference; read the TS source for the deep-dive.
