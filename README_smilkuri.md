# minimal-upload.mjs

S3 upload benchmark runner following the [aws-crt-s3-benchmarks](https://github.com/awslabs/aws-crt-s3-benchmarks) runner protocol.

## Prerequisites

```bash
sudo dnf install python3-pip -y
pip3 install boto3 botocore
```

## Step 1: Build the workload these are under workloads folder in aws-crt-s3-benchmarks

Transform the human-authored `.src.json` into a machine-readable `.run.json`:

```bash
cd /home/ec2-user/repo/aws-crt-s3-benchmarks
python3 scripts/build-workloads.py workloads/upload-5GiB-1x-ram.src.json
```

## Step 2: Prep S3 (create bucket + lifecycle rules)

```bash
python3 scripts/prep-s3-files.py \
  --bucket s3-tm-benchmarks \
  --region us-west-2 \
  --files-dir ~/files \
  --workloads workloads/upload-5GiB-1x-ram.run.json
```

## Step 3: Run the benchmark

```bash
cd /home/ec2-user/repo/s3-benchmark-runner
node minimal-upload.mjs s3 \
  /home/ec2-user/repo/aws-crt-s3-benchmarks/workloads/upload-5GiB-1x-ram.run.json \
  -- bucket \
  -- region \
  200
```

### Arguments

| Arg | Description |
|-----|-------------|
| `s3` | S3 client label |
| `<WORKLOAD>` | Path to the `.run.json` workload file |
| `<BUCKET>` | S3 bucket name |
| `<REGION>` | AWS region |
| `<TARGET_THROUGHPUT>` | Target throughput in Gb/s |

## Output

Results are printed to stdout and appended to `smilkuri_results.txt` in this directory.

## Available upload workloads
All the available workloads are under aws-crt-s3-benchmarks/workloads. Example description of files below

| Workload | Description |
|----------|-------------|
| `upload-64KiB-1x-ram.src.json` | Single 64 KiB file |
| `upload-256KiB-10_000x.src.json` | 10,000 × 256 KiB files |
| `upload-5GiB-1x-ram.src.json` | Single 5 GiB file (in-memory) |
| `upload-5GiB-10x.src.json` | 10 × 5 GiB files |
| `upload-30GiB-1x-ram.src.json` | Single 30 GiB file (in-memory) |
