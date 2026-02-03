# Container-Based Multi-Cloud Benchmark Runner

Run Iceberg benchmarks across AWS, GCP, and Azure from a reproducible container environment.

## Quick Start

```bash
# 1. Build the container
./run.sh build

# 2. Check cloud credentials
./run.sh check

# 3. Set up infrastructure (creates buckets, SSH keys, etc.)
./run.sh setup

# 4. Run benchmarks
./run.sh run

# 5. Monitor progress
./run.sh monitor

# 6. Collect results
./run.sh collect

# 7. Clean up VMs
./run.sh cleanup
```

## What's Included

The container bundles all cloud CLIs with pinned versions:

| Tool | Purpose |
|------|---------|
| AWS CLI v2 | EC2 VMs, S3 storage |
| Google Cloud SDK | Compute Engine VMs, GCS storage |
| Azure CLI | Azure VMs, ADLS Gen2 storage |
| yq | YAML configuration |
| Python 3 + pandas/matplotlib | Result analysis |

## Credential Mounting

The `run.sh` wrapper automatically detects and mounts credentials (read-only):

```
~/.aws           → AWS credentials and config
~/.config/gcloud → GCP application default credentials
~/.azure         → Azure CLI tokens
~/.ssh           → SSH keys for VM access
```

You can also pass credentials via environment variables:

```bash
AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=xxx ./run.sh check
```

## Configuration

Configuration is stored in `output/cloud-config.yaml`. Start with the example:

```bash
cp cloud-config.example.yaml output/cloud-config.yaml
# Edit with your settings, or let 'setup' create them
```

### AWS Configuration

```yaml
aws:
  region: us-west-2
  s3_bucket: my-iceberg-benchmark      # Leave empty for auto-create
  ssh_key_name: iceberg-benchmark      # EC2 key pair name
  instance_type: m5.xlarge             # ~$0.19/hr
```

### GCP Configuration

```yaml
gcp:
  project: my-gcp-project              # Required
  region: us-central1
  zone: us-central1-a
  gcs_bucket: my-iceberg-benchmark     # Leave empty for auto-create
  machine_type: n2-standard-4          # ~$0.19/hr
```

### Azure Configuration

```yaml
azure:
  resource_group: iceberg-benchmark-rg
  location: eastus
  storage_account: ""                  # Leave empty for auto-create
  storage_container: benchmark
  vm_size: Standard_D4s_v3             # ~$0.19/hr
```

## Commands

### `check` - Verify Credentials

Shows the status of each cloud with indicators:

```
$ ./run.sh check

=== Cloud Credential Status ===

Cloud      Status Details
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AWS        ✅  Account: 123456789012 | Region: us-west-2 | Bucket: my-bucket
GCP        ⚠️  Project: my-project | No GCS bucket configured
AZURE      ❌  Not configured

Summary: ✅ 1 ready | ⚠️ 1 warnings | ❌ 1 unable to run
```

### `setup` - Create Infrastructure

Creates missing resources (buckets, SSH keys, IAM profiles):

```bash
# Set up all clouds
./run.sh setup

# Set up specific clouds
./run.sh setup aws gcp
```

### `run` - Start Benchmarks

Starts benchmarks on all configured clouds in parallel:

```bash
# Run with default settings (quick.yaml config)
./run.sh run

# Run with specific config
./run.sh run --config configs/sigmod.yaml

# Run on specific clouds
./run.sh run --clouds aws,gcp
```

### `monitor` - Watch Progress

Real-time monitoring with auto-refresh:

```bash
./run.sh monitor        # Default 10s refresh
./run.sh monitor 5      # 5s refresh
```

### `collect` - Gather Results

Collects all logs and results to `output/results/`:

```bash
./run.sh collect
```

### `cleanup` - Terminate VMs

Stops running VMs and clears state:

```bash
./run.sh cleanup        # All clouds
./run.sh cleanup aws    # Specific cloud
```

### `shell` - Interactive Debug

Opens a shell inside the container:

```bash
./run.sh shell

# Inside container:
aws sts get-caller-identity
gcloud auth list
az account show
```

## Output Structure

All output is written to `output/` (bind-mounted as `/output` in container):

```
output/
├── cloud-config.yaml    # Configuration (auto-generated or user-edited)
├── state/               # Benchmark state tracking
│   ├── aws_status       # per-cloud status
│   ├── aws_timestamp
│   └── ...
├── logs/                # Benchmark logs
│   ├── aws_benchmark_20260128_143052.log
│   ├── gcp_benchmark_20260128_143055.log
│   └── ...
├── results/             # Collected results
│   ├── aws_20260128_143052/
│   │   ├── results.json
│   │   └── benchmark.log
│   ├── summary_20260128_160000.md
│   └── ...
└── ssh/                 # Auto-generated SSH keys
    └── iceberg-benchmark.pem
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Host Machine                              │
│                                                              │
│  ~/.aws ─────────────┐                                       │
│  ~/.config/gcloud ───┼── (read-only mounts)                  │
│  ~/.azure ───────────┤                                       │
│  ~/.ssh ─────────────┘                                       │
│                        ┌────────────────────────────────┐    │
│  ./output/ ────────────┤     Container                  │    │
│  (read-write)          │                                │    │
│                        │  ┌─────────────────────────┐   │    │
│  /path/to/iceberg ─────┼──│  benchmark-runner.sh    │   │    │
│  (read-only)           │  │                         │   │    │
│                        │  │  - AWS CLI v2           │   │    │
│                        │  │  - gcloud SDK           │   │    │
│                        │  │  - Azure CLI            │   │    │
│                        │  │  - yq, jq, python       │   │    │
│                        │  └─────────────────────────┘   │    │
│                        └────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                               │
                               ▼
        ┌──────────────────────┼──────────────────────┐
        │                      │                      │
   ┌────┴────┐           ┌─────┴────┐          ┌─────┴────┐
   │   AWS   │           │   GCP    │          │  Azure   │
   │         │           │          │          │          │
   │ EC2 VM  │           │  GCE VM  │          │ Azure VM │
   │ S3      │           │  GCS     │          │ ADLS Gen2│
   └─────────┘           └──────────┘          └──────────┘
```

## Troubleshooting

### "No credentials found"

Verify credentials exist on host:
```bash
ls -la ~/.aws/credentials
ls -la ~/.config/gcloud/
ls -la ~/.azure/
```

### "Permission denied" on cloud operations

Credentials might be mounted but expired:
```bash
./run.sh shell
aws sts get-caller-identity  # Check AWS
gcloud auth list             # Check GCP
az account show              # Check Azure
```

### Build fails

Try rebuilding with no cache:
```bash
docker build --no-cache -t iceberg-benchmark-runner .
```

### SSH connection issues

Check that your SSH key is readable:
```bash
ls -la ~/.ssh/
chmod 600 ~/.ssh/iceberg-benchmark.pem
```

## Cost Estimates

| Cloud | Instance Type | Hourly Cost | 4-Hour Benchmark |
|-------|---------------|-------------|------------------|
| AWS | m5.xlarge | ~$0.19 | ~$0.76 |
| GCP | n2-standard-4 | ~$0.19 | ~$0.76 |
| Azure | Standard_D4s_v3 | ~$0.19 | ~$0.76 |

Plus storage costs (typically <$0.10 for benchmark data).

## Reproducibility

This container ensures:

1. **Exact CLI versions**: Pinned in Dockerfile, no "works on my machine"
2. **Isolated environment**: No interference from host system packages
3. **Portable state**: Configuration and results in bind-mounted `output/`
4. **Resumable runs**: Interrupt and resume via state files

To reproduce a benchmark run:
```bash
# Copy output/ from previous run
cp -r old-run/output ./output

# Re-run with same config
./run.sh run
```
