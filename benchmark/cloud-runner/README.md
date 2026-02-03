# Cloud Benchmark Runner

Lightweight scripts for running Iceberg benchmarks on cloud VMs. Designed for simplicity and reentrance - you can interrupt and resume benchmark runs without losing state.

## Design Principles

- **No Terraform** - Uses cloud CLIs directly for VM lifecycle
- **No Docker** - Runs directly on VM for minimal overhead
- **Reentrant** - State persisted locally; benchmarks run via nohup
- **Simple** - One script per cloud, minimal dependencies
- **Separation of concerns** - One-time setup separate from per-run VM lifecycle

## Script Organization

Each cloud provider has three scripts:

| Script | Purpose | When to Run |
|--------|---------|-------------|
| `setup.sh` | Create storage, IAM roles, permissions | Once per cloud account |
| `run.sh` | VM lifecycle, deploy, run benchmarks | Each benchmark run |
| `teardown.sh` | Remove all infrastructure | When done with benchmarks |

## Infrastructure Setup (One-Time)

Before running benchmarks, set up the cloud infrastructure using the `setup.sh` script. This only needs to be done once per cloud account.

### Prerequisites

Install the cloud CLI for your provider:

```bash
# AWS
brew install awscli && aws configure

# GCP
brew install google-cloud-sdk && gcloud auth login && gcloud config set project YOUR_PROJECT

# Azure
brew install azure-cli && az login
```

### Using setup.sh (Recommended)

Each cloud has an automated setup script that creates all required infrastructure:

```bash
# AWS - creates S3 bucket, IAM role, instance profile, EC2 key pair
./aws/setup.sh
# Output: Configuration saved to aws/setup.conf

# GCP - creates GCS bucket, service account, IAM bindings
./gcp/setup.sh --project YOUR_PROJECT
# Output: Configuration saved to gcp/setup.conf

# Azure - creates resource group, storage account, container, RBAC
./azure/setup.sh
# Output: Configuration saved to azure/setup.conf
```

The setup scripts are **idempotent** - safe to run multiple times. They create a `setup.conf` file that `run.sh` will automatically load.

### Setup Options

```bash
# AWS
./aws/setup.sh --region us-east-1 --bucket my-benchmark-bucket

# GCP
./gcp/setup.sh --project my-project --region us-central1

# Azure
./azure/setup.sh --location eastus2 --storage-account mybenchstore
```

### Manual Setup

If you prefer manual setup or need customization, see the setup scripts for the exact resources created. Key requirements:

- **AWS**: S3 bucket, IAM instance profile with S3 access, EC2 key pair
- **GCP**: GCS bucket, service account with objectAdmin, firewall rule for SSH
- **Azure**: Resource group, storage account (ADLS Gen2), container, RBAC for managed identity

## Running Benchmarks

### Quick Start (Full Automated Run)

```bash
# Load configuration from setup.sh (or set env vars manually)
source aws/setup.conf   # or gcp/setup.conf or azure/setup.conf

# AWS
./aws/run.sh all --config ../remapping-microbenchmark/configs/quick.yaml

# GCP
./gcp/run.sh all --config ../remapping-microbenchmark/configs/quick.yaml

# Azure
./azure/run.sh all --config ../remapping-microbenchmark/configs/quick.yaml
```

If you didn't use `setup.sh`, set environment variables manually:

```bash
# AWS
export AWS_S3_BUCKET=my-iceberg-benchmark
export AWS_SSH_KEY_NAME=iceberg-benchmark

# GCP
export GCP_GCS_BUCKET=my-iceberg-benchmark
export GCP_PROJECT=my-project

# Azure
export AZURE_STORAGE_ACCOUNT=icebergbenchmarkXXXX
export AZURE_STORAGE_CONTAINER=benchmark
```

The `all` command runs the complete workflow:
1. Creates VM (or reuses existing)
2. Builds and uploads the benchmark JAR
3. Starts the benchmark
4. Waits for completion
5. Downloads results
6. Terminates VM

### Step-by-Step Run (Manual Control)

For more control, run each step separately:

```bash
# Set environment variables
export AWS_S3_BUCKET=my-iceberg-benchmark
export AWS_SSH_KEY_NAME=iceberg-benchmark

# 1. Create VM
./aws/run.sh start
# Output: Instance running: i-0abc123... (54.x.x.x)

# 2. Deploy benchmark JAR and config
./aws/run.sh deploy --config ../remapping-microbenchmark/configs/sigmod.yaml
# Builds JAR locally, uploads to VM

# 3. Start benchmark (runs in background)
./aws/run.sh run
# Output: Benchmark started (PID saved to benchmark.pid)

# 4. Monitor progress
./aws/run.sh status
# Shows VM state and benchmark progress

# Or tail the log in real-time
./aws/run.sh tail
# Press Ctrl+C to stop tailing (benchmark continues running)

# 5. Download results when complete
./aws/run.sh results
# Results saved to: benchmark/remapping-microbenchmark/results/aws_20260127_143052/

# 6. Cleanup
./aws/run.sh stop
```

### Choosing Instance Types

```bash
# AWS - compute optimized for CPU-bound benchmarks
./aws/run.sh start --instance-type c5.2xlarge

# AWS - memory optimized for large datasets
./aws/run.sh start --instance-type r5.2xlarge

# GCP
./gcp/run.sh start --instance-type n2-standard-8

# Azure
./azure/run.sh start --instance-type Standard_D8s_v3
```

### Keeping VM Running

Use `--keep` to preserve the VM after benchmark completes (useful for debugging or running multiple benchmarks):

```bash
# Run first benchmark, keep VM
./aws/run.sh all --config configs/quick.yaml --keep

# Deploy and run another config on same VM
./aws/run.sh deploy --config configs/sigmod.yaml
./aws/run.sh run

# ... later, cleanup
./aws/run.sh stop
```

## Commands Reference

### Infrastructure Scripts (run once)

| Script | Description |
|--------|-------------|
| `setup.sh` | Create storage, IAM, permissions (idempotent) |
| `teardown.sh` | Remove all infrastructure (destructive!) |

### Benchmark Commands (run.sh)

| Command | Description |
|---------|-------------|
| `start` | Create VM if not exists, return IP |
| `stop` | Terminate VM and cleanup |
| `status` | Show VM state and benchmark progress |
| `deploy` | Build and upload JAR + config to VM |
| `run` | Start benchmark (detached via nohup) |
| `tail` | Tail benchmark log (Ctrl+C to stop) |
| `results` | Download results from VM |
| `all` | Full workflow: start → deploy → run → wait → results → stop |

### Options

```
--instance-type TYPE   VM size (e.g., m5.xlarge, n2-standard-4)
--config FILE          Benchmark config YAML
--benchmark NAME       Which benchmark (remapping-microbenchmark)
--keep                 Don't terminate VM after 'all'
--force                Recreate VM even if exists
```

### Cloud-Specific Options

**AWS:**
```
--region REGION        AWS region (default: us-west-2)
--key-name NAME        EC2 key pair name (overrides AWS_SSH_KEY_NAME)
```

**GCP:**
```
--zone ZONE            GCP zone (default: us-central1-a)
--project PROJECT      GCP project ID (overrides gcloud default)
```

**Azure:**
```
--location LOCATION    Azure location (default: eastus)
--resource-group RG    Resource group name (default: iceberg-benchmark-rg)
```

## Reentrance

The scripts handle interruption gracefully:

1. **VM state** - Instance ID stored in `state/` directory
2. **Benchmark state** - Runs via nohup, survives SSH disconnect
3. **Idempotent** - `start` reuses existing VM, `deploy` overwrites

### Resuming an Interrupted Run

```bash
# Check what's running
./aws/run.sh status

# Tail log to see live output
./aws/run.sh tail
# (Ctrl+C to stop tailing)

# Collect results when done
./aws/run.sh results

# Cleanup
./aws/run.sh stop
```

### Resetting State

If state gets corrupted, delete the state directory:

```bash
rm -rf aws/state/
```

## State Management

Each cloud script maintains state in its `state/` directory:

```
aws/state/
├── instance-id      # EC2 instance ID
├── public-ip        # VM public IP
├── jar-name         # Deployed JAR filename
├── config-name      # Deployed config filename
├── storage-uri      # Cloud storage path for this run
└── run-timestamp    # Timestamp of current run
```

State is gitignored. To reset, delete the `state/` directory.

## Benchmark Output

Results are collected to:
```
benchmark/<benchmark-name>/results/<cloud>_<timestamp>/
├── config.yaml      # Config used
├── results.json     # Raw measurements
├── summary.json     # Aggregated stats
└── benchmark.log    # Full output log
```

### Analyzing Results

```bash
# Generate report
python3 ../remapping-microbenchmark/scripts/analyze-results.py \
    ../remapping-microbenchmark/results/aws_*/results.json \
    -o report.md

# Compare across clouds
python3 ../remapping-microbenchmark/scripts/analyze-results.py \
    ../remapping-microbenchmark/results/*/results.json \
    --compare
```

## Troubleshooting

### Quick Diagnostic Commands

```bash
# Check VM status without full logs
./aws/run.sh status 2>&1 | grep -E "State:|IP:|status:"

# Check benchmark status (minimal output)
ssh -o StrictHostKeyChecking=no user@IP "cat ~/benchmark/status.txt"

# Get last scenario being run
ssh user@IP "grep 'Running scenario' ~/benchmark/benchmark.log | tail -1"

# Check if Java process is running
ssh user@IP "pgrep -f 'java.*benchmark' && echo RUNNING || echo STOPPED"
```

### SSH connection refused
- Wait longer for VM startup (cloud-init takes 30-60 seconds)
- Check security group/firewall rules allow SSH (port 22)
- Verify SSH key: `ssh -i ~/.ssh/iceberg_benchmark_key ubuntu@<ip>` (AWS) or `azureuser@` (Azure)

### Benchmark fails immediately
- Check status: `ssh user@IP "cat ~/benchmark/status.txt"`
- Check last error: `ssh user@IP "tail -20 ~/benchmark/benchmark.log | grep -i error"`
- Verify Java: `ssh user@IP "java -version"`

### Permission denied on cloud storage (403 errors)

**Root cause**: VM doesn't have storage access permissions.

**Quick fix**: Run `setup.sh` which configures permissions automatically:
```bash
./aws/setup.sh    # Creates IAM instance profile with S3 access
./gcp/setup.sh    # Creates service account with GCS objectAdmin
./azure/setup.sh  # Assigns Storage Blob Data Contributor to managed identity
```

**Manual fix by cloud**:
- **AWS**: Attach `iceberg-benchmark-profile` instance profile to EC2
- **GCP**: Ensure VM uses `iceberg-benchmark` service account
- **Azure**: Assign role manually:
  ```bash
  PRINCIPAL_ID=$(az vm identity show -g iceberg-benchmark-rg -n iceberg-benchmark --query principalId -o tsv)
  STORAGE_ID=$(az storage account show -n YOUR_STORAGE_ACCOUNT --query id -o tsv)
  az role assignment create --assignee "$PRINCIPAL_ID" --role "Storage Blob Data Contributor" --scope "$STORAGE_ID"
  ```

### GCP network not found
- Set network explicitly: `export GCP_NETWORK=your-vpc-name`
- Or use default: `gcloud compute networks create default --subnet-mode=auto`

### VM already exists error
- Use `--force` to recreate: `./aws/run.sh start --force`
- Or stop first: `./aws/run.sh stop && ./aws/run.sh start`

### "No JAR deployed" error
- Run `deploy` before `run`: `./aws/run.sh deploy --config <config>`

### Config enum errors (e.g., "Cannot deserialize CloudProvider")
- Use correct enum values in config YAML:
  - AWS: `cloud-provider: AWS_S3` (not `AWS`)
  - GCP: `cloud-provider: GCP_GCS` (not `GCP`)
  - Azure: `cloud-provider: AZURE_BLOB` (not `AZURE`)

## Teardown (Cleanup)

When you're done with benchmarks, use `teardown.sh` to remove all infrastructure:

```bash
# Remove all AWS resources (bucket, IAM role, instance profile, key pair)
./aws/teardown.sh --yes

# Remove all GCP resources (bucket, service account, firewall rule)
./gcp/teardown.sh --yes

# Remove all Azure resources (entire resource group)
./azure/teardown.sh --yes
```

**WARNING**: Teardown scripts delete storage buckets/accounts including all benchmark data!

## Cost Management

- VMs are terminated after `all` completes (unless `--keep`)
- Use `stop` to manually terminate VMs
- Use `teardown.sh` to remove all infrastructure when done
- Check `status` to see if VMs are running
- Use smaller instance types for quick tests (`--instance-type t3.medium`)
- Consider spot/preemptible instances for long benchmarks (not yet implemented)

### Typical Costs (as of 2026)

| Cloud | Instance | Hourly Cost | 4-Hour Benchmark |
|-------|----------|-------------|------------------|
| AWS | m5.xlarge | ~$0.19 | ~$0.76 |
| GCP | n2-standard-4 | ~$0.19 | ~$0.76 |
| Azure | Standard_D4s_v3 | ~$0.19 | ~$0.76 |

Plus storage costs (typically negligible for benchmark data).
