# Cloud Benchmark Runner

Lightweight scripts for running Iceberg benchmarks on cloud VMs. Designed for simplicity and reentrance - you can interrupt and resume benchmark runs without losing state.

## Design Principles

- **No Terraform** - Uses cloud CLIs directly for VM lifecycle
- **No Docker** - Runs directly on VM for minimal overhead
- **Reentrant** - State persisted locally; benchmarks run via nohup
- **Simple** - One script per cloud, minimal dependencies

## Infrastructure Setup (One-Time)

Before running benchmarks, set up the cloud infrastructure. This only needs to be done once per cloud account.

### AWS Setup

```bash
# 1. Install AWS CLI
brew install awscli  # macOS
# or: sudo apt-get install awscli  # Ubuntu

# 2. Configure credentials
aws configure
# Enter your AWS Access Key ID, Secret Access Key, and default region

# 3. Create an EC2 key pair for SSH access
aws ec2 create-key-pair \
    --key-name iceberg-benchmark \
    --query 'KeyMaterial' \
    --output text > ~/.ssh/iceberg-benchmark.pem
chmod 600 ~/.ssh/iceberg-benchmark.pem

# 4. Create S3 bucket for benchmark data
aws s3 mb s3://my-iceberg-benchmark --region us-west-2

# 5. (Recommended) Create IAM instance profile for S3 access
# This allows the VM to access S3 without embedding credentials

# Create the trust policy
cat > /tmp/trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "ec2.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

# Create IAM role
aws iam create-role \
    --role-name iceberg-benchmark-role \
    --assume-role-policy-document file:///tmp/trust-policy.json

# Attach S3 access policy
aws iam attach-role-policy \
    --role-name iceberg-benchmark-role \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

# Create instance profile
aws iam create-instance-profile \
    --instance-profile-name iceberg-benchmark-profile

# Add role to instance profile
aws iam add-role-to-instance-profile \
    --instance-profile-name iceberg-benchmark-profile \
    --role-name iceberg-benchmark-role

# Wait for propagation
sleep 10
echo "AWS setup complete"
```

### GCP Setup

```bash
# 1. Install gcloud CLI
brew install google-cloud-sdk  # macOS
# or: see https://cloud.google.com/sdk/docs/install

# 2. Authenticate and set project
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

# 3. Enable required APIs
gcloud services enable compute.googleapis.com
gcloud services enable storage.googleapis.com

# 4. Create GCS bucket for benchmark data
gsutil mb -l us-central1 gs://my-iceberg-benchmark

# 5. Configure SSH (gcloud manages keys automatically)
# Ensure OS Login is enabled or project-wide SSH keys are configured
gcloud compute project-info add-metadata \
    --metadata enable-oslogin=TRUE

echo "GCP setup complete"
```

### Azure Setup

```bash
# 1. Install Azure CLI
brew install azure-cli  # macOS
# or: see https://docs.microsoft.com/cli/azure/install-azure-cli

# 2. Login
az login

# 3. Set subscription (if you have multiple)
az account set --subscription "Your Subscription Name"

# 4. Create resource group
az group create \
    --name iceberg-benchmark-rg \
    --location eastus

# 5. Create storage account and container
az storage account create \
    --name icebergbenchmark$RANDOM \
    --resource-group iceberg-benchmark-rg \
    --location eastus \
    --sku Standard_LRS \
    --kind StorageV2 \
    --hierarchical-namespace true  # Required for ADLS Gen2

# Get the storage account name
STORAGE_ACCOUNT=$(az storage account list \
    --resource-group iceberg-benchmark-rg \
    --query '[0].name' -o tsv)

# Create container
az storage container create \
    --name benchmark \
    --account-name $STORAGE_ACCOUNT

echo "Azure setup complete. Storage account: $STORAGE_ACCOUNT"
```

## Running Benchmarks

### Quick Start (Full Automated Run)

```bash
# AWS
export AWS_S3_BUCKET=my-iceberg-benchmark
export AWS_SSH_KEY_NAME=iceberg-benchmark
./aws/run.sh all --config ../remapping-microbenchmark/configs/quick.yaml

# GCP
export GCP_GCS_BUCKET=my-iceberg-benchmark
./gcp/run.sh all --config ../remapping-microbenchmark/configs/quick.yaml

# Azure
export AZURE_STORAGE_ACCOUNT=icebergbenchmarkXXXX  # your account name
export AZURE_STORAGE_CONTAINER=benchmark
./azure/run.sh all --config ../remapping-microbenchmark/configs/quick.yaml
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

### Running Different Benchmarks

```bash
# Run remapping-microbenchmark (default)
./aws/run.sh all --benchmark remapping-microbenchmark --config ../remapping-microbenchmark/configs/quick.yaml

# Run compaction-cloud benchmark
./aws/run.sh all --benchmark compaction-cloud --config ../compaction-cloud/configs/default.yaml
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
--benchmark NAME       Which benchmark (remapping-microbenchmark or compaction-cloud)
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

### SSH connection refused
- Wait longer for VM startup (cloud-init takes 30-60 seconds)
- Check security group/firewall rules allow SSH (port 22)
- Verify SSH key is correct: `ssh -i ~/.ssh/iceberg-benchmark.pem ubuntu@<ip>`

### Benchmark fails immediately
- Check `benchmark.log` via `tail` command or after `results`
- Verify Java is installed: `./aws/run.sh status` then SSH and run `java -version`
- Check cloud storage permissions

### Permission denied on cloud storage
- **AWS**: Verify IAM instance profile is attached and has S3 permissions
- **GCP**: Verify VM service account has `storage.objectAdmin` role
- **Azure**: Verify managed identity has `Storage Blob Data Contributor` role

### VM already exists error
- Use `--force` to recreate: `./aws/run.sh start --force`
- Or stop first: `./aws/run.sh stop && ./aws/run.sh start`

### "No JAR deployed" error
- Run `deploy` before `run`: `./aws/run.sh deploy --config <config>`

## Cost Management

- VMs are terminated after `all` completes (unless `--keep`)
- Use `stop` to manually terminate
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
