#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Multi-cloud CLI container for running Iceberg benchmarks
#
# Includes: AWS CLI v2, Google Cloud SDK, Azure CLI
# Plus: SSH, jq, yq for orchestration
#
# Build:
#   docker build -t iceberg-benchmark-runner -f Dockerfile .
#
# Run:
#   ./benchmark-runner.sh
#
# Or manually:
#   docker run -it --rm \
#     -v ~/.aws:/root/.aws:ro \
#     -v ~/.config/gcloud:/root/.config/gcloud:ro \
#     -v ~/.azure:/root/.azure:ro \
#     -v $(pwd)/output:/output \
#     iceberg-benchmark-runner check

FROM ubuntu:22.04

LABEL maintainer="Apache Iceberg"
LABEL description="Multi-cloud benchmark runner with AWS, GCP, and Azure CLIs"

# Avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# Install base dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Core utilities
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    unzip \
    # SSH for VM access
    openssh-client \
    # JSON/YAML processing
    jq \
    # Python for analysis
    python3 \
    python3-pip \
    # Git for version info
    git \
    && rm -rf /var/lib/apt/lists/*

# =============================================================================
# AWS CLI v2
# =============================================================================
RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip \
    && unzip -q /tmp/awscliv2.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/aws /tmp/awscliv2.zip \
    && aws --version

# =============================================================================
# Google Cloud SDK
# =============================================================================
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    | tee /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-cloud-cli \
    && rm -rf /var/lib/apt/lists/* \
    && gcloud version

# =============================================================================
# Azure CLI
# =============================================================================
RUN curl -fsSL https://aka.ms/InstallAzureCLIDeb | bash \
    && az version

# =============================================================================
# yq (YAML processor)
# =============================================================================
RUN curl -fsSL "https://github.com/mikefarah/yq/releases/download/v4.40.5/yq_linux_amd64" \
    -o /usr/local/bin/yq \
    && chmod +x /usr/local/bin/yq

# =============================================================================
# Python packages for analysis
# =============================================================================
RUN pip3 install --no-cache-dir \
    pyyaml \
    pandas \
    matplotlib

# =============================================================================
# Setup working directory and scripts
# =============================================================================
WORKDIR /runner

# Copy the runner scripts
COPY benchmark-runner.sh /runner/
COPY cloud-config.example.yaml /runner/
RUN chmod +x /runner/benchmark-runner.sh

# Create directories for bind mounts
RUN mkdir -p /output /credentials /workspace

# Environment variables (can be overridden)
ENV OUTPUT_DIR=/output
ENV WORKSPACE_DIR=/workspace

# Default entrypoint
ENTRYPOINT ["/runner/benchmark-runner.sh"]
CMD ["--help"]
