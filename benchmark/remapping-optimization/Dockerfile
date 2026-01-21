# Multi-stage Dockerfile for running JMH remapping benchmarks
#
# This builds only the iceberg-core JMH benchmarks in an isolated environment
# to ensure consistent, reproducible results without system noise.
#
# Build (from repository root):
#   docker build -t iceberg-jmh-benchmark -f benchmark/remapping-optimization/Dockerfile .
#
# Run:
#   docker run --rm iceberg-jmh-benchmark
#
# Note: The .dockerignore file at the repo root excludes unnecessary files
# (Spark, Flink, etc.) to speed up the build context transfer.
#
# See benchmark/remapping-optimization/README.md for detailed usage.

# =============================================================================
# Stage 1: Build the JMH benchmark jar
# =============================================================================
FROM eclipse-temurin:17-jdk AS builder

WORKDIR /build

# Copy Gradle wrapper first (changes rarely, good for caching)
COPY gradlew gradlew
COPY gradle/wrapper gradle/wrapper
RUN chmod +x gradlew

# Copy build configuration files
COPY build.gradle settings.gradle gradle.properties ./
COPY jmh.gradle ./
COPY baseline.gradle deploy.gradle tasks.gradle ./
COPY .baseline/ .baseline/

# Create version.txt (required since .git is not copied)
RUN echo "1.11.0" > version.txt

# Copy version catalog
COPY gradle/libs.versions.toml gradle/

# Copy only the modules needed for iceberg-core JMH benchmarks
# Core dependencies: api, common, bundled-guava, core
COPY api/ api/
COPY common/ common/
COPY bundled-guava/ bundled-guava/
COPY core/ core/

# Pre-download dependencies (better layer caching)
RUN ./gradlew :iceberg-core:dependencies --no-daemon -q || true

# Build the JMH jar
# Skip tests, formatting checks, and git-dependent tasks
RUN ./gradlew :iceberg-core:jmhJar \
    --no-daemon \
    -x test \
    -x spotlessCheck \
    -x checkstyleMain \
    -x checkstyleTest \
    -x checkstyleJmh \
    -x generateGitProperties

# =============================================================================
# Stage 2: Runtime image with just the JMH jar
# =============================================================================
FROM eclipse-temurin:17-jre AS runtime

WORKDIR /benchmark

# Copy the JMH fat jar from builder
COPY --from=builder /build/core/build/libs/*-jmh.jar ./iceberg-core-jmh.jar

# Copy analysis scripts
COPY benchmark/remapping-optimization/analyze_results.py ./
COPY benchmark/remapping-optimization/visualize_results.py ./

# Install Python for analysis scripts (optional, can be removed to save space)
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    pip3 install --no-cache-dir --break-system-packages pandas matplotlib && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create output directory
RUN mkdir -p /benchmark/results

# Set JVM options for benchmarking
# - Fixed heap to avoid GC variance (2GB is plenty for these benchmarks)
# - Use G1GC for consistent behavior
# Override via: docker run -e JAVA_OPTS="-Xms4g -Xmx4g" ...
ENV JAVA_OPTS="-Xms2g -Xmx2g -XX:+UseG1GC"

# Default entrypoint runs the remapping benchmarks
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar iceberg-core-jmh.jar $0 $@"]

# Default arguments: run RemappingAlgorithmBenchmark with reasonable settings
# Override with: docker run iceberg-jmh-benchmark [jmh-args]
CMD ["RemappingAlgorithmBenchmark", "-wi", "3", "-i", "5", "-f", "2", "-rf", "json", "-rff", "/benchmark/results/results.json"]
