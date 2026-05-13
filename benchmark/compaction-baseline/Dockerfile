# syntax=docker/dockerfile:1
#
# Self-contained image for the FuzzMain harness. Build from the repo root:
#
#   docker build -f benchmark/compaction-baseline/Dockerfile -t iceberg-compaction-baseline .
#
# Run, mounting an output directory:
#
#   docker run --rm -v $PWD/out:/out iceberg-compaction-baseline \
#     --seed-start 0 --seed-count 100 --workers 4 --output /out
#
# The image is intentionally minimal — no S3 credentials are needed because FuzzMain only writes
# to the mounted output directory. RunMain (perf benchmark, S3-backed) is invoked outside Docker.

FROM eclipse-temurin:17-jdk-jammy AS build
WORKDIR /src
COPY . /src
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon :benchmark:compaction-baseline:shadowJar -x test

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /src/benchmark/compaction-baseline/build/libs/compaction-baseline-*-all.jar /app/fuzz.jar
ENTRYPOINT ["java", "-cp", "/app/fuzz.jar", "org.apache.iceberg.benchmark.compaction.FuzzMain"]
