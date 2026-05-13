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
# Fail fast with a clear message if the build context is the module directory rather than the
# repo root. Without this, the next RUN fails with "./gradlew: not found" which doesn't hint at
# the cause.
RUN test -x ./gradlew || { \
    echo "ERROR: ./gradlew not found in build context." >&2; \
    echo "       The build context must be the iceberg repo root. Re-run from there:" >&2; \
    echo "         docker build -f benchmark/compaction-baseline/Dockerfile -t <tag> ." >&2; \
    exit 1; \
  }
# Iceberg's build.gradle derives the project version from `git describe` against the
# `apache-iceberg-*` tags and uses the `gradle-git-properties` plugin to bake commit metadata
# into the jar. The image's build context intentionally excludes `.git/` (huge, no value here),
# so we pin both a placeholder `version.txt` and a placeholder `iceberg-build.properties` at
# the repo root — both paths short-circuit the git-based codepaths in build.gradle. Nothing
# inside the image consumes the exact values (the jar is only invoked via ENTRYPOINT below).
RUN test -f version.txt || echo "0.0.0-docker" > version.txt
RUN test -f iceberg-build.properties || { \
    printf 'git.branch=docker\n'                          >  iceberg-build.properties; \
    printf 'git.build.version=0.0.0-docker\n'             >> iceberg-build.properties; \
    printf 'git.closest.tag.name=docker\n'                >> iceberg-build.properties; \
    printf 'git.commit.id=0000000000000000000000000000000000000000\n' >> iceberg-build.properties; \
    printf 'git.commit.id.abbrev=0000000\n'               >> iceberg-build.properties; \
    printf 'git.commit.message.short=docker-image-build\n'>> iceberg-build.properties; \
    printf 'git.commit.time=1970-01-01T00:00:00Z\n'       >> iceberg-build.properties; \
    printf 'git.tags=\n'                                  >> iceberg-build.properties; \
  }
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon :benchmark:compaction-baseline:shadowJar -x test

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
# build.gradle's `shadowJar { archiveClassifier.set('') }` means there's no "-all" suffix on
# the output jar — the shadow jar is the only artifact under build/libs/. Match the single jar
# directly rather than via a wildcard that produces no error if it fails to find a match.
COPY --from=build /src/benchmark/compaction-baseline/build/libs/compaction-baseline-*.jar /app/fuzz.jar
# Spark 3.5 reflects deeply into JDK internals on Java 17/21; without these --add-opens flags
# the FuzzMain JVM dies at SparkContext init with an IllegalAccessError on sun.nio.ch.DirectBuffer.
# Kept in lockstep with scripts/fuzz.sh on the host so the in-image and on-host runs use the
# same JVM configuration.
ENTRYPOINT ["java", \
  "--add-opens=java.base/java.io=ALL-UNNAMED", \
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", \
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", \
  "--add-opens=java.base/java.lang=ALL-UNNAMED", \
  "--add-opens=java.base/java.math=ALL-UNNAMED", \
  "--add-opens=java.base/java.net=ALL-UNNAMED", \
  "--add-opens=java.base/java.nio=ALL-UNNAMED", \
  "--add-opens=java.base/java.text=ALL-UNNAMED", \
  "--add-opens=java.base/java.time=ALL-UNNAMED", \
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED", \
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED", \
  "--add-opens=java.base/java.util.regex=ALL-UNNAMED", \
  "--add-opens=java.base/java.util=ALL-UNNAMED", \
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED", \
  "--add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED", \
  "--add-opens=java.sql/java.sql=ALL-UNNAMED", \
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED", \
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", \
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED", \
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED", \
  "-cp", "/app/fuzz.jar", \
  "org.apache.iceberg.benchmark.compaction.FuzzMain"]
