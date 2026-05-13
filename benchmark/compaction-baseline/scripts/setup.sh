#!/usr/bin/env bash
# Wrapper for SetupMain that supplies the --add-opens flags Spark 3.5 needs on Java 17/21.
# Usage:
#   scripts/setup.sh --output /tmp/out --bucket my-bucket [--only-k 1000] [--skip-upload] ...
#
# Requires the shadowJar to be built first:
#   ./gradlew :benchmark:compaction-baseline:shadowJar
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODULE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

JAR="$(ls -1 "$MODULE_DIR/build/libs"/compaction-baseline-*-all.jar 2>/dev/null \
        || ls -1 "$MODULE_DIR/build/libs"/compaction-baseline-*.jar 2>/dev/null \
        | head -1)"
if [[ -z "${JAR:-}" ]]; then
  echo "Couldn't find a shaded jar under $MODULE_DIR/build/libs/." >&2
  echo "Run: ./gradlew :benchmark:compaction-baseline:shadowJar" >&2
  exit 1
fi

# Mirror gradle/extraJvmArgs for JDK 17/21. Spark 3.5 reflects into many internal packages.
JVM_OPTS=(
  --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.math=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/java.text=ALL-UNNAMED
  --add-opens=java.base/java.time=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
  --add-opens=java.base/java.util.regex=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
  --add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED
  --add-opens=java.sql/java.sql=ALL-UNNAMED
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
  --add-opens=java.base/sun.security.action=ALL-UNNAMED
)

exec java "${JVM_OPTS[@]}" -cp "$JAR" \
  org.apache.iceberg.benchmark.compaction.SetupMain "$@"
