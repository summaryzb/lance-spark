#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
#
# Build all DFP-relevant zonemaps on the cluster-rewritten fact tables.
#
# Each fact table's cluster-column zonemap is built FIRST so it can double as the
# cluster-tightness verification target (separate verify-cluster.scala reads back
# per-fragment min/max from that zonemap). The remaining zonemaps follow in the
# same spark-sql session to avoid re-paying the JVM startup cost.
#
# Uses the consolidated build path
# (spark.lance.zonemap.consolidate.enabled=true) so each index commits exactly
# one IndexMetadata segment.
#
# Usage:
#   DST_DIR=/path/to/tpcds-sf-100-lance-clustered \
#   SPARK_HOME=/path/to/spark-4.0 \
#   ./build-zonemaps.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="${SCRIPT_DIR}/.."

# Configurable Spark/Scala versions (override via environment)
SPARK_VERSION="${SPARK_VERSION:-4.0}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"

DATA_DIR="${DATA_DIR:-${BENCHMARK_DIR}/data}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
DRIVER_MEMORY="${DRIVER_MEMORY:-8g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-8g}"

# Step 1: Build benchmark jar if needed
BENCHMARK_JAR="${BENCHMARK_DIR}/target/lance-spark-benchmark.jar"
if [ ! -f "${BENCHMARK_JAR}" ]; then
  echo "--- Building benchmark jar (Spark ${SPARK_VERSION}, Scala ${SCALA_VERSION}) ---"
  cd "${BENCHMARK_DIR}"
  ../mvnw package -DskipTests -q \
    -Dspark.compat.version="${SPARK_VERSION}" \
    -Dscala.compat.version="${SCALA_VERSION}"
  cd "${SCRIPT_DIR}"
fi

# Step 2: Find the bundle jar
BUNDLE_JAR=$(find "${BENCHMARK_DIR}/.." -path "*/lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION}/target/lance-spark-bundle-*.jar" -not -name "*sources*" -not -name "*javadoc*" | head -1)
if [ -z "${BUNDLE_JAR}" ]; then
  echo "WARNING: lance-spark bundle jar not found. Building it..."
  cd "${BENCHMARK_DIR}/.."
  make bundle SPARK_VERSION="${SPARK_VERSION}" SCALA_VERSION="${SCALA_VERSION}"
  BUNDLE_JAR=$(find "${BENCHMARK_DIR}/.." -path "*/lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION}/target/lance-spark-bundle-*.jar" -not -name "*sources*" -not -name "*javadoc*" | head -1)
  cd "${SCRIPT_DIR}"
fi

# Step 3: Resolve spark-submit
SPARK_SUBMIT="spark-submit"
if [ -n "${SPARK_HOME:-}" ]; then
  SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
fi
echo "Started: $(date '+%Y-%m-%d %H:%M:%S')"

"${SPARK_SUBMIT}" \
  --master "${SPARK_MASTER}" \
  --driver-memory "${DRIVER_MEMORY}" \
  --class org.lance.spark.benchmark.DfpZonemapBuilder \
  --jars "${BUNDLE_JAR}" \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.sql.catalog.lance_default=org.lance.spark.LanceNamespaceSparkCatalog \
  --conf spark.lance.zonemap.consolidate.enabled=true \
  --conf spark.driver.extraJavaOptions="-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true" \
  "${BENCHMARK_JAR}" \
  --data-dir "$DATA_DIR"
