#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
#
# Cluster-rewrite TPC-DS fact tables for DFP-eligible zonemap pruning.
#
# Reads each source Lance table, range-partitions globally on its zonemap cluster
# column (so consecutive output fragments cover disjoint slices of the cluster-key
# domain), and rewrites it in-place. The program writes to <table>.lance.staging,
# then atomically swaps staging into the original path (backup → rename → cleanup).
#
# Usage:
#   DATA_DIR=/path/to/tpcds-sf-100-lance-2.2 \
#   ./cluster-rewrite.sh
#
# Environment:
#   DATA_DIR        (required) source Lance table root (contains store_sales.lance etc.)
#   SPARK_HOME      (required) Spark 4.0 install
#   SPARK_LOCAL_DIR (optional, default /Volumes/ORICO/spark-shuffle) shuffle / spill dir.
#                   Default points at the external volume so large rewrites don't fill
#                   the system tmp on /.
#   DRIVER_MEMORY   (default 8g)
#   EXECUTOR_MEMORY (default 8g)
#   SPARK_MASTER    (default local[*])

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


echo "=== Cluster rewrite: single-submit batch mode ==="
echo "DATA_DIR: ${DATA_DIR}"
echo "Started:  $(date '+%Y-%m-%d %H:%M:%S')"

"${SPARK_SUBMIT}" \
  --class org.lance.spark.benchmark.DfpClusterRebuilder \
  --master "${SPARK_MASTER}" \
  --driver-memory "${DRIVER_MEMORY}" \
  --executor-memory "${EXECUTOR_MEMORY}" \
  --jars "${BUNDLE_JAR}" \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.driver.extraJavaOptions="-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true" \
  "${BENCHMARK_JAR}" \
  --data-dir "${DATA_DIR}" \
  --task "store_sales:ss_sold_date_sk:234" \
  --task "catalog_sales:cs_sold_date_sk:160" \
  --task "web_sales:ws_sold_date_sk:80" \
  --task "inventory:inv_date_sk:210" 

echo "=== All rewrites complete ==="
