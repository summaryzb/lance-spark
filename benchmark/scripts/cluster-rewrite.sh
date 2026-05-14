#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
#
# Cluster-rewrite TPC-DS fact tables for DFP-eligible zonemap pruning.
#
# Reads each source Lance table, range-partitions globally on its zonemap cluster
# column (so consecutive output fragments cover disjoint slices of the cluster-key
# domain), and writes to <table>_clustered.lance. The caller is responsible for
# swapping the clustered version into place after verifying it (recommended:
# build a zonemap on the new table and confirm per-fragment date span is tight).
#
# Usage:
#   DATA_DIR=/path/to/tpcds-sf-100-lance-2.2 \
#   DST_DIR=/path/to/tpcds-sf-100-lance-clustered \
#   ./cluster-rewrite.sh
#
# Output layout (DST_DIR after this script + symlink setup):
#   - Cluster-rewritten fact tables under their normal names (no _clustered suffix)
#   - Symlinks to unchanged dimension/returns tables from DATA_DIR
#   Effect: queries can point DATA_DIR at either to A/B the two physical layouts.
#
# Environment:
#   DATA_DIR        (required) source Lance table root (contains store_sales.lance etc.)
#   DST_DIR         (required) destination for clustered fact tables; this script writes
#                   <table>.lance (no suffix) directly here.
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

DATA_DIR="${DATA_DIR:?DATA_DIR is required}"
DST_DIR="${DST_DIR:?DST_DIR is required (destination for clustered tables + symlinks)}"
SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-/Volumes/ORICO/spark-shuffle}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
DRIVER_MEMORY="${DRIVER_MEMORY:-8g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-8g}"
mkdir -p "${SPARK_LOCAL_DIR}"

BENCHMARK_JAR="${BENCHMARK_DIR}/target/lance-spark-benchmark.jar"
BUNDLE_JAR=$(find "${BENCHMARK_DIR}/.." -path "*/lance-spark-bundle-4.0_2.13/target/lance-spark-bundle-*.jar" -not -name "*sources*" -not -name "*javadoc*" | head -1)

if [ -z "${BUNDLE_JAR}" ] || [ ! -f "${BENCHMARK_JAR}" ]; then
  echo "ERROR: missing bundle or benchmark jar. Run 'make bundle SPARK_VERSION=4.0 SCALA_VERSION=2.13' and rebuild the benchmark jar."
  exit 1
fi

SPARK_SUBMIT="${SPARK_HOME:?SPARK_HOME is required}/bin/spark-submit"

# Cluster-rewrite plan: (table, sort-column, target-fragments = current-fragment-count)
# Single-column cluster keys per the topic-2 analysis. Target counts match the existing
# fragment counts at sf=100 so the rewritten tables are drop-in replacements (same file
# count, same approximate file sizes). Uses --target-fragments so Lance's max_row_per_file
# default is overridden and each Spark range-partition writes exactly one Lance fragment.
REWRITES=(
  "store_sales:ss_sold_date_sk:234"
  "catalog_sales:cs_sold_date_sk:160"
  "web_sales:ws_sold_date_sk:80"
  "inventory:inv_date_sk:210"
)

mkdir -p "${BENCHMARK_DIR}/logs"
mkdir -p "${DST_DIR}"

# Symlink unchanged tables (dimensions + returns) so the destination is a queryable mirror
# of the source minus the rewritten fact tables. Idempotent: existing links are replaced.
echo "=== Symlinking unchanged tables into ${DST_DIR} ==="
REWRITTEN_NAMES=()
for entry in "${REWRITES[@]}"; do
  IFS=':' read -r r_table _ _ <<<"${entry}"
  REWRITTEN_NAMES+=("${r_table}")
done
for src_path in "${DATA_DIR}"/*.lance; do
  src_name=$(basename "${src_path}" .lance)
  case "${src_name}" in
    *_clustered) continue ;;
  esac
  skip=0
  for r in "${REWRITTEN_NAMES[@]}"; do
    if [ "${src_name}" = "${r}" ]; then
      skip=1; break
    fi
  done
  [ "${skip}" -eq 1 ] && continue
  ln -sfn "${src_path}" "${DST_DIR}/${src_name}.lance"
done

for entry in "${REWRITES[@]}"; do
  IFS=':' read -r table col parts <<<"${entry}"
  SRC="${DATA_DIR}/${table}.lance"
  DST="${DST_DIR}/${table}.lance"
  LOG="${BENCHMARK_DIR}/logs/cluster-rewrite-${table}.log"

  if [ ! -d "${SRC}" ]; then
    echo "[SKIP] ${table}: source ${SRC} not found"
    continue
  fi
  if [ -e "${DST}" ]; then
    echo "[SKIP] ${table}: destination ${DST} already exists. Remove it first to retry."
    continue
  fi

  echo "=== ${table}: cluster by ${col} → ${parts} partitions ==="
  echo "Source: ${SRC}"
  echo "Dest:   ${DST}"
  echo "Log:    ${LOG}"
  echo "Started: $(date '+%Y-%m-%d %H:%M:%S')" | tee "${LOG}"

  start=$(date +%s)
  "${SPARK_SUBMIT}" \
    --class org.lance.spark.benchmark.DfpClusterRebuilder \
    --master "${SPARK_MASTER}" \
    --driver-memory "${DRIVER_MEMORY}" \
    --executor-memory "${EXECUTOR_MEMORY}" \
    --jars "${BUNDLE_JAR}" \
    --conf spark.local.dir="${SPARK_LOCAL_DIR}" \
    --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
    --conf spark.driver.extraJavaOptions="-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true" \
    "${BENCHMARK_JAR}" \
    --src "${SRC}" \
    --dst "${DST}" \
    --sort-by "${col}" \
    --target-fragments "${parts}" \
    >>"${LOG}" 2>&1
  elapsed=$(($(date +%s) - start))
  echo "Finished: $(date '+%Y-%m-%d %H:%M:%S') (${elapsed}s)" | tee -a "${LOG}"
  echo ""
done

echo "=== All rewrites complete ==="
echo "Verify each <table>_clustered.lance has tight per-fragment zonemap min/max"
echo "before swapping the original table aside."
