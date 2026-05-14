#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
#
# Run the A/B/C TPC-DS benchmark sweep over the 60 DFP-eligible queries.
#
# Three modes, each pinned to one iteration to keep total wall-clock manageable
# (multiple iterations would mostly measure JIT/cache warmup, not DFP lift):
#
#   Mode A — DATA_DIR + dfp=off + no zonemaps
#            True unoptimized baseline.
#   Mode B — DST_DIR  + dfp=off + 22 zonemaps + 4 clustered facts
#            Isolates the static-pushdown contribution from zonemap+clustering.
#   Mode C — DST_DIR  + dfp=on  + 22 zonemaps + 4 clustered facts
#            Full optimization. C-vs-B delta is the DFP-specific lift.
#
# Each mode writes a separate CSV under RESULTS_DIR.
#
# Usage:
#   DATA_DIR=/path/to/source \
#   DST_DIR=/path/to/clustered \
#   RESULTS_DIR=/path/to/results \
#   SPARK_HOME=/path/to/spark-4.0 \
#   ./run-tpcds-abc.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="${SCRIPT_DIR}/.."

DATA_DIR="${DATA_DIR:?DATA_DIR is required (mode-A source)}"
DST_DIR="${DST_DIR:?DST_DIR is required (mode-B/C clustered+indexed)}"
RESULTS_DIR="${RESULTS_DIR:-${BENCHMARK_DIR}/results}"
SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-/Volumes/ORICO/spark-shuffle}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
DRIVER_MEMORY="${DRIVER_MEMORY:-16g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-16g}"
QUERY_TIMEOUT_SECONDS="${QUERY_TIMEOUT_SECONDS:-900}"

BUNDLE_JAR=$(find "${BENCHMARK_DIR}/.." -path "*/lance-spark-bundle-4.0_2.13/target/lance-spark-bundle-*.jar" -not -name "*sources*" -not -name "*javadoc*" | head -1)
BENCHMARK_JAR="${BENCHMARK_DIR}/target/lance-spark-benchmark.jar"
if [ -z "${BUNDLE_JAR}" ] || [ ! -f "${BENCHMARK_JAR}" ]; then
  echo "ERROR: missing bundle or benchmark jar." >&2
  exit 1
fi
SPARK_SUBMIT="${SPARK_HOME:?SPARK_HOME is required}/bin/spark-submit"

mkdir -p "${RESULTS_DIR}" "${SPARK_LOCAL_DIR}" "${BENCHMARK_DIR}/logs"

# 60 DFP-eligible queries (List A — see DFP-WORKFLOW.md). Narrow date_dim predicate
# joining a clustered fact table.
QUERIES="q3,q6,q7,q8,q10,q12,q13,q15,q17,q18,q19,q20,q21,q22,q23a,q23b,q25,q26,q27,q29,q31,q32,q33,q36,q37,q39a,q39b,q42,q43,q45,q48,q49,q52,q53,q54,q55,q56,q57,q58,q59,q60,q61,q63,q65,q67,q68,q69,q70,q71,q72,q73,q79,q82,q85,q86,q87,q89,q92,q97,q98"

run_mode() {
  local label="$1"     # A / B / C
  local data_dir="$2"
  local dfp_mode="$3"  # off / on
  local log="${BENCHMARK_DIR}/logs/benchmark-mode-${label}.log"
  local start_ts
  start_ts=$(date '+%Y-%m-%d %H:%M:%S')
  echo "=================================================================="
  echo "=== MODE ${label}: data-dir=${data_dir} dfp=${dfp_mode}"
  echo "=== Started: ${start_ts}"
  echo "=================================================================="
  echo "Started: ${start_ts}" > "${log}"

  "${SPARK_SUBMIT}" \
    --class org.lance.spark.benchmark.TpcdsBenchmarkRunner \
    --master "${SPARK_MASTER}" \
    --driver-memory "${DRIVER_MEMORY}" \
    --executor-memory "${EXECUTOR_MEMORY}" \
    --jars "${BUNDLE_JAR}" \
    --conf spark.local.dir="${SPARK_LOCAL_DIR}" \
    --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
    --conf spark.ui.enabled=true \
    --conf spark.ui.port=4040 \
    --conf spark.driver.extraJavaOptions="-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true" \
    "${BENCHMARK_JAR}" \
    --data-dir "${data_dir}" \
    --results-dir "${RESULTS_DIR}" \
    --formats lance \
    --iterations 1 \
    --queries "${QUERIES}" \
    --query-timeout-seconds "${QUERY_TIMEOUT_SECONDS}" \
    --dfp-mode "${dfp_mode}" \
    --metrics \
    >>"${log}" 2>&1

  local end_ts
  end_ts=$(date '+%Y-%m-%d %H:%M:%S')
  echo "Finished: ${end_ts} exit=$?" >> "${log}"
  # Rename most recent tpcds_*.csv to embed the mode label
  local latest_csv
  latest_csv=$(ls -t "${RESULTS_DIR}"/tpcds_*.csv 2>/dev/null | head -1)
  if [ -n "${latest_csv}" ]; then
    local renamed="${latest_csv%.csv}-mode-${label}.csv"
    mv "${latest_csv}" "${renamed}"
    echo "=== Output: ${renamed}"
  fi
  echo "=== Mode ${label} finished at ${end_ts}"
  echo ""
}

run_mode A "${DATA_DIR}" off
run_mode B "${DST_DIR}"  off
run_mode C "${DST_DIR}"  on

echo "=================================================================="
echo "=== All three modes done. Results in ${RESULTS_DIR}"
echo "=================================================================="
ls -lh "${RESULTS_DIR}"/tpcds_*.csv 2>/dev/null | tail -10
