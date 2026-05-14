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

DST_DIR="${DST_DIR:?DST_DIR is required (where the clustered tables live)}"
SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-/Volumes/ORICO/spark-shuffle}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
DRIVER_MEMORY="${DRIVER_MEMORY:-8g}"

BUNDLE_JAR=$(find "${BENCHMARK_DIR}/.." -path "*/lance-spark-bundle-4.0_2.13/target/lance-spark-bundle-*.jar" -not -name "*sources*" -not -name "*javadoc*" | head -1)
if [ -z "${BUNDLE_JAR}" ]; then
  echo "ERROR: lance-spark-bundle-4.0_2.13 jar not found. Run 'make bundle SPARK_VERSION=4.0 SCALA_VERSION=2.13'."
  exit 1
fi

SPARK_SQL="${SPARK_HOME:?SPARK_HOME is required}/bin/spark-sql"
mkdir -p "${SPARK_LOCAL_DIR}"
mkdir -p "${BENCHMARK_DIR}/logs"

# table:col1,col2,...  — cluster column FIRST in each list so it doubles as the tightness check
ZONEMAPS=(
  "store_sales:ss_sold_date_sk,ss_item_sk,ss_store_sk,ss_customer_sk,ss_addr_sk"
  "catalog_sales:cs_sold_date_sk,cs_item_sk,cs_bill_customer_sk"
  "web_sales:ws_sold_date_sk,ws_item_sk,ws_bill_customer_sk,ws_web_site_sk"
  "inventory:inv_date_sk,inv_item_sk,inv_warehouse_sk"
)

# Assemble all CREATE INDEX statements into one heredoc so we pay JVM/Spark startup once.
SQL_STMTS=""
for entry in "${ZONEMAPS[@]}"; do
  IFS=':' read -r table cols <<<"${entry}"
  IFS=',' read -ra col_array <<<"${cols}"
  for col in "${col_array[@]}"; do
    SQL_STMTS="${SQL_STMTS}
ALTER TABLE lance_default.\`${DST_DIR}/${table}.lance\`
  CREATE INDEX idx_zm_${col} USING zonemap (${col});"
  done
done

LOG="${BENCHMARK_DIR}/logs/build-zonemaps.log"
echo "Started: $(date '+%Y-%m-%d %H:%M:%S')" > "${LOG}"
echo "DST_DIR: ${DST_DIR}" >> "${LOG}"
echo "Zonemaps to build: $(grep -c CREATE <<<"${SQL_STMTS}")" >> "${LOG}"

"${SPARK_SQL}" \
  --master "${SPARK_MASTER}" \
  --driver-memory "${DRIVER_MEMORY}" \
  --jars "${BUNDLE_JAR}" \
  --conf spark.local.dir="${SPARK_LOCAL_DIR}" \
  --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
  --conf spark.sql.catalog.lance_default=org.lance.spark.LanceNamespaceSparkCatalog \
  --conf spark.lance.zonemap.consolidate.enabled=true \
  --conf spark.ui.enabled=true \
  --conf spark.ui.port=4040 \
  --conf spark.driver.extraJavaOptions="-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true" \
  -e "${SQL_STMTS}" \
  >> "${LOG}" 2>&1

echo "Finished: $(date '+%Y-%m-%d %H:%M:%S') exit=$?" >> "${LOG}"
echo "=== Build complete ==="
grep -E "Time taken|FAIL|Error" "${LOG}" | tail -20
