#!/bin/bash
#
# 103 TPC-DS benchmark for Lance executor cache evaluation.
#
# Runs up to three passes, each = 103 queries x 1 iteration:
#   baseline: cache OFF (control, app wall without cache)
#   cold:     cache ON, cache dir cleared (first-pass intra-sweep reuse)
#   warm:     cache ON, cache dir preserved (cross-app reuse, simulates re-running)
#
# Workload model: "1 Spark app x 103 queries executed sequentially" (the production
# target). With --iterations=1, cache accumulates within the sweep and early/late
# queries have potentially-overlapping cache entries.
#
# Output lands in a timestamped dir under $RESULTS_ROOT with one log per pass and
# a final summary. Benchmark JVM hangs after SparkContext.stop() due to a non-daemon
# Lance JNI thread; script detects bench completion via the log and force-kills the
# JVM after a grace period to avoid stalling the next pass.
#
# Usage:
#   bench-103-tpcds-cache.sh [baseline|cold|warm|all]     (default: all)
set -euo pipefail

MODE="${1:-all}"

SPARK_HOME=${SPARK_HOME:-/Users/yangjie01/Tools/spark-4.0.2-bin-hadoop3}
REPO_ROOT=${REPO_ROOT:-/Users/yangjie01/SourceCode/git/lance-spark}
BENCH_JAR=${BENCH_JAR:-$REPO_ROOT/benchmark/target/lance-spark-benchmark.jar}
LANCE_JAR=${LANCE_JAR:-/Users/yangjie01/.m2/repository/org/lance/lance-spark-bundle-4.0_2.13/0.4.0-beta.4/lance-spark-bundle-4.0_2.13-0.4.0-beta.4.jar}
DATA_DIR=${DATA_DIR:-/Users/yangjie01/Tools/tpcds-sf-100}
EXEC_CACHE_DIR=${EXEC_CACHE_DIR:-/Users/yangjie01/Tools/lance-cache}
SPARK_LOCAL_DIR=${SPARK_LOCAL_DIR:-/Users/yangjie01/Tools/spark-local}
CACHE_LIMIT_GB=${CACHE_LIMIT_GB:-300}
DRIVER_MEM=${DRIVER_MEM:-8g}
HARD_TIMEOUT_SECS=${HARD_TIMEOUT_SECS:-7200}   # 2h per pass cap
HANG_GRACE_SECS=${HANG_GRACE_SECS:-30}         # after "Benchmark Summary", give the reflective bench-end log time to flush
SPARK_UI_ENABLED=${SPARK_UI_ENABLED:-true}     # leave UI on so progress is visible at :4040
SPARK_UI_PORT=${SPARK_UI_PORT:-4040}

# local-cluster mode: separate executor JVMs (closer to production behavior).
# Format: local-cluster[numExecutors, coresPerExecutor, memPerExecutorMB]
NUM_EXECUTORS=${NUM_EXECUTORS:-2}
CORES_PER_EXECUTOR=${CORES_PER_EXECUTOR:-4}
EXECUTOR_MEM_MB=${EXECUTOR_MEM_MB:-30720}      # 30 GB per executor
SPARK_MASTER="local-cluster[${NUM_EXECUTORS},${CORES_PER_EXECUTOR},${EXECUTOR_MEM_MB}]"

RESULTS_ROOT=${RESULTS_ROOT:-/Users/yangjie01/Tools/lance-103-bench-$(date +%Y%m%d_%H%M%S)}
mkdir -p "$RESULTS_ROOT"
echo "Results dir: $RESULTS_ROOT"

for required in "$SPARK_HOME/bin/spark-submit" "$BENCH_JAR" "$LANCE_JAR" "$DATA_DIR"; do
  if [ ! -e "$required" ]; then
    echo "ERROR: missing $required" >&2
    exit 2
  fi
done

JVM_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false -Dio.netty.tryReflectionSetAccessible=true"

run_pass() {
  local label="$1"
  local cache_enabled="$2"
  local clear_cache="$3"
  local log_file="$RESULTS_ROOT/$label.log"
  local csv_dir="$RESULTS_ROOT/$label-csv"
  local time_file="$RESULTS_ROOT/$label.time"
  mkdir -p "$csv_dir"

  if [ "$clear_cache" = "yes" ]; then
    echo "Clearing cache dir: $EXEC_CACHE_DIR"
    rm -rf "$EXEC_CACHE_DIR"/* 2>/dev/null || true
  fi
  echo "Clearing spark local dir: $SPARK_LOCAL_DIR"
  rm -rf "$SPARK_LOCAL_DIR"/* 2>/dev/null || true

  local cache_conf=()
  if [ "$cache_enabled" = "yes" ]; then
    cache_conf=(
      --conf "spark.executorEnv.LANCE_EXEC_CACHE_ENABLED=true"
      --conf "spark.executorEnv.LANCE_EXEC_CACHE_DIR=$EXEC_CACHE_DIR"
      --conf "spark.executorEnv.LANCE_EXEC_CACHE_DISK_LIMIT_GB=$CACHE_LIMIT_GB"
    )
    export LANCE_EXEC_CACHE_ENABLED=true
    export LANCE_EXEC_CACHE_DIR="$EXEC_CACHE_DIR"
    export LANCE_EXEC_CACHE_DISK_LIMIT_GB="$CACHE_LIMIT_GB"
  else
    cache_conf=()
    unset LANCE_EXEC_CACHE_ENABLED 2>/dev/null || true
    unset LANCE_EXEC_CACHE_DIR 2>/dev/null || true
    unset LANCE_EXEC_CACHE_DISK_LIMIT_GB 2>/dev/null || true
  fi

  local start=$(date +%s)
  echo
  echo "=== Pass: $label | cache=$cache_enabled | start=$(date '+%F %T') ==="

  (
    set +e
    "$SPARK_HOME/bin/spark-submit" \
      --class org.lance.spark.benchmark.TpcdsBenchmarkRunner \
      --master "$SPARK_MASTER" \
      --driver-memory "$DRIVER_MEM" \
      --driver-java-options "$JVM_OPTS" \
      --conf spark.executor.extraJavaOptions="$JVM_OPTS" \
      --conf spark.driver.extraClassPath="$LANCE_JAR" \
      --conf spark.executor.extraClassPath="$LANCE_JAR" \
      --conf spark.ui.enabled="$SPARK_UI_ENABLED" \
      --conf spark.ui.port="$SPARK_UI_PORT" \
      --conf spark.sql.adaptive.enabled=false \
      --conf spark.local.dir="$SPARK_LOCAL_DIR" \
      ${cache_conf[@]+"${cache_conf[@]}"} \
      --jars "$LANCE_JAR" \
      "$BENCH_JAR" \
        --data-dir "$DATA_DIR" \
        --results-dir "$csv_dir" \
        --formats lance \
        --iterations 1
  ) 2>&1 | tee "$log_file" &
  local SUBMIT_PID=$!

  # Wait for bench completion. The JVM often hangs post-SparkContext.stop due to a
  # non-daemon Lance JNI thread; detect the summary terminator and force-kill the
  # process after a grace period so the next pass can start.
  local waited=0
  local post_done=0
  while kill -0 "$SUBMIT_PID" 2>/dev/null; do
    if grep -q "=== TPC-DS Benchmark Summary ===" "$log_file" 2>/dev/null; then
      post_done=$((post_done + 5))
      if [ "$post_done" -ge "$HANG_GRACE_SECS" ]; then
        echo "Bench summary seen, forcing JVM exit after ${HANG_GRACE_SECS}s grace."
        pkill -9 -f 'org.apache.spark.deploy.SparkSubmit' 2>/dev/null || true
        pkill -9 -f 'CoarseGrainedExecutorBackend' 2>/dev/null || true
        kill -9 "$SUBMIT_PID" 2>/dev/null || true
        break
      fi
    fi
    sleep 5
    waited=$((waited + 5))
    if [ "$waited" -gt "$HARD_TIMEOUT_SECS" ]; then
      echo "Hard timeout (${HARD_TIMEOUT_SECS}s); killing."
      pkill -9 -f 'org.apache.spark.deploy.SparkSubmit' 2>/dev/null || true
      pkill -9 -f 'CoarseGrainedExecutorBackend' 2>/dev/null || true
      kill -9 "$SUBMIT_PID" 2>/dev/null || true
      break
    fi
  done
  wait "$SUBMIT_PID" 2>/dev/null || true

  local end=$(date +%s)
  local elapsed=$((end - start))
  echo "=== Pass: $label | cache=$cache_enabled | elapsed=${elapsed}s ==="
  echo "elapsed=${elapsed}" > "$time_file"
  echo "start=$(date -r $start '+%F %T')" >> "$time_file"
  echo "end=$(date -r $end '+%F %T')" >> "$time_file"
}

case "$MODE" in
  baseline) run_pass baseline no yes ;;
  cold)     run_pass cold yes yes ;;
  warm)     run_pass warm yes no ;;
  all)
    run_pass baseline no yes
    run_pass cold yes yes
    run_pass warm yes no
    ;;
  *)
    echo "Usage: $0 [baseline|cold|warm|all]" >&2
    exit 1
    ;;
esac

# --- Summary ---
summary_file="$RESULTS_ROOT/SUMMARY.md"
{
  echo "# 103 TPC-DS cache bench summary"
  echo
  echo "Run dir: \`$RESULTS_ROOT\`"
  echo "Cache dir: \`$EXEC_CACHE_DIR\`, limit=${CACHE_LIMIT_GB} GB"
  echo
  echo "| pass | cache | elapsed (s) | queries OK | cache hits | misses | hitRate | entries | diskMB |"
  echo "|------|-------|-------------|------------|------------|--------|---------|---------|--------|"
  for label in baseline cold warm; do
    log="$RESULTS_ROOT/$label.log"
    time_file="$RESULTS_ROOT/$label.time"
    [ -f "$log" ] || continue
    elapsed=$(sed -n 's/^elapsed=//p' "$time_file" 2>/dev/null || echo "?")
    ok=$(grep -cE '^q[0-9]+.*PASS' "$log" 2>/dev/null || echo 0)
    cache_line=$(grep 'LanceExecutorCache\[bench-end\]' "$log" 2>/dev/null | tail -1)
    if [ -z "$cache_line" ]; then
      hits="-"; misses="-"; rate="-"; entries="-"; diskmb="-"
      cache_state="OFF"
    else
      hits=$(echo "$cache_line" | sed -n 's/.*hits=\([0-9]*\).*/\1/p')
      misses=$(echo "$cache_line" | sed -n 's/.*misses=\([0-9]*\).*/\1/p')
      rate=$(echo "$cache_line" | sed -n 's/.*hitRate=\([0-9.]*\).*/\1/p')
      entries=$(echo "$cache_line" | sed -n 's/.*entries=\([0-9]*\).*/\1/p')
      diskmb=$(echo "$cache_line" | sed -n 's/.*diskMB=\([0-9]*\).*/\1/p')
      cache_state="ON"
    fi
    echo "| $label | $cache_state | $elapsed | $ok | $hits | $misses | $rate | $entries | $diskmb |"
  done
  echo
  echo "## Per-query timings"
  for label in baseline cold warm; do
    csv=$(ls "$RESULTS_ROOT/$label-csv"/*.csv 2>/dev/null | head -1)
    [ -n "${csv:-}" ] || continue
    echo
    echo "### $label — \`$(basename "$csv")\`"
    echo '```'
    head -1 "$csv"
    tail -n +2 "$csv"
    echo '```'
  done
} > "$summary_file"

echo
echo "=== Done. Summary: $summary_file ==="
cat "$summary_file" | head -20
