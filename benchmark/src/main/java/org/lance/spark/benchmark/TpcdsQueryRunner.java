/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.benchmark;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.metric.SQLMetric;

public class TpcdsQueryRunner {

  /** SparkConf key for Lance's DFP kill-switch. */
  private static final String SPARK_CONF_RUNTIME_FILTERING_ENABLED =
      "spark.lance.runtime.filtering.enabled";

  /** Metric key surfaced by {@code FragmentsScannedMetric.NAME}. */
  private static final String LANCE_FRAGMENTS_SCANNED_METRIC = "fragmentsScanned";

  private final SparkSession spark;
  private final int iterations;
  private final boolean explain;
  private final QueryMetricsListener metricsListener;
  private final Set<String> queryFilter;
  private final long queryTimeoutMs;

  public TpcdsQueryRunner(SparkSession spark, int iterations) {
    this(spark, iterations, false, null, null, 0L);
  }

  public TpcdsQueryRunner(
      SparkSession spark,
      int iterations,
      boolean explain,
      QueryMetricsListener metricsListener,
      String queriesFilter) {
    this(spark, iterations, explain, metricsListener, queriesFilter, 0L);
  }

  public TpcdsQueryRunner(
      SparkSession spark,
      int iterations,
      boolean explain,
      QueryMetricsListener metricsListener,
      String queriesFilter,
      long queryTimeoutMs) {
    this.spark = spark;
    this.iterations = iterations;
    this.explain = explain;
    this.metricsListener = metricsListener;
    if (queriesFilter != null && !queriesFilter.isEmpty()) {
      this.queryFilter = new HashSet<>(Arrays.asList(queriesFilter.split(",")));
    } else {
      this.queryFilter = null;
    }
    this.queryTimeoutMs = Math.max(0L, queryTimeoutMs);
  }

  /**
   * Run queries for the given format with the DFP flag pinned. For non-Lance formats, {@code
   * dfpMode} is recorded in the results but has no effect on the actual Spark plan (the kill-switch
   * only gates {@code SupportsRuntimeV2Filtering} on Lance scans).
   *
   * @param format the data source format ("lance" or "parquet")
   * @param dfpMode one of {@link BenchmarkResult#DFP_ON}, {@link BenchmarkResult#DFP_OFF}, or
   *     {@link BenchmarkResult#DFP_NA}
   */
  public List<BenchmarkResult> runAllQueries(String format, String dfpMode) {
    List<BenchmarkResult> results = new ArrayList<>();
    List<String> queryNames = getAvailableQueries();
    if (queryFilter != null) {
      queryNames = queryNames.stream().filter(queryFilter::contains).collect(Collectors.toList());
    }

    System.out.println(
        "Running "
            + queryNames.size()
            + " queries x "
            + iterations
            + " iterations for "
            + format
            + " (dfp="
            + dfpMode
            + ")");
    System.out.flush();

    // Pin the DFP flag for the duration of this sweep. Parquet scans ignore it; Lance honors it.
    applyDfpConfig(dfpMode);

    for (String queryName : queryNames) {
      String sql = loadQuery(queryName);
      if (sql == null) {
        continue;
      }

      for (int i = 1; i <= iterations; i++) {
        BenchmarkResult result = runQuery(queryName, format, sql, i, dfpMode);
        results.add(result);

        String status = result.isSuccess() ? "OK" : "FAIL";
        System.out.printf(
            "  [%s] %s dfp=%s iter=%d time=%dms%n",
            status, queryName, dfpMode, i, result.getElapsedMs());
        if (result.getMetrics() != null) {
          System.out.println("       Metrics: " + result.getMetrics().toSummaryString());
        }
        if (!result.isSuccess()) {
          System.out.println("       Error: " + result.getErrorMessage());
        }
        System.out.flush();
      }
    }

    return results;
  }

  /** Back-compat wrapper — preserves the prior single-format signature for callers that don't
   * toggle DFP. Defaults to {@link BenchmarkResult#DFP_NA} so lance runs under this path don't
   * set the flag explicitly (Lance's own default = on).
   */
  public List<BenchmarkResult> runAllQueries(String format) {
    return runAllQueries(format, BenchmarkResult.DFP_NA);
  }

  private void applyDfpConfig(String dfpMode) {
    if (BenchmarkResult.DFP_ON.equals(dfpMode)) {
      spark.conf().set(SPARK_CONF_RUNTIME_FILTERING_ENABLED, "true");
    } else if (BenchmarkResult.DFP_OFF.equals(dfpMode)) {
      spark.conf().set(SPARK_CONF_RUNTIME_FILTERING_ENABLED, "false");
    }
    // DFP_NA: leave the conf untouched so Lance's own default applies.
  }

  private BenchmarkResult runQuery(
      String queryName, String format, String sql, int iteration, String dfpMode) {
    // Split on semicolons to handle multi-statement queries
    String[] statements = sql.split(";");

    // Find the first non-empty statement for EXPLAIN
    String firstStatement = null;
    for (String stmt : statements) {
      String trimmed = stmt.trim();
      if (!trimmed.isEmpty()) {
        firstStatement = trimmed;
        break;
      }
    }

    // Print EXPLAIN on first iteration if enabled
    if (explain && iteration == 1 && firstStatement != null) {
      try {
        System.out.println("  --- EXPLAIN " + queryName + " ---");
        spark.sql("EXPLAIN EXTENDED " + firstStatement).show(false);
        System.out.flush();
      } catch (Exception e) {
        System.out.println("  (EXPLAIN failed: " + e.getMessage() + ")");
      }
    }

    // Set job group and reset metrics listener. interruptOnCancel must be true so the
    // timeout-driven cancelJobGroup actually kills running tasks rather than just marking
    // them for stopping at the next checkpoint.
    String jobGroup = format + "." + queryName + ".iter" + iteration;
    spark.sparkContext().setJobGroup(jobGroup, queryName, queryTimeoutMs > 0L);
    if (metricsListener != null) {
      metricsListener.reset(jobGroup);
    }

    // Schedule a one-shot cancellation after queryTimeoutMs. The job group is the cancellation
    // unit; cancelling it raises an exception inside the spark.sql(...).save() call, which the
    // catch block below recognises via the `timedOut` flag and turns into a "TIMEOUT after Xs"
    // failure record. Daemon thread + shutdownNow in finally so no scheduler leak survives the
    // method.
    ScheduledExecutorService scheduler = null;
    ScheduledFuture<?> timeoutFuture = null;
    final AtomicBoolean timedOut = new AtomicBoolean(false);
    if (queryTimeoutMs > 0L) {
      scheduler =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                Thread t = new Thread(r, "query-timeout-" + queryName + "-iter" + iteration);
                t.setDaemon(true);
                return t;
              });
      timeoutFuture =
          scheduler.schedule(
              () -> {
                timedOut.set(true);
                spark.sparkContext().cancelJobGroup(jobGroup);
              },
              queryTimeoutMs,
              TimeUnit.MILLISECONDS);
    }

    long start = System.currentTimeMillis();
    long fragmentsScanned = -1L;
    try {
      // Walk every statement and sum the Lance fragmentsScanned metric across their executed
      // plans. Multi-statement queries rarely appear in TPC-DS, but when they do we want the
      // aggregate (mirrors what the SQL UI shows across the query).
      for (String stmt : statements) {
        String trimmed = stmt.trim();
        if (trimmed.isEmpty()) {
          continue;
        }
        Dataset<Row> result = spark.sql(trimmed);
        result.write().format("noop").mode("overwrite").save();
        // CustomMetric values travel to the driver via SparkListener events asynchronously.
        // Drain the listener bus before reading SQLMetric.value() so we see the aggregated
        // post-execution value rather than a partially-updated snapshot.
        spark.sparkContext().listenerBus().waitUntilEmpty();
        long stmtFragments = sumMetricAcrossPlan(result.queryExecution().executedPlan());
        if (stmtFragments >= 0) {
          fragmentsScanned = Math.max(0L, fragmentsScanned) + stmtFragments;
        }
      }
      long elapsed = System.currentTimeMillis() - start;

      QueryMetrics metrics = metricsListener != null ? metricsListener.getMetrics() : null;
      if (metrics != null) {
        metrics.setLanceFragmentsScanned(fragmentsScanned);
      }
      return BenchmarkResult.success(queryName, format, iteration, elapsed, metrics, dfpMode);
    } catch (Exception e) {
      long elapsed = System.currentTimeMillis() - start;
      if (timedOut.get()) {
        // Timeout-driven cancellation. Use the configured timeout in the message rather than
        // elapsed time so the user sees the budget they hit (elapsed can be slightly larger
        // because cancellation propagation is asynchronous).
        return BenchmarkResult.failure(
            queryName,
            format,
            iteration,
            elapsed,
            "TIMEOUT after " + (queryTimeoutMs / 1000L) + "s",
            dfpMode);
      }
      String msg = e.getMessage();
      if (msg != null && msg.length() > 200) {
        msg = msg.substring(0, 200) + "...";
      }
      return BenchmarkResult.failure(queryName, format, iteration, elapsed, msg, dfpMode);
    } finally {
      if (timeoutFuture != null) {
        timeoutFuture.cancel(false);
      }
      if (scheduler != null) {
        scheduler.shutdownNow();
      }
      spark.sparkContext().clearJobGroup();
    }
  }

  /**
   * Sum the {@code fragmentsScanned} SQLMetric from every scan (leaf) node in the executed plan.
   * Non-Lance scans (e.g. Parquet FileScan) don't advertise this metric; the walk returns
   * {@code -1L} in that case so the CSV can distinguish "metric not applicable" from "scan saw
   * zero fragments".
   *
   * <p>Uses {@link SparkPlan#collectLeaves()} rather than recursively walking {@code children()}
   * because the return type of {@code children()} changes between Scala 2.12 and 2.13, which
   * produces {@link NoSuchMethodError} when a jar compiled for one side is run under the other.
   * Scan metrics live on leaf operators only, so visiting leaves is both correct and
   * cross-version-safe.
   *
   * <p>Under AQE (the default in Spark 3.5+ and always-on in 4.x), the outer executed plan is
   * an {@code AdaptiveSparkPlanExec} and its wrapped plan is what contains the real scan leaves.
   * We detect that wrapper by class name (avoiding a compile-time dep on the adaptive package,
   * which shifted between versions) and recurse into its {@code executedPlan()}.
   */
  /**
   * Sum {@code fragmentsScanned} across all scan (leaf) nodes in the executed plan. Returns
   * {@code -1L} when the metric is not present on any leaf (non-Lance scans, or test plans that
   * short-circuit). Zero is a valid observation and is reported as {@code 0}.
   *
   * <p>Visits both the wrapper and the inner plan of any {@code AdaptiveSparkPlanExec} so that
   * whichever side carries the aggregated post-execution SQLMetric contributes.
   */
  private static long sumMetricAcrossPlan(SparkPlan root) {
    long sum = -1L;
    for (SparkPlan leaf : collectAllScanLeaves(root)) {
      scala.collection.Map<String, SQLMetric> metrics = leaf.metrics();
      scala.Option<SQLMetric> opt = metrics.get(LANCE_FRAGMENTS_SCANNED_METRIC);
      if (opt.isDefined()) {
        long v = opt.get().value();
        sum = Math.max(0L, sum) + v;
      }
    }
    return sum;
  }

  private static List<SparkPlan> collectAllScanLeaves(SparkPlan root) {
    List<SparkPlan> out = new ArrayList<>();
    scala.collection.Iterator<SparkPlan> leaves = root.collectLeaves().iterator();
    while (leaves.hasNext()) {
      SparkPlan leaf = leaves.next();
      SparkPlan unwrapped = unwrapAdaptive(leaf);
      if (unwrapped == leaf) {
        out.add(leaf);
      } else {
        out.addAll(collectAllScanLeaves(unwrapped));
      }
    }
    return out;
  }

  /**
   * If {@code node} is an {@code AdaptiveSparkPlanExec}, reach through to its current executed
   * plan; otherwise return it unchanged. Uses reflection so this harness compiles against any
   * Spark 3.x/4.x that has the class (the concrete accessor name has been stable since 3.0 but
   * the package import isn't worth pinning).
   */
  private static SparkPlan unwrapAdaptive(SparkPlan node) {
    String cls = node.getClass().getName();
    if (!cls.endsWith("AdaptiveSparkPlanExec")) {
      return node;
    }
    try {
      java.lang.reflect.Method m = node.getClass().getMethod("executedPlan");
      Object inner = m.invoke(node);
      if (inner instanceof SparkPlan) {
        return (SparkPlan) inner;
      }
    } catch (ReflectiveOperationException ignored) {
      // Fall through and return the wrapper as-is; we'll just miss the metric, not crash.
    }
    return node;
  }

  List<String> getAvailableQueries() {
    List<String> queries = new ArrayList<>();
    for (int i = 1; i <= 99; i++) {
      String name = "q" + i;
      String resourcePath = "/tpcds-queries/" + name + ".sql";
      if (getClass().getResourceAsStream(resourcePath) != null) {
        queries.add(name);
      }
      // Check for a/b variants (e.g., q14a, q14b)
      for (String suffix : new String[] {"a", "b"}) {
        String variantName = "q" + i + suffix;
        String variantPath = "/tpcds-queries/" + variantName + ".sql";
        if (getClass().getResourceAsStream(variantPath) != null) {
          queries.add(variantName);
        }
      }
    }
    return queries;
  }

  private String loadQuery(String queryName) {
    String resourcePath = "/tpcds-queries/" + queryName + ".sql";
    try (InputStream is = getClass().getResourceAsStream(resourcePath)) {
      if (is == null) {
        return null;
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining("\n"));
      }
    } catch (Exception e) {
      System.err.println("Failed to load query " + queryName + ": " + e.getMessage());
      return null;
    }
  }
}
