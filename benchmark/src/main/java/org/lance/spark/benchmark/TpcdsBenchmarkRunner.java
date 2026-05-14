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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SparkSession;

/**
 * Runs TPC-DS queries against pre-generated tables and reports results.
 *
 * <p>Tables must already exist under {@code <data-dir>/<format>/} — use
 * {@link TpcdsDataGenerator} to create them first.
 */
public class TpcdsBenchmarkRunner {

  public static void main(String[] args) throws Exception {
    String dataDir = null;
    String resultsDir = null;
    String formatsStr = "lance,parquet";
    int iterations = 3;
    boolean explain = false;
    boolean metrics = false;
    String queries = null;
    // Per-query wall-clock cap. 0 = no timeout. 15-minute default matches the recommendation in
    // benchmark/DFP-WORKFLOW.md — long enough for the heaviest sf=100 queries (q72/q14b) under
    // good plans, short enough that a pathological plan doesn't block the whole sweep.
    long queryTimeoutSeconds = 900L;
    // "default" means do NOT touch spark.lance.runtime.filtering.enabled — Lance's own default
    // (on) applies. "on" / "off" pin the flag. "both" runs each lance query twice, once with
    // DFP on and once with DFP off, so the output CSV can be pivoted into an A/B comparison.
    String dfpMode = "default";

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--data-dir":
          dataDir = args[++i];
          break;
        case "--results-dir":
          resultsDir = args[++i];
          break;
        case "--formats":
          formatsStr = args[++i];
          break;
        case "--iterations":
          iterations = Integer.parseInt(args[++i]);
          break;
        case "--explain":
          explain = true;
          break;
        case "--metrics":
          metrics = true;
          break;
        case "--queries":
          queries = args[++i];
          break;
        case "--query-timeout-seconds":
          queryTimeoutSeconds = Long.parseLong(args[++i]);
          if (queryTimeoutSeconds < 0L) {
            System.err.println("--query-timeout-seconds must be >= 0 (0 disables the timeout)");
            System.exit(1);
          }
          break;
        case "--dfp-mode":
          dfpMode = args[++i];
          if (!dfpMode.equals("on")
              && !dfpMode.equals("off")
              && !dfpMode.equals("both")
              && !dfpMode.equals("default")) {
            System.err.println(
                "Invalid --dfp-mode '" + dfpMode + "'. Expected on|off|both|default.");
            System.exit(1);
          }
          break;
        default:
          System.err.println("Unknown argument: " + args[i]);
          printUsage();
          System.exit(1);
      }
    }

    if (dataDir == null || resultsDir == null) {
      System.err.println("Missing required arguments.");
      printUsage();
      System.exit(1);
    }

    String[] formats = formatsStr.split(",");
    // Build the list of DFP modes to sweep. For non-lance formats the mode has no effect on the
    // plan, but we still iterate so the A/B output retains a complete format × query × mode
    // matrix (the non-lance rows will have identical numbers, which is the point of the control).
    String[] dfpModesToRun =
        dfpMode.equals("both")
            ? new String[] {BenchmarkResult.DFP_ON, BenchmarkResult.DFP_OFF}
            : dfpMode.equals("on")
                ? new String[] {BenchmarkResult.DFP_ON}
                : dfpMode.equals("off")
                    ? new String[] {BenchmarkResult.DFP_OFF}
                    : new String[] {BenchmarkResult.DFP_NA};

    SparkSession spark =
        SparkSession.builder().appName("TPC-DS Benchmark").getOrCreate();

    try {
      // Register metrics listener if requested
      QueryMetricsListener metricsListener = null;
      if (metrics) {
        metricsListener = new QueryMetricsListener();
        spark.sparkContext().addSparkListener(metricsListener);
      }

      TpcdsDataLoader loader = new TpcdsDataLoader(spark, dataDir);
      TpcdsQueryRunner runner =
          new TpcdsQueryRunner(
              spark, iterations, explain, metricsListener, queries, queryTimeoutSeconds * 1000L);
      List<BenchmarkResult> allResults = new ArrayList<>();

      for (String format : formats) {
        format = format.trim();
        // Register pre-generated tables as temp views once per format; the DFP sweep reuses them.
        loader.registerTables(format);

        for (String mode : dfpModesToRun) {
          // For non-lance formats, DFP is always N/A regardless of the requested mode. Record
          // each non-lance run once with DFP_NA rather than duplicating it under on/off, which
          // would produce meaningless duplicate rows.
          String effectiveMode = "lance".equals(format) ? mode : BenchmarkResult.DFP_NA;

          System.out.println();
          System.out.println("=== Format: " + format + " (dfp=" + effectiveMode + ") ===");
          System.out.flush();

          List<BenchmarkResult> formatResults = runner.runAllQueries(format, effectiveMode);
          allResults.addAll(formatResults);

          // For non-lance formats we only need one pass — skip any remaining modes in this
          // format's inner loop.
          if (!"lance".equals(format)) {
            break;
          }
        }

        loader.unregisterTables();
      }

      // Report
      BenchmarkReporter reporter = new BenchmarkReporter(allResults);

      String timestamp =
          LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
      String csvPath = resultsDir + "/tpcds_" + timestamp + ".csv";
      reporter.writeCsv(csvPath);
      reporter.printSummary();

    } finally {
      spark.stop();
    }
  }

  private static void printUsage() {
    System.err.println(
        "Usage: TpcdsBenchmarkRunner"
            + " --data-dir <path>"
            + " --results-dir <path>"
            + " [--formats lance,parquet]"
            + " [--iterations 3]"
            + " [--explain]"
            + " [--metrics]"
            + " [--queries q1,q3,q14a]"
            + " [--query-timeout-seconds 900]"
            + " [--dfp-mode on|off|both|default]");
  }
}
