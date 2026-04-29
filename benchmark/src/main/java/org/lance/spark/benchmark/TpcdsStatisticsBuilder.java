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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SparkSession;

/**
 * Spark job that computes column statistics on all 24 TPC-DS tables via
 * {@code ALTER TABLE ... ADD STATICS}.
 *
 * <p>Statistics are used by the Lance scan builder for query optimization
 * (e.g. min/max pushdown, row-count estimation).
 *
 * <p>Usage:
 * <pre>
 *   spark-submit --class org.lance.spark.benchmark.TpcdsStatisticsBuilder \
 *     benchmark.jar \
 *     --data-dir /path/to/tpcds/data
 * </pre>
 */
public class TpcdsStatisticsBuilder {

  public static void main(String[] args) {
    String dataDir = null;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--data-dir":
          dataDir = args[++i];
          break;
        default:
          System.err.println("Unknown argument: " + args[i]);
          printUsage();
          System.exit(1);
      }
    }

    if (dataDir == null) {
      System.err.println("Missing required argument: --data-dir");
      printUsage();
      System.exit(1);
    }

    String lanceRoot = dataDir + "/lance";

    SparkSession spark =
        SparkSession.builder().appName("TPC-DS Statistics").getOrCreate();

    // Ensure lance_default catalog is registered for path-based table loading
    if (!spark.conf().contains("spark.sql.catalog.lance_default")) {
      spark.conf().set("spark.sql.catalog.lance_default",
          "org.lance.spark.LanceNamespaceSparkCatalog");
    }

    try {
      List<String> statements = buildStatisticsStatements(lanceRoot);

      System.out.println("=== TPC-DS Statistics Builder ===");
      System.out.println("Data dir:    " + dataDir);
      System.out.println("Lance root:  " + lanceRoot);
      System.out.println("Tables:      " + statements.size());
      System.out.println();
      System.out.flush();

      int succeeded = 0;
      int failed = 0;

      for (int i = 0; i < statements.size(); i++) {
        String sql = statements.get(i);
        System.out.print("[" + (i + 1) + "/" + statements.size() + "] "
            + sql + " ...");
        System.out.flush();

        long start = System.currentTimeMillis();
        try {
          spark.sql(sql);
          long elapsed = System.currentTimeMillis() - start;
          System.out.println(" OK (" + elapsed + "ms)");
          succeeded++;
        } catch (Exception e) {
          long elapsed = System.currentTimeMillis() - start;
          System.out.println(" FAILED (" + elapsed + "ms)");
          System.out.println("  Error: " + e.getMessage());
          failed++;
        }
        System.out.flush();
      }

      System.out.println();
      System.out.println("=== Statistics creation complete ===");
      System.out.println("Succeeded: " + succeeded + ", Failed: " + failed);
      System.out.flush();
    } finally {
      spark.stop();
    }
  }

  private static List<String> buildStatisticsStatements(String lanceRoot) {
    List<String> statements = new ArrayList<>();
    for (String table : TpcdsDataGenerator.TPCDS_TABLES) {
      String tablePath =
          TpcdsDataGenerator.toLancePath(lanceRoot + "/" + table) + ".lance";
      statements.add("ALTER TABLE lance_default.`" + tablePath + "` ADD STATICS");
    }
    return statements;
  }

  private static void printUsage() {
    System.err.println(
        "Usage: TpcdsStatisticsBuilder --data-dir <path>");
  }
}
