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
 * Builds zonemap indexes on TPC-DS fact tables for DFP (Dynamic Fragment Pruning).
 *
 * <p>Each fact table's cluster-column zonemap is built first so it can double as the
 * cluster-tightness verification target. The remaining zonemaps follow in the same
 * Spark session to avoid re-paying JVM startup cost.
 *
 * <p>Usage:
 * <pre>
 *   spark-submit --class org.lance.spark.benchmark.DfpZonemapBuilder \
 *     benchmark.jar --data-dir /path/to/tpcds-clustered
 * </pre>
 */
public class DfpZonemapBuilder {

  private static final String[][] ZONEMAPS = {
      {"store_sales", "ss_sold_date_sk"},
      {"store_sales", "ss_item_sk"},
      {"store_sales", "ss_store_sk"},
      {"store_sales", "ss_customer_sk"},
      {"store_sales", "ss_addr_sk"},
      {"catalog_sales", "cs_sold_date_sk"},
      {"catalog_sales", "cs_item_sk"},
      {"catalog_sales", "cs_bill_customer_sk"},
      {"web_sales", "ws_sold_date_sk"},
      {"web_sales", "ws_item_sk"},
      {"web_sales", "ws_bill_customer_sk"},
      {"web_sales", "ws_web_site_sk"},
      {"inventory", "inv_date_sk"},
      {"inventory", "inv_item_sk"},
      {"inventory", "inv_warehouse_sk"},
  };

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

    List<String> statements = buildStatements(dataDir);

    SparkSession spark =
        SparkSession.builder().appName("DFP Zonemap Builder").getOrCreate();

    if (!spark.conf().contains("spark.sql.catalog.lance_default")) {
      spark.conf().set("spark.sql.catalog.lance_default",
          "org.lance.spark.LanceNamespaceSparkCatalog");
    }

    try {
      System.out.println("=== DFP Zonemap Builder ===");
      System.out.println("Data dir:          " + dataDir);
      System.out.println("Zonemaps to build: " + statements.size());
      System.out.println();
      System.out.flush();

      int succeeded = 0;
      int failed = 0;

      for (int i = 0; i < statements.size(); i++) {
        String sql = statements.get(i);
        System.out.printf("[%d/%d] %s ...%n", i + 1, statements.size(), sql);
        System.out.flush();

        long start = System.currentTimeMillis();
        try {
          spark.sql(sql);
          long elapsed = System.currentTimeMillis() - start;
          System.out.println("  OK (" + elapsed + "ms)");
          succeeded++;
        } catch (Exception e) {
          long elapsed = System.currentTimeMillis() - start;
          System.out.println("  FAILED (" + elapsed + "ms)");
          System.out.println("  Error: " + e.getMessage());
          failed++;
        }
        System.out.flush();
      }

      System.out.println();
      System.out.println("=== Zonemap build complete ===");
      System.out.println("Succeeded: " + succeeded + ", Failed: " + failed);
      System.out.flush();
    } finally {
      spark.stop();
    }
  }

  private static List<String> buildStatements(String dataDir) {
    List<String> statements = new ArrayList<>(ZONEMAPS.length);
    for (String[] entry : ZONEMAPS) {
      String table = entry[0];
      String col = entry[1];
      statements.add(String.format(
          "ALTER TABLE lance_default.`%s/lance/%s.lance` CREATE INDEX idx_zm_%s USING zonemap (%s)",
          dataDir, table, col, col));
    }
    return statements;
  }

  private static void printUsage() {
    System.err.println(
        "Usage: DfpZonemapBuilder --data-dir <path-to-clustered-tables>");
  }
}
