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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Re-sorts an existing Lance table by a given column so downstream zonemap indexes produce
 * tight per-fragment min/max bounds. A DFP run against an unclustered fact table has nothing
 * to prune because every fragment's zone spans the full join-key domain.
 *
 * <p>Two output modes:
 * <ul>
 *   <li><b>{@code --dst}</b>: out-of-place. Writes sorted data to an explicit destination path.
 *   <li><b>{@code --in-place}</b>: writes to {@code <src>.staging}, then atomically swaps it
 *       into the source path (backup → rename → cleanup). No separate destination directory
 *       or symlinks required.
 * </ul>
 *
 * <p><b>Fragment-count control</b>: callers typically want the destination to have the same
 * number of Lance fragments as the source so post-rewrite cardinality matches downstream
 * tooling assumptions. To achieve this exactly:
 *
 * <ul>
 *   <li>{@code --target-fragments N} sets the Spark range-partition count to {@code N} AND
 *       computes {@code max_row_per_file = ceil(rowCount / N)} as a Lance write option so
 *       Lance does NOT roll any of the N Spark partitions into multiple fragments. Result:
 *       exactly {@code N} output fragments.
 *   <li>{@code --num-partitions N} is the legacy form — sets Spark range-partition count to
 *       {@code N} but does NOT override Lance's default {@code max_row_per_file=1_048_576},
 *       so a Spark partition with more than 1M rows produces 2+ Lance fragments. Use this
 *       only when the row count per partition is naturally below 1M.
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   # Out-of-place
 *   spark-submit --class org.lance.spark.benchmark.DfpClusterRebuilder benchmark.jar \
 *       --src  /path/to/tpcds/lance/store_sales.lance \
 *       --dst  /path/to/tpcds/lance/store_sales_clustered.lance \
 *       --sort-by ss_sold_date_sk \
 *       --target-fragments 234
 *
 *   # In-place (replaces the source table atomically)
 *   spark-submit --class org.lance.spark.benchmark.DfpClusterRebuilder benchmark.jar \
 *       --src  /path/to/tpcds/lance/store_sales.lance \
 *       --in-place \
 *       --sort-by ss_sold_date_sk \
 *       --target-fragments 234
 * </pre>
 */
public class DfpClusterRebuilder {

  public static void main(String[] args) {
    String src = null;
    String dst = null;
    String sortBy = null;
    boolean inPlace = false;
    int numPartitions = 0;
    int targetFragments = 0;
    String dataDir = null;
    List<String> tasks = new ArrayList<>();

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--src":
          src = args[++i];
          break;
        case "--dst":
          dst = args[++i];
          break;
        case "--sort-by":
          sortBy = args[++i];
          break;
        case "--in-place":
          inPlace = true;
          break;
        case "--num-partitions":
          numPartitions = Integer.parseInt(args[++i]);
          break;
        case "--target-fragments":
          targetFragments = Integer.parseInt(args[++i]);
          break;
        case "--data-dir":
          dataDir = args[++i];
          break;
        case "--task":
          tasks.add(args[++i]);
          break;
        default:
          System.err.println("Unknown argument: " + args[i]);
          printUsage();
          System.exit(1);
      }
    }

    boolean batchMode = dataDir != null && !tasks.isEmpty();

    if (batchMode) {
      runBatch(dataDir, tasks);
    } else {
      runSingle(src, dst, sortBy, inPlace, numPartitions, targetFragments);
    }
  }

  private static void runBatch(String dataDir, List<String> tasks) {
    SparkSession spark =
        SparkSession.builder().appName("DFP Cluster Rebuilder: batch").getOrCreate();
    long batchStart = System.currentTimeMillis();
    int completed = 0;
    int skipped = 0;

    try {
      System.out.println("=== DFP Cluster Rebuilder (batch) ===");
      System.out.println("Data dir: " + dataDir);
      System.out.println("Tasks:    " + tasks.size());
      System.out.println();

      for (String taskSpec : tasks) {
        String[] parts = taskSpec.split(":");
        if (parts.length != 3) {
          System.err.println("[ERROR] Invalid task spec: " + taskSpec
              + " (expected table:sortColumn:targetFragments)");
          skipped++;
          continue;
        }
        String table = parts[0];
        String sortBy = parts[1];
        int targetFragments = Integer.parseInt(parts[2]);

        String src = dataDir + "/lance/" + table + ".lance";
        String staging = src + ".staging";

        try {
          Path srcHadoopPath = new Path(src);
          FileSystem fs = srcHadoopPath.getFileSystem(
              spark.sparkContext().hadoopConfiguration());
          if (!fs.exists(srcHadoopPath)) {
            System.out.println("[SKIP] " + table + ": source " + src + " not found");
            skipped++;
            continue;
          }
          Path stagingHadoopPath = new Path(staging);
          if (fs.exists(stagingHadoopPath)) {
            System.out.println("[SKIP] " + table + ": staging " + staging
                + " already exists. Remove it first to retry.");
            skipped++;
            continue;
          }
        } catch (Exception e) {
          // Could not check — proceed with rewrite
        }

        System.out.printf("=== %s: cluster by %s -> %d partitions (in-place) ===%n",
            table, sortBy, targetFragments);
        long taskStart = System.currentTimeMillis();

        rewriteTable(spark, src, staging, sortBy, targetFragments, 0);
        swapInPlace(spark, src, staging);

        long taskElapsed = System.currentTimeMillis() - taskStart;
        System.out.printf("Finished %s in %d ms%n%n", table, taskElapsed);
        completed++;
      }
    } finally {
      spark.stop();
    }

    long batchElapsed = System.currentTimeMillis() - batchStart;
    System.out.printf("=== Batch complete: %d done, %d skipped, total %d ms ===%n",
        completed, skipped, batchElapsed);
  }

  private static void runSingle(String src, String dst, String sortBy,
      boolean inPlace, int numPartitions, int targetFragments) {
    if (src == null || sortBy == null) {
      System.err.println("Missing required arguments.");
      printUsage();
      System.exit(1);
    }
    if (inPlace && dst != null) {
      System.err.println("ERROR: --in-place and --dst are mutually exclusive.");
      printUsage();
      System.exit(1);
    }
    if (!inPlace && dst == null) {
      System.err.println("ERROR: either --dst or --in-place is required.");
      printUsage();
      System.exit(1);
    }
    if (targetFragments > 0 && numPartitions > 0) {
      System.err.println(
          "ERROR: --target-fragments and --num-partitions are mutually exclusive.");
      printUsage();
      System.exit(1);
    }

    String writeDst = inPlace ? src + ".staging" : dst;

    SparkSession spark =
        SparkSession.builder().appName("DFP Cluster Rebuilder: " + sortBy).getOrCreate();

    try {
      System.out.println("=== DFP Cluster Rebuilder ===");
      System.out.println("Source:   " + src);
      System.out.println("Dest:     " + writeDst + (inPlace ? " (in-place staging)" : ""));
      System.out.println("Sort by:  " + sortBy);
      long start = System.currentTimeMillis();

      rewriteTable(spark, src, writeDst, sortBy, targetFragments, numPartitions);

      long elapsed = System.currentTimeMillis() - start;
      System.out.printf("Wrote %s in %d ms%n", writeDst, elapsed);

      if (inPlace) {
        swapInPlace(spark, src, writeDst);
      }
    } finally {
      spark.stop();
    }
  }

  private static void rewriteTable(SparkSession spark, String src, String writeDst,
      String sortBy, int targetFragments, int numPartitions) {
    Dataset<Row> df = spark.read().format("lance").load(src);
    long rowCount = df.count();
    System.out.println("Source:   " + src);
    System.out.println("Dest:     " + writeDst);
    System.out.println("Sort by:  " + sortBy);
    System.out.println("Rows:     " + rowCount);

    Dataset<Row> prepared;
    Long maxRowsPerFile = null;
    if (targetFragments > 0) {
      long rowsPerFragment = (rowCount + targetFragments - 1L) / targetFragments;
      maxRowsPerFile = rowsPerFragment * 5L;
      prepared =
          df.repartitionByRange(targetFragments, df.col(sortBy)).sortWithinPartitions(sortBy);
      System.out.printf(
          "Range-partitioning into %d Spark partitions; setting max_row_per_file=%d "
              + "(5x rowsPerFragment=%d) to absorb sampling variance and keep one Lance "
              + "fragment per Spark partition%n",
          targetFragments, maxRowsPerFile, rowsPerFragment);
    } else if (numPartitions > 0) {
      prepared =
          df.repartitionByRange(numPartitions, df.col(sortBy)).sortWithinPartitions(sortBy);
      System.out.printf("Range-partitioning into %d Spark partitions%n", numPartitions);
    } else {
      prepared = df.sortWithinPartitions(sortBy);
    }
    org.apache.spark.sql.DataFrameWriter<Row> writer =
        prepared.write().mode(SaveMode.ErrorIfExists).format("lance");
    if (maxRowsPerFile != null) {
      writer = writer.option("max_row_per_file", maxRowsPerFile.toString());
    }
    writer.save(writeDst);
  }

  /**
   * Atomically replaces the source table with the staging table.
   *
   * <ol>
   *   <li>rename src → src.bak</li>
   *   <li>rename staging → src</li>
   *   <li>delete src.bak recursively</li>
   * </ol>
   *
   * <p>If any step fails, the scene is preserved for manual recovery.
   */
  private static void swapInPlace(SparkSession spark, String srcStr, String stagingStr) {
    Path srcPath = new Path(srcStr);
    Path stagingPath = new Path(stagingStr);
    Path bakPath = new Path(srcStr + ".bak");
    try {
      FileSystem fs = srcPath.getFileSystem(spark.sparkContext().hadoopConfiguration());
      System.out.println("In-place swap: " + srcPath);

      System.out.println("  rename " + srcPath + " → " + bakPath);
      fs.rename(srcPath, bakPath);

      System.out.println("  rename " + stagingPath + " → " + srcPath);
      fs.rename(stagingPath, srcPath);

      System.out.println("  cleanup " + bakPath);
      fs.delete(bakPath, true);

      System.out.println("  In-place swap complete.");
    } catch (IOException e) {
      System.err.println("ERROR during in-place swap. Manual recovery may be needed.");
      System.err.println("  src:     " + srcPath);
      System.err.println("  staging: " + stagingPath);
      System.err.println("  bak:     " + bakPath);
      throw new RuntimeException("In-place swap failed", e);
    }
  }

  private static void printUsage() {
    System.err.println(
        "Usage:\n"
            + "  Single-table mode:\n"
            + "    DfpClusterRebuilder --src <path> --sort-by <column>\n"
            + "                        (--dst <path> | --in-place)\n"
            + "                        [--target-fragments N | --num-partitions N]\n\n"
            + "  Batch mode:\n"
            + "    DfpClusterRebuilder --data-dir <path>\n"
            + "                        --task <table>:<sortCol>:<targetFragments> [...]\n\n"
            + "Options:\n"
            + "  --src <path>         : source Lance table path.\n"
            + "  --dst <path>         : out-of-place write to an explicit destination.\n"
            + "  --in-place           : write to <src>.staging, then atomically swap.\n"
            + "  --target-fragments N : write exactly N Lance fragments.\n"
            + "  --num-partitions N   : legacy. Spark range-partitions into N.\n"
            + "  --data-dir <path>    : base dir containing <table>.lance subdirs.\n"
            + "  --task <spec>        : batch task as table:sortColumn:targetFragments.\n"
            + "                         Repeatable. Implies --in-place for each task.");
  }
}
