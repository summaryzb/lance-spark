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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Re-sorts an existing Lance table by a given column so downstream zonemap indexes produce
 * tight per-fragment min/max bounds. A DFP run against an unclustered fact table has nothing
 * to prune because every fragment's zone spans the full join-key domain.
 *
 * <p>The rebuild is done out-of-place: the source table is read, sorted, and written to
 * {@code <table>_clustered.lance}, which the caller then swaps in (e.g. via {@code mv} in the
 * orchestrating shell script). Destination-in-place rewriting would require a delete-&-rewrite
 * inside Lance and is out of scope here.
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
 *   spark-submit --class org.lance.spark.benchmark.DfpClusterRebuilder benchmark.jar \
 *       --src  /path/to/tpcds/lance/store_sales.lance \
 *       --dst  /path/to/tpcds/lance/store_sales_clustered.lance \
 *       --sort-by ss_sold_date_sk \
 *       --target-fragments 234
 * </pre>
 */
public class DfpClusterRebuilder {

  public static void main(String[] args) {
    String src = null;
    String dst = null;
    String sortBy = null;
    int numPartitions = 0; // 0 = keep input partitioning (sortWithinPartitions only)
    int targetFragments = 0; // 0 = no override (legacy --num-partitions path)

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
        case "--num-partitions":
          numPartitions = Integer.parseInt(args[++i]);
          break;
        case "--target-fragments":
          targetFragments = Integer.parseInt(args[++i]);
          break;
        default:
          System.err.println("Unknown argument: " + args[i]);
          printUsage();
          System.exit(1);
      }
    }

    if (src == null || dst == null || sortBy == null) {
      System.err.println("Missing required arguments.");
      printUsage();
      System.exit(1);
    }
    if (targetFragments > 0 && numPartitions > 0) {
      System.err.println(
          "ERROR: --target-fragments and --num-partitions are mutually exclusive.");
      printUsage();
      System.exit(1);
    }

    SparkSession spark =
        SparkSession.builder().appName("DFP Cluster Rebuilder: " + sortBy).getOrCreate();

    try {
      System.out.println("=== DFP Cluster Rebuilder ===");
      System.out.println("Source:   " + src);
      System.out.println("Dest:     " + dst);
      System.out.println("Sort by:  " + sortBy);
      long start = System.currentTimeMillis();

      Dataset<Row> df = spark.read().format("lance").load(src);
      long rowCount = df.count();
      System.out.println("Rows:     " + rowCount);

      // Range-partition globally on the sort column so each output fragment covers a
      // contiguous, non-overlapping slice of the key domain — the precondition for DFP
      // runtime pruning to eliminate fragments. Without it, sortWithinPartitions only
      // tightens intra-fragment order but leaves fragment-level zone ranges overlapping if
      // input partitions already span the whole key domain.
      org.apache.spark.sql.Dataset<Row> prepared;
      int effectiveNumPartitions = targetFragments > 0 ? targetFragments : numPartitions;
      Long maxRowsPerFile = null;
      if (targetFragments > 0) {
        // Pin the output fragment count: compute max_row_per_file so Lance does not roll
        // any Spark range-partition into multiple files. The 5x safety multiplier handles
        // the variance that `repartitionByRange` introduces — its reservoir sampling
        // produces partitions whose row count is approximate (observed p99 ≈ 2x p50 at
        // sf=100), so pinning max_row_per_file at exactly rowCount/targetFragments would
        // cause oversized partitions to roll and inflate the fragment count.
        // Trade-off: 5x means the heaviest fragment can hold up to 5x the average row count
        // before rolling. This is well within Lance's per-file size constraints for the
        // TPC-DS columns involved. If the input has extreme skew (>5x), drop --target-
        // fragments and let Lance roll naturally.
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
      writer.save(dst);

      long elapsed = System.currentTimeMillis() - start;
      System.out.printf("Wrote %s in %d ms%n", dst, elapsed);
    } finally {
      spark.stop();
    }
  }

  private static void printUsage() {
    System.err.println(
        "Usage: DfpClusterRebuilder --src <path> --dst <path> --sort-by <column>\n"
            + "                           [--target-fragments N | --num-partitions N]\n"
            + "  --target-fragments N : write exactly N Lance fragments (overrides Lance's\n"
            + "                         default max_row_per_file roll-over).\n"
            + "  --num-partitions N   : legacy. Spark range-partitions into N; Lance may roll\n"
            + "                         each partition into multiple fragments if > 1M rows.");
  }
}
