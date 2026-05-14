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
package org.lance.spark.read;

import org.lance.Dataset;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Utils;
import org.lance.spark.vectorized.LanceArrowColumnVector;

import com.google.common.collect.Lists;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;

/**
 * Partition reader for pushed down aggregates. This reader computes the aggregate result directly
 * on the Lance dataset.
 */
public class LanceCountStarPartitionReader implements PartitionReader<ColumnarBatch> {
  private final LanceInputPartition inputPartition;
  private final BufferAllocator allocator;
  private boolean computed = false;
  private long computedCount;

  /**
   * Fragments actually opened by {@link #computeCount()}. Set inside the scan, not inferred. {@code
   * volatile} because Spark may poll {@link #currentMetricsValues()} from a thread other than the
   * task thread — without it the poller could observe a stale 0 after {@link #computeCount()} has
   * already completed and assigned the final value.
   */
  private volatile long fragmentsOpened;

  private ColumnarBatch currentBatch;

  public LanceCountStarPartitionReader(LanceInputPartition inputPartition) {
    this.inputPartition = inputPartition;
    this.allocator = LanceRuntime.allocator();
  }

  @Override
  public boolean next() throws IOException {
    if (!computed) {
      // Do the actual scan here, in next(), not lazily in get(). This makes the
      // fragmentsOpened counter accurate whenever Spark polls currentMetricsValues(), and makes
      // get() a pure accessor that can be called multiple times without re-scanning.
      computedCount = computeCount();
      computed = true;
      return true;
    }
    return false;
  }

  private long computeCount() {
    // This reader is only used when there are filters (metadata-based count uses LocalScan)
    LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
    long totalCount = 0;

    try (Dataset dataset =
        Utils.openDatasetBuilder(readOptions)
            .initialStorageOptions(inputPartition.getInitialStorageOptions())
            .build()) {
      List<Integer> fragmentIds = inputPartition.getLanceSplit().getFragments();
      if (fragmentIds.isEmpty()) {
        return 0;
      }

      ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
      if (inputPartition.getWhereCondition().isPresent()) {
        scanOptionsBuilder.filter(inputPartition.getWhereCondition().get());
      }
      scanOptionsBuilder.withRowId(true);
      scanOptionsBuilder.columns(Lists.newArrayList());
      scanOptionsBuilder.fragmentIds(fragmentIds);
      try (LanceScanner scanner = dataset.newScan(scanOptionsBuilder.build())) {
        try (ArrowReader reader = scanner.scanBatches()) {
          while (reader.loadNextBatch()) {
            totalCount += reader.getVectorSchemaRoot().getRowCount();
          }
        }
        // Only record fragmentsOpened AFTER the scan completes successfully. If scanBatches() or
        // loadNextBatch() throws, the exception is re-thrown below and fragmentsOpened stays 0
        // — the metric must not report I/O that didn't happen.
        this.fragmentsOpened = fragmentIds.size();
      } catch (Exception e) {
        throw new RuntimeException("Failed to scan fragment " + fragmentIds, e);
      }
    }

    return totalCount;
  }

  private ColumnarBatch createCountResultBatch(long count, StructType resultSchema) {
    VectorSchemaRoot root =
        VectorSchemaRoot.create(
            LanceArrowUtils.toArrowSchema(resultSchema, "UTC", false), allocator);
    try {
      root.allocateNew();
      BigIntVector countVector = (BigIntVector) root.getVector("count");
      countVector.setSafe(0, count);
      root.setRowCount(1);

      LanceArrowColumnVector[] columns =
          root.getFieldVectors().stream()
              .map(LanceArrowColumnVector::new)
              .toArray(LanceArrowColumnVector[]::new);

      return new ColumnarBatch(columns, 1);
    } catch (Exception e) {
      root.close();
      throw e;
    }
  }

  @Override
  public ColumnarBatch get() {
    // next() already called computeCount(). Materialize the batch once and cache it so repeated
    // get() calls don't allocate new Arrow buffers each time — Spark's contract allows get() to
    // be called multiple times between next() calls, and without caching each call would leak an
    // uncached ColumnarBatch (only the latest would make it into currentBatch and be close()d).
    if (currentBatch == null) {
      StructType countSchema =
          new StructType().add("count", org.apache.spark.sql.types.DataTypes.LongType);
      currentBatch = createCountResultBatch(computedCount, countSchema);
    }
    return currentBatch;
  }

  @Override
  public void close() throws IOException {
    if (currentBatch != null) {
      currentBatch.close();
    }
  }

  /**
   * Reports the number of Lance fragments this count-star partition reader opened. Populated from
   * inside {@link #computeCount()} so the counter reflects actual I/O, not a proxy flag — the Spark
   * contract allows {@code currentMetricsValues()} to be polled at any point during task execution,
   * including before the first {@code get()} if a framework polls between {@code next()} and {@code
   * get()}.
   */
  @Override
  public CustomTaskMetric[] currentMetricsValues() {
    long snapshot = fragmentsOpened;
    return new CustomTaskMetric[] {
      new CustomTaskMetric() {
        @Override
        public String name() {
          return FragmentsScannedMetric.NAME;
        }

        @Override
        public long value() {
          return snapshot;
        }
      }
    };
  }
}
