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

import org.lance.spark.internal.LanceFragmentColumnarBatchScanner;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;

public class LanceColumnarPartitionReader implements PartitionReader<ColumnarBatch> {
  private final LanceInputPartition inputPartition;
  private int fragmentIndex;
  private LanceFragmentColumnarBatchScanner fragmentReader;
  private ColumnarBatch currentBatch;

  /**
   * Count of fragments opened so far on this task. Reported to Spark via {@link
   * #currentMetricsValues()} so the {@code fragmentsScanned} SQL metric aggregates correctly across
   * all tasks of the scan. {@code volatile} because Spark is allowed to poll {@code
   * currentMetricsValues()} from a thread other than the task thread — without it the poller could
   * observe a stale 0 after the task thread has already incremented.
   */
  private volatile long fragmentsOpened;

  public LanceColumnarPartitionReader(LanceInputPartition inputPartition) {
    this.inputPartition = inputPartition;
    this.fragmentIndex = 0;
  }

  /** Package-private accessor for tests and for the {@link LanceRowPartitionReader} delegate. */
  long getFragmentsOpened() {
    return fragmentsOpened;
  }

  @Override
  public boolean next() throws IOException {
    if (loadNextBatchFromCurrentReader()) {
      return true;
    }
    while (fragmentIndex < inputPartition.getLanceSplit().getFragments().size()) {
      if (fragmentReader != null) {
        fragmentReader.close();
      }
      fragmentReader =
          LanceFragmentColumnarBatchScanner.create(
              inputPartition.getLanceSplit().getFragments().get(fragmentIndex), inputPartition);
      fragmentIndex++;
      fragmentsOpened++;
      if (loadNextBatchFromCurrentReader()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public CustomTaskMetric[] currentMetricsValues() {
    return new CustomTaskMetric[] {
      new CustomTaskMetric() {
        @Override
        public String name() {
          return FragmentsScannedMetric.NAME;
        }

        @Override
        public long value() {
          return fragmentsOpened;
        }
      }
    };
  }

  private boolean loadNextBatchFromCurrentReader() throws IOException {
    if (fragmentReader != null && fragmentReader.loadNextBatch()) {
      currentBatch = fragmentReader.getCurrentBatch();
      return true;
    }
    return false;
  }

  @Override
  public ColumnarBatch get() {
    return currentBatch;
  }

  @Override
  public void close() throws IOException {
    if (fragmentReader != null) {
      try {
        fragmentReader.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
}
