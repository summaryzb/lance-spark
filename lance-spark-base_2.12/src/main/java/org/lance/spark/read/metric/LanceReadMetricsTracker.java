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
package org.lance.spark.read.metric;

import org.apache.spark.sql.connector.metric.CustomTaskMetric;

/**
 * Accumulates read-path metrics on the executor side. Thread-confined (one instance per
 * PartitionReader, single-threaded access). Returns snapshot values via {@link
 * #currentMetricsValues()} — Spark calls this once per {@code next()} invocation on the
 * PartitionReader.
 */
public class LanceReadMetricsTracker {
  private long fragmentsScanned;
  private long batchesRead;
  private long datasetOpenTimeNs;
  private long scannerCreateTimeNs;
  private long batchLoadTimeNs;

  public void addFragmentsScanned(long n) {
    fragmentsScanned += n;
  }

  public void addBatchesRead(long n) {
    batchesRead += n;
  }

  public void addDatasetOpenTimeNs(long ns) {
    datasetOpenTimeNs += ns;
  }

  public void addScannerCreateTimeNs(long ns) {
    scannerCreateTimeNs += ns;
  }

  public void addBatchLoadTimeNs(long ns) {
    batchLoadTimeNs += ns;
  }

  /** Returns current snapshot of all metrics. Called by PartitionReader.currentMetricsValues(). */
  public CustomTaskMetric[] currentMetricsValues() {
    // Derived metric: must be updated if new timing phases are added
    long scanTimeNs = datasetOpenTimeNs + scannerCreateTimeNs + batchLoadTimeNs;
    return new CustomTaskMetric[] {
      taskMetric(LanceCustomMetrics.FRAGMENTS_SCANNED, fragmentsScanned),
      taskMetric(LanceCustomMetrics.BATCHES_READ, batchesRead),
      taskMetric(LanceCustomMetrics.SCAN_TIME_NS, scanTimeNs),
      taskMetric(LanceCustomMetrics.DATASET_OPEN_TIME_NS, datasetOpenTimeNs),
      taskMetric(LanceCustomMetrics.SCANNER_CREATE_TIME_NS, scannerCreateTimeNs),
      taskMetric(LanceCustomMetrics.BATCH_LOAD_TIME_NS, batchLoadTimeNs),
    };
  }

  // Accessors for testing
  public long getFragmentsScanned() {
    return fragmentsScanned;
  }

  public long getBatchesRead() {
    return batchesRead;
  }

  public long getDatasetOpenTimeNs() {
    return datasetOpenTimeNs;
  }

  public long getScannerCreateTimeNs() {
    return scannerCreateTimeNs;
  }

  public long getBatchLoadTimeNs() {
    return batchLoadTimeNs;
  }

  private static CustomTaskMetric taskMetric(String name, long value) {
    return new CustomTaskMetric() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public long value() {
        return value;
      }
    };
  }
}
