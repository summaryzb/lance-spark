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

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomSumMetric;

/** Custom metrics for the Lance read path, displayed on the Spark UI Scan node. */
public final class LanceCustomMetrics {
  public static final String FRAGMENTS_SCANNED = "fragmentsScanned";
  public static final String BATCHES_READ = "batchesRead";

  public static final String SCAN_TIME_NS = "scanTimeNs";
  public static final String DATASET_OPEN_TIME_NS = "datasetOpenTimeNs";
  public static final String SCANNER_CREATE_TIME_NS = "scannerCreateTimeNs";
  public static final String BATCH_LOAD_TIME_NS = "batchLoadTimeNs";

  private LanceCustomMetrics() {}

  // Each inner class MUST have a public no-arg constructor (Spark instantiates via reflection).

  public static class FragmentsScannedMetric extends CustomSumMetric {
    @Override
    public String name() {
      return FRAGMENTS_SCANNED;
    }

    @Override
    public String description() {
      return "number of Lance fragments scanned";
    }
  }

  public static class BatchesReadMetric extends CustomSumMetric {
    @Override
    public String name() {
      return BATCHES_READ;
    }

    @Override
    public String description() {
      return "number of Arrow batches read";
    }
  }

  public static class ScanTimeNsMetric extends CustomSumMetric {
    @Override
    public String name() {
      return SCAN_TIME_NS;
    }

    @Override
    public String description() {
      return "total Lance scan time (sum of open + create + load) (ns)";
    }
  }

  public static class DatasetOpenTimeNsMetric extends CustomSumMetric {
    @Override
    public String name() {
      return DATASET_OPEN_TIME_NS;
    }

    @Override
    public String description() {
      return "time to open Lance dataset (ns)";
    }
  }

  public static class ScannerCreateTimeNsMetric extends CustomSumMetric {
    @Override
    public String name() {
      return SCANNER_CREATE_TIME_NS;
    }

    @Override
    public String description() {
      return "time to create fragment scanner (ns)";
    }
  }

  public static class BatchLoadTimeNsMetric extends CustomSumMetric {
    @Override
    public String name() {
      return BATCH_LOAD_TIME_NS;
    }

    // includes JNI + Arrow IPC deserialization (ns)
    @Override
    public String description() {
      return "time to arrow load batch (ns)";
    }
  }

  private static final CustomMetric[] ALL_METRICS = {
    new FragmentsScannedMetric(),
    new BatchesReadMetric(),
    new ScanTimeNsMetric(),
    new DatasetOpenTimeNsMetric(),
    new ScannerCreateTimeNsMetric(),
    new BatchLoadTimeNsMetric(),
  };

  /** Returns all supported custom metrics, used by LanceScan.supportedCustomMetrics(). */
  public static CustomMetric[] allMetrics() {
    return ALL_METRICS.clone();
  }
}
