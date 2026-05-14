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

import org.apache.spark.sql.connector.metric.CustomSumMetric;

/**
 * Sum metric for the number of Lance fragments actually opened by a scan's {@link
 * org.apache.spark.sql.connector.read.PartitionReader}s on the executors. Aggregated across all
 * tasks of the scan. The programmatic key is {@code "fragmentsScanned"} (see {@link #name()}); the
 * Spark SQL UI displays the metric using the {@link #description()} text. This is the most
 * operationally important DFP metric — it answers "did runtime filter pruning actually reduce I/O?"
 * Without executor-side counting (only driver-side counters), fragment opens would be invisible to
 * operators.
 *
 * <p>Spark constructs this class reflectively via a 0-arg constructor (see {@link
 * org.apache.spark.sql.connector.metric.CustomMetric}), so the class must remain public with a
 * no-arg constructor. Renaming either is a breaking stability-contract change per the DFP plan.
 */
public class FragmentsScannedMetric extends CustomSumMetric {

  public static final String NAME = "fragmentsScanned";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "number of Lance fragments opened by partition readers";
  }
}
