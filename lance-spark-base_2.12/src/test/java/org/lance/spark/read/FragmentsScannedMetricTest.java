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

import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 2.6 unit tests for the {@link FragmentsScannedMetric} stability contract and aggregation
 * semantics. Full executor-side integration (actual scan → UI metric) is exercised by the Phase 4
 * integration tests.
 */
public class FragmentsScannedMetricTest {

  @Test
  public void metricNameIsStable() {
    assertEquals("fragmentsScanned", FragmentsScannedMetric.NAME);
    assertEquals("fragmentsScanned", new FragmentsScannedMetric().name());
  }

  @Test
  public void metricHasDescription() {
    String desc = new FragmentsScannedMetric().description();
    assertNotNull(desc);
    assertTrue(desc.contains("fragment"), "description should mention fragments");
  }

  @Test
  public void sumAggregationAcrossTasks() {
    // Spark constructs the metric reflectively (0-arg ctor) and calls aggregateTaskMetrics with
    // the per-task values. Verify the sum behavior matches CustomSumMetric's contract. Parse the
    // return value numerically rather than string-equal'ing — Spark's format for
    // aggregateTaskMetrics is not a documented-stable contract (could change to add units,
    // padding, etc. across versions).
    FragmentsScannedMetric metric = new FragmentsScannedMetric();
    String result = metric.aggregateTaskMetrics(new long[] {3L, 5L, 2L});
    assertEquals(10L, Long.parseLong(result.trim()));
  }

  @Test
  public void emptyTaskArrayAggregatesToZero() {
    String result = new FragmentsScannedMetric().aggregateTaskMetrics(new long[0]);
    assertEquals(0L, Long.parseLong(result.trim()));
  }

  @Test
  public void zeroArgConstructorForSparkReflection() {
    // Spark requires the class to have a public no-arg constructor (see CustomMetric javadoc).
    FragmentsScannedMetric metric = new FragmentsScannedMetric();
    assertNotNull(metric);
  }

  @Test
  public void metricIsAcceptableCustomMetricInstance() {
    // Should be assignable to the CustomMetric interface so Scan.supportedCustomMetrics() returns
    // a valid array.
    CustomMetric m = new FragmentsScannedMetric();
    assertEquals("fragmentsScanned", m.name());
  }

  @Test
  public void customTaskMetricShapeForReaders() {
    // A readers's currentMetricsValues() return one CustomTaskMetric with name "fragmentsScanned"
    // and a long value. Verify the anonymous-class shape we rely on.
    long expectedValue = 42L;
    CustomTaskMetric taskMetric =
        new CustomTaskMetric() {
          @Override
          public String name() {
            return FragmentsScannedMetric.NAME;
          }

          @Override
          public long value() {
            return expectedValue;
          }
        };
    assertEquals("fragmentsScanned", taskMetric.name());
    assertEquals(42L, taskMetric.value());
  }
}
