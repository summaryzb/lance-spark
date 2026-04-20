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
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class BaseLanceCustomMetricsTest {

  @Test
  void testAllMetricsReturnsSixMetrics() {
    CustomMetric[] metrics = LanceCustomMetrics.allMetrics();
    assertEquals(6, metrics.length);
  }

  @Test
  void testMetricNamesAreUnique() {
    CustomMetric[] metrics = LanceCustomMetrics.allMetrics();
    Set<String> names = new HashSet<>();
    for (CustomMetric m : metrics) {
      assertTrue(names.add(m.name()), "Duplicate metric name: " + m.name());
    }
  }

  @Test
  void testMetricNamesMatchConstants() {
    Set<String> expected =
        new HashSet<>(
            Arrays.asList(
                LanceCustomMetrics.FRAGMENTS_SCANNED,
                LanceCustomMetrics.BATCHES_READ,
                LanceCustomMetrics.SCAN_TIME_NS,
                LanceCustomMetrics.DATASET_OPEN_TIME_NS,
                LanceCustomMetrics.SCANNER_CREATE_TIME_NS,
                LanceCustomMetrics.BATCH_LOAD_TIME_NS));
    CustomMetric[] metrics = LanceCustomMetrics.allMetrics();
    Set<String> actual = new HashSet<>();
    for (CustomMetric m : metrics) {
      actual.add(m.name());
    }
    assertEquals(expected, actual);
  }

  @Test
  void testAllMetricsHaveDescriptions() {
    for (CustomMetric m : LanceCustomMetrics.allMetrics()) {
      assertNotNull(m.description(), "Missing description for " + m.name());
      assertTrue(m.description().length() > 0, "Empty description for " + m.name());
    }
  }

  @Test
  void testMetricDescriptionsAreUnique() {
    CustomMetric[] metrics = LanceCustomMetrics.allMetrics();
    Set<String> descriptions = new HashSet<>();
    for (CustomMetric m : metrics) {
      assertTrue(
          descriptions.add(m.description()), "Duplicate metric description: " + m.description());
    }
  }

  @Test
  void testSumAggregation() {
    // CustomSumMetric.aggregateTaskMetrics should sum all values
    LanceCustomMetrics.FragmentsScannedMetric metric =
        new LanceCustomMetrics.FragmentsScannedMetric();
    String result = metric.aggregateTaskMetrics(new long[] {3, 5, 7});
    assertEquals("15", result);
  }

  @Test
  void testSumAggregationEmpty() {
    LanceCustomMetrics.FragmentsScannedMetric metric =
        new LanceCustomMetrics.FragmentsScannedMetric();
    String result = metric.aggregateTaskMetrics(new long[] {});
    assertEquals("0", result);
  }

  @Test
  void testNoArgConstructors() throws Exception {
    // Spark instantiates metric classes via reflection — verify no-arg constructors work
    Class<?>[] metricClasses =
        new Class<?>[] {
          LanceCustomMetrics.FragmentsScannedMetric.class,
          LanceCustomMetrics.BatchesReadMetric.class,
          LanceCustomMetrics.ScanTimeNsMetric.class,
          LanceCustomMetrics.DatasetOpenTimeNsMetric.class,
          LanceCustomMetrics.ScannerCreateTimeNsMetric.class,
          LanceCustomMetrics.BatchLoadTimeNsMetric.class,
        };
    for (Class<?> clazz : metricClasses) {
      Object instance = clazz.getDeclaredConstructor().newInstance();
      assertNotNull(instance, "Failed to instantiate " + clazz.getSimpleName());
    }
  }

  @Test
  void testTrackerInitialValues() {
    LanceReadMetricsTracker tracker = new LanceReadMetricsTracker();
    CustomTaskMetric[] values = tracker.currentMetricsValues();
    assertEquals(6, values.length);
    for (CustomTaskMetric m : values) {
      assertEquals(0L, m.value(), "Initial value should be 0 for " + m.name());
    }
  }

  @Test
  void testTrackerAccumulation() {
    LanceReadMetricsTracker tracker = new LanceReadMetricsTracker();
    tracker.addFragmentsScanned(1);
    tracker.addFragmentsScanned(1);
    tracker.addBatchesRead(5);
    tracker.addDatasetOpenTimeNs(100_000);
    tracker.addScannerCreateTimeNs(50_000);
    tracker.addBatchLoadTimeNs(200_000);

    assertEquals(2, tracker.getFragmentsScanned());
    assertEquals(5, tracker.getBatchesRead());
    assertEquals(100_000, tracker.getDatasetOpenTimeNs());
    assertEquals(50_000, tracker.getScannerCreateTimeNs());
    assertEquals(200_000, tracker.getBatchLoadTimeNs());

    // Verify scanTimeNs = sum of three sub-timings
    CustomTaskMetric[] values = tracker.currentMetricsValues();
    long scanTimeNs = 0;
    for (CustomTaskMetric m : values) {
      if (m.name().equals(LanceCustomMetrics.SCAN_TIME_NS)) {
        scanTimeNs = m.value();
      }
    }
    assertEquals(100_000 + 50_000 + 200_000, scanTimeNs);
  }

  @Test
  void testTrackerBulkAddFragmentsScanned() {
    LanceReadMetricsTracker tracker = new LanceReadMetricsTracker();
    tracker.addFragmentsScanned(5);
    tracker.addFragmentsScanned(3);
    assertEquals(8, tracker.getFragmentsScanned());
  }

  @Test
  void testTrackerMetricNamesMatchDefinitions() {
    LanceReadMetricsTracker tracker = new LanceReadMetricsTracker();
    CustomTaskMetric[] taskMetrics = tracker.currentMetricsValues();
    CustomMetric[] definitions = LanceCustomMetrics.allMetrics();

    Set<String> taskNames = new HashSet<>();
    for (CustomTaskMetric m : taskMetrics) {
      taskNames.add(m.name());
    }
    Set<String> defNames = new HashSet<>();
    for (CustomMetric m : definitions) {
      defNames.add(m.name());
    }
    assertEquals(
        defNames,
        taskNames,
        "Task metric names must match definition names for Spark to aggregate");
  }
}
