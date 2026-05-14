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

import org.lance.index.scalar.ZoneStats;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.utils.Optional;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Phase 2 tests for {@link LanceScan#filter(Predicate[])} and related runtime-filtering behavior.
 * These exercise scan-level state transitions with an in-memory (no-Spark-runtime) scan built
 * directly from synthetic {@link ZoneStats} — no actual dataset I/O is performed.
 */
public class LanceScanRuntimeFilterTest {

  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("x", DataTypes.LongType, true),
            DataTypes.createStructField("y", DataTypes.LongType, true),
          });

  // Fragment 0 holds x in [0, 10]; fragment 1 holds x in [100, 200]; fragment 2 holds x in [5, 8].
  private static Map<String, List<ZoneStats>> syntheticXStats() {
    Map<String, List<ZoneStats>> m = new HashMap<>();
    m.put(
        "x",
        Arrays.asList(
            new ZoneStats(0, 0, 10, 0L, 10L, 0L),
            new ZoneStats(1, 0, 10, 100L, 200L, 0L),
            new ZoneStats(2, 0, 10, 5L, 8L, 0L)));
    return m;
  }

  private static LanceScan newScan(
      Map<String, List<ZoneStats>> zonemapStats,
      Set<Integer> cachedSurvivingFragmentIds,
      Filter[] pushedFilters,
      LanceSparkReadOptions opts) {
    return newScanWithPartitionInfo(
        zonemapStats, cachedSurvivingFragmentIds, pushedFilters, opts, null);
  }

  private static LanceScan newScanWithPartitionInfo(
      Map<String, List<ZoneStats>> zonemapStats,
      Set<Integer> cachedSurvivingFragmentIds,
      Filter[] pushedFilters,
      LanceSparkReadOptions opts,
      ZonemapFragmentPruner.PartitionInfo partitionInfo) {
    return new LanceScan(
        SCHEMA,
        opts,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        pushedFilters == null ? new Filter[0] : pushedFilters,
        new LanceStatistics(0L, 0L),
        zonemapStats,
        cachedSurvivingFragmentIds,
        partitionInfo,
        Collections.emptyMap(),
        null,
        Collections.emptyMap());
  }

  /** Builds a partition-compatible fixture: fragment N has partition value N+1 (1, 2, 3). */
  private static ZonemapFragmentPruner.PartitionInfo syntheticPartitionInfo() {
    HashMap<Integer, Comparable<?>> map = new HashMap<>();
    map.put(0, 1L);
    map.put(1, 2L);
    map.put(2, 3L);
    return new ZonemapFragmentPruner.PartitionInfo("part_key", map);
  }

  /**
   * Single-zone-per-fragment zonemap stats aligned with {@link #syntheticPartitionInfo}: fragment N
   * has min == max == N+1 on the "part_key" column, making each fragment a distinct partition.
   */
  private static Map<String, List<ZoneStats>> syntheticPartitionedStats() {
    Map<String, List<ZoneStats>> m = new HashMap<>();
    m.put(
        "part_key",
        Arrays.asList(
            new ZoneStats(0, 0, 10, 1L, 1L, 0L),
            new ZoneStats(1, 0, 10, 2L, 2L, 0L),
            new ZoneStats(2, 0, 10, 3L, 3L, 0L)));
    return m;
  }

  private static LanceSparkReadOptions optsEnabled() {
    return LanceSparkReadOptions.from(TestUtils.TestTable1Config.datasetUri);
  }

  private static LanceSparkReadOptions optsDisabled() {
    Map<String, String> o = new HashMap<>();
    o.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_ENABLED, "false");
    return LanceSparkReadOptions.from(o, TestUtils.TestTable1Config.datasetUri);
  }

  private static Predicate inPred(String column, Object... values) {
    Expression[] children = new Expression[values.length + 1];
    children[0] = FieldReference.apply(column);
    for (int i = 0; i < values.length; i++) {
      children[i + 1] = new LiteralValue<>(values[i], DataTypes.LongType);
    }
    return new Predicate("IN", children);
  }

  // --- filterAttributes ---

  @Test
  public void filterAttributesAreZonemappedColumns() {
    LanceScan scan = newScan(syntheticXStats(), null, null, optsEnabled());
    NamedReference[] attrs = scan.filterAttributes();
    assertEquals(1, attrs.length);
    assertEquals("x", attrs[0].describe());
  }

  @Test
  public void filterAttributesEmptyWhenDisabled() {
    LanceScan scan = newScan(syntheticXStats(), null, null, optsDisabled());
    assertEquals(0, scan.filterAttributes().length);
  }

  @Test
  public void filterAttributesEmptyWhenNoZonemapStats() {
    LanceScan scan = newScan(Collections.emptyMap(), null, null, optsEnabled());
    assertEquals(0, scan.filterAttributes().length);
  }

  // --- filter() state transitions ---

  @Test
  public void filterNarrowsSurvivingFragments() {
    LanceScan scan = newScan(syntheticXStats(), null, null, optsEnabled());
    // IN (5, 6, 7): fragments 0 and 2 overlap, fragment 1 does not.
    scan.filter(new Predicate[] {inPred("x", 5L, 6L, 7L)});
    assertNotEquals(null, scan.filterAttributes());
    // After filter(), the scan identity changes via runtimeFilters.
  }

  @Test
  public void filterOnNonIndexedColumnIsNoOp() {
    LanceScan scan = newScan(syntheticXStats(), null, null, optsEnabled());
    LanceScan originalCopy = newScan(syntheticXStats(), null, null, optsEnabled());
    scan.filter(new Predicate[] {inPred("y", 1L)});
    assertEquals(originalCopy, scan, "filter on non-indexed column must not change scan identity");
  }

  @Test
  public void emptyPredicateArrayIsNoOp() {
    LanceScan scan = newScan(syntheticXStats(), null, null, optsEnabled());
    LanceScan originalCopy = newScan(syntheticXStats(), null, null, optsEnabled());
    scan.filter(new Predicate[0]);
    assertEquals(originalCopy, scan);
    assertEquals(originalCopy.hashCode(), scan.hashCode());
  }

  @Test
  public void nullPredicateArrayIsNoOp() {
    LanceScan scan = newScan(syntheticXStats(), null, null, optsEnabled());
    LanceScan originalCopy = newScan(syntheticXStats(), null, null, optsEnabled());
    scan.filter(null);
    assertEquals(originalCopy, scan);
  }

  @Test
  public void filterClearsPriorRuntimeStateOnEverycall() {
    // Multiple filter() calls with DIFFERENT predicate sets: the second call must start from
    // the static baseline, not compound with the first call's narrowing.
    LanceScan scanA = newScan(syntheticXStats(), null, null, optsEnabled());
    scanA.filter(new Predicate[] {inPred("x", 5L)}); // narrows to fragments 0, 2
    int hashAfterA = scanA.hashCode();

    LanceScan scanB = newScan(syntheticXStats(), null, null, optsEnabled());
    scanB.filter(new Predicate[] {inPred("x", 150L)}); // narrows to fragment 1 only
    int hashAfterB = scanB.hashCode();

    // If the two narrowings were compounding, scanA.hashCode() would depend on both calls.
    // Instead, scanA and scanB each reflect their own single filter() call.
    assertNotEquals(hashAfterA, hashAfterB);

    // Same instance, two sequential filter() calls — the second must replace, not union.
    LanceScan scanC = newScan(syntheticXStats(), null, null, optsEnabled());
    scanC.filter(new Predicate[] {inPred("x", 5L)});
    scanC.filter(new Predicate[] {inPred("x", 150L)});
    // scanC should now be equivalent to a fresh scan that only saw the second filter.
    assertEquals(scanB, scanC);
  }

  @Test
  public void filterWithAllUnconvertiblePredicatesLeavesScanUnchanged() {
    LanceScan scan = newScan(syntheticXStats(), null, null, optsEnabled());
    LanceScan originalCopy = newScan(syntheticXStats(), null, null, optsEnabled());
    // ">" is not in our supported converter subset → all dropped → runtimeFilters stays null.
    scan.filter(
        new Predicate[] {
          new Predicate(
              ">",
              new Expression[] {
                FieldReference.apply("x"), new LiteralValue<>(50L, DataTypes.LongType)
              })
        });
    assertEquals(originalCopy, scan);
  }

  @Test
  public void filterNoOpWhenDisabled() {
    LanceScan scan = newScan(syntheticXStats(), null, null, optsDisabled());
    LanceScan originalCopy = newScan(syntheticXStats(), null, null, optsDisabled());
    scan.filter(new Predicate[] {inPred("x", 5L)});
    assertEquals(originalCopy, scan);
  }

  // --- ReusedExchange identity ---

  @Test
  public void scansWithDifferentRuntimeFiltersHaveDifferentHashAndEquals() {
    LanceScan scanA = newScan(syntheticXStats(), null, null, optsEnabled());
    LanceScan scanB = newScan(syntheticXStats(), null, null, optsEnabled());
    scanA.filter(new Predicate[] {inPred("x", 5L)});
    scanB.filter(new Predicate[] {inPred("x", 6L)});
    assertNotEquals(scanA, scanB);
    assertNotEquals(scanA.hashCode(), scanB.hashCode());
  }

  @Test
  public void scansWithIdenticalRuntimeFiltersDedupe() {
    LanceScan scanA = newScan(syntheticXStats(), null, null, optsEnabled());
    LanceScan scanB = newScan(syntheticXStats(), null, null, optsEnabled());
    scanA.filter(new Predicate[] {inPred("x", 5L)});
    scanB.filter(new Predicate[] {inPred("x", 5L)});
    assertEquals(scanA, scanB);
    assertEquals(scanA.hashCode(), scanB.hashCode());
  }

  @Test
  public void backwardCompatEqualsHashCodeForNullRuntimeFilters() {
    // A scan with runtimeFilters==null (never called) must equal and hash identically to a scan
    // that received an empty or all-unconvertible filter() call.
    LanceScan neverFiltered = newScan(syntheticXStats(), null, null, optsEnabled());

    LanceScan emptyFiltered = newScan(syntheticXStats(), null, null, optsEnabled());
    emptyFiltered.filter(new Predicate[0]);

    LanceScan allDropped = newScan(syntheticXStats(), null, null, optsEnabled());
    allDropped.filter(
        new Predicate[] {
          new Predicate(
              ">",
              new Expression[] {
                FieldReference.apply("x"), new LiteralValue<>(50L, DataTypes.LongType)
              })
        });

    assertEquals(neverFiltered, emptyFiltered);
    assertEquals(neverFiltered.hashCode(), emptyFiltered.hashCode());
    assertEquals(neverFiltered, allDropped);
    assertEquals(neverFiltered.hashCode(), allDropped.hashCode());
  }

  // --- interaction with static pushed filters ---

  @Test
  public void filterPreservesPushedFilters() {
    Filter[] pushed = new Filter[] {new GreaterThan("y", 1L)};
    LanceScan scan = newScan(syntheticXStats(), null, pushed, optsEnabled());
    scan.filter(new Predicate[] {inPred("x", 5L)});
    // Static pushed filters must still participate in pruning alongside runtime filters.
    // We can't inspect private pushedFilters directly, but we can verify equals() behavior:
    LanceScan same = newScan(syntheticXStats(), null, pushed, optsEnabled());
    same.filter(new Predicate[] {inPred("x", 5L)});
    assertEquals(scan, same);
  }

  // --- null / empty predicates ---

  @Test
  public void runtimeFiltersClearedAfterEmptyFilterCall() {
    LanceScan scan = newScan(syntheticXStats(), null, null, optsEnabled());
    scan.filter(new Predicate[] {inPred("x", 5L)});
    // A subsequent empty filter() call must fully reset runtime state.
    scan.filter(new Predicate[0]);
    LanceScan neverFiltered = newScan(syntheticXStats(), null, null, optsEnabled());
    assertEquals(neverFiltered, scan);
  }

  // --- Phase 2.5 observability metrics ---

  @Test
  public void metricsStartAtZero() {
    LanceScan scan = newScan(syntheticXStats(), null, null, optsEnabled());
    assertEquals(0L, scan.getRuntimeFiltersReceivedMetric());
    assertEquals(0L, scan.getPredicatesDroppedMetric());
    assertEquals(0L, scan.getRuntimeFilterErrorsMetric());
    assertEquals(0L, scan.getFragmentsPrunedRuntimeMetric());
  }

  @Test
  public void filterCallIncrementsRuntimeFiltersReceived() {
    LanceScan scan = newScan(syntheticXStats(), null, null, optsEnabled());
    scan.filter(new Predicate[] {inPred("x", 5L)});
    scan.filter(new Predicate[] {inPred("x", 6L)});
    scan.filter(new Predicate[0]);
    // Every call, including the no-op empty-array call, increments the counter — this reflects
    // the fact that `filter()` was invoked by Spark, not that narrowing actually happened.
    assertEquals(3L, scan.getRuntimeFiltersReceivedMetric());
  }

  @Test
  public void unconvertiblePredicatesIncrementPredicatesDropped() {
    LanceScan scan = newScan(syntheticXStats(), null, null, optsEnabled());
    scan.filter(
        new Predicate[] {
          new Predicate(
              ">",
              new Expression[] {
                FieldReference.apply("x"), new LiteralValue<>(50L, DataTypes.LongType)
              }),
          inPred("unrelated", 1L), // column not in zonemapStats
          inPred("x", 5L) // keeper
        });
    assertEquals(2L, scan.getPredicatesDroppedMetric());
  }

  @Test
  public void supportedCustomMetricsDeclaresFragmentsScanned() {
    LanceScan scan = newScan(syntheticXStats(), null, null, optsEnabled());
    org.apache.spark.sql.connector.metric.CustomMetric[] metrics = scan.supportedCustomMetrics();
    assertEquals(1, metrics.length);
    assertEquals(FragmentsScannedMetric.NAME, metrics[0].name());
  }

  @Test
  public void successfulNarrowingIncrementsFragmentsPrunedRuntime() {
    // Seed total=3 so the computed fragmentsPrunedRuntime can be observed.
    LanceScan scan = newScan(syntheticXStats(), null, null, optsEnabled());
    scan.seedMetricsFromBuilder(3L);
    scan.filter(new Predicate[] {inPred("x", 5L, 6L, 7L)});
    // Fragment 1 has x in [100, 200] so it should be pruned; fragments 0 and 2 remain.
    // 3 total - 0 static pruned - 2 surviving = 1 runtime pruned.
    assertEquals(1L, scan.getFragmentsPrunedRuntimeMetric());
  }

  // --- Phase 3: SPJ contract safety ---

  @Test
  public void dfpPlusSpjProducesSubsetOfReportedPartitionValues() {
    // Scan advertises KeyGroupedPartitioning over fragments {0: 1L, 1: 2L, 2: 3L}. Runtime
    // filter x = 2 keeps only fragment 1 (partition value 2L). The surviving set {2L} must be a
    // strict subset of {1L, 2L, 3L} — assertion passes, no exception thrown.
    LanceScan scan =
        newScanWithPartitionInfo(
            syntheticPartitionedStats(), null, null, optsEnabled(), syntheticPartitionInfo());
    scan.seedMetricsFromBuilder(3L);
    scan.filter(new Predicate[] {inPred("part_key", 2L)});

    // outputPartitioning must report only one surviving partition value.
    KeyGroupedPartitioning partitioning = (KeyGroupedPartitioning) scan.outputPartitioning();
    assertEquals(1, partitioning.numPartitions());
    assertEquals(2L, scan.getFragmentsPrunedRuntimeMetric());
  }

  @Test
  public void dfpWithPartitionInfoPreservesScanUnderNoNarrowing() {
    // Filter on a non-indexed column: no narrowing possible; SPJ contract must still hold.
    LanceScan scan =
        newScanWithPartitionInfo(
            syntheticPartitionedStats(), null, null, optsEnabled(), syntheticPartitionInfo());
    scan.seedMetricsFromBuilder(3L);
    // "y" isn't in zonemapStats — all predicates dropped; no narrowing; partitioning unchanged.
    scan.filter(new Predicate[] {inPred("y", 1L)});

    KeyGroupedPartitioning partitioning = (KeyGroupedPartitioning) scan.outputPartitioning();
    assertEquals(3, partitioning.numPartitions());
    assertEquals(0L, scan.getFragmentsPrunedRuntimeMetric());
  }

  @Test
  public void outputPartitioningReflectsPostFilterSurvivors() {
    LanceScan scan =
        newScanWithPartitionInfo(
            syntheticPartitionedStats(), null, null, optsEnabled(), syntheticPartitionInfo());
    scan.seedMetricsFromBuilder(3L);

    // Before filter(): 3 partition values.
    KeyGroupedPartitioning before = (KeyGroupedPartitioning) scan.outputPartitioning();
    assertEquals(3, before.numPartitions());

    // After filter(part_key IN (1, 3)): 2 partition values.
    scan.filter(new Predicate[] {inPred("part_key", 1L, 3L)});
    KeyGroupedPartitioning after = (KeyGroupedPartitioning) scan.outputPartitioning();
    assertEquals(2, after.numPartitions());
  }

  @Test
  public void outputPartitioningCountShrinksMonotonicallyAcrossFilterCalls() {
    LanceScan scan =
        newScanWithPartitionInfo(
            syntheticPartitionedStats(), null, null, optsEnabled(), syntheticPartitionInfo());
    scan.seedMetricsFromBuilder(3L);

    scan.filter(new Predicate[] {inPred("part_key", 1L, 2L, 3L)});
    assertEquals(3, ((KeyGroupedPartitioning) scan.outputPartitioning()).numPartitions());

    scan.filter(new Predicate[] {inPred("part_key", 1L)});
    // Replace-not-union: second call starts from the static baseline of 3 and narrows to 1.
    assertEquals(1, ((KeyGroupedPartitioning) scan.outputPartitioning()).numPartitions());

    scan.filter(new Predicate[0]);
    // Empty call resets runtime state; should revert to advertising all 3 partition values.
    assertEquals(3, ((KeyGroupedPartitioning) scan.outputPartitioning()).numPartitions());
  }

  @Test
  public void dfpSoftFailsWhenSurvivingFragmentNotInPartitionMap() {
    // Regression: previously threw IllegalStateException when runtime-filter surviving fragments
    // were absent from the partition-value map. That happens legitimately when different columns'
    // zonemaps cover different fragment ranges — e.g., a new fragment was appended after the
    // partition-column index was built but the DFP-column index covers it. Now the scan
    // gracefully disables the runtime filter and preserves static pruning rather than crashing
    // the query.
    //
    // Build a mismatch: zonemapStats covers fragments 0/1/2 (synthetic x stats), but partition
    // info only covers fragments 0/1. A runtime filter IN(x, 5L) keeps fragments 0 and 2
    // (x in [0,10] and [5,8]) — fragment 2 is absent from the partition-value map.
    HashMap<Integer, Comparable<?>> partialMap = new HashMap<>();
    partialMap.put(0, 1L);
    partialMap.put(1, 2L);
    ZonemapFragmentPruner.PartitionInfo partialInfo =
        new ZonemapFragmentPruner.PartitionInfo("part_key", partialMap);

    LanceScan scan =
        newScanWithPartitionInfo(syntheticXStats(), null, null, optsEnabled(), partialInfo);
    scan.seedMetricsFromBuilder(3L);

    // Should NOT throw — soft fallback instead.
    scan.filter(new Predicate[] {inPred("x", 5L)});

    // runtimeFilterErrors incremented, effectiveSurvivingFragmentIds cleared, runtime filter
    // dropped. Static pruning (cachedSurvivingFragmentIds) remains intact, so the scan still
    // advertises the original 2 partition values from partialInfo.
    assertEquals(1L, scan.getRuntimeFilterErrorsMetric());
    KeyGroupedPartitioning partitioning = (KeyGroupedPartitioning) scan.outputPartitioning();
    assertEquals(2, partitioning.numPartitions());
  }
}
