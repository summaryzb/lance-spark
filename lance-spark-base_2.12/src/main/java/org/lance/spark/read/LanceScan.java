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
import org.lance.ipc.ColumnOrdering;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Optional;

import org.apache.arrow.util.Preconditions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsMerge;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.apache.spark.sql.internal.connector.SupportsMetadata;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Map;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class LanceScan
    implements Batch,
        Scan,
        SupportsMerge,
        SupportsMetadata,
        SupportsReportStatistics,
        SupportsReportPartitioning,
        SupportsRuntimeV2Filtering,
        Serializable {
  private static final long serialVersionUID = 947284762748623947L;
  private static final Logger LOG = LoggerFactory.getLogger(LanceScan.class);

  private final StructType schema;
  private final LanceSparkReadOptions readOptions;
  private final Optional<String> whereConditions;
  private final Optional<Integer> limit;
  private final Optional<Integer> offset;
  private final Optional<List<ColumnOrdering>> topNSortOrders;
  private final Optional<Aggregation> pushedAggregation;
  private final Filter[] pushedFilters;
  private final LanceStatistics statistics;
  private final String scanId = UUID.randomUUID().toString();

  /**
   * Per-column zonemap statistics loaded on the driver during scan building. Used for
   * fragment-level pruning in {@link #planInputPartitions()}.
   */
  private final java.util.Map<String, List<ZoneStats>> zonemapStats;

  /**
   * Pre-computed surviving fragment IDs from zonemap pruning in LanceScanBuilder. When non-null,
   * {@link #pruneByZonemapStats} uses these directly instead of recomputing.
   *
   * <p>Not {@code final}: {@link #filter(Predicate[])} nulls this out on a successful
   * runtime-narrowing so the pruner reads {@link #effectiveSurvivingFragmentIds} instead. {@code
   * volatile} for the same reason the other mutable runtime-state fields are — gives a stable
   * happens-before for the {@code filter() → planInputPartitions()} hand-off on the driver thread,
   * so the null-out is guaranteed visible to subsequent reads even if speculative planning or
   * post-deserialization access happens on a different thread.
   */
  private volatile Set<Integer> cachedSurvivingFragmentIds;

  /**
   * Immutable snapshot of the static-pruning survivor count captured at scan construction. Used as
   * the invariant baseline for {@link #fragmentsPrunedRuntime} — {@link
   * #cachedSurvivingFragmentIds} cannot serve that role because {@code filter()} nulls it out on
   * the first successful narrowing, making subsequent {@code filter()} calls lose the baseline.
   * When no static pruning was done at build time, this equals the full fragment count (or -1 if
   * the build-time manifest did not report a total; caller falls back to {@code fragmentsTotal}).
   */
  private final long staticSurvivorCountAtBuild;

  /**
   * Number of partitions after pruning, set during {@link #planInputPartitions()} and reset in
   * {@link #filter(Predicate[])}. Marked {@code volatile} to give a stable happens-before for the
   * {@code filter() → outputPartitioning() → planInputPartitions()} hand-off on the driver thread;
   * without it, {@code outputPartitioning()} could read a stale value and the Phase 3 assertion
   * could spuriously trip.
   */
  private transient volatile int numPartitions = -1;

  /**
   * Runtime filters most recently received via {@link #filter(Predicate[])}, after conversion to V1
   * and column-level filtering. Participates in {@link #equals(Object)} / {@link #hashCode()} so
   * that scans with distinct runtime filters do not wrongly dedupe under {@code ReusedExchange}.
   *
   * <p>{@code transient} because runtime filters are driver-only state; executors consume the
   * already-pruned {@link LanceInputPartition} payloads. {@code volatile} documents the intended
   * happens-before for the sequential {@code filter()} → {@code planInputPartitions()} hand-off on
   * the driver thread.
   */
  private transient volatile Filter[] runtimeFilters;

  /**
   * Surviving fragment IDs after applying runtime filters, populated by {@link
   * #filter(Predicate[])}. When non-null, {@link #pruneByZonemapStats} prefers this over {@link
   * #cachedSurvivingFragmentIds}. When null, no runtime narrowing has happened on this scan.
   */
  private transient volatile Set<Integer> effectiveSurvivingFragmentIds;

  // ---------------------------------------------------------------------------------------------
  // Observability metrics (Phase 2.5). Instance-local counters exposed via package-private
  // getters for tests and log emission. Names are a stability contract: breaking changes require
  // a major version bump. Full Spark SQL UI integration via the DataSourceV2 CustomMetric API is
  // a deferred follow-up; these fields provide the observable behavior today via INFO logs and
  // test assertions. `transient` because metrics are driver-only state.
  // ---------------------------------------------------------------------------------------------
  // Counters that accumulate (received-count, predicates-dropped, errors) use AtomicLong so the
  // `++` increment operator remains correct even if a future Spark execution model invokes
  // filter() concurrently. The pure-assignment counters (fragmentsTotal,
  // fragmentsPrunedStatic, fragmentsPrunedRuntime) are `volatile long` — assignment is already
  // atomic for longs when `volatile`, and there is no read-modify-write pattern on them.
  // `transient` because these are driver-only state and are not serialized to executors.
  /** Fragments in the dataset at plan time (set by {@link #planInputPartitions()}). */
  private transient volatile long fragmentsTotal;

  /**
   * Fragments eliminated by static filters + limit pruning (set by {@link #planInputPartitions()}).
   */
  private transient volatile long fragmentsPrunedStatic;

  /** Fragments eliminated by {@link #filter(Predicate[])} runtime filters. */
  private transient volatile long fragmentsPrunedRuntime;

  /** Count of calls to {@link #filter(Predicate[])} for this scan. */
  private transient java.util.concurrent.atomic.AtomicLong runtimeFiltersReceived =
      new java.util.concurrent.atomic.AtomicLong();

  /** Count of V2 predicates the converter could not translate. */
  private transient java.util.concurrent.atomic.AtomicLong predicatesDropped =
      new java.util.concurrent.atomic.AtomicLong();

  /** Exceptions caught inside {@link #filter(Predicate[])} (graceful fallback). */
  private transient java.util.concurrent.atomic.AtomicLong runtimeFilterErrors =
      new java.util.concurrent.atomic.AtomicLong();

  /**
   * Rehydrates transient metric counters after Java deserialization. The default deserializer
   * initializes transient fields to {@code null}, which would NPE on the first {@code
   * .incrementAndGet()}. Executors never call these counters, but defensively reinitialize so any
   * code path reading them post-deserialization sees a zero-valued counter instead of NPE.
   */
  private void readObject(java.io.ObjectInputStream in)
      throws java.io.IOException, ClassNotFoundException {
    in.defaultReadObject();
    this.runtimeFiltersReceived = new java.util.concurrent.atomic.AtomicLong();
    this.predicatesDropped = new java.util.concurrent.atomic.AtomicLong();
    this.runtimeFilterErrors = new java.util.concurrent.atomic.AtomicLong();
  }

  /**
   * Seeds the driver-side metrics populated in {@link LanceScanBuilder#build()} — currently only
   * the fragment total is known at construction time. Other counters start at zero and are updated
   * in {@link #filter(Predicate[])} and {@link #planInputPartitions()}.
   */
  void seedMetricsFromBuilder(long totalFragmentsAtBuild) {
    this.fragmentsTotal = totalFragmentsAtBuild;
  }

  long getFragmentsTotalMetric() {
    return fragmentsTotal;
  }

  long getFragmentsPrunedStaticMetric() {
    return fragmentsPrunedStatic;
  }

  long getFragmentsPrunedRuntimeMetric() {
    return fragmentsPrunedRuntime;
  }

  long getRuntimeFiltersReceivedMetric() {
    return runtimeFiltersReceived.get();
  }

  long getPredicatesDroppedMetric() {
    return predicatesDropped.get();
  }

  long getRuntimeFilterErrorsMetric() {
    return runtimeFilterErrors.get();
  }

  /**
   * Partition info detected from zonemap stats. When present, enables storage-partitioned joins
   * (SPJ) by reporting the partition column as the output partitioning key instead of {@code
   * _fragid}. Null when no partition-compatible column is detected.
   */
  private final ZonemapFragmentPruner.PartitionInfo partitionInfo;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final java.util.Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final java.util.Map<String, String> namespaceProperties;

  public LanceScan(
      StructType schema,
      LanceSparkReadOptions readOptions,
      Optional<String> whereConditions,
      Optional<Integer> limit,
      Optional<Integer> offset,
      Optional<List<ColumnOrdering>> topNSortOrders,
      Optional<Aggregation> pushedAggregation,
      Filter[] pushedFilters,
      LanceStatistics statistics,
      java.util.Map<String, List<ZoneStats>> zonemapStats,
      Set<Integer> survivingFragmentIds,
      ZonemapFragmentPruner.PartitionInfo partitionInfo,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties) {
    this.schema = schema;
    this.readOptions = readOptions;
    this.whereConditions = whereConditions;
    this.limit = limit;
    this.offset = offset;
    this.topNSortOrders = topNSortOrders;
    this.pushedAggregation = pushedAggregation;
    this.pushedFilters =
        pushedFilters != null ? Arrays.copyOf(pushedFilters, pushedFilters.length) : new Filter[0];
    this.statistics = statistics;
    this.zonemapStats = zonemapStats != null ? zonemapStats : Collections.emptyMap();
    this.cachedSurvivingFragmentIds = survivingFragmentIds;
    // Capture the static-pruning survivor count at construction so fragmentsPrunedRuntime can
    // use it as an immutable baseline across multiple filter() calls. -1 means "unknown — use
    // fragmentsTotal as a fallback baseline at filter() time".
    this.staticSurvivorCountAtBuild =
        survivingFragmentIds != null ? survivingFragmentIds.size() : -1L;
    this.partitionInfo = partitionInfo;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    LanceSplit.ScanPlanResult planResult = LanceSplit.planScan(readOptions);
    int totalSplits = planResult.getSplits().size();
    // Refresh fragmentsTotal in case the dataset/version changed between build() and plan time.
    this.fragmentsTotal = totalSplits;

    List<LanceSplit> prunedSplits = pruneByRowAddrFilters(planResult.getSplits());

    // Zonemap-based fragment pruning: uses per-column min/max/null_count
    // statistics to eliminate fragments that provably cannot match
    // pushed filters.
    prunedSplits = pruneByZonemapStats(prunedSplits);

    // Limit-based split pruning: when a LIMIT is pushed down without filters or TopN sort,
    // use per-fragment row counts to plan only enough splits to satisfy the limit.
    // This avoids scheduling hundreds of unnecessary tasks. Correctness is guaranteed
    // because Spark still keeps a global CollectLimit on top (isPartiallyPushed = true).
    prunedSplits = pruneByLimit(prunedSplits, planResult.getFragmentRowCounts());

    // fragmentsPrunedRuntime is computed in filter() (which runs before planInputPartitions
    // on the driver). fragmentsPrunedStatic is the remaining reduction: total minus both the
    // runtime-survivors and whatever additional pruning the limit/rowaddr paths removed.
    int survived = prunedSplits.size();
    long totalEliminated = Math.max(0L, (long) totalSplits - survived);
    this.fragmentsPrunedStatic = Math.max(0L, totalEliminated - fragmentsPrunedRuntime);
    // Byte-size-based fragment packing: merge multiple small fragments into a single
    // multi-fragment split so the emitted Spark task count tracks IO cost, not fragment count.
    // This MUST run after all pruning — otherwise already-eliminated fragments would be
    // re-bundled. Skipped when SPJ is active (partitionInfo != null) because SPJ requires one
    // fragment per InputPartition so the emitted partition key matches a single value. Also
    // skipped when a TopN is pushed, because multi-fragment per-partition scans would break
    // Lance's per-scan ordering contract.
    prunedSplits = maybePackFragments(prunedSplits, planResult.getFragmentByteSizes());

    // Capture as effectively final for use in lambda
    final List<LanceSplit> finalSplits = prunedSplits;

    // Use resolved version for snapshot isolation - ensures all workers read the same version
    LanceSparkReadOptions resolvedReadOptions =
        readOptions.withVersion((int) planResult.getResolvedVersion());

    // Compute preferred locations via consistent hash affinity (deterministic, no runtime
    // feedback).
    String[][] affinityLocations = new String[finalSplits.size()][];
    org.lance.spark.internal.LanceSoftAffinityManager affinity =
        org.lance.spark.internal.LanceSoftAffinityManager.getInstance();
    if (org.lance.spark.internal.LanceExecutorCache.isEnabled() && affinity.executorCount() > 0) {
      java.util.Map<String, String> baseOpts =
          resolvedReadOptions.getStorageOptions() != null
              ? resolvedReadOptions.getStorageOptions()
              : java.util.Collections.emptyMap();
      java.util.Map<String, String> mergedOpts =
          org.lance.spark.LanceRuntime.mergeStorageOptions(
              baseOpts,
              initialStorageOptions != null
                  ? initialStorageOptions
                  : java.util.Collections.emptyMap());
      for (int idx = 0; idx < finalSplits.size(); idx++) {
        int fragId = finalSplits.get(idx).getFragments().get(0);
        org.lance.spark.internal.LanceExecutorCacheKey key =
            new org.lance.spark.internal.LanceExecutorCacheKey(
                resolvedReadOptions.getDatasetUri(),
                resolvedReadOptions.getVersion(),
                fragId,
                resolvedReadOptions.getBatchSize(),
                mergedOpts);
        affinityLocations[idx] = affinity.getPreferredLocations(key.fingerprint());
      }
    } else {
      java.util.Arrays.fill(affinityLocations, new String[0]);
    }

    InputPartition[] result =
        IntStream.range(0, finalSplits.size())
            .mapToObj(
                i -> {
                  LanceSplit split = finalSplits.get(i);
                  InternalRow partKeyRow = null;
                  if (partitionInfo != null && split.getFragments().size() == 1) {
                    int fragId = split.getFragments().get(0);
                    partKeyRow = partitionInfo.partitionKeyForFragment(fragId);
                  }
                  return new LanceInputPartition(
                      schema,
                      i,
                      split,
                      resolvedReadOptions,
                      whereConditions,
                      limit,
                      offset,
                      topNSortOrders,
                      pushedAggregation,
                      scanId,
                      initialStorageOptions,
                      namespaceImpl,
                      namespaceProperties,
                      partKeyRow,
                      affinityLocations[i]);
                })
            .toArray(InputPartition[]::new);

    this.numPartitions = result.length;
    return result;
  }

  /**
   * Prunes splits based on {@code _rowaddr} filters — skipping fragment opens, scan setup, and task
   * scheduling for fragments that provably cannot match the query predicate.
   *
   * <p>CONTRACT: {@link LanceSplit#getFragments()} returns Lance fragment IDs as Integer values
   * that match {@code (int)(rowAddr >>> 32)} — the same encoding used by {@link
   * org.lance.spark.join.FragmentAwareJoinUtils}. This is verified by {@link
   * LanceSplit#planScan(LanceSparkReadOptions)} which maps {@code Fragment.getId()} directly.
   *
   * <p>Note: an empty allowedIds set is valid — it means the filter is unsatisfiable (e.g. {@code
   * _rowaddr = 0 AND _rowaddr = 4294967296L}) and no fragments can match, resulting in zero rows
   * returned.
   */
  private List<LanceSplit> pruneByRowAddrFilters(List<LanceSplit> allSplits) {
    java.util.Optional<Set<Integer>> targetFragmentIds =
        RowAddressFilterAnalyzer.extractTargetFragmentIds(pushedFilters);
    if (!targetFragmentIds.isPresent()) {
      return allSplits;
    }
    Set<Integer> allowedIds = targetFragmentIds.get();
    // Assumes each LanceSplit maps to a single fragment. If splits ever
    // bundle multiple fragments, consider sub-split level pruning.
    List<LanceSplit> pruned =
        allSplits.stream()
            .filter(
                split -> {
                  if (split.getFragments().size() > 1) {
                    LOG.warn(
                        "Split contains {} fragments;" + " sub-split pruning not implemented",
                        split.getFragments().size());
                  }
                  return split.getFragments().stream().anyMatch(allowedIds::contains);
                })
            .collect(Collectors.toList());
    if (pruned.size() < allSplits.size()) {
      LOG.debug(
          "Pruned fragments by _rowaddr filters: {} of {} splits retained,"
              + " allowed fragment IDs: {}",
          pruned.size(),
          allSplits.size(),
          allowedIds);
    } else {
      LOG.debug(
          "No fragments pruned by _rowaddr filters: all {} splits retained,"
              + " allowed fragment IDs: {}",
          allSplits.size(),
          allowedIds);
    }
    return pruned;
  }

  /**
   * Prunes splits based on pushed LIMIT using per-fragment row counts from the manifest.
   *
   * <p>When a LIMIT is pushed down without filters or TopN sort orders, we can use the per-fragment
   * logical row counts (which account for deletions) to determine how many fragments are needed to
   * satisfy the limit. This avoids scheduling hundreds of unnecessary tasks for large tables.
   *
   * <p>This optimization is skipped when:
   *
   * <ul>
   *   <li>No limit is pushed
   *   <li>Filters are present (unknown selectivity makes row count estimation unreliable)
   *   <li>TopN sort orders are present (all fragments needed for global sort)
   *   <li>Aggregation is pushed (e.g., COUNT(*) LIMIT — row counts don't apply)
   *   <li>Vector search (nearest) is active (needs global search across all fragments)
   *   <li>Fragment row counts are unavailable
   * </ul>
   *
   * <p>Correctness is guaranteed because Spark keeps a global {@code CollectLimit} on top (since
   * {@code isPartiallyPushed()} returns {@code true}). If we under-estimate due to concurrent
   * deletions, the query simply returns fewer rows than the limit — which is valid LIMIT semantics.
   */
  private List<LanceSplit> pruneByLimit(
      List<LanceSplit> allSplits, java.util.Map<Integer, Long> fragmentRowCounts) {
    if (!limit.isPresent()
        || whereConditions.isPresent()
        || runtimeFilters != null
        || topNSortOrders.isPresent()
        || pushedAggregation.isPresent()
        || readOptions.getNearest() != null
        || fragmentRowCounts.isEmpty()) {
      // whereConditions covers Spark-pushed static filters; runtimeFilters covers DFP runtime
      // filters. Both mean the per-fragment manifest row count over-estimates what the executor
      // will actually emit, so stopping early based on manifest counts can drop fragments that
      // contain the only rows satisfying the filter — returning fewer rows than LIMIT requested
      // despite more matches existing in skipped fragments.
      return allSplits;
    }

    int requestedLimit = limit.get();
    long rowsAccumulated = 0;
    List<LanceSplit> pruned = new java.util.ArrayList<>();

    for (LanceSplit split : allSplits) {
      pruned.add(split);
      for (int fragmentId : split.getFragments()) {
        Long rowCount = fragmentRowCounts.get(fragmentId);
        if (rowCount != null) {
          rowsAccumulated += rowCount;
        }
      }
      if (rowsAccumulated >= requestedLimit) {
        break;
      }
    }

    if (pruned.size() < allSplits.size()) {
      LOG.debug(
          "Limit-based pruning: {} of {} splits retained for LIMIT {} "
              + "(accumulated {} rows from selected fragments)",
          pruned.size(),
          allSplits.size(),
          requestedLimit,
          rowsAccumulated);
    }

    return pruned;
  }

  /**
   * Merges multiple small fragments into multi-fragment {@link LanceSplit}s using {@link
   * LanceFragmentPacker}. Returns {@code allSplits} unchanged when packing is inapplicable:
   *
   * <ul>
   *   <li>Storage-partitioned joins (SPJ): {@code partitionInfo != null} — Spark's {@link
   *       KeyGroupedPartitioning} contract requires one fragment per input partition so the
   *       advertised partition-key value is unique.
   *   <li>TopN push-down: {@code topNSortOrders.isPresent()} — Lance's fragment scan emits rows
   *       ordered per-fragment, and merging fragments across a single reader breaks that contract.
   *   <li>The list contains fewer than two single-fragment splits: nothing to merge.
   * </ul>
   */
  private List<LanceSplit> maybePackFragments(
      List<LanceSplit> allSplits, java.util.Map<Integer, Long> fragmentByteSizes) {
    if (allSplits.size() < 2) {
      return allSplits;
    }
    if (partitionInfo != null) {
      return allSplits;
    }
    if (topNSortOrders.isPresent()) {
      return allSplits;
    }

    SparkSession session;
    try {
      session = SparkSession.active();
    } catch (Throwable t) {
      // No active session (e.g., some unit-test contexts). Skip packing, preserve semantics.
      LOG.debug("No active SparkSession for fragment packing; skipping.");
      return allSplits;
    }

    List<LanceSplit> packed =
        LanceFragmentPacker.packFragmentsIntoSplits(session, allSplits, fragmentByteSizes);
    if (packed.size() < allSplits.size()) {
      LOG.info(
          "LanceFragmentPacker merged {} single-fragment splits into {} byte-sized splits"
              + " for scan={}",
          allSplits.size(),
          packed.size(),
          description());
    }
    return packed;
  }

  /**
   * Prunes splits based on zonemap index statistics — using per-column min/max/null_count to
   * eliminate fragments that provably cannot match the pushed filters.
   *
   * <p>This is analogous to partition pruning in Hive/Iceberg: fragments whose zones all fail the
   * predicate are skipped entirely, avoiding fragment opens, scan setup, and task scheduling.
   */
  private List<LanceSplit> pruneByZonemapStats(List<LanceSplit> allSplits) {
    // Preference order:
    //   1. effectiveSurvivingFragmentIds populated by filter() — reflects runtime narrowing.
    //   2. cachedSurvivingFragmentIds pre-computed by LanceScanBuilder — static pruning only.
    //   3. Recompute from pushedFilters + zonemapStats (fallback when neither cache is present).
    Set<Integer> allowedIds;
    if (effectiveSurvivingFragmentIds != null) {
      allowedIds = effectiveSurvivingFragmentIds;
    } else if (cachedSurvivingFragmentIds != null) {
      allowedIds = cachedSurvivingFragmentIds;
    } else if (!zonemapStats.isEmpty()) {
      allowedIds = ZonemapFragmentPruner.pruneFragments(pushedFilters, zonemapStats).orElse(null);
    } else {
      return allSplits;
    }

    if (allowedIds == null) {
      return allSplits;
    }
    List<LanceSplit> pruned =
        allSplits.stream()
            .filter(split -> split.getFragments().stream().anyMatch(allowedIds::contains))
            .collect(Collectors.toList());

    if (pruned.size() < allSplits.size()) {
      LOG.debug(
          "Zonemap pruning: {} of {} splits retained," + " allowed fragment IDs: {}",
          pruned.size(),
          allSplits.size(),
          allowedIds);
    }

    return pruned;
  }

  /**
   * Reports the output partitioning to Spark's optimizer.
   *
   * <p>When a partition-compatible column is detected via zonemap stats (every fragment has a
   * single distinct value for that column), we report the data column as the partition key. This
   * enables Spark's storage-partitioned join (SPJ) protocol — allowing shuffle-free joins between
   * Lance tables or between Lance and other data sources (e.g., Iceberg) that share the same
   * partition column.
   *
   * <p>When no partition column is detected, returns {@link UnknownPartitioning}.
   */
  @Override
  public Partitioning outputPartitioning() {
    if (partitionInfo != null) {
      // Number of distinct partition values currently advertised. Post-filter, only surviving
      // fragments contribute; pre-filter, all fragments in partitionInfo do. This preserves the
      // SPJ contract: never introduce new partition values, never exceed the per-key partition
      // count.
      int derivedCount = derivedPartitionCount();

      // Spark's SPJ contract (BatchScanExec.scala:78-117) requires KeyGroupedPartitioning's
      // numPartitions to match the emitted InputPartition count. When the dataset has multiple
      // fragments sharing the same partition value (e.g., 6 fragments covering 2 distinct values),
      // planInputPartitions() emits one split per fragment, so the emitted count can exceed the
      // distinct-value count. In that case SPJ cannot be represented with the current splitting
      // strategy — degrade to UnknownPartitioning instead of crashing the query. The kill-switch
      // is still available for users who need SPJ and can tolerate the fallback scan shape.
      if (numPartitions > derivedCount) {
        LOG.warn(
            "SPJ disabled for scan={}: emitted {} splits exceed {} distinct partition values."
                + " Set {}=false to also disable DFP if needed.",
            description(),
            numPartitions,
            derivedCount,
            LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_ENABLED);
        return new UnknownPartitioning(numPartitions);
      }
      Expression[] keys = new Expression[] {FieldReference.apply(partitionInfo.getColumnName())};
      return new KeyGroupedPartitioning(keys, derivedCount);
    }
    return new UnknownPartitioning(numPartitions >= 0 ? numPartitions : 0);
  }

  /**
   * The number of distinct partition values advertised by {@link #outputPartitioning()}. Uses
   * {@link #effectiveSurvivingFragmentIds} when populated (DFP post-filter), otherwise the
   * pre-filter partitionInfo map.
   */
  private int derivedPartitionCount() {
    java.util.Map<Integer, Comparable<?>> allPartValues =
        partitionInfo.getFragmentPartitionValues();
    // Snapshot the volatile field once to avoid a torn-read NPE if a concurrent filter() call
    // nulls it between the null check and the iteration. In the normal single-threaded driver
    // path this is equivalent; the local binding only matters under hypothetical re-entrant use.
    Set<Integer> survivors = this.effectiveSurvivingFragmentIds;
    if (survivors == null) {
      // Distinct partition values across all fragments. Previously returned the map's size (the
      // fragment count), which was wrong for datasets where multiple fragments share the same
      // partition value — it over-advertised KeyGroupedPartitioning.numPartitions and then the
      // subsequent planInputPartitions() count check fired a spurious violation.
      return new HashSet<>(allPartValues.values()).size();
    }
    Set<Comparable<?>> surviving = new HashSet<>();
    Set<Integer> missing = null;
    for (Integer fragId : survivors) {
      Comparable<?> v = allPartValues.get(fragId);
      if (v == null) {
        // filter()'s Phase 3 subset check already rejects this case; reaching here means that
        // guard was bypassed (e.g., a test set effectiveSurvivingFragmentIds directly, or a
        // future refactor removed it). Fail loudly rather than silently under-count and advertise
        // an invalid KeyGroupedPartitioning to Spark.
        if (missing == null) {
          missing = new HashSet<>();
        }
        missing.add(fragId);
      } else {
        surviving.add(v);
      }
    }
    if (missing != null) {
      throw new IllegalStateException(
          "LanceScan.outputPartitioning(): surviving fragments "
              + missing
              + " have no entry in the build-time partition-value map for scan="
              + description()
              + ". This violates the SPJ contract. Set "
              + LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_ENABLED
              + "=false to disable DFP while this is investigated.");
    }
    return surviving.size();
  }

  // ---------------------------------------------------------------------------------------------
  // Dynamic File Pruning (DFP) — SupportsRuntimeV2Filtering implementation (Phase 2).
  // See dfp-implementation-plan.md for the full design; the step numbers below mirror the plan.
  // ---------------------------------------------------------------------------------------------

  /**
   * Columns we can accept runtime filters on. These are exactly the columns for which zonemap stats
   * were loaded on the driver during {@link LanceScanBuilder#build()}; loading for columns outside
   * this set is intentionally impossible after {@code build()} has closed the dataset.
   */
  @Override
  public NamedReference[] filterAttributes() {
    if (!readOptions.isRuntimeFilteringEnabled() || zonemapStats.isEmpty()) {
      return new NamedReference[0];
    }
    // Intersect zonemap-indexed columns with the current scan's projected schema. Advertising a
    // column that was pruned away by SupportsPushDownRequiredColumns makes Spark's attribute
    // resolver throw "Unable to resolve X given [projected cols]" during runtime-filter planning,
    // because Spark builds an AttributeReference from the NamedReference and expects the scan to
    // actually output that column. This is particularly easy to hit on star-schema queries that
    // join on ss_sold_date_sk and ss_item_sk but don't read ss_customer_sk: the fact-side scan
    // doesn't include it in the output, so Spark can't construct a runtime filter on it.
    Set<String> projected = new HashSet<>();
    for (org.apache.spark.sql.types.StructField f : schema.fields()) {
      projected.add(f.name());
    }
    return zonemapStats.keySet().stream()
        .filter(projected::contains)
        .map(FieldReference::apply)
        .toArray(NamedReference[]::new);
  }

  /**
   * Receives runtime filters derived by Spark's {@code PartitionPruning} rule from a small build
   * side of a join. The method mutates the scan in place — Spark re-reads {@link
   * #planInputPartitions()} on the same reference via {@code BatchScanExec.scala:76}.
   *
   * <p>Step order (see plan):
   *
   * <ol>
   *   <li>Clear all prior runtime state unconditionally (<code>runtimeFilters</code>, <code>
   *       effectiveSurvivingFragmentIds</code>, <code>numPartitions</code>). Replace-not-union
   *       semantics: each call starts from the static baseline.
   *   <li>Convert {@code predicates} to V1 filters via {@link PredicateToFilterConverter}, dropping
   *       entries that reference columns not in {@link #zonemapStats}. If the converted array is
   *       empty (no-op call), leave {@code runtimeFilters} null.
   *   <li>Otherwise assign the converted array to {@link #runtimeFilters} so {@code equals/
   *       hashCode} reflect the new predicates.
   *   <li>Merge with {@link #pushedFilters} (logical AND), re-run {@link
   *       ZonemapFragmentPruner#pruneFragments}.
   *   <li>If the pruner returns a narrowed set, store it in {@link #effectiveSurvivingFragmentIds}
   *       and null out {@link #cachedSurvivingFragmentIds}. Otherwise leave {@code
   *       cachedSurvivingFragmentIds} intact.
   * </ol>
   *
   * <p>Error handling: generic exceptions fall back to static pruning with a WARN. An {@code
   * IllegalStateException} — thrown by the Phase 3 SPJ subset check — is rethrown so correctness
   * violations are fail-fast rather than silently masked.
   */
  @Override
  public void filter(Predicate[] predicates) {
    // Step 1 — unconditional reset of runtime state.
    this.effectiveSurvivingFragmentIds = null;
    this.numPartitions = -1;
    this.runtimeFilters = null;
    this.fragmentsPrunedRuntime = 0L;
    this.runtimeFiltersReceived.incrementAndGet();

    if (!readOptions.isRuntimeFilteringEnabled()) {
      return;
    }
    if (predicates == null || predicates.length == 0) {
      return;
    }

    try {
      int maxInValues = readOptions.getRuntimeFilteringMaxInValues();
      int predicatesIn = predicates.length;
      Filter[] converted =
          PredicateToFilterConverter.convertAll(
              predicates, zonemapStats.keySet(), maxInValues, LOG);
      // Every input predicate that did not survive conversion is counted as dropped.
      this.predicatesDropped.addAndGet(Math.max(0, predicatesIn - converted.length));

      if (converted.length == 0) {
        // Step 2 — empty conversion. Two distinct cases with DIFFERENT pruneByLimit semantics:
        //   (a) predicatesIn == 0 : caller passed no predicates. Spark did not inject a runtime
        //       filter. No Filter operator sits above the scan; manifest row counts are accurate
        //       and pruneByLimit is safe to run.
        //   (b) predicatesIn >  0 : caller passed predicates but every one was dropped (cap
        //       exceeded, unsupported shape, wrong column). Spark STILL has a Filter operator
        //       above BatchScanExec — runtime-filter row-level filtering happens at execution
        //       regardless of what the scan did. Manifest row counts over-estimate post-Filter
        //       emission, so pruneByLimit MUST stay suppressed, otherwise it drops fragments
        //       containing the only matching rows and LIMIT returns fewer rows than requested.
        //
        // Install an empty-array sentinel for case (b). pruneByLimit's runtimeFilters != null
        // guard fires. equals/hashCode treat null and Filter[0] as equivalent (both mean "no
        // narrowing filter to compare against"), so ReusedExchange dedup is preserved across
        // scans that received no-op vs. cap-dropped filters.
        if (predicatesIn > 0) {
          this.runtimeFilters = new Filter[0];
        }
        LOG.info(
            "DFP filter() no-op for scan={}: {} predicates in, 0 converted;"
                + " runtimeFiltersReceived={} predicatesDropped={}",
            description(),
            predicatesIn,
            runtimeFiltersReceived.get(),
            predicatesDropped.get());
        return;
      }

      // Step 3 — record the converted filters for scan identity.
      this.runtimeFilters = converted;

      // Step 4 — merge with the static baseline (not with any prior runtimeFilters) and re-prune.
      Filter[] combined =
          Stream.concat(Arrays.stream(pushedFilters), Arrays.stream(converted))
              .toArray(Filter[]::new);
      Set<Integer> newSurviving =
          ZonemapFragmentPruner.pruneFragments(combined, zonemapStats).orElse(null);
      if (newSurviving == null) {
        // Pruner could not narrow (e.g., value overlaps every zone, or filter on unindexed
        // column). Keep cachedSurvivingFragmentIds intact so static pruning still applies. Leave
        // runtimeFilters = converted: Spark wraps BatchScanExec in a Filter operator for the
        // runtime-filter predicate, so row-level filtering still happens at execution even
        // though the scan emitted every fragment. pruneByLimit MUST stay suppressed because
        // manifest row counts over-estimate post-Filter emission by the same mechanism as when
        // the pruner does narrow. Two scans with the same runtime filter here still equals()
        // each other via equivalentRuntimeFilters, so ReusedExchange dedup continues to work;
        // clearing runtimeFilters would incorrectly equate them with a never-filtered scan.
        LOG.info(
            "DFP filter() for scan={}: {} predicates in, {} converted, no narrowing derived",
            description(),
            predicatesIn,
            converted.length);
        return;
      }

      // Phase 3 — SPJ contract defensive subset check. When partitionInfo is present, the scan
      // advertises KeyGroupedPartitioning with a fixed set of partition values. After runtime
      // filtering, the surviving fragments' partition-value set MUST be a subset of the
      // originally reported set (BatchScanExec.scala:78-117 enforces this at the Spark side).
      // The ZonemapFragmentPruner can only shrink the surviving set, so a violation here
      // indicates an upstream pruner bug; fail-fast rather than silently masking incorrect
      // results downstream.
      if (partitionInfo != null) {
        java.util.Map<Integer, Comparable<?>> allPartValues =
            partitionInfo.getFragmentPartitionValues();
        Set<Comparable<?>> originalValues = new HashSet<>(allPartValues.values());
        Set<Comparable<?>> survivingValues = new HashSet<>();
        Set<Integer> missingFragIds = new HashSet<>();
        for (Integer fragId : newSurviving) {
          Comparable<?> v = allPartValues.get(fragId);
          if (v == null) {
            missingFragIds.add(fragId);
          } else {
            survivingValues.add(v);
          }
        }
        if (!missingFragIds.isEmpty()) {
          // Surviving fragments aren't in the partition-column zonemap. This can legitimately
          // happen when different columns' indexes cover different fragment sets (e.g., a new
          // fragment was appended after the partition column's index was built, but the DFP
          // column's index was rebuilt and covers it). We can't represent the narrowed survivor
          // set without partition values, so fall back to pre-DFP static pruning for this scan.
          // CRITICAL: leave runtimeFilters = converted intact. Spark keeps the runtime filter in
          // a Filter operator above BatchScanExec regardless of what the scan does with it, so
          // post-scan rows are still filtered at execution. pruneByLimit's runtimeFilters != null
          // guard (R30) must stay armed, otherwise manifest-row-count-based limit pruning would
          // drop fragments that contain the only matching rows — returning fewer rows than LIMIT.
          LOG.warn(
              "DFP SPJ: surviving fragments {} absent from partition-value map for scan={};"
                  + " dropping runtime narrowing, static pruning preserved.",
              missingFragIds,
              description());
          this.runtimeFilterErrors.incrementAndGet();
          this.effectiveSurvivingFragmentIds = null;
          return;
        }
        if (!originalValues.containsAll(survivingValues)) {
          // Surviving partition values weren't in the originally reported set. Same graceful
          // fallback as above — signals an upstream pruner bug but crashing the query doesn't
          // help the user, and static pruning is still correct. Same critical rule: keep
          // runtimeFilters set so pruneByLimit stays suppressed for this scan.
          Set<Comparable<?>> extras = new HashSet<>(survivingValues);
          extras.removeAll(originalValues);
          LOG.warn(
              "DFP SPJ: surviving partition values {} not in originally reported set {} for"
                  + " scan={}; dropping runtime narrowing, static pruning preserved.",
              extras,
              originalValues,
              description());
          this.runtimeFilterErrors.incrementAndGet();
          this.effectiveSurvivingFragmentIds = null;
          return;
        }
      }

      // Step 5 — install the narrowed set; invalidate the build-time cache so pruneByZonemapStats
      // consults effectiveSurvivingFragmentIds on the subsequent planInputPartitions() call.
      // Compute the runtime-pruned count BEFORE nulling cachedSurvivingFragmentIds. Use the
      // IMMUTABLE staticSurvivorCountAtBuild as the baseline — not cachedSurvivingFragmentIds,
      // because a prior filter() call may have nulled that out; not fragmentsPrunedStatic,
      // because it is populated in planInputPartitions() which runs AFTER filter(). When
      // staticSurvivorCountAtBuild is -1 (no build-time static pruning), fall back to
      // fragmentsTotal which is seeded by seedMetricsFromBuilder() and refreshed in
      // planInputPartitions().
      long preRuntimeSurvivors =
          staticSurvivorCountAtBuild >= 0 ? staticSurvivorCountAtBuild : fragmentsTotal;
      if (preRuntimeSurvivors > 0 && newSurviving.size() <= preRuntimeSurvivors) {
        this.fragmentsPrunedRuntime = preRuntimeSurvivors - newSurviving.size();
      }
      this.effectiveSurvivingFragmentIds = newSurviving;
      this.cachedSurvivingFragmentIds = null;
      LOG.info(
          "DFP filter() narrowed scan={}: {} -> {} fragments ({} pruned) via {} predicates",
          description(),
          preRuntimeSurvivors,
          newSurviving.size(),
          fragmentsPrunedRuntime,
          converted.length);
    } catch (IllegalStateException ise) {
      // Preserved as a defensive rethrow for any genuine correctness-critical programming error
      // that surfaces as IllegalStateException. Phase 3's SPJ subset check now degrades softly
      // (WARN + disable runtime filter) rather than throwing, so this branch is currently dead
      // for that path — but any other caller in the try block that signals IllegalStateException
      // semantically indicates a bug that should not be silently swallowed.
      throw ise;
    } catch (Throwable t) {
      this.runtimeFilterErrors.incrementAndGet();
      LOG.warn(
          "Runtime filter failed for scan={}, falling back to static pruning."
              + " Predicates={} runtimeFilterErrors={} Error={}",
          description(),
          Arrays.toString(predicates),
          runtimeFilterErrors.get(),
          t.getMessage());
      // Ensure any partial state is cleared.
      this.effectiveSurvivingFragmentIds = null;
      // Mirror the R50 sentinel: if Spark handed us real predicates then it placed a Filter
      // operator above BatchScanExec, which will apply the predicate at row level regardless of
      // whether we successfully pruned. pruneByLimit's runtimeFilters != null guard must stay
      // armed so manifest row counts don't drive early split termination and silently drop
      // fragments containing the only matching rows. An empty-array sentinel preserves the
      // guard without affecting equals/hashCode (equivalentRuntimeFilters treats null and
      // Filter[0] as equivalent).
      if (predicates != null && predicates.length > 0) {
        this.runtimeFilters = new Filter[0];
      } else {
        this.runtimeFilters = null;
      }
    }
  }

  /** Short human-readable description used in log/exception messages. */
  @Override
  public String description() {
    return "LanceScan(" + readOptions.getDatasetName() + ")";
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new LanceReaderFactory();
  }

  @Override
  public StructType readSchema() {
    if (pushedAggregation.isPresent()) {
      return new StructType().add("count", org.apache.spark.sql.types.DataTypes.LongType);
    }
    return schema;
  }

  @Override
  public Map<String, String> getMetaData() {
    scala.collection.immutable.Map<String, String> empty =
        scala.collection.immutable.Map$.MODULE$.empty();
    scala.collection.immutable.Map<String, String> result = empty;
    result = result.$plus(scala.Tuple2.apply("whereConditions", whereConditions.toString()));
    result = result.$plus(scala.Tuple2.apply("limit", limit.toString()));
    result = result.$plus(scala.Tuple2.apply("offset", offset.toString()));
    result = result.$plus(scala.Tuple2.apply("topNSortOrders", topNSortOrders.toString()));
    result = result.$plus(scala.Tuple2.apply("pushedAggregation", pushedAggregation.toString()));
    return result;
  }

  @Override
  public Statistics estimateStatistics() {
    return statistics;
  }

  /**
   * Declares the executor-side DFP metrics that partition readers may emit via {@link
   * org.apache.spark.sql.connector.read.PartitionReader#currentMetricsValues()}. Today this is a
   * single counter — {@link FragmentsScannedMetric} — which reports how many Lance fragments each
   * task actually opened. Spark aggregates across tasks and surfaces the total in the SQL UI.
   *
   * <p>The eight driver-side counters ({@code fragmentsTotal}, {@code fragmentsPrunedStatic}, etc.)
   * are not declared here — they are computed on the driver and emitted via INFO logs, not through
   * the task-metric pipeline.
   */
  @Override
  public CustomMetric[] supportedCustomMetrics() {
    return new CustomMetric[] {new FragmentsScannedMetric()};
  }

  // ---------- SupportsMerge implementation ----------

  /**
   * Attempts to merge this scan with {@code other} to eliminate redundant I/O when the {@code
   * MergeScalarSubqueries} optimizer rule detects duplicate scalar subqueries over the same table.
   *
   * <p>Two strategies are tried following ParquetScan's pattern:
   *
   * <ol>
   *   <li><b>Relaxed merge</b> — when {@code spark.sql.planMerge.ignorePushedDataFilters} is
   *       enabled (default): the scans must share the same {@code readOptions} and namespace
   *       config, and must not have limit/offset/topN/aggregation pushed. Filters are OR-merged.
   *   <li><b>Strict merge</b> — when the scans have identical pushdowns (filters, limit, topN,
   *       aggregation), only the schema differs. The merged scan reuses statistics from {@code
   *       this}.
   * </ol>
   */
  @Override
  public java.util.Optional<SupportsMerge> mergeWith(SupportsMerge other, SupportsRead table) {
    if (other == null) {
      return java.util.Optional.empty();
    }
    LanceScan o = (LanceScan) other;
    if (!similarScan(o)) {
      return java.util.Optional.empty();
    }
    Filter[] newFilters =
        new Filter[] {
          new Or(combineFiltersWithAnd(this.pushedFilters), combineFiltersWithAnd(o.pushedFilters))
        };
    Optional<String> whereCondition = FilterPushDown.compileFiltersToSqlWhereClause(newFilters);
    zonemapStats.putAll(o.zonemapStats);
    return java.util.Optional.of(
        new LanceScan(
            schema.merge(o.schema, true),
            readOptions,
            whereCondition,
            limit,
            offset,
            topNSortOrders,
            pushedAggregation,
            newFilters,
            statistics,
            zonemapStats,
            null,
            partitionInfo,
            initialStorageOptions,
            namespaceImpl,
            namespaceProperties));
  }

  /**
   * Relaxed compatibility: same dataset and namespace config, no limit/offset/topN/aggregation on
   * either side. Filters may differ.
   */
  private boolean similarScan(LanceScan other) {
    if (this == other) {
      return true;
    }
    if (getClass() != other.getClass()) {
      return false;
    }
    return Objects.equals(readOptions, other.readOptions)
        && !offset.isPresent()
        && !other.offset.isPresent()
        && !topNSortOrders.isPresent()
        && !other.topNSortOrders.isPresent()
        && !pushedAggregation.isPresent()
        && !other.pushedAggregation.isPresent()
        && Objects.equals(namespaceImpl, other.namespaceImpl)
        && Objects.equals(namespaceProperties, other.namespaceProperties)
        && (isIgnorePushedDataFiltersEnabled()
            ? similarPartition(other)
            : equivalentFilters(pushedFilters, other.pushedFilters));
  }

  private boolean similarPartition(LanceScan other) {
    if (this.partitionInfo == null && other.partitionInfo == null) {
      return true;
    }
    if (this.partitionInfo != null && other.partitionInfo != null) {
      return partitionInfo.getColumnName().equals(other.partitionInfo.getColumnName());
    }
    return false;
  }

  /** Reduces a filter array to a single {@link And} tree. */
  private static Filter combineFiltersWithAnd(Filter[] filters) {
    if (filters.length == 0) {
      throw new IllegalArgumentException("Cannot combine zero filters");
    }
    Filter result = filters[0];
    for (int i = 1; i < filters.length; i++) {
      result = new And(result, filters[i]);
    }
    return result;
  }

  /**
   * Reads {@code spark.sql.planMerge.ignorePushedDataFilters} from the active SparkSession.
   * Defaults to {@code true}.
   */
  private static boolean isIgnorePushedDataFiltersEnabled() {
    try {
      return SparkSession.active()
          .conf()
          .get("spark.sql.planMerge.ignorePushedDataFilters", "true")
          .equalsIgnoreCase("true");
    } catch (Exception e) {
      // No active session (e.g., unit test) — use default
      return true;
    }
  }

  /**
   * Required for Spark's ReusedExchange: {@code BatchScanExec.equals()} compares {@code batch}
   * objects, which delegate to this method since LanceScan implements Batch.
   *
   * <p>Excludes {@code scanId} (per-instance tracing UUID, not scan identity).
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LanceScan that = (LanceScan) o;
    return Objects.equals(schema, that.schema)
        && Objects.equals(readOptions, that.readOptions)
        && Objects.equals(whereConditions, that.whereConditions)
        && Objects.equals(limit, that.limit)
        && Objects.equals(offset, that.offset)
        && Objects.equals(topNSortOrders.toString(), that.topNSortOrders.toString())
        && aggregationEquals(pushedAggregation, that.pushedAggregation)
        && equivalentFilters(pushedFilters, that.pushedFilters)
        && equivalentRuntimeFilters(runtimeFilters, that.runtimeFilters);
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            schema, readOptions, whereConditions, limit, offset, topNSortOrders.toString());
    result = 31 * result + Arrays.hashCode(sortedByHash(pushedFilters));
    result = 31 * result + aggregationHashCode(pushedAggregation);
    // Runtime filters participate in scan identity so ReusedExchange does not collide two
    // logically distinct DFP-narrowed scans. Null is treated identically to Filter[0] so a scan
    // that never had filter() called hashes identically to a scan that received a no-op call.
    Filter[] rf = runtimeFilters;
    if (rf != null && rf.length > 0) {
      result = 31 * result + Arrays.hashCode(sortedByHash(rf));
    }
    return result;
  }

  /**
   * Null-and-empty-array tolerant equivalence check for runtime filters: a scan that never had
   * {@link #filter(Predicate[])} called ({@code null}) compares equal to one that received a no-op
   * {@code filter(new Predicate[0])} ({@code empty}). Both represent "no runtime filter".
   */
  private static boolean equivalentRuntimeFilters(Filter[] a, Filter[] b) {
    int lenA = a == null ? 0 : a.length;
    int lenB = b == null ? 0 : b.length;
    if (lenA == 0 && lenB == 0) {
      return true;
    }
    if (lenA != lenB) {
      return false;
    }
    return equivalentFilters(a, b);
  }

  /**
   * Compares two Optional&lt;Aggregation&gt; by value. {@code Aggregation}'s auto-generated {@code
   * equals()} uses reference identity for its array components, so we sort by hashCode and compare
   * element-wise — following {@code AggregatePushDownUtils.equivalentAggregations()}.
   */
  private static boolean aggregationEquals(Optional<Aggregation> a, Optional<Aggregation> b) {
    if (a.isPresent() != b.isPresent()) {
      return false;
    }
    if (!a.isPresent()) {
      return true;
    }
    Aggregation agg1 = a.get();
    Aggregation agg2 = b.get();
    return Arrays.equals(
            sortedByHash(agg1.aggregateExpressions()), sortedByHash(agg2.aggregateExpressions()))
        && Arrays.equals(
            sortedByHash(agg1.groupByExpressions()), sortedByHash(agg2.groupByExpressions()));
  }

  private static int aggregationHashCode(Optional<Aggregation> agg) {
    if (!agg.isPresent()) {
      return 0;
    }
    return Objects.hash(
        Arrays.hashCode(sortedByHash(agg.get().aggregateExpressions())),
        Arrays.hashCode(sortedByHash(agg.get().groupByExpressions())));
  }

  /**
   * Returns whether two filter arrays are equivalent regardless of order. Follows Spark's {@code
   * FileScan.equivalentFilters()}: sort by hashCode, then compare element-wise.
   */
  private static boolean equivalentFilters(Filter[] a, Filter[] b) {
    return Arrays.equals(sortedByHash(a), sortedByHash(b));
  }

  private static <T> T[] sortedByHash(T[] arr) {
    T[] copy = Arrays.copyOf(arr, arr.length);
    Arrays.sort(copy, (x, y) -> Integer.compare(x.hashCode(), y.hashCode()));
    return copy;
  }

  private static class LanceReaderFactory implements PartitionReaderFactory {
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      Preconditions.checkArgument(
          partition instanceof LanceInputPartition,
          "Unknown InputPartition type. Expecting LanceInputPartition");
      return LanceRowPartitionReader.create((LanceInputPartition) partition);
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      Preconditions.checkArgument(
          partition instanceof LanceInputPartition,
          "Unknown InputPartition type. Expecting LanceInputPartition");

      LanceInputPartition lancePartition = (LanceInputPartition) partition;
      if (lancePartition.getPushedAggregation().isPresent()) {
        AggregateFunc[] aggFunc =
            lancePartition.getPushedAggregation().get().aggregateExpressions();
        if (aggFunc.length == 1 && aggFunc[0] instanceof CountStar) {
          return new LanceCountStarPartitionReader(lancePartition);
        }
      }

      return new LanceColumnarPartitionReader(lancePartition);
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
      return true;
    }
  }
}
