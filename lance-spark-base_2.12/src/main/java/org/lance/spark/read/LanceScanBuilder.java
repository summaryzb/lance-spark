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
import org.lance.Fragment;
import org.lance.ManifestSummary;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;
import org.lance.index.scalar.ZoneStats;
import org.lance.ipc.ColumnOrdering;
import org.lance.schema.LanceField;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.utils.Optional;
import org.lance.spark.utils.Utils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownOffset;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownTopN;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class LanceScanBuilder
    implements SupportsPushDownRequiredColumns,
        SupportsPushDownFilters,
        SupportsPushDownLimit,
        SupportsPushDownOffset,
        SupportsPushDownTopN,
        SupportsPushDownAggregates {
  private static final Logger LOG = LoggerFactory.getLogger(LanceScanBuilder.class);

  private final LanceSparkReadOptions readOptions;
  private final StructType fullSchema;
  private StructType schema;

  private Filter[] pushedFilters = new Filter[0];
  private Optional<Integer> limit = Optional.empty();
  private Optional<Integer> offset = Optional.empty();
  private Optional<List<ColumnOrdering>> topNSortOrders = Optional.empty();
  private Optional<Aggregation> pushedAggregation = Optional.empty();
  private LanceLocalScan localScan = null;

  // Lazily opened dataset for reuse during scan building
  private Dataset lazyDataset = null;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final java.util.Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final java.util.Map<String, String> namespaceProperties;

  private final java.util.Map<String, String> tableProperties;

  // --- DFP observability metrics (Phase 2.5) ---
  // Scoped to this builder instance, set during build() and handed to the caller via
  // package-private getters. AtomicLong because the parallel zonemap-loading path increments
  // from worker threads while the driver thread later reads them in build() — without atomic
  // access the read may miss increments.
  private final java.util.concurrent.atomic.AtomicLong statsLoadAttemptsMetric =
      new java.util.concurrent.atomic.AtomicLong();
  private final java.util.concurrent.atomic.AtomicLong statsLoadErrorsMetric =
      new java.util.concurrent.atomic.AtomicLong();

  long getStatsLoadAttemptsMetric() {
    return statsLoadAttemptsMetric.get();
  }

  long getStatsLoadErrorsMetric() {
    return statsLoadErrorsMetric.get();
  }

  public LanceScanBuilder(
      StructType schema,
      LanceSparkReadOptions readOptions,
      java.util.Map<String, String> initialStorageOptions,
      String namespaceImpl,
      java.util.Map<String, String> namespaceProperties,
      java.util.Map<String, String> tableProperties) {
    this.fullSchema = schema;
    this.schema = schema;
    this.readOptions = readOptions;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableProperties = tableProperties != null ? tableProperties : Collections.emptyMap();
  }

  /**
   * Gets or opens a dataset for reuse during scan building. The dataset is lazily opened on first
   * access and reused for subsequent calls.
   */
  private Dataset getOrOpenDataset() {
    if (lazyDataset == null) {
      lazyDataset = Utils.openDatasetBuilder(readOptions).build();
    }
    return lazyDataset;
  }

  /** Closes the lazily opened dataset if it was opened. */
  private void closeLazyDataset() {
    if (lazyDataset != null) {
      lazyDataset.close();
      lazyDataset = null;
    }
  }

  @Override
  public Scan build() {
    // Wrap the entire body in try/finally so any exception in manifest lookup, zonemap loading,
    // byte-cap estimation, or scan construction still closes the lazily-opened native dataset
    // handle. Without this, an exception path silently leaks a native Dataset every build().
    try {
      return buildInternal();
    } finally {
      closeLazyDataset();
    }
  }

  private Scan buildInternal() {
    // Return LocalScan if we have a metadata-only aggregation result
    if (localScan != null) {
      return localScan;
    }

    // Get statistics from manifest summary before closing dataset
    ManifestSummary summary = getOrOpenDataset().getVersion().getManifestSummary();

    // Collect all columns that need zonemap stats. Base set is always filter columns + partition
    // column (if declared). When runtime filtering (DFP) is enabled, the set expands to every
    // ZONEMAP-indexed column so that LanceScan.filterAttributes() (added in Phase 2) can advertise
    // all runtime-filterable columns.
    Set<String> columnsToLoad = extractReferencedColumns(pushedFilters);
    String partitionColumn = tableProperties.get(LanceConstant.TABLE_OPT_PARTITION_COLUMNS);
    if (partitionColumn != null && !partitionColumn.trim().isEmpty()) {
      partitionColumn = partitionColumn.trim();
      columnsToLoad.add(partitionColumn);
    } else {
      partitionColumn = null;
    }

    Dataset dataset = getOrOpenDataset();
    // Cache the result of findZonemapIndexedColumns for the lifetime of this build() call so
    // (a) we avoid calling describeIndices() twice per build and (b) the expansion step and the
    // loading step share one consistent view of the index set (no TOCTOU window).
    Set<String> zonemapIndexedColumns = null;
    if (readOptions.isRuntimeFilteringEnabled()) {
      zonemapIndexedColumns = findZonemapIndexedColumns(dataset);
      Set<String> zonemapIndexed = zonemapIndexedColumns;
      int cap = readOptions.getRuntimeFilteringMaxColumns();
      if (zonemapIndexed.size() > cap) {
        // Deterministically prefer already-referenced columns + lowest field IDs, then truncate.
        List<String> sorted = sortIndexedColumnsByFieldId(dataset, zonemapIndexed);
        LinkedHashSet<String> selected = new LinkedHashSet<>(columnsToLoad);
        for (String col : sorted) {
          if (selected.size() >= cap) {
            break;
          }
          selected.add(col);
        }
        List<String> excluded = new ArrayList<>(zonemapIndexed);
        excluded.removeAll(selected);
        LOG.warn(
            "ZONEMAP-indexed column count ({}) exceeds {}={}. Loading stats for {} columns;"
                + " excluded columns: {}",
            zonemapIndexed.size(),
            LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_MAX_COLUMNS,
            cap,
            selected.size(),
            excluded);
        columnsToLoad = selected;
      } else {
        columnsToLoad.addAll(zonemapIndexed);
      }
    }

    // Load zonemap stats for all requested columns in one pass. Pass the cached index set so
    // loadZonemapStats doesn't re-describe indices on the native side.
    Map<String, List<ZoneStats>> zonemapStats =
        loadZonemapStats(dataset, columnsToLoad, zonemapIndexedColumns);

    // Enforce in-memory byte cap. If the loaded stats exceed the configured budget, drop all
    // stats (preserving static pruning on the pre-filter baseline) and WARN. Future scans pick up
    // the same decision per-call, so the byte-cap breach does not persist state.
    if (!zonemapStats.isEmpty()) {
      long estimatedBytes = estimateZonemapBytes(zonemapStats);
      long budget = readOptions.getRuntimeFilteringMaxStatsBytes();
      if (estimatedBytes > budget) {
        LOG.warn(
            "Loaded zonemap stats estimated at {} bytes exceed {}={} bytes; disabling DFP for"
                + " this scan (static pruning preserved). Column sizes (bytes): {}",
            estimatedBytes,
            LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_MAX_STATS_BYTES,
            budget,
            perColumnByteEstimate(zonemapStats));
        zonemapStats = Collections.emptyMap();
      }
    }

    // Detect partition-compatible columns, gated on lance.partition.columns table property.
    // Currently a partitioned column is only valid if each fragment contains only a single
    // value for that column (i.e., all zonemap zones have min == max with the same value).
    ZonemapFragmentPruner.PartitionInfo partitionInfo = null;
    if (partitionColumn != null) {
      if (!zonemapStats.containsKey(partitionColumn)) {
        LOG.warn(
            "Partition column '{}' declared in {} has no zonemap index or stats;"
                + " partition detection disabled",
            partitionColumn,
            LanceConstant.TABLE_OPT_PARTITION_COLUMNS);
      } else {
        Map<Integer, Comparable<?>> partValues =
            ZonemapFragmentPruner.computeFragmentPartitionValues(zonemapStats.get(partitionColumn))
                .orElse(null);
        if (partValues != null) {
          partitionInfo = new ZonemapFragmentPruner.PartitionInfo(partitionColumn, partValues);
          LOG.info(
              "Detected partition-compatible column '{}' with {} fragments",
              partitionColumn,
              partValues.size());
        }
      }
    }

    // Pre-compute fragment pruning so we can (a) estimate post-pruning statistics for
    // JoinSelection (BroadcastHashJoin vs SortMergeJoin) and (b) pass the cached result
    // to LanceScan to avoid re-computing during planInputPartitions().
    Set<Integer> survivingFragmentIds = null;
    if (pushedFilters.length > 0 && !zonemapStats.isEmpty()) {
      survivingFragmentIds =
          ZonemapFragmentPruner.pruneFragments(pushedFilters, zonemapStats).orElse(null);
    }

    // Scale rows and full size by the zonemap fragment-pruning ratio first, then let
    // LanceStatistics.estimateProjected apply the column-width ratio on top
    // (when the projected schema is narrower than the full schema).
    long projectedRows = summary.getTotalRows();
    long projectedFullSize = summary.getTotalFilesSize();
    if (survivingFragmentIds != null && summary.getTotalFragments() > 0) {
      double ratio = (double) survivingFragmentIds.size() / summary.getTotalFragments();
      projectedRows = (long) (projectedRows * ratio);
      projectedFullSize = (long) (projectedFullSize * ratio);
    }
    LanceStatistics statistics =
        CatalogColumnStatAdapter.estimateProjected(
            projectedRows, projectedFullSize, fullSchema, tableProperties);
    if (survivingFragmentIds != null) {
      LOG.debug(
          "Scan statistics after pruning: {} of {} fragments survive,"
              + " estimatedSize={}, estimatedRows={} (full: size={}, rows={})",
          survivingFragmentIds.size(),
          summary.getTotalFragments(),
          statistics.sizeInBytes(),
          statistics.numRows(),
          summary.getTotalFilesSize(),
          summary.getTotalRows());
    }

    // Dataset close is handled by the try/finally wrapper in build() — no explicit close here.

    Optional<String> whereCondition = FilterPushDown.compileFiltersToSqlWhereClause(pushedFilters);
    LanceScan scan =
        new LanceScan(
            schema,
            readOptions,
            whereCondition,
            limit,
            offset,
            topNSortOrders,
            pushedAggregation,
            pushedFilters,
            statistics,
            zonemapStats,
            survivingFragmentIds,
            partitionInfo,
            initialStorageOptions,
            namespaceImpl,
            namespaceProperties);
    scan.seedMetricsFromBuilder(summary.getTotalFragments());
    if (readOptions.isRuntimeFilteringEnabled() && !zonemapStats.isEmpty()) {
      LOG.info(
          "DFP scan built for {}: fragmentsTotal={} zonemapColumnsLoaded={}"
              + " statsLoadAttempts={} statsLoadErrors={}",
          readOptions.getDatasetName(),
          summary.getTotalFragments(),
          zonemapStats.size(),
          statsLoadAttemptsMetric.get(),
          statsLoadErrorsMetric.get());
    }
    return scan;
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.schema = requiredSchema;
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    if (!readOptions.isPushDownFilters()) {
      return filters;
    }
    Filter[][] processFilters = FilterPushDown.processFilters(filters);
    pushedFilters = processFilters[0];
    // Return ALL input filters, not just the ones we cannot push. This mirrors Iceberg's
    // SparkScanBuilder.pushPredicates (classifies all pushable filters as "partially pushed"
    // and returns them as post-scan). Rationale: Spark's optimizer rules that inject runtime
    // filters — PartitionPruning (DPP/DFP) via hasSelectivePredicate, and InjectRuntimeFilter
    // via extractBeneficialFilterCreatePlan — both pattern-match on a Filter(pred, scan) node
    // above the relation. If we tell Spark "I fully handled it" (by returning only rejected
    // filters), Spark eliminates the Filter node, the optimizer rules see a bare scan with no
    // selective predicate, and no runtime filter gets injected on the probe side. The pushed
    // subset (recorded in pushedFilters() below) is still pre-applied at scan time via Lance's
    // where condition; Spark's re-evaluation above the scan is an idempotent pass-through on
    // already-matching rows and costs negligible CPU.
    return filters;
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public boolean pushLimit(int limit) {
    this.limit = Optional.of(limit);
    return true;
  }

  @Override
  public boolean pushOffset(int offset) {
    // Only one data file can be pushed down the offset.
    List<Integer> fragmentIds =
        getOrOpenDataset().getFragments().stream()
            .map(Fragment::getId)
            .collect(Collectors.toList());
    if (fragmentIds.size() == 1) {
      this.offset = Optional.of(offset);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean isPartiallyPushed() {
    return true;
  }

  @Override
  public boolean pushTopN(SortOrder[] orders, int limit) {
    // The Order by operator will use compute thread in lance.
    // So it's better to have an option to enable it.
    if (!readOptions.isTopNPushDown()) {
      return false;
    }
    this.limit = Optional.of(limit);
    List<ColumnOrdering> topNSortOrders = new ArrayList<>();
    for (SortOrder sortOrder : orders) {
      ColumnOrdering.Builder builder = new ColumnOrdering.Builder();
      builder.setNullFirst(sortOrder.nullOrdering() == NullOrdering.NULLS_FIRST);
      builder.setAscending(sortOrder.direction() == SortDirection.ASCENDING);
      if (!(sortOrder.expression() instanceof FieldReference)) {
        return false;
      }
      FieldReference reference = (FieldReference) sortOrder.expression();
      builder.setColumnName(reference.fieldNames()[0]);
      topNSortOrders.add(builder.build());
    }
    this.topNSortOrders = Optional.of(topNSortOrders);
    return true;
  }

  @Override
  public boolean pushAggregation(Aggregation aggregation) {
    AggregateFunc[] funcs = aggregation.aggregateExpressions();
    if (aggregation.groupByExpressions().length > 0) {
      return false;
    }
    if (funcs.length == 1 && funcs[0] instanceof CountStar) {
      // Check if we can use metadata-based count (no filters pushed)
      if (pushedFilters.length == 0) {
        Optional<Long> metadataCount = getCountFromMetadata(getOrOpenDataset());
        if (metadataCount.isPresent()) {
          // Create LocalScan with pre-computed count result
          StructType countSchema = new StructType().add("count", DataTypes.LongType);
          InternalRow[] rows = new InternalRow[1];
          rows[0] = new GenericInternalRow(new Object[] {metadataCount.get()});
          this.localScan = new LanceLocalScan(countSchema, rows, readOptions.getDatasetUri());
          return true;
        }
      }
      // Fall back to scan-based count (with filters or metadata unavailable)
      this.pushedAggregation = Optional.of(aggregation);
      return true;
    }

    return false;
  }

  private static Optional<Long> getCountFromMetadata(Dataset dataset) {
    try {
      ManifestSummary summary = dataset.getVersion().getManifestSummary();
      return Optional.of(summary.getTotalRows());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  /**
   * Loads zonemap statistics for the requested columns. Only loads stats for columns that have a
   * zonemap index. Per-column loading is bounded by {@code lance.runtime.filtering.load.timeout.ms}
   * and optionally parallelized when {@code lance.runtime.filtering.load.parallelism > 1}; timeouts
   * and exceptions cause the column to be silently skipped with a WARN.
   */
  private Map<String, List<ZoneStats>> loadZonemapStats(
      Dataset dataset, Set<String> columns, Set<String> cachedIndexedColumns) {
    if (columns.isEmpty()) {
      return Collections.emptyMap();
    }

    Set<String> zonemapColumns =
        cachedIndexedColumns != null ? cachedIndexedColumns : findZonemapIndexedColumns(dataset);
    if (zonemapColumns.isEmpty()) {
      return Collections.emptyMap();
    }

    List<String> toLoad = new ArrayList<>();
    for (String col : columns) {
      if (zonemapColumns.contains(col)) {
        toLoad.add(col);
      }
    }
    if (toLoad.isEmpty()) {
      return Collections.emptyMap();
    }

    long timeoutMs = readOptions.getRuntimeFilteringLoadTimeoutMs();
    int parallelism =
        Math.max(1, Math.min(readOptions.getRuntimeFilteringLoadParallelism(), toLoad.size()));
    // Count one attempt per column we will try to load; the success/failure breakdown is tallied
    // inside the per-column helpers and surfaced via statsLoadErrorsMetric.
    this.statsLoadAttemptsMetric.addAndGet(toLoad.size());

    Map<String, List<ZoneStats>> result = new HashMap<>();
    // Single shared bounded pool for both serial (parallelism=1) and parallel paths. Bounds
    // the number of daemon threads per build() call to `parallelism` — if any native call is
    // non-interruptible and exceeds the timeout, at most `parallelism` threads leak before the
    // pool is shut down (and they terminate naturally once native returns).
    ExecutorService pool =
        Executors.newFixedThreadPool(
            parallelism,
            r -> {
              Thread t = new Thread(r, "lance-zonemap-load");
              t.setDaemon(true);
              return t;
            });
    try {
      List<Map.Entry<String, Future<Optional<List<ZoneStats>>>>> futures = new ArrayList<>();
      for (String col : toLoad) {
        Future<Optional<List<ZoneStats>>> f =
            pool.submit(() -> loadOneColumnNoTimeout(dataset, col));
        futures.add(new AbstractMap.SimpleEntry<>(col, f));
      }
      // Timeout budget — use a global deadline so queued tasks in the parallelism>1 path don't
      // get extra budget from earlier columns finishing quickly. Each Future.get call waits only
      // up to the remaining time. In the serial case (parallelism=1) this means N columns share
      // a total of timeoutMs × N wall-clock (enforced by per-get remaining-time math below).
      // Actually, to match the plan's "per-column timeout" intent we want *each column* to have
      // up to timeoutMs. We keep that semantics below by waiting at most timeoutMs per column,
      // but also capping by a global deadline to bound overall build() time.
      long perColumnBudgetMs = timeoutMs;
      long globalDeadlineNanos =
          System.nanoTime()
              + TimeUnit.MILLISECONDS.toNanos(timeoutMs * Math.max(1L, (long) toLoad.size()));
      for (Map.Entry<String, Future<Optional<List<ZoneStats>>>> entry : futures) {
        String col = entry.getKey();
        Future<Optional<List<ZoneStats>>> f = entry.getValue();
        long remainingGlobalNanos = Math.max(0L, globalDeadlineNanos - System.nanoTime());
        long waitNanos =
            Math.min(remainingGlobalNanos, TimeUnit.MILLISECONDS.toNanos(perColumnBudgetMs));
        try {
          Optional<List<ZoneStats>> stats = f.get(waitNanos, TimeUnit.NANOSECONDS);
          stats.ifPresent(s -> result.put(col, s));
        } catch (TimeoutException te) {
          this.statsLoadErrorsMetric.incrementAndGet();
          LOG.warn(
              "Timed out loading zonemap stats for column '{}' after {} ms; skipping column",
              col,
              timeoutMs);
          f.cancel(true);
        } catch (java.util.concurrent.ExecutionException ee) {
          // loadOneColumnNoTimeout rethrows; unwrap and count once here.
          this.statsLoadErrorsMetric.incrementAndGet();
          LOG.warn(
              "Failed to load zonemap stats for column '{}': {}",
              col,
              ee.getCause() != null ? ee.getCause().getMessage() : ee.getMessage());
        } catch (InterruptedException ie) {
          // Preserve the interrupt flag so the driver can shut down cleanly; stop loading.
          Thread.currentThread().interrupt();
          this.statsLoadErrorsMetric.incrementAndGet();
          LOG.warn(
              "Interrupted while loading zonemap stats for column '{}'; aborting remaining loads",
              col);
          f.cancel(true);
          break;
        } catch (Exception e) {
          this.statsLoadErrorsMetric.incrementAndGet();
          LOG.warn("Failed to load zonemap stats for column '{}': {}", col, e.getMessage());
        }
      }
    } finally {
      pool.shutdownNow();
      try {
        // Wait up to the full per-column timeout for tasks to exit. This bounds the window in
        // which a still-running native getZonemapStats() call could hold a reference to the
        // lazyDataset that build()'s outer finally is about to close. Non-interruptible JNI
        // calls longer than this may still outlive the wait — that is a documented trade-off:
        // making the native call interruptible is out of scope. If awaitTermination reports
        // false, WARN so operators can investigate (it may indicate a stuck native call plus a
        // concurrent scan-close race).
        if (!pool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
          LOG.warn(
              "Zonemap-load pool for dataset '{}' did not terminate within {} ms after"
                  + " shutdownNow(); non-interruptible native tasks may outlive the dataset close."
                  + " Increase lance.runtime.filtering.load.timeout.ms if this persists.",
              readOptions.getDatasetName(),
              timeoutMs);
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }

    if (!result.isEmpty()) {
      LOG.debug("Loaded zonemap stats for {} columns: {}", result.size(), result.keySet());
    }

    return result;
  }

  /**
   * Worker-side loader. Does NOT catch exceptions — the outer {@link #loadZonemapStats} loop is the
   * sole owner of the {@link #statsLoadErrorsMetric} counter and handles logging on the driver
   * thread. Rethrowing here lets {@code Future.get()} surface errors via {@code ExecutionException}
   * so counting happens exactly once.
   */
  private Optional<List<ZoneStats>> loadOneColumnNoTimeout(Dataset dataset, String col) {
    List<ZoneStats> stats = dataset.getZonemapStats(col);
    if (stats.isEmpty()) {
      return Optional.empty();
    }
    LOG.debug("Loaded {} zonemap zones for column '{}'", stats.size(), col);
    return Optional.of(stats);
  }

  /**
   * Deterministic ordering of ZONEMAP-indexed columns by ascending field ID. Used when truncating
   * the loaded set to {@code lance.runtime.filtering.max.columns}.
   */
  private static List<String> sortIndexedColumnsByFieldId(Dataset dataset, Set<String> columns) {
    Map<String, Integer> columnToId = new HashMap<>();
    try {
      for (LanceField field : dataset.getLanceSchema().fields()) {
        if (columns.contains(field.getName())) {
          columnToId.put(field.getName(), field.getId());
        }
      }
    } catch (Exception e) {
      // Schema lookup failed; fall back to a deterministic name-order sort so the max.columns cap
      // truncation remains reproducible. Logged at WARN so operators can see schema issues rather
      // than silently getting a different column ordering than field-ID-based truncation would.
      LOG.warn(
          "Failed to read dataset schema for zonemap-column sorting;"
              + " falling back to name order: {}",
          e.getMessage());
    }
    List<String> sorted = new ArrayList<>(columns);
    sorted.sort(
        Comparator.comparingInt((String c) -> columnToId.getOrDefault(c, Integer.MAX_VALUE))
            .thenComparing(Comparator.naturalOrder()));
    return sorted;
  }

  /**
   * Rough estimate of the in-memory footprint of loaded {@link ZoneStats}. The 96-byte base covers
   * primitive fields + object headers + two {@code min}/{@code max} references. For numeric columns
   * the boxed primitives fit inside that base; for {@link String} and {@code byte[]} min/max values
   * the actual referent is variable-length and must be added separately — otherwise the byte-cap is
   * bypassed on large string-keyed ZONEMAP indexes. A first-zone sample is used as a representative
   * per-column length; this is O(1) per column and does not scan the whole list.
   */
  static long estimateZonemapBytes(Map<String, List<ZoneStats>> stats) {
    long total = 0L;
    for (List<ZoneStats> zones : stats.values()) {
      total += 16L + (long) zones.size() * perZoneBytes(zones);
    }
    return total;
  }

  private static Map<String, Long> perColumnByteEstimate(Map<String, List<ZoneStats>> stats) {
    Map<String, Long> out = new LinkedHashMap<>();
    for (Map.Entry<String, List<ZoneStats>> entry : stats.entrySet()) {
      List<ZoneStats> zones = entry.getValue();
      out.put(entry.getKey(), 16L + (long) zones.size() * perZoneBytes(zones));
    }
    return out;
  }

  /** Per-zone heap cost including variable-length min/max for string/binary columns. */
  private static long perZoneBytes(List<ZoneStats> zones) {
    long perZone = 96L;
    if (!zones.isEmpty()) {
      ZoneStats sample = zones.get(0);
      perZone += variableReferentBytes(sample.getMin());
      perZone += variableReferentBytes(sample.getMax());
    }
    return perZone;
  }

  /**
   * Additional heap bytes beyond the fixed-size reference slot already counted in the 96-byte base.
   * Returns 0 for numeric boxed primitives (which fit inside the base); approximates {@link String}
   * as {@code 24 + 2 * length} (header + UTF-16 chars). {@code byte[]} doesn't implement {@link
   * Comparable}, so Lance never stores it in {@link ZoneStats#getMin()}/{@code getMax()} — no
   * branch is needed. An all-null sample falls back to a conservative 256 bytes so a column whose
   * first zone happens to be all-null still over-counts rather than under-counts.
   */
  private static long variableReferentBytes(Comparable<?> v) {
    if (v == null) {
      return 256L;
    }
    if (v instanceof String) {
      return 24L + 2L * ((String) v).length();
    }
    return 0L;
  }

  private Set<String> findZonemapIndexedColumns(Dataset dataset) {
    Set<String> columns = new HashSet<>();
    try {
      Map<Integer, String> fieldIdToName = new HashMap<>();
      for (LanceField field : dataset.getLanceSchema().fields()) {
        fieldIdToName.put(field.getId(), field.getName());
      }

      // Use the criteria-based overload so that indexes missing index_details
      // (created by older versions) are silently skipped instead of causing errors.
      IndexCriteria criteria = new IndexCriteria.Builder().build();
      for (IndexDescription idx : dataset.describeIndices(criteria)) {
        if ("ZONEMAP".equalsIgnoreCase(idx.getIndexType())) {
          for (int fieldId : idx.getFieldIds()) {
            String name = fieldIdToName.get(fieldId);
            if (name != null) {
              columns.add(name);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to query zonemap indexes: {}", e.getMessage());
    }
    return columns;
  }

  private static Set<String> extractReferencedColumns(Filter[] filters) {
    Set<String> columns = new HashSet<>();
    for (Filter filter : filters) {
      for (String attr : filter.references()) {
        columns.add(attr);
      }
    }
    return columns;
  }
}
