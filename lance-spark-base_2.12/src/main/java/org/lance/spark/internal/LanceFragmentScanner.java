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
package org.lance.spark.internal;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.utils.LazyResource;
import org.lance.spark.utils.Utils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LanceFragmentScanner implements AutoCloseable {
  private final LazyResource<Dataset> datasetSupplier;
  private final LazyResource<LanceScanner> scannerSupplier;
  private final int fragmentId;
  private final boolean withFragemtId;
  private final LanceInputPartition inputPartition;

  /**
   * When true, {@link #datasetSupplier} is owned externally by the caller (e.g. a per-partition
   * reader that shares one dataset across many fragments). {@link #close()} will NOT close the
   * dataset even if it has been initialized.
   */
  private final boolean datasetExternallyOwned;

  private LanceFragmentScanner(
      LazyResource<Dataset> datasetSupplier,
      LazyResource<LanceScanner> scannerSupplier,
      int fragmentId,
      boolean withFragmentId,
      LanceInputPartition inputPartition,
      boolean datasetExternallyOwned) {
    this.datasetSupplier = datasetSupplier;
    this.scannerSupplier = scannerSupplier;
    this.fragmentId = fragmentId;
    this.withFragemtId = withFragmentId;
    this.inputPartition = inputPartition;
    this.datasetExternallyOwned = datasetExternallyOwned;
  }

  public static LanceFragmentScanner create(int fragmentId, LanceInputPartition inputPartition) {
    LazyResource<Dataset> owned = new LazyResource<>(() -> openDataset(inputPartition));
    return buildScanner(fragmentId, inputPartition, owned, /* externallyOwned= */ false);
  }

  /**
   * Variant that reuses a dataset supplier opened by the caller. The returned scanner will NOT
   * close the dataset when {@link #close()} is invoked; lifecycle responsibility stays with the
   * caller.
   *
   * <p>Used by {@link org.lance.spark.read.LanceColumnarPartitionReader} when one partition packs
   * multiple fragments — avoids re-opening the dataset per fragment AND skips opening at all when
   * every fragment is served from the executor disk cache.
   */
  public static LanceFragmentScanner create(
      int fragmentId,
      LanceInputPartition inputPartition,
      LazyResource<Dataset> sharedDatasetSupplier) {
    if (sharedDatasetSupplier == null) {
      return create(fragmentId, inputPartition);
    }
    return buildScanner(
        fragmentId, inputPartition, sharedDatasetSupplier, /* externallyOwned= */ true);
  }

  /**
   * Test-only factory: lets a test inject counting suppliers so it can assert the cache-hit short
   * circuit never invokes them.
   */
  static LanceFragmentScanner createForTest(
      int fragmentId,
      LanceInputPartition inputPartition,
      LazyResource<Dataset> datasetSupplier,
      LazyResource<LanceScanner> scannerSupplier) {
    boolean withFragmentId =
        inputPartition.getSchema().getFieldIndex(LanceConstant.FRAGMENT_ID).nonEmpty();
    return new LanceFragmentScanner(
        datasetSupplier,
        scannerSupplier,
        fragmentId,
        withFragmentId,
        inputPartition,
        /* datasetExternallyOwned= */ false);
  }

  private static LanceFragmentScanner buildScanner(
      int fragmentId,
      LanceInputPartition inputPartition,
      LazyResource<Dataset> datasetSupplier,
      boolean externallyOwned) {
    boolean withFragmentId =
        inputPartition.getSchema().getFieldIndex(LanceConstant.FRAGMENT_ID).nonEmpty();
    LazyResource<LanceScanner> scannerSupplier =
        new LazyResource<>(() -> buildLanceScanner(fragmentId, inputPartition, datasetSupplier));
    return new LanceFragmentScanner(
        datasetSupplier,
        scannerSupplier,
        fragmentId,
        withFragmentId,
        inputPartition,
        externallyOwned);
  }

  private static Dataset openDataset(LanceInputPartition inputPartition) {
    LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
    // Optionally rebuild the namespace client on the executor so the dataset open routes through
    // Utils.OpenDatasetBuilder's namespaceClient branch. Preserves the storage options provider on
    // the Rust side, which refreshes short-lived vended credentials (e.g. STS tokens) during
    // long-running scans. The price is an eager describeTable() RPC against the namespace on
    // every fragment open.
    //
    // For catalogs whose backing service authenticates per-call (e.g. Hive Metastore over
    // Kerberos) executors typically lack a TGT and that RPC fails with "GSS initiate failed".
    // Setting LanceSparkReadOptions.CONFIG_EXECUTOR_CREDENTIAL_REFRESH=false makes executors skip
    // the rebuild and open the dataset by URI using the initialStorageOptions the driver already
    // obtained, at the cost of losing the Rust-side credential refresh callback.
    if (inputPartition.getNamespaceImpl() != null && readOptions.isExecutorCredentialRefresh()) {
      if (LanceRuntime.useNamespaceOnWorkers(inputPartition.getNamespaceImpl())) {
        readOptions.setNamespace(
            LanceRuntime.getOrCreateNamespace(
                inputPartition.getNamespaceImpl(), inputPartition.getNamespaceProperties()));
      } else {
        readOptions.setNamespace(null);
      }
    }
    return Utils.openDatasetBuilder(readOptions)
        .initialStorageOptions(inputPartition.getInitialStorageOptions())
        .build();
  }

  private static LanceScanner buildLanceScanner(
      int fragmentId, LanceInputPartition inputPartition, LazyResource<Dataset> datasetSupplier) {
    Dataset dataset = datasetSupplier.get();
    Fragment fragment = dataset.getFragment(fragmentId);
    LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
    if (fragment == null) {
      throw new IllegalStateException(
          String.format(
              "Fragment %d not found in dataset at %s (version=%s)",
              fragmentId, readOptions.getDatasetUri(), readOptions.getVersion()));
    }
    ScanOptions.Builder scanOptions = new ScanOptions.Builder();
    List<String> projectedColumns = getColumnNames(inputPartition.getSchema());
    if (projectedColumns.isEmpty() && inputPartition.getSchema().isEmpty()) {
      // Lance requires at least one projected column. Use _rowid as a lightweight sentinel so the
      // scanner still returns the correct row count (e.g. SELECT 1). Only do this when the schema
      // is truly empty; when the schema contains virtual columns (e.g. _fragid, blob position/
      // size) that are not passed to the scanner but added later by the batch scanner, adding
      // _rowid here would shift column indices and cause Spark to read wrong data.
      scanOptions.withRowId(true);
    }
    scanOptions.columns(projectedColumns);
    if (inputPartition.getWhereCondition().isPresent()) {
      scanOptions.filter(inputPartition.getWhereCondition().get());
    }
    scanOptions.batchSize(readOptions.getBatchSize());
    if (readOptions.getNearest() != null) {
      scanOptions.nearest(readOptions.getNearest());
      // We strictly set `prefilter = true` here to ensure query correctness:
      // 1. Spark currently performs the vector search by individually scanning each fragment.
      // 2. Lance mandates that `prefilter` must be enabled for fragmented vector queries.
      scanOptions.prefilter(true);
    }
    if (inputPartition.getLimit().isPresent()) {
      scanOptions.limit(inputPartition.getLimit().get());
    }
    if (inputPartition.getOffset().isPresent()) {
      scanOptions.offset(inputPartition.getOffset().get());
    }
    if (inputPartition.getTopNSortOrders().isPresent()) {
      scanOptions.setColumnOrderings(inputPartition.getTopNSortOrders().get());
    }
    return fragment.newScan(scanOptions.build());
  }

  /**
   * @return the arrow reader. The caller is responsible for closing the reader.
   */
  public ArrowReader getArrowReader() {
    LanceSparkReadOptions readOptions = inputPartition.getReadOptions();
    BufferAllocator allocator = LanceRuntime.allocator();

    boolean useExecutorCache = LanceExecutorCache.isEnabled() && readOptions.getVersion() != null;
    if (useExecutorCache) {
      LanceExecutorCacheKey execKey = buildExecutorCacheKey(readOptions);
      List<String> projectedCols = getColumnNames(inputPartition.getSchema());
      try {
        return LanceExecutorCache.getInstance()
            .getOrLoadColumns(
                execKey, projectedCols, allocator, missCols -> scannerSupplier.get().scanBatches());
      } catch (IOException e) {
        throw new RuntimeException("load Lance fragment from executor disk cache", e);
      }
    }

    LanceScanner scanner = scannerSupplier.get();
    ArrowReader reader = scanner.scanBatches();
    int queueDepth = inputPartition.getReadOptions().getBatchPrefetchQueueDepth();
    if (queueDepth <= 0) {
      return reader;
    }
    BufferAllocator parent;
    try {
      parent = LanceRuntime.allocator();
    } catch (RuntimeException | Error e) {
      try {
        reader.close();
      } catch (Exception closeEx) {
        e.addSuppressed(closeEx);
      }
      throw e;
    }
    return new PrefetchingArrowReader(reader, queueDepth, parent);
  }

  /**
   * Builds an {@link LanceExecutorCacheKey} identifying the decoded output of this fragment scan
   * under the requested projection/batchSize. Merged-auth storage options (minus connector-owned
   * keys) are hashed into the key so two queries with different credentials never share a cache
   * slot.
   *
   * <p>The WHERE clause and projected columns are <b>not</b> part of the key — this is a per-column
   * pre-filter cache. Each column is stored independently under the fragment directory, enabling
   * partial hits across queries with different projections. {@code LanceScanBuilder.pushFilters}
   * declines to push filters when the executor cache is enabled, so {@code scanner::scanBatches}
   * returns unfiltered rows and Spark's {@code Filter} operator applies the predicate post-cache.
   */
  private LanceExecutorCacheKey buildExecutorCacheKey(LanceSparkReadOptions readOptions) {
    java.util.Map<String, String> base =
        readOptions.getStorageOptions() != null
            ? readOptions.getStorageOptions()
            : java.util.Collections.emptyMap();
    java.util.Map<String, String> merged =
        LanceRuntime.mergeStorageOptions(base, inputPartition.getInitialStorageOptions());
    return new LanceExecutorCacheKey(
        readOptions.getDatasetUri(),
        readOptions.getVersion(),
        fragmentId,
        readOptions.getBatchSize(),
        merged);
  }

  @Override
  public void close() throws IOException {
    Throwable primary = null;
    LanceScanner scanner = scannerSupplier.getIfInitialized();
    if (scanner != null) {
      try {
        scanner.close();
      } catch (Throwable t) {
        primary = t;
      }
    }
    if (!datasetExternallyOwned) {
      Dataset dataset = datasetSupplier.getIfInitialized();
      if (dataset != null) {
        try {
          dataset.close();
        } catch (Throwable t) {
          if (primary != null) {
            primary.addSuppressed(t);
          } else {
            primary = t;
          }
        }
      }
    }
    if (primary != null) {
      if (primary instanceof IOException) {
        throw (IOException) primary;
      }
      if (primary instanceof RuntimeException) {
        throw (RuntimeException) primary;
      }
      if (primary instanceof Error) {
        throw (Error) primary;
      }
      throw new IOException(primary);
    }
  }

  public int fragmentId() {
    return fragmentId;
  }

  public boolean withFragemtId() {
    return withFragemtId;
  }

  public LanceInputPartition getInputPartition() {
    return inputPartition;
  }

  /**
   * Builds the projection column list for the scanner. Regular data columns come first, followed by
   * special metadata columns in the order matching {@link
   * org.lance.spark.LanceDataset#METADATA_COLUMNS}. All special columns (_rowid, _rowaddr, version
   * columns) go through scanner.project() for consistent output ordering.
   */
  private static List<String> getColumnNames(StructType schema) {
    java.util.Set<String> schemaFields = new java.util.HashSet<>();
    for (StructField field : schema.fields()) {
      schemaFields.add(field.name());
    }
    List<String> columns =
        Arrays.stream(schema.fields())
            .map(StructField::name)
            .filter(
                name ->
                    !name.equals(LanceConstant.FRAGMENT_ID)
                        && !name.equals(LanceConstant.ROW_ID)
                        && !name.equals(LanceConstant.ROW_ADDRESS)
                        && !name.equals(LanceConstant.ROW_CREATED_AT_VERSION)
                        && !name.equals(LanceConstant.ROW_LAST_UPDATED_AT_VERSION)
                        && !name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX)
                        && !name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX))
            .collect(Collectors.toList());
    if (schemaFields.contains(LanceConstant.ROW_ID)) {
      columns.add(LanceConstant.ROW_ID);
    }
    if (schemaFields.contains(LanceConstant.ROW_ADDRESS)) {
      columns.add(LanceConstant.ROW_ADDRESS);
    }
    if (schemaFields.contains(LanceConstant.ROW_LAST_UPDATED_AT_VERSION)) {
      columns.add(LanceConstant.ROW_LAST_UPDATED_AT_VERSION);
    }
    if (schemaFields.contains(LanceConstant.ROW_CREATED_AT_VERSION)) {
      columns.add(LanceConstant.ROW_CREATED_AT_VERSION);
    }
    return columns;
  }
}
