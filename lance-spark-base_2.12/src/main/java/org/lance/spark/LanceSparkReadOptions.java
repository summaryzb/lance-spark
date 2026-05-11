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
package org.lance.spark;

import org.lance.ReadOptions;
import org.lance.ipc.Query;
import org.lance.namespace.LanceNamespace;
import org.lance.spark.utils.QueryUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Read-specific options for Lance Spark connector.
 *
 * <p>These options override catalog-level settings for read operations.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * LanceSparkReadOptions options = LanceSparkReadOptions.builder()
 *     .datasetUri("s3://bucket/path")
 *     .pushDownFilters(true)
 *     .batchSize(1024)
 *     .namespace(namespace)
 *     .tableId(tableId)
 *     .build();
 * }</pre>
 */
public class LanceSparkReadOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(LanceSparkReadOptions.class);

  public static final String CONFIG_DATASET_URI = "path";
  public static final String CONFIG_PUSH_DOWN_FILTERS = "pushDownFilters";
  public static final String CONFIG_BLOCK_SIZE = "block_size";
  public static final String CONFIG_VERSION = "version";
  public static final String CONFIG_INDEX_CACHE_SIZE = "index_cache_size";
  public static final String CONFIG_METADATA_CACHE_SIZE = "metadata_cache_size";
  public static final String CONFIG_BATCH_SIZE = "batch_size";
  public static final String CONFIG_BATCH_READAHEAD = "batch_readahead";
  public static final String CONFIG_BATCH_PREFETCH_QUEUE_DEPTH = "batch_prefetch_queue_depth";
  public static final String CONFIG_TOP_N_PUSH_DOWN = "topN_push_down";

  public static final String CONFIG_NEAREST = "nearest";

  /**
   * Whether executors should rebuild the namespace client and re-fetch storage options via {@code
   * namespace.describeTable()} when opening a dataset for fragment scans.
   *
   * <p>When {@code true} (the default), executors reconstruct the namespace client and route the
   * dataset open through the namespace path. This keeps the Rust-side storage-options provider
   * attached so that short-lived vended credentials returned by {@code describeTable()} (e.g. STS
   * tokens from Iceberg REST, Polaris, Unity) can be refreshed mid-scan.
   *
   * <p>When {@code false}, executors open the dataset directly by URI using the storage options the
   * driver already obtained (passed in via {@code initialStorageOptions}). This skips the eager
   * {@code describeTable()} RPC on every fragment scan, which is required for catalogs whose
   * backing service authenticates per-call (e.g. Hive Metastore over Kerberos): executors typically
   * do not have a Kerberos TGT and the call would otherwise fail with {@code GSS initiate failed}.
   *
   * <p>Whether disabling this option actually costs anything depends on the namespace impl:
   *
   * <ul>
   *   <li>{@code Hive2Namespace} / {@code Hive3Namespace}: {@code describeTable()} returns only the
   *       table location, never storage options. The refresh callback is a no-op, so setting this
   *       option to {@code false} has no downside. The underlying object-store credentials (e.g.
   *       IAM-role / {@code hive-site.xml} / env-vars on the executor) are rotated by the storage
   *       client SDK independently of Lance.
   *   <li>{@code GlueNamespace}: storage options come from a static {@code
   *       config.getStorageOptions()} and are typically not time-bound; setting {@code false} is
   *       usually safe unless you rely on LakeFormation-vended temporary credentials.
   *   <li>{@code IcebergNamespace} (REST), {@code PolarisNamespace}, {@code UnityNamespace}: {@code
   *       describeTable()} commonly returns vended temporary credentials. Leave this option at the
   *       default ({@code true}) unless every scan is guaranteed to finish within the credential
   *       TTL.
   * </ul>
   */
  public static final String CONFIG_EXECUTOR_CREDENTIAL_REFRESH = "executor_credential_refresh";

  public static final String LANCE_FILE_SUFFIX = ".lance";

  // --- Dynamic File Pruning (DFP) / runtime filtering options ---
  // Per-scan keys (used with .option("...") and OPTIONS (...))
  public static final String CONFIG_RUNTIME_FILTERING_ENABLED = "lance.runtime.filtering.enabled";
  public static final String CONFIG_RUNTIME_FILTERING_MAX_COLUMNS =
      "lance.runtime.filtering.max.columns";
  public static final String CONFIG_RUNTIME_FILTERING_MAX_STATS_BYTES =
      "lance.runtime.filtering.max.stats.bytes";
  public static final String CONFIG_RUNTIME_FILTERING_LOAD_PARALLELISM =
      "lance.runtime.filtering.load.parallelism";
  public static final String CONFIG_RUNTIME_FILTERING_LOAD_TIMEOUT_MS =
      "lance.runtime.filtering.load.timeout.ms";
  public static final String CONFIG_RUNTIME_FILTERING_MAX_IN_VALUES =
      "lance.runtime.filtering.max.in.values";
  // SparkConf fallback keys (session-wide), consulted when per-scan options are absent.
  public static final String SPARK_CONF_RUNTIME_FILTERING_ENABLED =
      "spark.lance.runtime.filtering.enabled";
  public static final String SPARK_CONF_RUNTIME_FILTERING_MAX_COLUMNS =
      "spark.lance.runtime.filtering.max.columns";
  public static final String SPARK_CONF_RUNTIME_FILTERING_MAX_STATS_BYTES =
      "spark.lance.runtime.filtering.max.stats.bytes";
  public static final String SPARK_CONF_RUNTIME_FILTERING_LOAD_PARALLELISM =
      "spark.lance.runtime.filtering.load.parallelism";
  public static final String SPARK_CONF_RUNTIME_FILTERING_LOAD_TIMEOUT_MS =
      "spark.lance.runtime.filtering.load.timeout.ms";
  public static final String SPARK_CONF_RUNTIME_FILTERING_MAX_IN_VALUES =
      "spark.lance.runtime.filtering.max.in.values";

  private static final boolean DEFAULT_PUSH_DOWN_FILTERS = true;
  // Changed from 512 to 8192 for better OLAP scan performance (33x improvement)
  private static final int DEFAULT_BATCH_SIZE = 8192;
  private static final int DEFAULT_BATCH_READAHEAD = 16;

  /**
   * Default queue depth for the JVM-side async batch prefetch pipeline in {@code
   * LanceFragmentScanner.getArrowReader()}. {@code 0} disables the prefetch wrapper entirely (the
   * synchronous Arrow reader from Lance is used as-is), preserving historical behavior. Set to
   * {@code >=1} to enable a background thread that overlaps Lance JNI {@code loadNextBatch()} +
   * buffer-transfer with Spark batch consumption.
   */
  private static final int DEFAULT_BATCH_PREFETCH_QUEUE_DEPTH = 0;

  /**
   * Upper bound accepted for {@code batch_prefetch_queue_depth}. The JVM wrapper only needs to stay
   * one batch ahead of Spark consumption; values beyond a few dozen add memory pressure without
   * improving latency hiding. The hard cap at 128 exists to reject absurd configurations (e.g.
   * {@link Integer#MAX_VALUE}) that would OOME every task when {@link
   * java.util.concurrent.ArrayBlockingQueue} allocates its backing array. Tune the practical value
   * per workload via {@code batch_prefetch_queue_depth} itself; this constant is a safety rail, not
   * a recommendation.
   */
  public static final int MAX_BATCH_PREFETCH_QUEUE_DEPTH = 128;

  private static final boolean DEFAULT_TOP_N_PUSH_DOWN = true;
  private static final boolean DEFAULT_EXECUTOR_CREDENTIAL_REFRESH = true;
  private static final boolean DEFAULT_RUNTIME_FILTERING_ENABLED = true;
  private static final int DEFAULT_RUNTIME_FILTERING_MAX_COLUMNS = 20;
  private static final long DEFAULT_RUNTIME_FILTERING_MAX_STATS_BYTES = 64L * 1024L * 1024L;
  private static final int DEFAULT_RUNTIME_FILTERING_LOAD_PARALLELISM = 4;
  private static final long DEFAULT_RUNTIME_FILTERING_LOAD_TIMEOUT_MS = 5000L;
  private static final int DEFAULT_RUNTIME_FILTERING_MAX_IN_VALUES = 10000;

  private final String datasetUri;
  private final String dbPath;
  private final String datasetName;
  private final boolean pushDownFilters;
  private final Integer blockSize;
  private final Integer version;
  private final Integer indexCacheSize;
  private final Integer metadataCacheSize;
  private final int batchSize;
  private final int batchReadahead;
  private final int batchPrefetchQueueDepth;
  private transient Query nearest;
  private final boolean topNPushDown;
  private final Map<String, String> storageOptions;

  /** The namespace for credential vending. Transient as LanceNamespace is not serializable. */
  private transient LanceNamespace namespace;

  /** The table identifier within the namespace, used for credential refresh. */
  private final List<String> tableId;

  /** The catalog name for cache isolation when multiple catalogs are configured. */
  private final String catalogName;

  /**
   * Whether executors should rebuild the namespace client for credential refresh. See {@link
   * #CONFIG_EXECUTOR_CREDENTIAL_REFRESH} for details.
   */
  private final boolean executorCredentialRefresh;

  // --- Dynamic File Pruning (DFP) options ---
  private final boolean runtimeFilteringEnabled;
  private final int runtimeFilteringMaxColumns;
  private final long runtimeFilteringMaxStatsBytes;
  private final int runtimeFilteringLoadParallelism;
  private final long runtimeFilteringLoadTimeoutMs;
  private final int runtimeFilteringMaxInValues;

  private LanceSparkReadOptions(Builder builder) {
    this.datasetUri = builder.datasetUri;
    String[] paths = extractDbPathAndDatasetName(datasetUri);
    this.dbPath = paths[0];
    this.datasetName = paths[1];
    this.pushDownFilters = builder.pushDownFilters;
    this.blockSize = builder.blockSize;
    this.version = builder.version;
    this.indexCacheSize = builder.indexCacheSize;
    this.metadataCacheSize = builder.metadataCacheSize;
    this.batchSize = builder.batchSize;
    this.batchReadahead = builder.batchReadahead;
    this.batchPrefetchQueueDepth = builder.batchPrefetchQueueDepth;
    this.nearest = builder.nearest;
    this.topNPushDown = builder.topNPushDown;
    this.storageOptions = new HashMap<>(builder.storageOptions);
    this.namespace = builder.namespace;
    this.tableId = builder.tableId;
    this.catalogName = builder.catalogName;
    this.executorCredentialRefresh = builder.executorCredentialRefresh;
    this.runtimeFilteringEnabled = builder.runtimeFilteringEnabled;
    this.runtimeFilteringMaxColumns = builder.runtimeFilteringMaxColumns;
    this.runtimeFilteringMaxStatsBytes = builder.runtimeFilteringMaxStatsBytes;
    this.runtimeFilteringLoadParallelism = builder.runtimeFilteringLoadParallelism;
    this.runtimeFilteringLoadTimeoutMs = builder.runtimeFilteringLoadTimeoutMs;
    this.runtimeFilteringMaxInValues = builder.runtimeFilteringMaxInValues;
  }

  /** Creates a new builder for LanceSparkReadOptions. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates read options from a map of properties. The path key must be present.
   *
   * @param properties the properties map containing 'path' key
   * @return a new LanceSparkReadOptions
   */
  public static LanceSparkReadOptions from(Map<String, String> properties) {
    String datasetUri = properties.get(CONFIG_DATASET_URI);
    if (datasetUri == null) {
      throw new IllegalArgumentException("Missing required option: " + CONFIG_DATASET_URI);
    }
    return builder().datasetUri(datasetUri).fromOptions(properties).build();
  }

  /**
   * Creates read options from a map of properties and dataset URI.
   *
   * @param properties the properties map
   * @param datasetUri the dataset URI
   * @return a new LanceSparkReadOptions
   */
  public static LanceSparkReadOptions from(Map<String, String> properties, String datasetUri) {
    return builder().datasetUri(datasetUri).fromOptions(properties).build();
  }

  /**
   * Creates read options from a dataset URI only.
   *
   * @param datasetUri the dataset URI
   * @return a new LanceSparkReadOptions
   */
  public static LanceSparkReadOptions from(String datasetUri) {
    return builder().datasetUri(datasetUri).build();
  }

  // ========== Helper methods ==========

  private static String[] extractDbPathAndDatasetName(String datasetUri) {
    if (datasetUri == null) {
      throw new IllegalArgumentException("The dataset uri should not be null");
    }

    int lastSlashIndex = datasetUri.lastIndexOf('/');
    if (lastSlashIndex == -1) {
      throw new IllegalArgumentException("Invalid dataset uri: " + datasetUri);
    }

    String dbPath = datasetUri.substring(0, lastSlashIndex + 1);
    String datasetNameWithSuffix = datasetUri.substring(lastSlashIndex + 1);
    String datasetName;
    if (datasetUri.endsWith(LANCE_FILE_SUFFIX)) {
      datasetName =
          datasetNameWithSuffix.substring(
              0, datasetNameWithSuffix.length() - LANCE_FILE_SUFFIX.length());
    } else {
      datasetName = datasetNameWithSuffix;
    }

    return new String[] {dbPath, datasetName};
  }

  // ========== Getters ==========

  public String getDatasetUri() {
    return datasetUri;
  }

  public String getDbPath() {
    return dbPath;
  }

  public String getDatasetName() {
    return datasetName;
  }

  public boolean isPushDownFilters() {
    return pushDownFilters;
  }

  public Integer getBlockSize() {
    return blockSize;
  }

  public Integer getVersion() {
    return version;
  }

  public Integer getIndexCacheSize() {
    return indexCacheSize;
  }

  public Integer getMetadataCacheSize() {
    return metadataCacheSize;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public int getBatchReadahead() {
    return batchReadahead;
  }

  /**
   * Returns the configured queue depth for the JVM-side async batch prefetch pipeline. A value of
   * {@code 0} (default) disables prefetch and the Arrow reader from Lance is returned directly. A
   * positive value enables a background thread in {@code LanceFragmentScanner.getArrowReader()}
   * that overlaps Lance JNI {@code loadNextBatch()} + buffer-transfer with Spark batch consumption,
   * at the cost of one extra in-flight batch per executor task.
   */
  public int getBatchPrefetchQueueDepth() {
    return batchPrefetchQueueDepth;
  }

  public Query getNearest() {
    return nearest;
  }

  public boolean isTopNPushDown() {
    return topNPushDown;
  }

  public Map<String, String> getStorageOptions() {
    return storageOptions;
  }

  public String getNearestJson() {
    return QueryUtils.queryToString(nearest);
  }

  public LanceNamespace getNamespace() {
    return namespace;
  }

  public List<String> getTableId() {
    return tableId;
  }

  public String getCatalogName() {
    return catalogName;
  }

  /**
   * Returns whether executors should rebuild the namespace client and route the dataset open
   * through the namespace path (for credential refresh). See {@link
   * #CONFIG_EXECUTOR_CREDENTIAL_REFRESH}.
   */
  public boolean isExecutorCredentialRefresh() {
    return executorCredentialRefresh;
  }

  public boolean hasNamespace() {
    return namespace != null && tableId != null;
  }

  public boolean isRuntimeFilteringEnabled() {
    return runtimeFilteringEnabled;
  }

  public int getRuntimeFilteringMaxColumns() {
    return runtimeFilteringMaxColumns;
  }

  public long getRuntimeFilteringMaxStatsBytes() {
    return runtimeFilteringMaxStatsBytes;
  }

  public int getRuntimeFilteringLoadParallelism() {
    return runtimeFilteringLoadParallelism;
  }

  public long getRuntimeFilteringLoadTimeoutMs() {
    return runtimeFilteringLoadTimeoutMs;
  }

  public int getRuntimeFilteringMaxInValues() {
    return runtimeFilteringMaxInValues;
  }

  /**
   * Sets the namespace for this options. Used after deserialization to restore the namespace.
   *
   * @param namespace the namespace to set
   */
  public void setNamespace(LanceNamespace namespace) {
    this.namespace = namespace;
  }

  /**
   * Creates a copy of this options with a different version.
   *
   * <p>This is used to pin the version during scan planning for snapshot isolation.
   *
   * @param newVersion the version to use
   * @return a new LanceSparkReadOptions with the specified version
   */
  public LanceSparkReadOptions withVersion(int newVersion) {
    return builder()
        .datasetUri(this.datasetUri)
        .pushDownFilters(this.pushDownFilters)
        .blockSize(this.blockSize)
        .version(newVersion)
        .indexCacheSize(this.indexCacheSize)
        .metadataCacheSize(this.metadataCacheSize)
        .batchSize(this.batchSize)
        .batchReadahead(this.batchReadahead)
        .batchPrefetchQueueDepth(this.batchPrefetchQueueDepth)
        .nearest(this.nearest)
        .topNPushDown(this.topNPushDown)
        .storageOptions(this.storageOptions)
        .namespace(this.namespace)
        .tableId(this.tableId)
        .catalogName(this.catalogName)
        .executorCredentialRefresh(this.executorCredentialRefresh)
        .runtimeFilteringEnabled(this.runtimeFilteringEnabled)
        .runtimeFilteringMaxColumns(this.runtimeFilteringMaxColumns)
        .runtimeFilteringMaxStatsBytes(this.runtimeFilteringMaxStatsBytes)
        .runtimeFilteringLoadParallelism(this.runtimeFilteringLoadParallelism)
        .runtimeFilteringLoadTimeoutMs(this.runtimeFilteringLoadTimeoutMs)
        .runtimeFilteringMaxInValues(this.runtimeFilteringMaxInValues)
        .build();
  }

  /**
   * Converts this to Lance ReadOptions for the native library.
   *
   * @return ReadOptions for the Lance native library
   */
  public ReadOptions toReadOptions() {
    ReadOptions.Builder builder = new ReadOptions.Builder();
    builder.setSession(LanceRuntime.session());
    if (blockSize != null) {
      builder.setBlockSize(blockSize);
    }
    if (version != null) {
      builder.setVersion(version);
    }
    if (indexCacheSize != null) {
      builder.setIndexCacheSize(indexCacheSize);
    }
    if (metadataCacheSize != null) {
      builder.setMetadataCacheSize(metadataCacheSize);
    }
    if (!storageOptions.isEmpty()) {
      builder.setStorageOptions(storageOptions);
    }
    return builder.build();
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    out.writeObject(QueryUtils.queryToString(nearest));
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    String json = (String) in.readObject();
    this.nearest = QueryUtils.stringToQuery(json);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LanceSparkReadOptions that = (LanceSparkReadOptions) o;
    return pushDownFilters == that.pushDownFilters
        && batchSize == that.batchSize
        && batchReadahead == that.batchReadahead
        && batchPrefetchQueueDepth == that.batchPrefetchQueueDepth
        && topNPushDown == that.topNPushDown
        && executorCredentialRefresh == that.executorCredentialRefresh
        && runtimeFilteringEnabled == that.runtimeFilteringEnabled
        && runtimeFilteringMaxColumns == that.runtimeFilteringMaxColumns
        && runtimeFilteringMaxStatsBytes == that.runtimeFilteringMaxStatsBytes
        && runtimeFilteringLoadParallelism == that.runtimeFilteringLoadParallelism
        && runtimeFilteringLoadTimeoutMs == that.runtimeFilteringLoadTimeoutMs
        && runtimeFilteringMaxInValues == that.runtimeFilteringMaxInValues
        && Objects.equals(nearest, that.nearest)
        && Objects.equals(datasetUri, that.datasetUri)
        && Objects.equals(blockSize, that.blockSize)
        && Objects.equals(version, that.version)
        && Objects.equals(indexCacheSize, that.indexCacheSize)
        && Objects.equals(metadataCacheSize, that.metadataCacheSize)
        && Objects.equals(storageOptions, that.storageOptions)
        && Objects.equals(tableId, that.tableId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        datasetUri,
        pushDownFilters,
        blockSize,
        version,
        indexCacheSize,
        metadataCacheSize,
        batchSize,
        batchReadahead,
        batchPrefetchQueueDepth,
        nearest,
        topNPushDown,
        storageOptions,
        tableId,
        executorCredentialRefresh,
        runtimeFilteringEnabled,
        runtimeFilteringMaxColumns,
        runtimeFilteringMaxStatsBytes,
        runtimeFilteringLoadParallelism,
        runtimeFilteringLoadTimeoutMs,
        runtimeFilteringMaxInValues);
  }

  /** Builder for creating LanceSparkReadOptions instances. */
  public static class Builder {
    private String datasetUri;
    private boolean pushDownFilters = DEFAULT_PUSH_DOWN_FILTERS;
    private Integer blockSize;
    private Query nearest;
    private Integer version;
    private Integer indexCacheSize;
    private Integer metadataCacheSize;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private int batchReadahead = DEFAULT_BATCH_READAHEAD;
    private int batchPrefetchQueueDepth = DEFAULT_BATCH_PREFETCH_QUEUE_DEPTH;
    private boolean topNPushDown = DEFAULT_TOP_N_PUSH_DOWN;
    private Map<String, String> storageOptions = new HashMap<>();
    private LanceNamespace namespace;
    private List<String> tableId;
    private String catalogName;
    private boolean executorCredentialRefresh = DEFAULT_EXECUTOR_CREDENTIAL_REFRESH;
    private boolean runtimeFilteringEnabled = DEFAULT_RUNTIME_FILTERING_ENABLED;
    private int runtimeFilteringMaxColumns = DEFAULT_RUNTIME_FILTERING_MAX_COLUMNS;
    private long runtimeFilteringMaxStatsBytes = DEFAULT_RUNTIME_FILTERING_MAX_STATS_BYTES;
    private int runtimeFilteringLoadParallelism = DEFAULT_RUNTIME_FILTERING_LOAD_PARALLELISM;
    private long runtimeFilteringLoadTimeoutMs = DEFAULT_RUNTIME_FILTERING_LOAD_TIMEOUT_MS;
    private int runtimeFilteringMaxInValues = DEFAULT_RUNTIME_FILTERING_MAX_IN_VALUES;

    private Builder() {}

    public Builder datasetUri(String datasetUri) {
      this.datasetUri = datasetUri;
      return this;
    }

    public Builder pushDownFilters(boolean pushDownFilters) {
      this.pushDownFilters = pushDownFilters;
      return this;
    }

    public Builder blockSize(Integer blockSize) {
      this.blockSize = blockSize;
      return this;
    }

    public Builder nearest(Query nearest) {
      this.nearest = nearest;
      return this;
    }

    public Builder nearest(String json) {
      try {
        this.nearest = QueryUtils.stringToQuery(json);
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to parse nearest query from json: " + json, e);
      }
      return this;
    }

    public Builder version(Integer version) {
      this.version = version;
      return this;
    }

    public Builder indexCacheSize(Integer indexCacheSize) {
      this.indexCacheSize = indexCacheSize;
      return this;
    }

    public Builder metadataCacheSize(Integer metadataCacheSize) {
      this.metadataCacheSize = metadataCacheSize;
      return this;
    }

    public Builder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder batchReadahead(int batchReadahead) {
      this.batchReadahead = batchReadahead;
      return this;
    }

    public Builder batchPrefetchQueueDepth(int batchPrefetchQueueDepth) {
      // Symmetric with the parser's bound check in parseTypedFlags so the programmatic
      // builder path cannot bypass MAX_BATCH_PREFETCH_QUEUE_DEPTH. Without this guard,
      // callers could construct an options instance with a pathological value that later
      // fails inside LanceFragmentScanner.getArrowReader when ArrayBlockingQueue allocates
      // a multi-GiB backing array on every task.
      Preconditions.checkArgument(
          batchPrefetchQueueDepth >= 0 && batchPrefetchQueueDepth <= MAX_BATCH_PREFETCH_QUEUE_DEPTH,
          "batch_prefetch_queue_depth must be in [0, %s]",
          MAX_BATCH_PREFETCH_QUEUE_DEPTH);
      this.batchPrefetchQueueDepth = batchPrefetchQueueDepth;
      return this;
    }

    public Builder topNPushDown(boolean topNPushDown) {
      this.topNPushDown = topNPushDown;
      return this;
    }

    public Builder storageOptions(Map<String, String> storageOptions) {
      this.storageOptions = new HashMap<>(storageOptions);
      return this;
    }

    public Builder namespace(LanceNamespace namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder tableId(List<String> tableId) {
      this.tableId = tableId;
      return this;
    }

    public Builder catalogName(String catalogName) {
      this.catalogName = catalogName;
      return this;
    }

    public Builder executorCredentialRefresh(boolean executorCredentialRefresh) {
      this.executorCredentialRefresh = executorCredentialRefresh;
      return this;
    }

    public Builder runtimeFilteringEnabled(boolean runtimeFilteringEnabled) {
      this.runtimeFilteringEnabled = runtimeFilteringEnabled;
      return this;
    }

    public Builder runtimeFilteringMaxColumns(int runtimeFilteringMaxColumns) {
      Preconditions.checkArgument(
          runtimeFilteringMaxColumns >= 0, "%s must be >= 0", CONFIG_RUNTIME_FILTERING_MAX_COLUMNS);
      this.runtimeFilteringMaxColumns = runtimeFilteringMaxColumns;
      return this;
    }

    public Builder runtimeFilteringMaxStatsBytes(long runtimeFilteringMaxStatsBytes) {
      Preconditions.checkArgument(
          runtimeFilteringMaxStatsBytes >= 0,
          "%s must be >= 0",
          CONFIG_RUNTIME_FILTERING_MAX_STATS_BYTES);
      this.runtimeFilteringMaxStatsBytes = runtimeFilteringMaxStatsBytes;
      return this;
    }

    public Builder runtimeFilteringLoadParallelism(int runtimeFilteringLoadParallelism) {
      Preconditions.checkArgument(
          runtimeFilteringLoadParallelism >= 1,
          "%s must be >= 1",
          CONFIG_RUNTIME_FILTERING_LOAD_PARALLELISM);
      this.runtimeFilteringLoadParallelism = runtimeFilteringLoadParallelism;
      return this;
    }

    public Builder runtimeFilteringLoadTimeoutMs(long runtimeFilteringLoadTimeoutMs) {
      Preconditions.checkArgument(
          runtimeFilteringLoadTimeoutMs > 0,
          "%s must be > 0",
          CONFIG_RUNTIME_FILTERING_LOAD_TIMEOUT_MS);
      this.runtimeFilteringLoadTimeoutMs = runtimeFilteringLoadTimeoutMs;
      return this;
    }

    public Builder runtimeFilteringMaxInValues(int runtimeFilteringMaxInValues) {
      Preconditions.checkArgument(
          runtimeFilteringMaxInValues >= 0,
          "%s must be >= 0",
          CONFIG_RUNTIME_FILTERING_MAX_IN_VALUES);
      this.runtimeFilteringMaxInValues = runtimeFilteringMaxInValues;
      return this;
    }

    /**
     * Parses options from a map, extracting read-specific settings.
     *
     * @param options the options map
     * @return this builder
     */
    public Builder fromOptions(Map<String, String> options) {
      this.storageOptions = new HashMap<>(options);
      parseTypedFlags(options);
      return this;
    }

    /**
     * Parses a runtime-filtering numeric option, logging a WARN and returning {@link
     * java.util.Optional#empty()} when the raw value is missing or unparseable. Callers use {@code
     * ifPresent(setter)} so the builder's coded default survives a typo instead of the query
     * failing with a {@link NumberFormatException} leaking the internal key name.
     *
     * <p>Only parse-level errors are forgiven. Range violations (e.g. {@code parallelism=0}, a
     * negative {@code max.stats.bytes}) still throw {@link IllegalArgumentException} from the
     * setter's {@code Preconditions.checkArgument} — that distinguishes "user typo'd a number"
     * (silent fallback) from "user deliberately picked an invalid value" (loud failure so they
     * notice it didn't take effect). The {@code rangeViolationStillFailsLoudly} test enforces this
     * split behavior.
     */
    private static java.util.Optional<Integer> parseIntOrWarn(String raw, String configKey) {
      if (raw == null) {
        return java.util.Optional.empty();
      }
      try {
        return java.util.Optional.of(Integer.parseInt(raw));
      } catch (NumberFormatException e) {
        LOG.warn("Invalid integer value '{}' for {}; using default", raw, configKey);
        return java.util.Optional.empty();
      }
    }

    private static java.util.Optional<Long> parseLongOrWarn(String raw, String configKey) {
      if (raw == null) {
        return java.util.Optional.empty();
      }
      try {
        return java.util.Optional.of(Long.parseLong(raw));
      } catch (NumberFormatException e) {
        LOG.warn("Invalid long value '{}' for {}; using default", raw, configKey);
        return java.util.Optional.empty();
      }
    }

    /**
     * Resolves a runtime-filtering config value with the two-level lookup documented in the DFP
     * implementation plan: per-scan DataSourceV2 option (no {@code spark.} prefix) wins, SparkConf
     * (with {@code spark.} prefix) is the fallback. Returns {@code null} if neither is set.
     *
     * <p>The SparkConf fallback uses {@code SparkSession.active()} and gracefully returns {@code
     * null} if no session is active (e.g., in pure unit tests that construct options without Spark
     * running).
     */
    private static String resolveConfig(
        Map<String, String> options, String perScanKey, String sparkConfKey) {
      if (options != null && options.containsKey(perScanKey)) {
        return options.get(perScanKey);
      }
      try {
        org.apache.spark.sql.SparkSession session = org.apache.spark.sql.SparkSession.active();
        if (session == null) {
          return null;
        }
        scala.Option<String> opt = session.conf().getOption(sparkConfKey);
        return opt.isDefined() ? opt.get() : null;
      } catch (Throwable t) {
        // No active SparkSession (tests) or conf lookup failed - fall through to default.
        return null;
      }
    }

    /**
     * Merges catalog config options as defaults (read options override).
     *
     * <p>Also promotes recognized typed flags from the catalog config into their corresponding
     * Builder fields so that catalog-level settings (e.g. {@code spark.sql.catalog.<name>.<key>})
     * take effect on paths that do not later go through {@link #fromOptions(Map)} — notably SQL DML
     * (DELETE / UPDATE / MERGE INTO) and plain SELECT without per-read {@code .option(...)}.
     *
     * @param catalogConfig the catalog config
     * @return this builder
     */
    public Builder withCatalogDefaults(LanceSparkCatalogConfig catalogConfig) {
      // Merge storage options: catalog options are defaults, current options override
      Map<String, String> merged = new HashMap<>(catalogConfig.getStorageOptions());
      merged.putAll(this.storageOptions);
      return fromOptions(merged);
    }

    /**
     * Applies typed-flag parsing for every known read option present in {@code opts}. Shared by
     * {@link #fromOptions(Map)} and {@link #withCatalogDefaults(LanceSparkCatalogConfig)} so that
     * both call sites stay in sync and catalog-level configs reach the typed fields.
     */
    private void parseTypedFlags(Map<String, String> opts) {
      if (opts.containsKey(CONFIG_PUSH_DOWN_FILTERS)) {
        this.pushDownFilters = Boolean.parseBoolean(opts.get(CONFIG_PUSH_DOWN_FILTERS));
      }
      if (opts.containsKey(CONFIG_BLOCK_SIZE)) {
        this.blockSize = Integer.parseInt(opts.get(CONFIG_BLOCK_SIZE));
      }
      if (opts.containsKey(CONFIG_VERSION)) {
        this.version = Integer.parseInt(opts.get(CONFIG_VERSION));
      }
      if (opts.containsKey(CONFIG_INDEX_CACHE_SIZE)) {
        this.indexCacheSize = Integer.parseInt(opts.get(CONFIG_INDEX_CACHE_SIZE));
      }
      if (opts.containsKey(CONFIG_METADATA_CACHE_SIZE)) {
        this.metadataCacheSize = Integer.parseInt(opts.get(CONFIG_METADATA_CACHE_SIZE));
      }
      if (opts.containsKey(CONFIG_BATCH_SIZE)) {
        int parsedBatchSize = Integer.parseInt(opts.get(CONFIG_BATCH_SIZE));
        Preconditions.checkArgument(parsedBatchSize > 0, "batch_size must be positive");
        this.batchSize = parsedBatchSize;
      }
      if (opts.containsKey(CONFIG_EXECUTOR_CREDENTIAL_REFRESH)) {
        this.executorCredentialRefresh =
            Boolean.parseBoolean(opts.get(CONFIG_EXECUTOR_CREDENTIAL_REFRESH));
      }
      if (opts.containsKey(CONFIG_TOP_N_PUSH_DOWN)) {
        this.topNPushDown = Boolean.parseBoolean(opts.get(CONFIG_TOP_N_PUSH_DOWN));
      }
      if (opts.containsKey(CONFIG_BATCH_READAHEAD)) {
        int parsedReadahead = Integer.parseInt(opts.get(CONFIG_BATCH_READAHEAD));
        Preconditions.checkArgument(parsedReadahead > 0, "batch_readahead must be positive");
        this.batchReadahead = parsedReadahead;
      }
      if (opts.containsKey(CONFIG_BATCH_PREFETCH_QUEUE_DEPTH)) {
        int parsedQueueDepth = Integer.parseInt(opts.get(CONFIG_BATCH_PREFETCH_QUEUE_DEPTH));
        Preconditions.checkArgument(
            parsedQueueDepth >= 0 && parsedQueueDepth <= MAX_BATCH_PREFETCH_QUEUE_DEPTH,
            "batch_prefetch_queue_depth must be in [0, %s]",
            MAX_BATCH_PREFETCH_QUEUE_DEPTH);
        this.batchPrefetchQueueDepth = parsedQueueDepth;
      }
      if (opts.containsKey(CONFIG_NEAREST)) {
        String json = opts.get(CONFIG_NEAREST);
        nearest(json);
      }
      // Runtime filtering (DFP) options: per-scan key wins, SparkConf is fallback.
      // Kept in fromOptions (not parseTypedFlags) because the resolution checks both layers.
      String enabledRaw =
          resolveConfig(
              opts, CONFIG_RUNTIME_FILTERING_ENABLED, SPARK_CONF_RUNTIME_FILTERING_ENABLED);
      if (enabledRaw != null) {
        this.runtimeFilteringEnabled = Boolean.parseBoolean(enabledRaw);
      }
      String maxColsRaw =
          resolveConfig(
              opts, CONFIG_RUNTIME_FILTERING_MAX_COLUMNS, SPARK_CONF_RUNTIME_FILTERING_MAX_COLUMNS);
      parseIntOrWarn(maxColsRaw, CONFIG_RUNTIME_FILTERING_MAX_COLUMNS)
          .ifPresent(this::runtimeFilteringMaxColumns);
      String maxBytesRaw =
          resolveConfig(
              opts,
              CONFIG_RUNTIME_FILTERING_MAX_STATS_BYTES,
              SPARK_CONF_RUNTIME_FILTERING_MAX_STATS_BYTES);
      parseLongOrWarn(maxBytesRaw, CONFIG_RUNTIME_FILTERING_MAX_STATS_BYTES)
          .ifPresent(this::runtimeFilteringMaxStatsBytes);
      String parallelismRaw =
          resolveConfig(
              opts,
              CONFIG_RUNTIME_FILTERING_LOAD_PARALLELISM,
              SPARK_CONF_RUNTIME_FILTERING_LOAD_PARALLELISM);
      parseIntOrWarn(parallelismRaw, CONFIG_RUNTIME_FILTERING_LOAD_PARALLELISM)
          .ifPresent(this::runtimeFilteringLoadParallelism);
      String timeoutRaw =
          resolveConfig(
              opts,
              CONFIG_RUNTIME_FILTERING_LOAD_TIMEOUT_MS,
              SPARK_CONF_RUNTIME_FILTERING_LOAD_TIMEOUT_MS);
      parseLongOrWarn(timeoutRaw, CONFIG_RUNTIME_FILTERING_LOAD_TIMEOUT_MS)
          .ifPresent(this::runtimeFilteringLoadTimeoutMs);
      String maxInRaw =
          resolveConfig(
              opts,
              CONFIG_RUNTIME_FILTERING_MAX_IN_VALUES,
              SPARK_CONF_RUNTIME_FILTERING_MAX_IN_VALUES);
      parseIntOrWarn(maxInRaw, CONFIG_RUNTIME_FILTERING_MAX_IN_VALUES)
          .ifPresent(this::runtimeFilteringMaxInValues);
    }

    public LanceSparkReadOptions build() {
      if (datasetUri == null) {
        throw new IllegalArgumentException("datasetUri is required");
      }
      return new LanceSparkReadOptions(this);
    }
  }
}
