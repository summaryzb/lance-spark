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

import org.lance.namespace.LanceNamespace;
import org.lance.spark.LanceConstant;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.read.LanceSplit;
import org.lance.spark.utils.LazyResource;
import org.lance.spark.utils.Optional;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LanceFragmentScannerTest {

  private List<String> callGetColumnNames(StructType schema)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method method =
        LanceFragmentScanner.class.getDeclaredMethod("getColumnNames", StructType.class);
    method.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) method.invoke(null, schema);
    return result;
  }

  @Test
  public void testGetColumnNamesWithOnlyDataColumns() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("age", DataTypes.IntegerType, true)
            });

    List<String> result = callGetColumnNames(schema);
    List<String> expected = Arrays.asList("id", "name", "age");
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesWithRowId() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField(LanceConstant.ROW_ID, DataTypes.LongType, true)
            });

    List<String> result = callGetColumnNames(schema);
    List<String> expected = Arrays.asList("id", "name", LanceConstant.ROW_ID);
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesWithRowAddress() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField(LanceConstant.ROW_ADDRESS, DataTypes.LongType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });

    List<String> result = callGetColumnNames(schema);
    List<String> expected = Arrays.asList("id", "name", LanceConstant.ROW_ADDRESS);
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesWithVersionColumns() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField(
                  LanceConstant.ROW_CREATED_AT_VERSION, DataTypes.LongType, true),
              DataTypes.createStructField(
                  LanceConstant.ROW_LAST_UPDATED_AT_VERSION, DataTypes.LongType, true)
            });

    List<String> result = callGetColumnNames(schema);
    List<String> expected =
        Arrays.asList(
            "id",
            "name",
            LanceConstant.ROW_LAST_UPDATED_AT_VERSION,
            LanceConstant.ROW_CREATED_AT_VERSION);
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesWithAllMetadataColumns() throws Exception {
    // Test with all metadata columns in the order defined in LanceDataset.METADATA_COLUMNS
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField(LanceConstant.ROW_ID, DataTypes.LongType, true),
              DataTypes.createStructField(LanceConstant.ROW_ADDRESS, DataTypes.LongType, true),
              DataTypes.createStructField(
                  LanceConstant.ROW_LAST_UPDATED_AT_VERSION, DataTypes.LongType, true),
              DataTypes.createStructField(
                  LanceConstant.ROW_CREATED_AT_VERSION, DataTypes.LongType, true),
              DataTypes.createStructField(LanceConstant.FRAGMENT_ID, DataTypes.IntegerType, true)
            });

    List<String> result = callGetColumnNames(schema);
    // Data columns first, then metadata columns in METADATA_COLUMNS order
    // Note: FRAGMENT_ID is excluded as it's not included in the projection
    List<String> expected =
        Arrays.asList(
            "id",
            "name",
            LanceConstant.ROW_ID,
            LanceConstant.ROW_ADDRESS,
            LanceConstant.ROW_LAST_UPDATED_AT_VERSION,
            LanceConstant.ROW_CREATED_AT_VERSION);
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesExcludesBlobColumns() throws Exception {
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField("data", DataTypes.BinaryType, true),
              DataTypes.createStructField(
                  "data" + LanceConstant.BLOB_POSITION_SUFFIX, DataTypes.LongType, true),
              DataTypes.createStructField(
                  "data" + LanceConstant.BLOB_SIZE_SUFFIX, DataTypes.LongType, true)
            });

    List<String> result = callGetColumnNames(schema);
    // Blob metadata columns should be excluded
    List<String> expected = Arrays.asList("id", "data");
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesOrderingWithMixedColumns() throws Exception {
    // Test that regular columns come first, then metadata columns in the correct order
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField(LanceConstant.ROW_ID, DataTypes.LongType, true),
              DataTypes.createStructField("z_last_column", DataTypes.StringType, true),
              DataTypes.createStructField(
                  LanceConstant.ROW_CREATED_AT_VERSION, DataTypes.LongType, true),
              DataTypes.createStructField("a_first_column", DataTypes.LongType, true),
              DataTypes.createStructField(LanceConstant.ROW_ADDRESS, DataTypes.LongType, true),
              DataTypes.createStructField("m_middle_column", DataTypes.IntegerType, true)
            });

    List<String> result = callGetColumnNames(schema);
    // Regular data columns in schema order, then metadata columns in METADATA_COLUMNS order
    List<String> expected =
        Arrays.asList(
            "z_last_column",
            "a_first_column",
            "m_middle_column",
            LanceConstant.ROW_ID,
            LanceConstant.ROW_ADDRESS,
            LanceConstant.ROW_CREATED_AT_VERSION);
    assertEquals(expected, result);
  }

  @Test
  public void testGetColumnNamesWithFragmentId() throws Exception {
    // FRAGMENT_ID should be excluded from the projection
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField(LanceConstant.FRAGMENT_ID, DataTypes.IntegerType, true)
            });

    List<String> result = callGetColumnNames(schema);
    List<String> expected = Arrays.asList("id");
    assertEquals(expected, result);
  }

  /**
   * Locks down the executor-branch contract for {@code executor_credential_refresh=false}: when an
   * executor opens a fragment for a namespace-backed table with the flag disabled, the dataset
   * supplier must <i>not</i> reconstruct the namespace client. Without this gate, executors of
   * Kerberized HMS catalogs hit {@code GSS initiate failed} because they lack a TGT for the eager
   * {@code describeTable()} RPC.
   *
   * <p>Strategy: hand a real, loadable {@link LanceNamespace} impl to the partition. If the gate
   * regresses (rebuild not skipped), {@code LanceNamespace.connect} would succeed via {@code
   * Class.forName}, {@link RecordingNamespace#initialize} would run, and {@code
   * readOptions.setNamespace(...)} would fire — all observable here. The bogus dataset URI lets the
   * outer {@code Utils.openDatasetBuilder().build()} call fail predictably, since the gate runs
   * <i>before</i> the dataset is opened. No real Lance dataset is required.
   *
   * <p>Lazy-supplier note: dataset open is now deferred until {@link
   * LanceFragmentScanner#getArrowReader()} forces the supplier, so that's where the gate executes.
   * The {@link RuntimeException} from the missing URI is consequently surfaced from {@code
   * getArrowReader()}, not {@code create()}.
   */
  @Test
  public void testCreateSkipsNamespaceRebuildWhenExecutorCredentialRefreshDisabled()
      throws Exception {
    RecordingNamespace.INITIALIZE_CALLS.set(0);

    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri("file:///tmp/__lance_nonexistent_for_executor_gate_test__")
            .executorCredentialRefresh(false)
            .build();

    LanceInputPartition partition =
        new LanceInputPartition(
            new StructType(),
            0,
            null,
            readOptions,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            "test-scan",
            Collections.emptyMap(),
            RecordingNamespace.class.getName(),
            Collections.emptyMap(),
            null);

    LanceFragmentScanner scanner = LanceFragmentScanner.create(0, partition);
    try {
      assertThrows(RuntimeException.class, scanner::getArrowReader);
    } finally {
      try {
        scanner.close();
      } catch (Exception ignored) {
        // close() may rethrow if the supplier captured a partial open — not the focus of the test
      }
    }

    assertNull(
        readOptions.getNamespace(),
        "executor_credential_refresh=false must skip namespace rebuild on the executor");
    assertEquals(
        0,
        RecordingNamespace.INITIALIZE_CALLS.get(),
        "executor_credential_refresh=false must not load or initialize the namespace impl");
  }

  /**
   * Regression test for the headline optimization: with the executor disk cache pre-warmed for the
   * exact (key, columns) pair, a second scan must NOT invoke the dataset or scanner supplier.
   *
   * <p>Strategy: pass 1 uses the production {@code create()} factory to populate the cache. Pass 2
   * uses the package-private {@link LanceFragmentScanner#createForTest} factory to inject counting
   * suppliers — if the cache-hit short-circuit regresses, either counter will be non-zero.
   */
  @Test
  public void fullCacheHitDoesNotInvokeDatasetOrScannerSupplier(@TempDir Path cacheDir)
      throws Exception {
    StructType projection =
        new StructType(
            new StructField[] {DataTypes.createStructField("x", DataTypes.LongType, true)});
    LanceInputPartition partition = buildVersionedPartition(projection);

    // Reset the executor cache singleton to point at a fresh temp directory and enable it via
    // the system property override (LanceExecutorCache.isEnabled() consults the property first).
    LanceExecutorCache.resetForTesting(new LanceExecutorCache(cacheDir, Long.MAX_VALUE));
    String previousProp = System.getProperty(LANCE_EXEC_CACHE_ENABLED);
    System.setProperty(LANCE_EXEC_CACHE_ENABLED, "true");
    try {
      // Pass 1: warm the cache via the production code path.
      try (LanceFragmentScanner warm = LanceFragmentScanner.create(0, partition);
          ArrowReader r = warm.getArrowReader()) {
        while (r.loadNextBatch()) {
          // drain — populates the per-column disk cache
        }
      }

      // Pass 2: spy suppliers MUST stay at zero invocations under a full cache hit.
      AtomicInteger datasetOpens = new AtomicInteger();
      AtomicInteger scannerBuilds = new AtomicInteger();
      LazyResource<org.lance.Dataset> spyDataset =
          new LazyResource<>(
              () -> {
                datasetOpens.incrementAndGet();
                LanceSparkReadOptions ro = partition.getReadOptions();
                return org.lance.spark.utils.Utils.openDatasetBuilder(ro)
                    .initialStorageOptions(partition.getInitialStorageOptions())
                    .build();
              });
      LazyResource<org.lance.ipc.LanceScanner> spyScanner =
          new LazyResource<>(
              () -> {
                scannerBuilds.incrementAndGet();
                org.lance.Dataset ds = spyDataset.get();
                org.lance.Fragment frag = ds.getFragment(0);
                return frag.newScan(new org.lance.ipc.ScanOptions.Builder().build());
              });

      try (LanceFragmentScanner second =
              LanceFragmentScanner.createForTest(0, partition, spyDataset, spyScanner);
          ArrowReader r = second.getArrowReader()) {
        while (r.loadNextBatch()) {
          // drain — full cache hit must avoid the suppliers
        }
      }
      assertEquals(0, datasetOpens.get(), "dataset must not open on full cache hit");
      assertEquals(0, scannerBuilds.get(), "scanner must not be built on full cache hit");
    } finally {
      if (previousProp == null) {
        System.clearProperty(LANCE_EXEC_CACHE_ENABLED);
      } else {
        System.setProperty(LANCE_EXEC_CACHE_ENABLED, previousProp);
      }
    }
  }

  @AfterEach
  public void resetExecutorCacheState() {
    // Make sure no property-mutation from our tests bleeds into subsequent tests in the same JVM.
    System.clearProperty(LANCE_EXEC_CACHE_ENABLED);
  }

  /** Build a {@link LanceInputPartition} pointing at the shared example_db dataset, version 1. */
  private LanceInputPartition buildVersionedPartition(StructType projection) {
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.datasetUri)
            .version(1)
            .build();
    return new LanceInputPartition(
        projection,
        0,
        new LanceSplit(Collections.singletonList(0)),
        readOptions,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        "lazy-cache-test",
        Collections.emptyMap(),
        null,
        null,
        null);
  }

  private static final String LANCE_EXEC_CACHE_ENABLED = "LANCE_EXEC_CACHE_ENABLED";

  /**
   * Public, top-level-by-FQCN, no-arg {@link LanceNamespace} so that {@link
   * LanceNamespace#connect(String, Map, BufferAllocator)} can resolve it via {@code Class.forName}
   * if the executor branch is (incorrectly) taken.
   */
  public static class RecordingNamespace implements LanceNamespace {
    static final AtomicInteger INITIALIZE_CALLS = new AtomicInteger();

    public RecordingNamespace() {}

    @Override
    public void initialize(Map<String, String> properties, BufferAllocator allocator) {
      INITIALIZE_CALLS.incrementAndGet();
    }

    @Override
    public String namespaceId() {
      return "recording";
    }
  }

  /**
   * Locks down the lazy-open contract: {@code create()} followed by {@code close()} without ever
   * calling {@code getArrowReader()} must not open the dataset or build the scanner. We assert this
   * by pointing the partition at a non-existent dataset URI; before the lazy refactor, the eager
   * {@code Utils.openDatasetBuilder().build()} inside {@code create()} would fail with "dataset not
   * found", wrapped in a {@link RuntimeException}.
   */
  @Test
  public void closeWithoutGetArrowReaderDoesNotOpenDatasetOrScanner() throws Exception {
    LanceInputPartition partition = newPartitionPointingAtMissingDataset();
    try (LanceFragmentScanner scanner = LanceFragmentScanner.create(0, partition)) {
      // intentionally do not call getArrowReader() — verifies lazy semantics
    }
  }

  private LanceInputPartition newPartitionPointingAtMissingDataset() {
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri("/tmp/lance-spark-no-such-dataset")
            .version(1)
            .build();
    return new LanceInputPartition(
        new StructType(),
        0,
        new org.lance.spark.read.LanceSplit(Collections.singletonList(0)),
        readOptions,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        "lazy-test",
        Collections.emptyMap(),
        null,
        null,
        null);
  }
}
