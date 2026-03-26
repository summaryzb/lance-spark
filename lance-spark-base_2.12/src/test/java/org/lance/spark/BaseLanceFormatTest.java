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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base test class for reading/writing Lance tables using spark.read.format("lance").load() and
 * spark.write.format("lance").save() without configuring a catalog. This verifies that the
 * DataSource works directly via path-based access (auto-registered default catalog).
 *
 * <p>Also tests data_storage_version configuration at catalog level. Verifies that setting
 * data_storage_version (STABLE or LEGACY) at catalog level propagates correctly to table creation,
 * so subsequent INSERT operations do not fail due to format mismatch.
 */
public abstract class BaseLanceFormatTest {
  private static SparkSession spark;
  private static String datasetUri;

  @TempDir static Path tempDir;

  /**
   * Separate temp dir for data_storage_version tests (instance-level for per-test isolation).
   * Requires PER_METHOD lifecycle (JUnit 5 default) so each parameterized invocation gets its own
   * directory. Do NOT switch to PER_CLASS without adjusting this field to avoid cross-test leakage.
   */
  @TempDir Path dsvTempDir;

  /**
   * DSV test session tracked per-test so it can be stopped in @AfterEach without affecting the
   * shared static session used by non-DSV tests.
   */
  private SparkSession dsvSpark;

  @BeforeAll
  static void setup() {
    spark = SparkSession.builder().appName("lance-format-read-test").master("local").getOrCreate();
    datasetUri =
        TestUtils.getDatasetUri(
            TestUtils.TestTable1Config.dbPath, TestUtils.TestTable1Config.datasetName);
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @AfterEach
  void tearDownDsv() {
    if (dsvSpark != null) {
      dsvSpark.stop();
      dsvSpark = null;
      // Re-create the shared session since stop() kills the global SparkContext
      spark =
          SparkSession.builder().appName("lance-format-read-test").master("local").getOrCreate();
    }
  }

  private void validateData(Dataset<Row> data, List<List<Long>> expectedValues) {
    List<Row> rows = data.collectAsList();
    assertEquals(expectedValues.size(), rows.size());

    for (int i = 0; i < rows.size(); i++) {
      Row row = rows.get(i);
      List<Long> expectedRow = expectedValues.get(i);
      assertEquals(expectedRow.size(), row.size());

      for (int j = 0; j < expectedRow.size(); j++) {
        long expectedValue = expectedRow.get(j);
        long actualValue = row.getLong(j);
        assertEquals(expectedValue, actualValue, "Mismatch at row " + i + " column " + j);
      }
    }
  }

  @Test
  public void testReadWithLoad() {
    // Test reading using spark.read.format("lance").load(path)
    Dataset<Row> df = spark.read().format("lance").load(datasetUri);
    validateData(df, TestUtils.TestTable1Config.expectedValues);
  }

  @Test
  public void testReadWithPathOption() {
    // Test reading using spark.read.format("lance").option("path", ...).load()
    Dataset<Row> df =
        spark
            .read()
            .format("lance")
            .option(LanceSparkReadOptions.CONFIG_DATASET_URI, datasetUri)
            .load();
    validateData(df, TestUtils.TestTable1Config.expectedValues);
  }

  @Test
  public void testFilterPushdown() {
    Dataset<Row> df = spark.read().format("lance").load(datasetUri);
    validateData(
        df.filter("x > 1"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .filter(row -> row.get(0) > 1)
            .collect(Collectors.toList()));
  }

  @Test
  public void testColumnProjection() {
    Dataset<Row> df = spark.read().format("lance").load(datasetUri);
    validateData(
        df.select("y", "b"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .map(row -> Arrays.asList(row.get(1), row.get(2)))
            .collect(Collectors.toList()));
  }

  @Test
  public void testFilterAndSelect() {
    Dataset<Row> df = spark.read().format("lance").load(datasetUri);
    validateData(
        df.select("y", "b").filter("y > 3"),
        TestUtils.TestTable1Config.expectedValues.stream()
            .map(row -> Arrays.asList(row.get(1), row.get(2)))
            .filter(row -> row.get(0) > 3)
            .collect(Collectors.toList()));
  }

  @Test
  public void testWriteAndReadWithFormat() {
    // Test writing and reading using spark.write.format("lance").save(path)
    String outputPath = tempDir.resolve("test_write_format.lance").toString();

    // Create test data
    StructType schema =
        new StructType()
            .add("id", DataTypes.LongType)
            .add("name", DataTypes.StringType)
            .add("value", DataTypes.DoubleType);

    List<Row> data =
        Arrays.asList(
            RowFactory.create(1L, "Alice", 100.0),
            RowFactory.create(2L, "Bob", 200.0),
            RowFactory.create(3L, "Charlie", 300.0));

    Dataset<Row> df = spark.createDataFrame(data, schema);

    // Write using format("lance").save(path) - use ErrorIfExists for creating new table
    df.write().format("lance").mode(SaveMode.ErrorIfExists).save(outputPath);

    // Read back and verify
    Dataset<Row> readDf = spark.read().format("lance").load(outputPath);
    List<Row> result = readDf.orderBy("id").collectAsList();

    assertEquals(3, result.size());
    assertEquals(1L, result.get(0).getLong(0));
    assertEquals("Alice", result.get(0).getString(1));
    assertEquals(100.0, result.get(0).getDouble(2), 0.001);
    assertEquals(2L, result.get(1).getLong(0));
    assertEquals("Bob", result.get(1).getString(1));
    assertEquals(200.0, result.get(1).getDouble(2), 0.001);
    assertEquals(3L, result.get(2).getLong(0));
    assertEquals("Charlie", result.get(2).getString(1));
    assertEquals(300.0, result.get(2).getDouble(2), 0.001);
  }

  @Test
  public void testAppendMode() {
    // Test appending data using format("lance")
    String outputPath = tempDir.resolve("test_append_format.lance").toString();

    StructType schema =
        new StructType().add("id", DataTypes.LongType).add("value", DataTypes.LongType);

    // Create table first using ErrorIfExists mode
    List<Row> data1 = Arrays.asList(RowFactory.create(1L, 100L), RowFactory.create(2L, 200L));
    Dataset<Row> df1 = spark.createDataFrame(data1, schema);
    df1.write().format("lance").mode(SaveMode.ErrorIfExists).save(outputPath);

    // Append more data
    List<Row> data2 = Arrays.asList(RowFactory.create(3L, 300L), RowFactory.create(4L, 400L));
    Dataset<Row> df2 = spark.createDataFrame(data2, schema);
    df2.write().format("lance").mode(SaveMode.Append).save(outputPath);

    // Verify all data is present
    Dataset<Row> readDf = spark.read().format("lance").load(outputPath);
    assertEquals(4, readDf.count());

    List<Row> result = readDf.orderBy("id").collectAsList();
    assertEquals(1L, result.get(0).getLong(0));
    assertEquals(2L, result.get(1).getLong(0));
    assertEquals(3L, result.get(2).getLong(0));
    assertEquals(4L, result.get(3).getLong(0));
  }

  /** Helper: writes data to a Lance dataset and reads it back ordered by "id". */
  private List<Row> writeAndReadStruct(
      List<Row> data, StructType schema, String version, String tag) {
    String outputPath = tempDir.resolve("test_struct_" + tag + "_" + version + ".lance").toString();
    spark
        .createDataFrame(data, schema)
        .write()
        .format("lance")
        .option("data_storage_version", version)
        .mode(SaveMode.ErrorIfExists)
        .save(outputPath);
    return spark.read().format("lance").load(outputPath).orderBy("id").collectAsList();
  }

  private static StructType createAddressSchema() {
    return new StructType(
        new StructField[] {
          DataTypes.createStructField("city", DataTypes.StringType, true),
          DataTypes.createStructField("country", DataTypes.StringType, true)
        });
  }

  private static StructType createIdAddressSchema(StructType addressSchema) {
    return new StructType(
        new StructField[] {
          DataTypes.createStructField("id", DataTypes.IntegerType, false),
          DataTypes.createStructField("address", addressSchema, true)
        });
  }

  /**
   * Tests that struct columns are correctly written and read back for all Lance format versions.
   * Reproduces issue #119: struct columns written with Lance format v2.1/v2.2 were always NULL
   * because StructWriter did not set the validity bit on the parent StructVector.
   */
  @ParameterizedTest
  @ValueSource(strings = {"2.0", "2.1", "2.2"})
  public void testStructColumnWithFormatVersion(String version) {
    StructType schema = createIdAddressSchema(createAddressSchema());
    List<Row> data =
        Arrays.asList(
            RowFactory.create(1, RowFactory.create("Beijing", "China")),
            RowFactory.create(2, RowFactory.create("New York", "USA")));

    List<Row> result = writeAndReadStruct(data, schema, version, "non_null");

    assertEquals(2, result.size());
    assertNotNull(
        result.get(0).getStruct(1),
        "Struct column should not be NULL for format version " + version);
    assertEquals("Beijing", result.get(0).getStruct(1).getString(0));
    assertEquals("China", result.get(0).getStruct(1).getString(1));
    assertNotNull(
        result.get(1).getStruct(1),
        "Struct column should not be NULL for format version " + version);
    assertEquals("New York", result.get(1).getStruct(1).getString(0));
    assertEquals("USA", result.get(1).getStruct(1).getString(1));
  }

  /**
   * Tests that null struct values mixed with non-null structs are correctly written and read back.
   * Verifies the setNull() fix in StructWriter that propagates null to child vectors, keeping child
   * vector counts aligned with the parent StructVector. Without this fix, child vectors become
   * shorter than the parent, causing index misalignment for all rows after a null struct.
   *
   * <p>Only tests V2_1+ because V2_0 legacy encoders read null structs as empty structs rather than
   * null — this is a known behavioral difference in V2_0's CoreFieldEncodingStrategy, not a bug.
   */
  @ParameterizedTest
  @ValueSource(strings = {"2.1", "2.2"})
  public void testNullStructColumnWithFormatVersion(String version) {
    StructType schema = createIdAddressSchema(createAddressSchema());
    List<Row> data =
        Arrays.asList(
            RowFactory.create(1, RowFactory.create("Beijing", "China")),
            RowFactory.create(2, null),
            RowFactory.create(3, null), // consecutive nulls
            RowFactory.create(4, RowFactory.create("New York", "USA")));

    List<Row> result = writeAndReadStruct(data, schema, version, "null_mixed");

    assertEquals(4, result.size());
    assertNotNull(result.get(0).getStruct(1));
    assertEquals("Beijing", result.get(0).getStruct(1).getString(0));
    assertEquals("China", result.get(0).getStruct(1).getString(1));
    assertTrue(
        result.get(1).isNullAt(1),
        "Null struct should be read back as NULL for format version " + version);
    assertTrue(
        result.get(2).isNullAt(1),
        "Consecutive null struct should be read back as NULL for format version " + version);
    // Row after consecutive nulls — validates count alignment accumulates correctly
    assertNotNull(
        result.get(3).getStruct(1),
        "Non-null struct after consecutive nulls should not be NULL for format version " + version);
    assertEquals("New York", result.get(3).getStruct(1).getString(0));
    assertEquals("USA", result.get(3).getStruct(1).getString(1));
  }

  /**
   * Tests that V2_0 legacy encoders read null structs as empty structs rather than null. This is a
   * known behavioral difference: V2_0's CoreFieldEncodingStrategy does not preserve null semantics
   * for struct types, so a null struct is read back as a non-null struct with null child fields.
   */
  @Test
  public void testNullStructV2_0ReadsAsEmptyStruct() {
    StructType schema = createIdAddressSchema(createAddressSchema());
    List<Row> data =
        Arrays.asList(
            RowFactory.create(1, RowFactory.create("Beijing", "China")),
            RowFactory.create(2, null),
            RowFactory.create(3, RowFactory.create("New York", "USA")));

    List<Row> result = writeAndReadStruct(data, schema, "2.0", "v20_null_behavior");

    assertEquals(3, result.size());
    // V2_0: null struct is read back as non-null with null child fields (known behavior)
    assertFalse(
        result.get(1).isNullAt(1),
        "V2_0 legacy encoder reads null struct as empty struct, not null");
    Row emptyStruct = result.get(1).getStruct(1);
    assertNotNull(emptyStruct);
    assertTrue(emptyStruct.isNullAt(0), "city should be null in V2_0 empty struct");
    assertTrue(emptyStruct.isNullAt(1), "country should be null in V2_0 empty struct");
    // Non-null rows should still be correct
    assertEquals("Beijing", result.get(0).getStruct(1).getString(0));
    assertEquals("New York", result.get(2).getStruct(1).getString(0));
  }

  /**
   * Tests that nested structs with null values at different levels are correctly handled. Verifies
   * that StructWriter.setNull() recursively propagates null to all descendant child vectors,
   * keeping counts aligned at every nesting level.
   */
  @ParameterizedTest
  @ValueSource(strings = {"2.1", "2.2"})
  public void testNestedStructWithNullValues(String version) {
    // Schema: id INT, person STRUCT<name: STRING, address: STRUCT<city: STRING, country: STRING>>
    StructType addressSchema = createAddressSchema();
    StructType personSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("address", addressSchema, true)
            });
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("person", personSchema, true)
            });

    List<Row> data =
        Arrays.asList(
            RowFactory.create(1, RowFactory.create("Alice", RowFactory.create("Beijing", "China"))),
            RowFactory.create(2, null), // entire outer struct is null
            RowFactory.create(3, RowFactory.create("Bob", RowFactory.create("New York", "USA"))),
            RowFactory.create(4, RowFactory.create("Charlie", null)), // inner struct is null
            RowFactory.create(5, RowFactory.create("Diana", RowFactory.create("Tokyo", "Japan"))));

    List<Row> result = writeAndReadStruct(data, schema, version, "nested");

    assertEquals(5, result.size());

    // Row 1 (id=1): fully populated nested struct
    assertNotNull(result.get(0).getStruct(1));
    assertEquals("Alice", result.get(0).getStruct(1).getString(0));
    assertNotNull(result.get(0).getStruct(1).getStruct(1));
    assertEquals("Beijing", result.get(0).getStruct(1).getStruct(1).getString(0));

    // Row 2 (id=2): entire outer struct is null
    assertTrue(
        result.get(1).isNullAt(1), "Outer struct should be NULL for format version " + version);

    // Row 3 (id=3): fully populated — validates alignment after outer null
    assertNotNull(result.get(2).getStruct(1));
    assertEquals("Bob", result.get(2).getStruct(1).getString(0));
    assertEquals("New York", result.get(2).getStruct(1).getStruct(1).getString(0));
    assertEquals("USA", result.get(2).getStruct(1).getStruct(1).getString(1));

    // Row 4 (id=4): outer struct non-null, inner address struct is null
    assertNotNull(result.get(3).getStruct(1));
    assertEquals("Charlie", result.get(3).getStruct(1).getString(0));
    assertTrue(
        result.get(3).getStruct(1).isNullAt(1),
        "Inner address struct should be NULL for format version " + version);

    // Row 5 (id=5): fully populated — validates alignment after inner null
    assertNotNull(result.get(4).getStruct(1));
    assertEquals("Diana", result.get(4).getStruct(1).getString(0));
    assertEquals("Tokyo", result.get(4).getStruct(1).getStruct(1).getString(0));
    assertEquals("Japan", result.get(4).getStruct(1).getStruct(1).getString(1));
  }

  /**
   * Tests data_storage_version propagation at catalog level. Same-version create+insert should
   * succeed; LEGACY→STABLE upgrade succeeds; STABLE→LEGACY downgrade is rejected by lance-core.
   */
  @ParameterizedTest
  @CsvSource({
    "STABLE, STABLE, true",
    "LEGACY, LEGACY, true",
    "LEGACY, STABLE, true",
    "STABLE, LEGACY, false"
  })
  public void testCatalogLevelDataStorageVersion(
      String createVersion, String insertVersion, boolean expectSuccess) {
    runDsvCrossVersionTest(createVersion, insertVersion, expectSuccess);
  }

  /**
   * Creates a table with {@code createVersion}, then inserts data with {@code insertVersion}. When
   * {@code expectSuccess} is true, verifies the inserted rows; otherwise asserts that the insert
   * throws an exception.
   */
  private void runDsvCrossVersionTest(
      String createVersion, String insertVersion, boolean expectSuccess) {
    String tableName = "dsv_" + UUID.randomUUID().toString().replace("-", "");
    String catalogCreate = dsvCatalogName("create");

    // Phase 1: Create table with createVersion
    dsvSpark = dsvSparkSession(catalogCreate, createVersion);
    String fqnCreate = catalogCreate + ".default." + tableName;
    dsvSpark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogCreate + ".default");
    dsvSpark.sql(
        String.format("CREATE TABLE %s (id INT NOT NULL, name STRING, value INT)", fqnCreate));

    // Same-version: reuse the session for insert
    if (createVersion.equals(insertVersion)) {
      dsvSpark.sql(
          String.format("INSERT INTO %s VALUES (1, 'Alice', 100), (2, 'Bob', 200)", fqnCreate));
      verifyDsvRows(dsvSpark, fqnCreate);
      return;
    }

    // Stop phase-1 session before creating phase-2 with different config
    dsvSpark.stop();

    // Phase 2: Insert with a different version
    String catalogInsert = dsvCatalogName("insert");
    dsvSpark = dsvSparkSession(catalogInsert, insertVersion);
    String fqnInsert = catalogInsert + ".default." + tableName;
    dsvSpark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogInsert + ".default");
    String crossInsertSql =
        String.format("INSERT INTO %s VALUES (1, 'Alice', 100), (2, 'Bob', 200)", fqnInsert);

    if (expectSuccess) {
      dsvSpark.sql(crossInsertSql);
      verifyDsvRows(dsvSpark, fqnInsert);
    } else {
      SparkSession sessionRef = dsvSpark;
      assertThrows(
          Exception.class,
          () -> sessionRef.sql(crossInsertSql),
          String.format(
              "Expected error when creating with %s and inserting with %s",
              createVersion, insertVersion));
    }
  }

  private void verifyDsvRows(SparkSession session, String fqn) {
    List<Row> rows =
        session.sql(String.format("SELECT * FROM %s ORDER BY id", fqn)).collectAsList();
    assertEquals(2, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("Alice", rows.get(0).getString(1));
    assertEquals(100, rows.get(0).getInt(2));
    assertEquals(2, rows.get(1).getInt(0));
    assertEquals("Bob", rows.get(1).getString(1));
    assertEquals(200, rows.get(1).getInt(2));
  }

  private String dsvCatalogName(String suffix) {
    String uid = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    return suffix == null ? "lance_dsv_" + uid : "lance_dsv_" + suffix + "_" + uid;
  }

  private SparkSession dsvSparkSession(String catalog, String version) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("lance-dsv-config-test")
            .master("local")
            .config("spark.sql.catalog." + catalog, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalog + ".impl", "dir")
            .config("spark.sql.catalog." + catalog + ".root", dsvTempDir.toString());

    if (version != null) {
      builder.config("spark.sql.catalog." + catalog + ".data_storage_version", version);
    }

    return builder.getOrCreate();
  }
}
