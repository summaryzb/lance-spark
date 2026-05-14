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

import org.lance.spark.LanceSparkReadOptions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Phase 4 end-to-end integration tests for Dynamic File Pruning (DFP).
 *
 * <p>Exercises the full pipeline on a real {@link SparkSession} backed by the Lance catalog: create
 * a partition-compatible table, build a btree index (which also produces zonemap stats), run
 * queries that exercise DFP-relevant scenarios, and assert the end-to-end behavior.
 *
 * <p>The heavy lifting for DFP semantics is already covered by scan-level unit tests ({@link
 * LanceScanRuntimeFilterTest}, {@link PredicateToFilterConverterTest}). These tests fill in the
 * remaining assurance that the config surface, kill-switch, and star-schema patterns work when
 * wired through a real Spark session — without requiring a cross-version TPC-DS fixture (which is a
 * separate artifact).
 */
public abstract class BaseDfpIntegrationTest {

  // Unique per test instance so catalog caching inside a reused SparkSession never binds the
  // same catalog name to two different tempDirs. JUnit 5 creates a fresh test-class instance
  // per test method, so this field is re-initialised between tests.
  protected String catalogName = "lance_dfp_test_" + UUID.randomUUID().toString().replace("-", "");
  protected SparkSession spark;

  /**
   * True if {@link #setup()} created a brand-new {@link SparkSession}. When false, {@code spark}
   * points at a pre-existing session owned by another test class and {@link #tearDown()} must not
   * stop it (stopping would leave that other class's remaining tests unable to use the shared
   * SparkContext).
   */
  private boolean ownedSession;

  @TempDir Path tempDir;

  @BeforeEach
  public void setup() {
    // Intentionally do NOT call SparkSession.getActiveSession().stop() here: Maven Surefire
    // reuses a single JVM across test classes (reuseForks=true is the default), so another
    // test class may hold a live @BeforeAll/@AfterAll-managed session that we must not tear
    // down. getOrCreate() below will return the existing session if one is active; our per-test
    // uniqueness comes from the UUID-scoped tempDir + unique table names, not from session
    // isolation. We track whether the session was pre-existing so tearDown() only closes
    // sessions this test actually created.
    boolean hadActive = SparkSession.getActiveSession().isDefined();
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    rootPath.toFile().mkdirs();
    String testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-dfp-integration-test")
            .master("local[4]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .config("spark.sql.sources.v2.bucketing.enabled", "true")
            .config("spark.sql.autoBroadcastJoinThreshold", "1048576")
            // Turn DPP on explicitly so our filter() path is exercised where appropriate.
            .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
            .getOrCreate();
    ownedSession = !hadActive;
  }

  @AfterEach
  public void tearDown() throws IOException {
    // Only close sessions this test created. If hadActive was true at setup() time we're
    // borrowing someone else's session (owner is responsible for stopping it).
    if (spark != null && ownedSession) {
      spark.close();
    }
  }

  /**
   * Creates a fact-style table with one fragment per distinct region value and a btree index on the
   * region column. The btree index produces zonemap stats that DFP consumes; the table property
   * makes every fragment a distinct partition value so SPJ lights up.
   */
  private String createFactTable() {
    String tableName = "fact_" + UUID.randomUUID().toString().replace("-", "");
    String fullTable = catalogName + ".default." + tableName;
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, region STRING, amount DOUBLE) USING lance "
                + "TBLPROPERTIES ('lance.partition.columns' = 'region')",
            fullTable));
    String[] regions = {"east", "west", "north", "south", "central"};
    for (int r = 0; r < regions.length; r++) {
      int base = r * 20;
      String region = regions[r];
      String values =
          IntStream.range(base, base + 20)
              .mapToObj(i -> String.format("(%d, '%s', %f)", i, region, i * 2.0))
              .collect(Collectors.joining(","));
      spark.sql(String.format("INSERT INTO %s (id, region, amount) VALUES %s", fullTable, values));
    }
    spark.sql(
        String.format("ALTER TABLE %s CREATE INDEX region_idx USING btree (region)", fullTable));
    return fullTable;
  }

  // ---- tests ----

  /**
   * End-to-end sanity: a star-schema filter reaches the scan and the query returns correct rows.
   * This is the minimal proof that DFP plumbing doesn't break query correctness.
   */
  @Test
  public void testStarSchemaFilteredJoinReturnsCorrectRows() {
    String factA = createFactTable();
    String factB = createFactTable();

    Dataset<Row> result =
        spark.sql(
            String.format(
                "SELECT a.id, a.region, b.amount "
                    + "FROM %s a JOIN %s b ON a.region = b.region "
                    + "WHERE a.region = 'east'",
                factA, factB));

    long count = result.count();
    // 20 'east' rows on each side; join on 'east' region produces 20x20 = 400 tuples.
    assertEquals(400L, count, "star-schema filtered join should return 20×20 = 400 rows");
  }

  /**
   * Kill-switch parity: running the same query with {@code spark.lance.runtime.filtering.enabled =
   * false} must produce byte-identical results. DFP must never change query results — only
   * performance.
   *
   * <p>Each {@code @BeforeEach} creates a fresh {@link SparkSession} so conf changes don't leak
   * across tests; the try/finally here is belt-and-suspenders for readers and for the (rare) case
   * where a future refactor reuses a session across tests.
   */
  @Test
  public void testKillSwitchProducesIdenticalResults() {
    String factA = createFactTable();
    String factB = createFactTable();
    // Projecting (a.id, a.region, b.amount) uniquely identifies each output row: the join
    // 'a.region = b.region' with fact-side filter 'a.region = west' fans out 20×20 = 400 rows,
    // and because both tables were generated by createFactTable() with amount = i*2.0, every
    // (a.id, b.amount) pair is distinct. Client-side sort below makes comparison deterministic.
    String query =
        String.format(
            "SELECT a.id, a.region, b.amount FROM %s a JOIN %s b ON a.region = b.region "
                + "WHERE a.region = 'west'",
            factA, factB);

    try {
      spark.conf().set(LanceSparkReadOptions.SPARK_CONF_RUNTIME_FILTERING_ENABLED, "true");
      java.util.List<String> dfpEnabledRows =
          spark.sql(query).collectAsList().stream()
              .map(Row::toString)
              .sorted()
              .collect(Collectors.toList());

      spark.conf().set(LanceSparkReadOptions.SPARK_CONF_RUNTIME_FILTERING_ENABLED, "false");
      java.util.List<String> dfpDisabledRows =
          spark.sql(query).collectAsList().stream()
              .map(Row::toString)
              .sorted()
              .collect(Collectors.toList());

      // Row-content equality, not just count. DFP must never change query results: silently
      // dropping rows while keeping count stable is the bug this test is meant to catch. Both
      // lists are sorted client-side so comparison is deterministic.
      assertEquals(
          dfpEnabledRows,
          dfpDisabledRows,
          "DFP kill-switch must produce byte-identical query results");
    } finally {
      spark.conf().set(LanceSparkReadOptions.SPARK_CONF_RUNTIME_FILTERING_ENABLED, "true");
    }
  }

  /**
   * Single-table selective predicate: DFP is not engaged (no join) but the scan still compiles
   * cleanly and the SPJ + zonemap pruning path produces correct results. This guards against Phase
   * 1's expanded zonemap loading breaking non-DFP queries.
   */
  @Test
  public void testSingleTableFilterUnaffectedByDfp() {
    String table = createFactTable();
    // Use collectAsList().size() here: on single-table scans in the Spark 3.5 module, SQL
    // COUNT(*) and Dataset.count() both route through the aggregate-pushdown path, which hits an
    // unrelated Spark 3.5 Sum-constructor binary-compat issue (see failing count-pushdown tests
    // elsewhere in this module). Join-shaped queries avoid that path, so other tests here can
    // use .count() on joined results without issue.
    int count =
        spark
            .sql(String.format("SELECT * FROM %s WHERE region = 'central'", table))
            .collectAsList()
            .size();
    assertEquals(20, count, "single-region filter should return 20 rows");
  }

  /**
   * DPP disabled at the Spark level: our {@code filter()} must still be safe to invoke (Spark
   * simply won't call it). Queries should return correct results via static pruning only.
   */
  @Test
  public void testDppDisabledFallsBackToStaticPruning() {
    spark.conf().set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false");
    try {
      String factA = createFactTable();
      String factB = createFactTable();
      long count =
          spark
              .sql(
                  String.format(
                      "SELECT a.id FROM %s a JOIN %s b ON a.region = b.region "
                          + "WHERE a.region = 'north'",
                      factA, factB))
              .count();
      assertEquals(400L, count, "query must produce correct results even with DPP off");
    } finally {
      spark.conf().set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true");
    }
  }

  /**
   * Non-selective join: no WHERE predicate on the dim side → Spark's {@code PartitionPruning} rule
   * will not inject runtime filters. The query must still produce the full join result.
   */
  @Test
  public void testNonSelectiveJoinUnaffectedByDfp() {
    String factA = createFactTable();
    String factB = createFactTable();
    long count =
        spark
            .sql(
                String.format(
                    "SELECT a.id FROM %s a JOIN %s b ON a.region = b.region", factA, factB))
            .count();
    // Full cross-region join: each of 5 regions has 20×20 = 400 rows, total = 5 × 400 = 2000.
    assertEquals(2000L, count, "non-selective join should return the full join size");
  }

  /**
   * Multi-row INSERT with varied regions in a single SQL statement (rather than the 5 separate
   * inserts used by the partition-compatible setup). This exercises DFP on a table whose zonemap
   * bounds are wider — DFP should still return correct results, potentially with less pruning.
   */
  @Test
  public void testMixedRegionFragmentReturnsCorrectRows() {
    String tableName = "mixed_" + UUID.randomUUID().toString().replace("-", "");
    String fullTable = catalogName + ".default." + tableName;
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, region STRING, amount DOUBLE) USING lance", fullTable));
    // Single insert with mixed regions — all go into one fragment with min != max on region.
    String values =
        IntStream.range(0, 100)
            .mapToObj(
                i -> String.format("(%d, '%s', %f)", i, (i % 2 == 0 ? "east" : "west"), i * 1.0))
            .collect(Collectors.joining(","));
    spark.sql(String.format("INSERT INTO %s (id, region, amount) VALUES %s", fullTable, values));
    spark.sql(
        String.format("ALTER TABLE %s CREATE INDEX region_idx USING btree (region)", fullTable));

    int eastCount =
        spark
            .sql(String.format("SELECT * FROM %s WHERE region = 'east'", fullTable))
            .collectAsList()
            .size();
    assertEquals(50, eastCount, "mixed-region fragment should still return correct row count");
  }
}
