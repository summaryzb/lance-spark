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
import org.lance.spark.TestUtils;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.aggregate.Sum;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class LanceScanBuilderTest {

  private static final StructType TEST_SCHEMA = TestUtils.TestTable1Config.schema;

  private LanceScanBuilder createBuilder() {
    return new LanceScanBuilder(
        TEST_SCHEMA,
        TestUtils.TestTable1Config.readOptions,
        Collections.emptyMap(),
        null,
        Collections.emptyMap(),
        Collections.emptyMap());
  }

  // --- pruneColumns ---

  @Test
  public void testPruneColumnsUpdatesSchema() {
    LanceScanBuilder builder = createBuilder();
    StructType requiredSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("x", DataTypes.LongType, true),
            });
    builder.pruneColumns(requiredSchema);
    Scan scan = builder.build();
    assertEquals(requiredSchema, scan.readSchema());
  }

  @Test
  public void testPruneColumnsToEmptySchema() {
    LanceScanBuilder builder = createBuilder();
    StructType emptySchema = new StructType();
    builder.pruneColumns(emptySchema);
    Scan scan = builder.build();
    assertEquals(emptySchema, scan.readSchema());
  }

  // --- pushFilters ---

  @Test
  public void testPushFiltersAllSupported() {
    LanceScanBuilder builder = createBuilder();
    Filter[] filters =
        new Filter[] {
          new GreaterThan("x", 1L), new LessThan("y", 10L), new IsNotNull("b"),
        };
    // Contract (matches Iceberg): return ALL input filters so Spark keeps the Filter node above
    // the scan — required by PartitionPruning.hasSelectivePredicate and
    // InjectRuntimeFilter.extractBeneficialFilterCreatePlan for DFP injection to fire. The
    // pushed subset is reported separately via pushedFilters() for EXPLAIN visibility and is
    // still pre-applied at scan time via the Lance where condition.
    Filter[] postScanFilters = builder.pushFilters(filters);
    assertEquals(3, postScanFilters.length);
    assertEquals(3, builder.pushedFilters().length);
  }

  @Test
  public void testPushFiltersMixedSupportedAndUnsupported() {
    LanceScanBuilder builder = createBuilder();
    // StringContains is not supported for push-down
    Filter[] filters =
        new Filter[] {
          new GreaterThan("x", 1L), new StringContains("b", "test"),
        };
    Filter[] postScanFilters = builder.pushFilters(filters);
    // All input filters are returned as post-scan (see testPushFiltersAllSupported comment).
    assertEquals(2, postScanFilters.length);
    // Only the supported one is reported as pushed.
    assertEquals(1, builder.pushedFilters().length);
    assertInstanceOf(GreaterThan.class, builder.pushedFilters()[0]);
  }

  @Test
  public void testPushFiltersEmptyArray() {
    LanceScanBuilder builder = createBuilder();
    Filter[] result = builder.pushFilters(new Filter[0]);
    assertEquals(0, result.length);
    assertEquals(0, builder.pushedFilters().length);
  }

  @Test
  public void testPushFiltersDisabledByConfig() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.from(
            Collections.singletonMap(LanceSparkReadOptions.CONFIG_PUSH_DOWN_FILTERS, "false"),
            TestUtils.TestTable1Config.datasetUri);
    LanceScanBuilder builder =
        new LanceScanBuilder(TEST_SCHEMA, options, Collections.emptyMap(), null, null, null);
    Filter[] filters = new Filter[] {new GreaterThan("x", 1L)};
    Filter[] result = builder.pushFilters(filters);
    assertEquals(1, result.length);
    assertEquals(0, builder.pushedFilters().length);
  }

  // --- DFP runtime filtering (Phase 1) ---

  @Test
  public void testBuildSucceedsWithRuntimeFilteringEnabled() {
    // Fixture has no ZONEMAP indexes, so the expanded-loading branch is a no-op at the data level;
    // this test guards that the Phase 1 code path in build() compiles and runs cleanly under
    // enabled=true without affecting the resulting Scan.
    java.util.Map<String, String> opts = new java.util.HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_ENABLED, "true");
    LanceSparkReadOptions options =
        LanceSparkReadOptions.from(opts, TestUtils.TestTable1Config.datasetUri);
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA,
            options,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            Collections.emptyMap());
    Filter[] filters = new Filter[] {new GreaterThan("x", 1L)};
    builder.pushFilters(filters);
    Scan scan = builder.build();
    assertEquals(TEST_SCHEMA, scan.readSchema());
    assertTrue(options.isRuntimeFilteringEnabled());
  }

  @Test
  public void testBuildSucceedsWithRuntimeFilteringDisabled() {
    // Verifies the Phase 1 kill-switch: when enabled=false, build() takes the legacy filter-only
    // loading path (no expansion to all ZONEMAP-indexed columns) and produces a working Scan.
    java.util.Map<String, String> opts = new java.util.HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_ENABLED, "false");
    LanceSparkReadOptions options =
        LanceSparkReadOptions.from(opts, TestUtils.TestTable1Config.datasetUri);
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA,
            options,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            Collections.emptyMap());
    Filter[] filters = new Filter[] {new GreaterThan("x", 1L)};
    builder.pushFilters(filters);
    Scan scan = builder.build();
    assertEquals(TEST_SCHEMA, scan.readSchema());
    assertFalse(options.isRuntimeFilteringEnabled());
  }

  @Test
  public void testBuildRespectsZeroMaxColumnsCap() {
    // When max.columns=0, no ZONEMAP-indexed columns can be added beyond the filter baseline.
    // On the no-index fixture this is still a no-op; the test guards that the cap branch runs
    // cleanly and does not throw for a zero budget.
    java.util.Map<String, String> opts = new java.util.HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_ENABLED, "true");
    opts.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_MAX_COLUMNS, "0");
    LanceSparkReadOptions options =
        LanceSparkReadOptions.from(opts, TestUtils.TestTable1Config.datasetUri);
    LanceScanBuilder builder =
        new LanceScanBuilder(
            TEST_SCHEMA,
            options,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            Collections.emptyMap());
    Scan scan = builder.build();
    assertEquals(TEST_SCHEMA, scan.readSchema());
    assertEquals(0, options.getRuntimeFilteringMaxColumns());
  }

  @Test
  public void testPushFiltersWithNestedArrayOfStruct() {
    // Filters on non-Array<Struct> columns should be pushed down normally.
    StructType nestedSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
              DataTypes.createStructField(
                  "items",
                  new ArrayType(
                      new StructType(
                          new StructField[] {
                            DataTypes.createStructField("name", DataTypes.StringType, true)
                          }),
                      true),
                  true),
            });
    LanceScanBuilder builder =
        new LanceScanBuilder(
            nestedSchema,
            TestUtils.TestTable1Config.readOptions,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            Collections.emptyMap());
    Filter[] filters = new Filter[] {new GreaterThan("id", 1L)};
    Filter[] result = builder.pushFilters(filters);
    // See testPushFiltersAllSupported: all input filters are returned as post-scan so
    // Spark's Filter node survives for the DFP/runtime-filter optimizer rules.
    assertEquals(1, result.length);
    assertEquals(1, builder.pushedFilters().length);
  }

  // --- pushLimit ---

  @Test
  public void testPushLimitAlwaysSucceeds() {
    LanceScanBuilder builder = createBuilder();
    assertTrue(builder.pushLimit(100));
  }

  // --- pushOffset ---

  @Test
  public void testPushOffsetRejectsMultiFragmentDataset() {
    // TestTable1 has 2 fragments, so offset cannot be pushed
    LanceScanBuilder builder = createBuilder();
    assertFalse(builder.pushOffset(10));
  }

  @Test
  public void testIsPartiallyPushedAlwaysTrue() {
    LanceScanBuilder builder = createBuilder();
    assertTrue(builder.isPartiallyPushed());
  }

  // --- pushTopN ---

  @Test
  public void testPushTopNEnabledByDefault() {
    LanceScanBuilder builder = createBuilder();
    SortOrder order = new TestSortOrder("x", SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    assertTrue(builder.pushTopN(new SortOrder[] {order}, 10));
  }

  @Test
  public void testPushTopNDisabledByConfig() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.from(
            Collections.singletonMap(LanceSparkReadOptions.CONFIG_TOP_N_PUSH_DOWN, "false"),
            TestUtils.TestTable1Config.datasetUri);
    LanceScanBuilder builder =
        new LanceScanBuilder(TEST_SCHEMA, options, Collections.emptyMap(), null, null, null);
    SortOrder order = new TestSortOrder("x", SortDirection.ASCENDING, NullOrdering.NULLS_FIRST);
    assertFalse(builder.pushTopN(new SortOrder[] {order}, 10));
  }

  @Test
  public void testPushTopNRejectsNonFieldReferenceExpression() {
    LanceScanBuilder builder = createBuilder();
    // A SortOrder whose expression is not a FieldReference should be rejected
    SortOrder nonFieldOrder =
        new SortOrder() {
          @Override
          public Expression expression() {
            return new Expression() {
              @Override
              public Expression[] children() {
                return new Expression[0];
              }

              @Override
              public String toString() {
                return "custom_expression";
              }
            };
          }

          @Override
          public SortDirection direction() {
            return SortDirection.ASCENDING;
          }

          @Override
          public NullOrdering nullOrdering() {
            return NullOrdering.NULLS_FIRST;
          }
        };
    assertFalse(builder.pushTopN(new SortOrder[] {nonFieldOrder}, 10));
  }

  // --- pushAggregation ---

  @Test
  public void testPushAggregationCountStarFromMetadata() {
    LanceScanBuilder builder = createBuilder();
    Aggregation countStar =
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {});
    assertTrue(builder.pushAggregation(countStar));
  }

  @Test
  public void testPushAggregationCountStarWithFiltersFallsBackToScanner() {
    LanceScanBuilder builder = createBuilder();
    builder.pushFilters(new Filter[] {new GreaterThan("x", 0L)});
    Aggregation countStar =
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {});
    // With pushed filters, metadata count cannot be used; falls back to scanner-based count
    assertTrue(builder.pushAggregation(countStar));
  }

  @Test
  public void testPushAggregationRejectsGroupBy() {
    LanceScanBuilder builder = createBuilder();
    Aggregation groupedAgg =
        new Aggregation(
            new AggregateFunc[] {new CountStar()}, new Expression[] {FieldReference.apply("x")});
    assertFalse(builder.pushAggregation(groupedAgg));
  }

  @Test
  public void testPushAggregationRejectsNonCountStar() {
    LanceScanBuilder builder = createBuilder();
    Aggregation sumAgg =
        new Aggregation(
            new AggregateFunc[] {new Sum(FieldReference.apply("x"), false)}, new Expression[] {});
    assertFalse(builder.pushAggregation(sumAgg));
  }

  // --- build ---

  @Test
  public void testBuildReturnsLanceScan() {
    LanceScanBuilder builder = createBuilder();
    Scan scan = builder.build();
    assertNotNull(scan);
    assertInstanceOf(LanceScan.class, scan);
    assertEquals(TEST_SCHEMA, scan.readSchema());
  }

  @Test
  public void testBuildWithCountStarReturnsLocalScan() {
    LanceScanBuilder builder = createBuilder();
    Aggregation countStar =
        new Aggregation(new AggregateFunc[] {new CountStar()}, new Expression[] {});
    builder.pushAggregation(countStar);
    Scan scan = builder.build();
    // Metadata-based COUNT(*) without filters returns LanceLocalScan
    assertNotNull(scan);
    assertInstanceOf(LanceLocalScan.class, scan);
  }

  @Test
  public void testEstimateZonemapBytesAccountsForStringMinMax() {
    // Regression: previously the estimator used a fixed 96 bytes per zone, which was accurate
    // only for numeric boxed primitives. A string-keyed ZONEMAP index would silently bypass the
    // byte cap because each ZoneStats carries two String references (min, max) whose referent
    // size can be hundreds of bytes. Now the estimator samples the first zone's min/max and
    // adds the variable-length portion.
    String longMin = "a".repeat(200);
    String longMax = "z".repeat(200);
    java.util.List<org.lance.index.scalar.ZoneStats> stringZones =
        java.util.Arrays.asList(
            new org.lance.index.scalar.ZoneStats(0, 0, 1024, longMin, longMax, 0),
            new org.lance.index.scalar.ZoneStats(1, 0, 1024, longMin, longMax, 0),
            new org.lance.index.scalar.ZoneStats(2, 0, 1024, longMin, longMax, 0));
    java.util.Map<String, java.util.List<org.lance.index.scalar.ZoneStats>> stringStats =
        Collections.singletonMap("description", stringZones);

    // Per zone: 96 base + (24 + 2*200) for min + (24 + 2*200) for max = 96 + 848 = 944 bytes
    // Expected total: 16 (map overhead) + 3 * 944 = 2848 bytes
    long stringEstimate = LanceScanBuilder.estimateZonemapBytes(stringStats);
    assertEquals(16L + 3L * 944L, stringEstimate);

    // Numeric column stays at the 96-byte base per zone — no String min/max penalty.
    java.util.List<org.lance.index.scalar.ZoneStats> numericZones =
        java.util.Arrays.asList(
            new org.lance.index.scalar.ZoneStats(0, 0, 1024, 1L, 100L, 0),
            new org.lance.index.scalar.ZoneStats(1, 0, 1024, 1L, 100L, 0),
            new org.lance.index.scalar.ZoneStats(2, 0, 1024, 1L, 100L, 0));
    java.util.Map<String, java.util.List<org.lance.index.scalar.ZoneStats>> numericStats =
        Collections.singletonMap("order_id", numericZones);
    assertEquals(16L + 3L * 96L, LanceScanBuilder.estimateZonemapBytes(numericStats));
  }

  /** Minimal SortOrder implementation for testing pushTopN. */
  private static class TestSortOrder implements SortOrder {
    private final String columnName;
    private final SortDirection direction;
    private final NullOrdering nullOrdering;

    TestSortOrder(String columnName, SortDirection direction, NullOrdering nullOrdering) {
      this.columnName = columnName;
      this.direction = direction;
      this.nullOrdering = nullOrdering;
    }

    @Override
    public Expression expression() {
      return FieldReference.apply(columnName);
    }

    @Override
    public SortDirection direction() {
      return direction;
    }

    @Override
    public NullOrdering nullOrdering() {
      return nullOrdering;
    }
  }
}
