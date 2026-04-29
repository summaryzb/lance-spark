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

import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Adapts a {@link CatalogColumnStat} (Hive-style catalog stat) to the connector {@link
 * ColumnStatistics} interface expected by {@code Statistics.columnStats()}.
 *
 * <p>Also provides {@link #estimateProjected} factory methods for building {@link LanceStatistics}
 * instances with column-width or catalog-property-based size estimation.
 *
 * <p>Scala interop notes:
 *
 * <ul>
 *   <li>{@code Option[BigInt]} fields ({@code distinctCount}, {@code nullCount}) are visible from
 *       Java as {@code scala.Option<scala.math.BigInt>}.
 *   <li>{@code Option[Long]} fields ({@code avgLen}, {@code maxLen}) are visible from Java as
 *       {@code scala.Option<Object>} because Scala's {@code Long} is a value type; the boxed value
 *       must be cast to {@code java.lang.Long} before converting.
 *   <li>{@code Option[String]} fields ({@code min}, {@code max}) are passed through as-is wrapped
 *       in {@code Optional<Object>}.
 *   <li>Histogram adaptation is omitted (returns empty) because Catalyst's internal {@code
 *       Histogram} type differs from the connector's interface.
 * </ul>
 */
final class CatalogColumnStatAdapter implements ColumnStatistics, Serializable {
  private static final long serialVersionUID = 1L;
  private final CatalogColumnStat stat;
  private final DataType dataType;
  private final String fieldName;

  CatalogColumnStatAdapter(CatalogColumnStat stat, DataType dataType, String fieldName) {
    this.stat = stat;
    this.dataType = dataType;
    this.fieldName = fieldName;
  }

  @Override
  public OptionalLong distinctCount() {
    return stat.distinctCount().isDefined()
        ? OptionalLong.of(stat.distinctCount().get().longValue())
        : OptionalLong.empty();
  }

  @Override
  public OptionalLong nullCount() {
    return stat.nullCount().isDefined()
        ? OptionalLong.of(stat.nullCount().get().longValue())
        : OptionalLong.empty();
  }

  @Override
  @SuppressWarnings("unchecked")
  public OptionalLong avgLen() {
    return stat.avgLen().isDefined()
        ? OptionalLong.of((Long) stat.avgLen().get())
        : OptionalLong.empty();
  }

  @Override
  @SuppressWarnings("unchecked")
  public OptionalLong maxLen() {
    return stat.maxLen().isDefined()
        ? OptionalLong.of((Long) stat.maxLen().get())
        : OptionalLong.empty();
  }

  @Override
  public Optional<Object> min() {
    return stat.min().isDefined()
        ? Optional.of(
            CatalogColumnStat.fromExternalString(
                stat.min().get(), fieldName, dataType, stat.version()))
        : Optional.empty();
  }

  @Override
  public Optional<Object> max() {
    return stat.max().isDefined()
        ? Optional.of(
            CatalogColumnStat.fromExternalString(
                stat.max().get(), fieldName, dataType, stat.version()))
        : Optional.empty();
  }

  // histogram: omitted — Catalyst's Histogram type differs from the connector interface.
  // Default implementation in ColumnStatistics returns Optional.empty().

  /**
   * Estimate post-projection size using {@code sizeInBytes × (projectedWidths / fullWidths)}, the
   * same formula Spark's DSv2 {@code FileScan.estimateStatistics} applies after column pruning (see
   * {@code org.apache.spark.sql.execution.datasources.v2.FileScan}). Lets {@code JoinSelection} see
   * a projection-aware size so broadcast decisions line up with what DSv1 Parquet produces through
   * the optimizer's {@code Project.computeStats()} path — within the 8-byte-per-row overhead DSv1
   * adds via {@code EstimationUtils.getSizePerRow}, which rounds estimates apart by a few percent
   * but doesn't shift broadcast thresholds in practice.
   *
   * <p>Width-weighted (not {@code numRows × sumOfWidths}) on purpose: the ratio preserves the
   * compression baked into {@code fullSizeInBytes}, whereas a row-count formula would ignore
   * on-disk encoding and systematically over-count columnar sources.
   *
   * @param numRows projected row count (post fragment-pruning if any)
   * @param fullSizeInBytes on-disk size of the full (pre-projection) dataset in bytes
   * @param fullSchema schema of the full dataset before column pruning
   * @param projectedSchema pruned schema containing only columns actually read by the scan
   * @return statistics with {@code sizeInBytes} scaled by width-weighted column ratio
   */
  public static LanceStatistics estimateProjected(
      long numRows, long fullSizeInBytes, StructType fullSchema, StructType projectedSchema) {
    long projWidth = sumWidths(projectedSchema);
    long fullWidth = sumWidths(fullSchema);
    long sizeInBytes;
    if (fullWidth <= 0 || projWidth <= 0 || projWidth >= fullWidth) {
      sizeInBytes = fullSizeInBytes;
    } else {
      // Promote to double to avoid long overflow in fullSizeInBytes * projWidth.
      sizeInBytes = (long) ((double) fullSizeInBytes * projWidth / fullWidth);
    }
    // Clamp to 1: integer truncation can round very small scaled sizes to 0, which
    // JoinSelection reads as "below threshold" and would unintentionally force a broadcast.
    return new LanceStatistics(numRows, Math.max(sizeInBytes, 1L));
  }

  /**
   * Estimate statistics by parsing column-level stats stored in table properties, using the same
   * approach as {@code HiveExternalCatalog.statsFromProperties} (Spark lines 1166–1180):
   *
   * <ol>
   *   <li>Filter entries whose key starts with {@link LanceStatistics#STATS_PREFIX}; strip the
   *       prefix to obtain {@code colStatsProps} (keys of the form {@code "colName.fieldName"}).
   *   <li>Find column names by locating keys that end with {@code ".version"}.
   *   <li>For each column name build a {@link ColumnStatistics} from the parsed fields.
   * </ol>
   *
   * @param numRows row count (post fragment-pruning if any)
   * @param fullSizeInBytes on-disk size of the full dataset in bytes
   * @param fullSchema full schema (unused here but kept for API symmetry)
   * @param properties table properties that may contain serialized column statistics
   * @return statistics with column stats populated from properties
   */
  public static LanceStatistics estimateProjected(
      long numRows, long fullSizeInBytes, StructType fullSchema, Map<String, String> properties) {
    // 1. Strip STATS_PREFIX from matching keys → colStatsProps ("colName.field" -> value).
    Map<String, String> colStatsProps = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(LanceStatistics.STATS_PREFIX)) {
        colStatsProps.put(key.substring(LanceStatistics.STATS_PREFIX.length()), entry.getValue());
      }
    }

    // 2. Find column names via ".version" sentinel keys, parse each CatalogColumnStat,
    //    then adapt to Map<NamedReference, ColumnStatistics> using proper types.
    // CatalogColumnStat.fromMap requires a Scala immutable.Map; convert using the same
    // Map$.MODULE$.empty() + $plus() loop pattern used in LanceScan.getMetaData().
    scala.collection.immutable.Map<String, String> scalaColStatsProps =
        scala.collection.immutable.Map$.MODULE$.empty();
    for (java.util.Map.Entry<String, String> e : colStatsProps.entrySet()) {
      scalaColStatsProps = scalaColStatsProps.$plus(scala.Tuple2.apply(e.getKey(), e.getValue()));
    }
    Map<String, DataType> nameTypeMap = new HashMap<>();
    for (StructField f : fullSchema.fields()) {
      nameTypeMap.put(f.name(), f.dataType());
    }
    Map<NamedReference, ColumnStatistics> colStats = new HashMap<>();
    for (String key : new java.util.ArrayList<>(colStatsProps.keySet())) {
      if (key.endsWith(".version")) {
        String fieldName = key.substring(0, key.length() - ".version".length());
        scala.Option<CatalogColumnStat> parsed =
            CatalogColumnStat.fromMap("", fieldName, scalaColStatsProps);
        if (parsed.isDefined()) {
          colStats.put(
              FieldReference.column(fieldName),
              new CatalogColumnStatAdapter(parsed.get(), nameTypeMap.get(fieldName), fieldName));
        }
      }
    }
    return new LanceStatistics(
        numRows, Math.max(fullSizeInBytes, 1L), colStats.isEmpty() ? null : colStats);
  }

  private static long sumWidths(StructType schema) {
    if (schema == null) {
      return 0;
    }
    long sum = 0;
    for (StructField field : schema.fields()) {
      sum += field.dataType().defaultSize();
    }
    return sum;
  }
}
