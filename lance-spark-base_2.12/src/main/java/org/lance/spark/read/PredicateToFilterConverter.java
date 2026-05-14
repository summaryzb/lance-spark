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

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.Or;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Converts V2 {@link Predicate}s emitted by Spark's {@code PartitionPruning} rule into V1 {@link
 * Filter}s that {@link ZonemapFragmentPruner} can evaluate. Supports exactly the subset that Spark
 * emits for runtime filtering:
 *
 * <ul>
 *   <li>{@code IN(column, [lit0, lit1, ...])} — most common shape for build-side broadcast values.
 *   <li>{@code column = lit} — single-valued runtime filter.
 *   <li>{@code column IS NULL} — degenerate case.
 *   <li>{@code AND(a, b)}, {@code OR(a, b)} — recursively.
 * </ul>
 *
 * <p>Anything outside the table (including {@code NOT}, range comparisons, string functions, nested
 * non-trivial children) is dropped. The converter never <em>weakens</em> a predicate: if a child of
 * an AND or OR is unconvertible, the entire tree is dropped. Dropping is logged at DEBUG and
 * surfaced via the {@code predicatesDropped} SQL metric (wired from {@link LanceScan#filter}); it
 * never throws.
 *
 * <p>Spark's own {@code PredicateUtils.toV1} is {@code private[sql]} and unreachable from external
 * connectors, which is why we reimplement a pinned subset here. Parity with Spark across 3.4 / 3.5
 * / 4.0 is guarded by a cross-version integration test in Phase 4.
 */
public final class PredicateToFilterConverter {

  private PredicateToFilterConverter() {
    // Utility class; not instantiable.
  }

  /**
   * Convert a batch of V2 predicates to V1 filters, dropping entries whose referenced column is not
   * in {@code filterableColumns} or whose shape cannot be translated. Returns a possibly-empty
   * array; never returns {@code null}.
   *
   * @param predicates predicates from {@link
   *     org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering#filter}
   * @param filterableColumns columns for which we hold driver-side zonemap stats; other columns are
   *     silently dropped because the pruner cannot evaluate them
   * @param maxInValues cap from {@link org.lance.spark.LanceSparkReadOptions}; {@code IN} lists
   *     above this size are dropped to avoid O(fragments * zones * values) blowups. A value of 0
   *     disables the cap.
   * @param log logger used for DEBUG-level drop messages
   */
  public static Filter[] convertAll(
      Predicate[] predicates, Set<String> filterableColumns, int maxInValues, Logger log) {
    if (predicates == null || predicates.length == 0) {
      return new Filter[0];
    }
    List<Filter> out = new ArrayList<>(predicates.length);
    for (Predicate predicate : predicates) {
      Filter converted = convert(predicate, filterableColumns, maxInValues, log);
      if (converted != null) {
        out.add(converted);
      }
    }
    return out.toArray(new Filter[0]);
  }

  /**
   * Convert a single V2 predicate. Returns {@code null} on any unconvertible input — including an
   * {@code AND}/{@code OR} where any child is unconvertible (see class javadoc for rationale).
   */
  static Filter convert(
      Predicate predicate, Set<String> filterableColumns, int maxInValues, Logger log) {
    if (predicate == null) {
      return null;
    }
    String name = predicate.name();
    Expression[] children = predicate.children();

    switch (name) {
      case "IN":
        return convertIn(predicate, children, filterableColumns, maxInValues, log);
      case "=":
        return convertEqualTo(predicate, children, filterableColumns, log);
      case "IS_NULL":
        return convertIsNull(predicate, children, filterableColumns, log);
      case "AND":
        {
          if (children.length != 2
              || !(children[0] instanceof Predicate)
              || !(children[1] instanceof Predicate)) {
            logDrop(log, predicate, "AND with non-binary or non-predicate children");
            return null;
          }
          Filter left = convert((Predicate) children[0], filterableColumns, maxInValues, log);
          Filter right = convert((Predicate) children[1], filterableColumns, maxInValues, log);
          if (left == null || right == null) {
            // Never weaken: drop the whole tree when any child is unconvertible.
            logDrop(log, predicate, "AND child unconvertible");
            return null;
          }
          return new And(left, right);
        }
      case "OR":
        {
          if (children.length != 2
              || !(children[0] instanceof Predicate)
              || !(children[1] instanceof Predicate)) {
            logDrop(log, predicate, "OR with non-binary or non-predicate children");
            return null;
          }
          Filter left = convert((Predicate) children[0], filterableColumns, maxInValues, log);
          Filter right = convert((Predicate) children[1], filterableColumns, maxInValues, log);
          if (left == null || right == null) {
            // For OR, keeping just the convertible side would apply a stricter predicate than the
            // original and over-prune — drop the whole tree instead.
            logDrop(log, predicate, "OR child unconvertible");
            return null;
          }
          return new Or(left, right);
        }
      default:
        logDrop(log, predicate, "unsupported operator: " + name);
        return null;
    }
  }

  private static Filter convertIn(
      Predicate predicate,
      Expression[] children,
      Set<String> filterableColumns,
      int maxInValues,
      Logger log) {
    // Canonical IN shape: child[0] = FieldReference, child[1..] = LiteralValue
    if (children.length < 2 || !(children[0] instanceof NamedReference)) {
      logDrop(log, predicate, "IN with non-canonical shape");
      return null;
    }
    String column = fieldName((NamedReference) children[0]);
    if (column == null || !filterableColumns.contains(column)) {
      logDrop(log, predicate, "IN on non-filterable column: " + column);
      return null;
    }
    int valueCount = children.length - 1;
    if (maxInValues > 0 && valueCount > maxInValues) {
      if (log != null) {
        log.warn(
            "Dropping IN predicate with {} values for column '{}'; exceeds max.in.values cap {}.",
            valueCount,
            column,
            maxInValues);
      }
      return null;
    }
    Object[] values = new Object[valueCount];
    for (int i = 1; i < children.length; i++) {
      if (!(children[i] instanceof LiteralValue)) {
        logDrop(log, predicate, "IN child " + i + " is not a LiteralValue");
        return null;
      }
      values[i - 1] = unwrapCatalystInternal(((LiteralValue<?>) children[i]).value());
    }
    return new In(column, values);
  }

  private static Filter convertEqualTo(
      Predicate predicate, Expression[] children, Set<String> filterableColumns, Logger log) {
    if (children.length != 2) {
      logDrop(log, predicate, "= with " + children.length + " children");
      return null;
    }
    // Accept either (field, literal) or (literal, field).
    NamedReference ref = null;
    LiteralValue<?> lit = null;
    if (children[0] instanceof NamedReference && children[1] instanceof LiteralValue) {
      ref = (NamedReference) children[0];
      lit = (LiteralValue<?>) children[1];
    } else if (children[1] instanceof NamedReference && children[0] instanceof LiteralValue) {
      ref = (NamedReference) children[1];
      lit = (LiteralValue<?>) children[0];
    }
    if (ref == null) {
      logDrop(log, predicate, "= requires (field, literal)");
      return null;
    }
    String column = fieldName(ref);
    if (column == null || !filterableColumns.contains(column)) {
      logDrop(log, predicate, "= on non-filterable column: " + column);
      return null;
    }
    // Plan Phase 2 null semantics: the pruner itself returns Optional.empty for null literals,
    // so emitting EqualTo(column, null) is safe — it yields conservative no-prune behavior.
    return new EqualTo(column, unwrapCatalystInternal(lit.value()));
  }

  /**
   * Normalize Catalyst-internal literal types to the Java types that Lance's JNI layer surfaces in
   * {@link org.lance.index.scalar.ZoneStats#getMin()}/{@code getMax()}. Without this, {@code
   * ZonemapFragmentPruner.zoneMatchesComparison} calls {@code target.compareTo(min)} across
   * mismatched concrete types and trips a {@link ClassCastException} that its catch block silently
   * turns into "include every fragment" — zero pruning, no error signal.
   *
   * <p>Lance's {@code lance-jni/src/utils.rs::scalar_value_to_java} widens:
   *
   * <ul>
   *   <li>Int8/16/32/64 + UInt8/16/32/64 + Date32/Date64 → {@code Long}
   *   <li>Float16/32/64 → {@code Double}
   *   <li>Timestamp* → {@code Long}
   *   <li>String → {@code String}
   * </ul>
   *
   * <p>Spark's {@code LiteralValue.value()} returns the narrower Catalyst-internal type: {@link
   * org.apache.spark.unsafe.types.UTF8String} for StringType, {@link Integer} for {@code
   * DateType}/{@code IntegerType}, {@link Short} for {@code ShortType}, {@link Byte} for {@code
   * ByteType}, {@link Float} for {@code FloatType}. Widen to match the Lance side before the values
   * enter the V1 {@link org.apache.spark.sql.sources.Filter} that the pruner reads.
   *
   * <p>{@code Long}, {@code Double}, {@code Boolean}, and already-Java {@code String} pass through
   * unchanged.
   */
  private static Object unwrapCatalystInternal(Object value) {
    if (value instanceof org.apache.spark.unsafe.types.UTF8String) {
      return value.toString();
    }
    if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
      return ((Number) value).longValue();
    }
    if (value instanceof Float) {
      return ((Float) value).doubleValue();
    }
    return value;
  }

  private static Filter convertIsNull(
      Predicate predicate, Expression[] children, Set<String> filterableColumns, Logger log) {
    if (children.length != 1 || !(children[0] instanceof NamedReference)) {
      logDrop(log, predicate, "IS_NULL with non-canonical shape");
      return null;
    }
    String column = fieldName((NamedReference) children[0]);
    if (column == null || !filterableColumns.contains(column)) {
      logDrop(log, predicate, "IS_NULL on non-filterable column: " + column);
      return null;
    }
    return new IsNull(column);
  }

  private static String fieldName(NamedReference ref) {
    String[] parts = ref.fieldNames();
    if (parts == null || parts.length == 0) {
      return null;
    }
    // Nested references (struct.field) are reported as multi-part names; our zonemap stats keys
    // use dotted notation, so concatenate with '.'.
    if (parts.length == 1) {
      return parts[0];
    }
    return String.join(".", parts);
  }

  private static void logDrop(Logger log, Predicate predicate, String reason) {
    if (log != null && log.isDebugEnabled()) {
      log.debug("Dropped runtime predicate {}: {}", predicate.describe(), reason);
    }
  }

  /** Test-only helper: expose the converter for unit tests without instantiating the utility. */
  static Filter convertForTest(
      Predicate predicate, Set<String> filterableColumns, int maxInValues, Logger log) {
    return convert(predicate, filterableColumns, maxInValues, log);
  }

  /** Test-only helper for cross-version parity tests. */
  static Filter[] convertAllForTest(
      Predicate[] predicates, Set<String> filterableColumns, int maxInValues) {
    return convertAll(predicates, filterableColumns, maxInValues, null);
  }

  /** Test-only: expose supported operator names so a parity test can assert the subset. */
  static List<String> supportedOperators() {
    return Arrays.asList("IN", "=", "IS_NULL", "AND", "OR");
  }
}
