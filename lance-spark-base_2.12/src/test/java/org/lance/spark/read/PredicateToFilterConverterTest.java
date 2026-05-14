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
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.AlwaysTrue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link PredicateToFilterConverter}. Exercises the pinned subset that Spark's
 * {@code PartitionPruning} rule emits for runtime filtering, plus the drop-whole-tree behavior for
 * unconvertible operators and for columns outside the filterable set.
 */
public class PredicateToFilterConverterTest {

  private static final Set<String> FILTERABLE =
      new HashSet<>(Arrays.asList("x", "y", "b", "nested.leaf"));

  // --- simple shapes ---

  @Test
  public void convertsInWithLiterals() {
    Predicate p = inPredicate("x", 1L, 2L, 3L);
    Filter f = PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null);
    In in = assertInstanceOf(In.class, f);
    assertEquals("x", in.attribute());
    assertEquals(Arrays.asList(1L, 2L, 3L), Arrays.asList(in.values()));
  }

  @Test
  public void convertsEqualToFieldFirst() {
    Predicate p = new Predicate("=", new Expression[] {ref("y"), lit(42L)});
    Filter f = PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null);
    EqualTo eq = assertInstanceOf(EqualTo.class, f);
    assertEquals("y", eq.attribute());
    assertEquals(42L, eq.value());
  }

  @Test
  public void convertsEqualToLiteralFirst() {
    Predicate p = new Predicate("=", new Expression[] {lit(42L), ref("y")});
    EqualTo eq =
        assertInstanceOf(
            EqualTo.class, PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null));
    assertEquals("y", eq.attribute());
    assertEquals(42L, eq.value());
  }

  @Test
  public void convertsIsNull() {
    Predicate p = new Predicate("IS_NULL", new Expression[] {ref("b")});
    IsNull isNull =
        assertInstanceOf(
            IsNull.class, PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null));
    assertEquals("b", isNull.attribute());
  }

  // --- nested logical operators ---

  @Test
  public void convertsAndOfConvertibleChildren() {
    Predicate p =
        new Predicate(
            "AND",
            new Expression[] {
              new Predicate("=", new Expression[] {ref("x"), lit(1L)}), inPredicate("y", 5L, 6L)
            });
    Filter f = PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null);
    And and = assertInstanceOf(And.class, f);
    assertInstanceOf(EqualTo.class, and.left());
    assertInstanceOf(In.class, and.right());
  }

  @Test
  public void convertsOrOfConvertibleChildren() {
    Predicate p =
        new Predicate(
            "OR",
            new Expression[] {
              new Predicate("=", new Expression[] {ref("x"), lit(1L)}),
              new Predicate("=", new Expression[] {ref("y"), lit(2L)})
            });
    Or or =
        assertInstanceOf(
            Or.class, PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null));
    assertNotNull(or.left());
    assertNotNull(or.right());
  }

  @Test
  public void dropsAndWhenOneChildIsUnconvertible() {
    Predicate p =
        new Predicate(
            "AND",
            new Expression[] {
              new Predicate("=", new Expression[] {ref("x"), lit(1L)}),
              new Predicate(">", new Expression[] {ref("y"), lit(10L)}) // unsupported op
            });
    assertNull(PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null));
  }

  @Test
  public void dropsOrWhenOneChildIsUnconvertible() {
    Predicate p =
        new Predicate(
            "OR",
            new Expression[] {
              new Predicate("=", new Expression[] {ref("x"), lit(1L)}),
              new Predicate(">", new Expression[] {ref("y"), lit(10L)})
            });
    // Dropping the whole tree prevents over-pruning (keeping only the convertible side would be
    // strictly narrower than the original OR).
    assertNull(PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null));
  }

  // --- filterable-column gating ---

  @Test
  public void dropsPredicateOnUnfilterableColumn() {
    Predicate p = new Predicate("=", new Expression[] {ref("unrelated"), lit(1L)});
    assertNull(PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null));
  }

  @Test
  public void dropsInOnUnfilterableColumn() {
    Predicate p = inPredicate("unrelated", 1L, 2L);
    assertNull(PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null));
  }

  // --- max.in.values cap ---

  @Test
  public void dropsInPredicateExceedingMaxInValues() {
    Predicate p = inPredicate("x", 1L, 2L, 3L);
    // cap=2 means 3 values exceeds the cap.
    assertNull(PredicateToFilterConverter.convertForTest(p, FILTERABLE, 2, null));
  }

  @Test
  public void zeroMaxInValuesDisablesCap() {
    Predicate p = inPredicate("x", 1L, 2L, 3L, 4L, 5L);
    Filter f = PredicateToFilterConverter.convertForTest(p, FILTERABLE, 0, null);
    assertInstanceOf(In.class, f);
  }

  // --- unsupported shapes ---

  @Test
  public void dropsUnsupportedOperator() {
    Predicate p = new Predicate(">", new Expression[] {ref("x"), lit(1L)});
    assertNull(PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null));
  }

  @Test
  public void dropsAlwaysTrue() {
    // AlwaysTrue is technically a Predicate but not in our supported set.
    assertNull(
        PredicateToFilterConverter.convertForTest(new AlwaysTrue(), FILTERABLE, 10_000, null));
  }

  @Test
  public void dropsEqualToWithNonLiteralRhs() {
    // Both children are FieldReferences — not a literal on either side.
    Predicate p = new Predicate("=", new Expression[] {ref("x"), ref("y")});
    assertNull(PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null));
  }

  // --- null-literal handling ---

  @Test
  public void equalToNullLiteralIsConverted() {
    // Pruner's analyzeComparison returns Optional.empty on null, so a no-op EqualTo(col, null)
    // flows through without incorrect pruning.
    Predicate p = new Predicate("=", new Expression[] {ref("x"), lit(null)});
    EqualTo eq =
        assertInstanceOf(
            EqualTo.class, PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null));
    assertEquals("x", eq.attribute());
    assertNull(eq.value());
  }

  @Test
  public void inWithNullAmongLiteralsIsConverted() {
    Predicate p = inPredicate("x", 1L, null, 3L);
    In in =
        assertInstanceOf(
            In.class, PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null));
    assertEquals(3, in.values().length);
  }

  // --- empty / null input ---

  @Test
  public void convertAllHandlesEmptyArray() {
    assertEquals(
        0,
        PredicateToFilterConverter.convertAll(new Predicate[0], FILTERABLE, 10_000, null).length);
  }

  @Test
  public void convertAllHandlesNullInput() {
    assertEquals(0, PredicateToFilterConverter.convertAll(null, FILTERABLE, 10_000, null).length);
  }

  @Test
  public void convertAllDropsMixedUnconvertibleEntries() {
    Predicate[] predicates =
        new Predicate[] {
          new Predicate("=", new Expression[] {ref("x"), lit(1L)}), // keeper
          new Predicate(">", new Expression[] {ref("y"), lit(2L)}), // dropped
          new Predicate("IS_NULL", new Expression[] {ref("b")}) // keeper
        };
    Filter[] result = PredicateToFilterConverter.convertAll(predicates, FILTERABLE, 10_000, null);
    assertEquals(2, result.length);
  }

  // --- nested field names ---

  @Test
  public void convertsNestedFieldReference() {
    Predicate p =
        new Predicate("=", new Expression[] {FieldReference.apply("nested.leaf"), lit(1L)});
    EqualTo eq =
        assertInstanceOf(
            EqualTo.class, PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null));
    assertEquals("nested.leaf", eq.attribute());
  }

  // --- empty filterable set ---

  @Test
  public void emptyFilterableSetDropsEverything() {
    Predicate p = new Predicate("=", new Expression[] {ref("x"), lit(1L)});
    assertNull(PredicateToFilterConverter.convertForTest(p, Collections.emptySet(), 10_000, null));
  }

  // --- UTF8String unwrap for string literal runtime filters ---

  @Test
  public void inWithUtf8StringValuesUnwrapsToJavaString() {
    // Regression: Spark's V2 LiteralValue for StringType columns carries UTF8String as its raw
    // value. Without unwrapping, downstream ZonemapFragmentPruner calls UTF8String.compareTo on a
    // Java String min/max from ZoneStats, hits ClassCastException, and silently includes every
    // fragment — zero pruning for string-keyed runtime filters, the most common DFP use case.
    org.apache.spark.unsafe.types.UTF8String eastU8 =
        org.apache.spark.unsafe.types.UTF8String.fromString("east");
    org.apache.spark.unsafe.types.UTF8String westU8 =
        org.apache.spark.unsafe.types.UTF8String.fromString("west");
    Expression[] children =
        new Expression[] {
          ref("x"),
          new LiteralValue<>(eastU8, DataTypes.StringType),
          new LiteralValue<>(westU8, DataTypes.StringType)
        };
    Predicate p = new Predicate("IN", children);
    Filter result = PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null);
    assertInstanceOf(In.class, result);
    Object[] values = ((In) result).values();
    assertEquals(2, values.length);
    assertInstanceOf(String.class, values[0]);
    assertInstanceOf(String.class, values[1]);
    assertEquals("east", values[0]);
    assertEquals("west", values[1]);
  }

  @Test
  public void equalToWithUtf8StringUnwrapsToJavaString() {
    org.apache.spark.unsafe.types.UTF8String eastU8 =
        org.apache.spark.unsafe.types.UTF8String.fromString("east");
    Predicate p =
        new Predicate(
            "=", new Expression[] {ref("x"), new LiteralValue<>(eastU8, DataTypes.StringType)});
    Filter result = PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null);
    assertInstanceOf(EqualTo.class, result);
    Object value = ((EqualTo) result).value();
    assertInstanceOf(String.class, value);
    assertEquals("east", value);
  }

  // --- numeric widening: Spark sends Integer/Short/Byte/Float, Lance stores Long/Double ---

  @Test
  public void inWithDateLiteralWidensIntegerToLong() {
    // DateType uses PhysicalIntegerType (days since epoch as Integer) on Spark; Lance ZoneStats
    // stores Date32 as Long. Without widening, Integer.compareTo(Long) throws CCE in the pruner
    // and every fragment gets conservatively included — zero DFP pruning for date-keyed joins.
    Expression[] children =
        new Expression[] {
          ref("x"),
          new LiteralValue<>(18628, DataTypes.DateType),
          new LiteralValue<>(18629, DataTypes.DateType)
        };
    Predicate p = new Predicate("IN", children);
    Filter result = PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null);
    Object[] values = ((In) result).values();
    assertInstanceOf(Long.class, values[0]);
    assertInstanceOf(Long.class, values[1]);
    assertEquals(18628L, values[0]);
    assertEquals(18629L, values[1]);
  }

  @Test
  public void equalToWithShortAndByteWidenToLong() {
    Predicate shortP =
        new Predicate(
            "=", new Expression[] {ref("x"), new LiteralValue<>((short) 42, DataTypes.ShortType)});
    EqualTo shortEq =
        assertInstanceOf(
            EqualTo.class,
            PredicateToFilterConverter.convertForTest(shortP, FILTERABLE, 10_000, null));
    assertInstanceOf(Long.class, shortEq.value());
    assertEquals(42L, shortEq.value());

    Predicate byteP =
        new Predicate(
            "=", new Expression[] {ref("x"), new LiteralValue<>((byte) 7, DataTypes.ByteType)});
    EqualTo byteEq =
        assertInstanceOf(
            EqualTo.class,
            PredicateToFilterConverter.convertForTest(byteP, FILTERABLE, 10_000, null));
    assertInstanceOf(Long.class, byteEq.value());
    assertEquals(7L, byteEq.value());
  }

  @Test
  public void equalToWithFloatWidensToDouble() {
    Predicate p =
        new Predicate(
            "=", new Expression[] {ref("x"), new LiteralValue<>(3.14f, DataTypes.FloatType)});
    EqualTo eq =
        assertInstanceOf(
            EqualTo.class, PredicateToFilterConverter.convertForTest(p, FILTERABLE, 10_000, null));
    assertInstanceOf(Double.class, eq.value());
    assertEquals((double) 3.14f, eq.value());
  }

  @Test
  public void longAndDoublePassThroughUnchanged() {
    // Pre-widened types must not be re-boxed.
    Predicate longP = new Predicate("=", new Expression[] {ref("x"), lit(100L)});
    EqualTo longEq =
        assertInstanceOf(
            EqualTo.class,
            PredicateToFilterConverter.convertForTest(longP, FILTERABLE, 10_000, null));
    assertInstanceOf(Long.class, longEq.value());
    assertEquals(100L, longEq.value());

    Predicate doubleP =
        new Predicate(
            "=", new Expression[] {ref("x"), new LiteralValue<>(2.5, DataTypes.DoubleType)});
    EqualTo doubleEq =
        assertInstanceOf(
            EqualTo.class,
            PredicateToFilterConverter.convertForTest(doubleP, FILTERABLE, 10_000, null));
    assertInstanceOf(Double.class, doubleEq.value());
    assertEquals(2.5, doubleEq.value());
  }

  // --- helpers ---

  private static NamedReference ref(String name) {
    return FieldReference.apply(name);
  }

  private static LiteralValue<Object> lit(Object value) {
    return new LiteralValue<>(value, DataTypes.LongType);
  }

  private static Predicate inPredicate(String column, Object... values) {
    Expression[] children = new Expression[values.length + 1];
    children[0] = ref(column);
    for (int i = 0; i < values.length; i++) {
      children[i + 1] = lit(values[i]);
    }
    return new Predicate("IN", children);
  }
}
