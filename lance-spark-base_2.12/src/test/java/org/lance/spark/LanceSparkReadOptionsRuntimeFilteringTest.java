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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 1 unit tests for the Dynamic File Pruning (DFP) runtime-filtering options declared in
 * {@link LanceSparkReadOptions}. Exercises defaults, per-scan DataSourceV2 option parsing, and
 * builder-level validation. SparkConf-fallback behavior is covered by integration tests in later
 * phases (it requires an active SparkSession).
 */
public class LanceSparkReadOptionsRuntimeFilteringTest {

  private static final String DUMMY_URI = "file:///tmp/unused.lance";

  @Test
  public void defaultsAreAppliedWhenNoOptionsProvided() {
    LanceSparkReadOptions opts = LanceSparkReadOptions.from(DUMMY_URI);

    assertTrue(opts.isRuntimeFilteringEnabled(), "runtime filtering should default to true");
    assertEquals(20, opts.getRuntimeFilteringMaxColumns());
    assertEquals(64L * 1024L * 1024L, opts.getRuntimeFilteringMaxStatsBytes());
    assertEquals(4, opts.getRuntimeFilteringLoadParallelism());
    assertEquals(5000L, opts.getRuntimeFilteringLoadTimeoutMs());
    assertEquals(10000, opts.getRuntimeFilteringMaxInValues());
  }

  @Test
  public void perScanOptionOverridesEnabled() {
    Map<String, String> options = new HashMap<>();
    options.put("path", DUMMY_URI);
    options.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_ENABLED, "false");

    LanceSparkReadOptions opts = LanceSparkReadOptions.from(options);
    assertFalse(opts.isRuntimeFilteringEnabled());
  }

  @Test
  public void perScanOptionsSetAllRuntimeFilteringFields() {
    Map<String, String> options = new HashMap<>();
    options.put("path", DUMMY_URI);
    options.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_ENABLED, "true");
    options.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_MAX_COLUMNS, "7");
    options.put(
        LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_MAX_STATS_BYTES,
        Long.toString(123L * 1024L));
    options.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_LOAD_PARALLELISM, "2");
    options.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_LOAD_TIMEOUT_MS, "250");
    options.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_MAX_IN_VALUES, "42");

    LanceSparkReadOptions opts = LanceSparkReadOptions.from(options);

    assertTrue(opts.isRuntimeFilteringEnabled());
    assertEquals(7, opts.getRuntimeFilteringMaxColumns());
    assertEquals(123L * 1024L, opts.getRuntimeFilteringMaxStatsBytes());
    assertEquals(2, opts.getRuntimeFilteringLoadParallelism());
    assertEquals(250L, opts.getRuntimeFilteringLoadTimeoutMs());
    assertEquals(42, opts.getRuntimeFilteringMaxInValues());
  }

  @Test
  public void builderRejectsNonPositiveMaxColumns() {
    // Zero is allowed (semantically: load nothing). Negative is not.
    LanceSparkReadOptions.builder().datasetUri(DUMMY_URI).runtimeFilteringMaxColumns(0).build();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LanceSparkReadOptions.builder()
                .datasetUri(DUMMY_URI)
                .runtimeFilteringMaxColumns(-1)
                .build());
  }

  @Test
  public void builderRejectsNonPositiveParallelism() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LanceSparkReadOptions.builder()
                .datasetUri(DUMMY_URI)
                .runtimeFilteringLoadParallelism(0)
                .build());
  }

  @Test
  public void builderRejectsNonPositiveTimeout() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LanceSparkReadOptions.builder()
                .datasetUri(DUMMY_URI)
                .runtimeFilteringLoadTimeoutMs(0L)
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LanceSparkReadOptions.builder()
                .datasetUri(DUMMY_URI)
                .runtimeFilteringLoadTimeoutMs(-1L)
                .build());
  }

  @Test
  public void builderRejectsNegativeMaxStatsBytes() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LanceSparkReadOptions.builder()
                .datasetUri(DUMMY_URI)
                .runtimeFilteringMaxStatsBytes(-1L)
                .build());
  }

  @Test
  public void builderRejectsNegativeMaxInValues() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LanceSparkReadOptions.builder()
                .datasetUri(DUMMY_URI)
                .runtimeFilteringMaxInValues(-5)
                .build());
  }

  @Test
  public void withVersionPropagatesRuntimeFilteringFields() {
    LanceSparkReadOptions original =
        LanceSparkReadOptions.builder()
            .datasetUri(DUMMY_URI)
            .runtimeFilteringEnabled(false)
            .runtimeFilteringMaxColumns(5)
            .runtimeFilteringMaxStatsBytes(2048L)
            .runtimeFilteringLoadParallelism(1)
            .runtimeFilteringLoadTimeoutMs(100L)
            .runtimeFilteringMaxInValues(25)
            .build();

    LanceSparkReadOptions versioned = original.withVersion(42);

    assertFalse(versioned.isRuntimeFilteringEnabled());
    assertEquals(5, versioned.getRuntimeFilteringMaxColumns());
    assertEquals(2048L, versioned.getRuntimeFilteringMaxStatsBytes());
    assertEquals(1, versioned.getRuntimeFilteringLoadParallelism());
    assertEquals(100L, versioned.getRuntimeFilteringLoadTimeoutMs());
    assertEquals(25, versioned.getRuntimeFilteringMaxInValues());
    assertEquals(42, versioned.getVersion());
  }

  @Test
  public void equalsHashCodeReflectRuntimeFilteringFields() {
    LanceSparkReadOptions a =
        LanceSparkReadOptions.builder().datasetUri(DUMMY_URI).runtimeFilteringMaxColumns(5).build();
    LanceSparkReadOptions b =
        LanceSparkReadOptions.builder().datasetUri(DUMMY_URI).runtimeFilteringMaxColumns(5).build();
    LanceSparkReadOptions c =
        LanceSparkReadOptions.builder().datasetUri(DUMMY_URI).runtimeFilteringMaxColumns(6).build();

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertFalse(a.equals(c));
  }

  @Test
  public void invalidNumericOptionFallsBackToDefaultWithWarn() {
    Map<String, String> options = new HashMap<>();
    options.put("path", DUMMY_URI);
    options.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_MAX_COLUMNS, "not-an-integer");
    // DFP tuning knobs are optional — a typo WARNs and silently falls back to the coded default
    // rather than killing the query. Range violations (e.g. negative values) still fail loudly
    // via Preconditions.checkArgument in the setter; only parse-level malformation is forgiven.
    LanceSparkReadOptions opts = LanceSparkReadOptions.from(options);
    assertEquals(20, opts.getRuntimeFilteringMaxColumns());
  }

  @Test
  public void rangeViolationStillFailsLoudly() {
    Map<String, String> options = new HashMap<>();
    options.put("path", DUMMY_URI);
    options.put(LanceSparkReadOptions.CONFIG_RUNTIME_FILTERING_MAX_COLUMNS, "-5");
    // Parses successfully but the setter's Preconditions.checkArgument(>= 0) rejects it.
    // This distinguishes "user typo'd a number" from "user explicitly picked an invalid value".
    assertThrows(IllegalArgumentException.class, () -> LanceSparkReadOptions.from(options));
  }
}
