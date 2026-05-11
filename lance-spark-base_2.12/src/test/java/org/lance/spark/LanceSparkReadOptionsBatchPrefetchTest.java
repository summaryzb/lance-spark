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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Parsing-level coverage for the {@code batch_prefetch_queue_depth} option. The option is a new
 * public knob on {@link LanceSparkReadOptions}; missing, valid, out-of-range, and non-integer
 * values all need explicit checks so future refactors of the parse chain don't silently drop
 * support.
 */
public class LanceSparkReadOptionsBatchPrefetchTest {

  private static Map<String, String> base() {
    Map<String, String> opts = new HashMap<>();
    opts.put(LanceSparkReadOptions.CONFIG_DATASET_URI, "s3://bucket/path");
    return opts;
  }

  @Test
  public void missingKeyFallsBackToDefaultZero() {
    LanceSparkReadOptions options = LanceSparkReadOptions.from(base());
    assertEquals(
        0,
        options.getBatchPrefetchQueueDepth(),
        "missing batch_prefetch_queue_depth must leave the default 0");
  }

  @Test
  public void validPositiveIntegerParsesThrough() {
    Map<String, String> opts = base();
    opts.put(LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH, "4");
    LanceSparkReadOptions options = LanceSparkReadOptions.from(opts);
    assertEquals(4, options.getBatchPrefetchQueueDepth());
  }

  @Test
  public void zeroIsAcceptedAsExplicitDisable() {
    Map<String, String> opts = base();
    opts.put(LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH, "0");
    LanceSparkReadOptions options = LanceSparkReadOptions.from(opts);
    assertEquals(0, options.getBatchPrefetchQueueDepth());
  }

  @Test
  public void atMaximumAccepted() {
    Map<String, String> opts = base();
    opts.put(
        LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH,
        Integer.toString(LanceSparkReadOptions.MAX_BATCH_PREFETCH_QUEUE_DEPTH));
    LanceSparkReadOptions options = LanceSparkReadOptions.from(opts);
    assertEquals(
        LanceSparkReadOptions.MAX_BATCH_PREFETCH_QUEUE_DEPTH, options.getBatchPrefetchQueueDepth());
  }

  @Test
  public void negativeValueRejected() {
    Map<String, String> opts = base();
    opts.put(LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH, "-1");
    assertThrows(
        IllegalArgumentException.class,
        () -> LanceSparkReadOptions.from(opts),
        "negative batch_prefetch_queue_depth must throw IllegalArgumentException");
  }

  @Test
  public void aboveMaximumRejected() {
    Map<String, String> opts = base();
    opts.put(
        LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH,
        Integer.toString(LanceSparkReadOptions.MAX_BATCH_PREFETCH_QUEUE_DEPTH + 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> LanceSparkReadOptions.from(opts),
        "batch_prefetch_queue_depth above MAX must be rejected to prevent OOME on queue alloc");
  }

  @Test
  public void absurdIntegerMaxValueRejected() {
    Map<String, String> opts = base();
    opts.put(
        LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH,
        Integer.toString(Integer.MAX_VALUE));
    assertThrows(
        IllegalArgumentException.class,
        () -> LanceSparkReadOptions.from(opts),
        "Integer.MAX_VALUE must be rejected; ArrayBlockingQueue cannot allocate a 2-billion-"
            + "element backing array");
  }

  @Test
  public void nonIntegerRejected() {
    Map<String, String> opts = base();
    opts.put(LanceSparkReadOptions.CONFIG_BATCH_PREFETCH_QUEUE_DEPTH, "not-a-number");
    assertThrows(
        NumberFormatException.class,
        () -> LanceSparkReadOptions.from(opts),
        "non-integer batch_prefetch_queue_depth must surface NumberFormatException from parse");
  }

  @Test
  public void builderRejectsNegative() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LanceSparkReadOptions.builder()
                .datasetUri("s3://bucket/path")
                .batchPrefetchQueueDepth(-1)
                .build(),
        "Builder.batchPrefetchQueueDepth(<0) must throw — parser MAX check must not be bypassable");
  }

  @Test
  public void builderRejectsAboveMaximum() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LanceSparkReadOptions.builder()
                .datasetUri("s3://bucket/path")
                .batchPrefetchQueueDepth(LanceSparkReadOptions.MAX_BATCH_PREFETCH_QUEUE_DEPTH + 1)
                .build(),
        "Builder must enforce MAX_BATCH_PREFETCH_QUEUE_DEPTH to prevent OOME on queue alloc");
  }

  @Test
  public void builderAcceptsAtMaximum() {
    LanceSparkReadOptions options =
        LanceSparkReadOptions.builder()
            .datasetUri("s3://bucket/path")
            .batchPrefetchQueueDepth(LanceSparkReadOptions.MAX_BATCH_PREFETCH_QUEUE_DEPTH)
            .build();
    assertEquals(
        LanceSparkReadOptions.MAX_BATCH_PREFETCH_QUEUE_DEPTH, options.getBatchPrefetchQueueDepth());
  }
}
