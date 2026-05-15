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

import org.lance.spark.write.SingleBatchArrowReader;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Abstract base test for {@link LanceExecutorCache}'s concurrency contract: lock-free fast path,
 * eviction self-heal, restart hydration, and bounded {@code keyLocks} growth. Concrete subclasses
 * live in versioned modules per the project's "Base*" test-layout convention.
 */
public abstract class BaseLanceExecutorCacheConcurrencyTest {

  @TempDir Path tmpRoot;

  private BufferAllocator allocator;
  private List<VectorSchemaRoot> rootsToClose;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    rootsToClose = Collections.synchronizedList(new ArrayList<>());
  }

  @AfterEach
  void tearDown() {
    for (VectorSchemaRoot r : rootsToClose) {
      try {
        r.close();
      } catch (Exception ignored) {
        // best-effort cleanup
      }
    }
    allocator.close();
  }

  private Path[] makeDirs(int n) throws IOException {
    Path[] dirs = new Path[n];
    for (int i = 0; i < n; i++) {
      dirs[i] = tmpRoot.resolve("disk" + i);
      Files.createDirectories(dirs[i]);
    }
    return dirs;
  }

  private LanceExecutorCacheKey keyFor(int fragmentId) {
    return new LanceExecutorCacheKey(
        "memory://test", 1, fragmentId, 1024, Collections.<String, String>emptyMap());
  }

  /** Returns a column loader that emits a single Arrow batch containing each requested column. */
  private Function<List<String>, ArrowReader> singleBatchLoader(int payloadInts) {
    return missCols -> {
      List<Field> fields = new ArrayList<>(missCols.size());
      for (String c : missCols) {
        fields.add(new Field(c, FieldType.nullable(Types.MinorType.INT.getType()), null));
      }
      Schema schema = new Schema(fields);
      VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
      rootsToClose.add(root);
      for (String c : missCols) {
        IntVector v = (IntVector) root.getVector(c);
        v.allocateNew(payloadInts);
        for (int i = 0; i < payloadInts; i++) {
          v.set(i, i + 1);
        }
        v.setValueCount(payloadInts);
      }
      root.setRowCount(payloadInts);
      return new SingleBatchArrowReader(allocator, root);
    };
  }

  /**
   * After pre-warming a single fragment, many concurrent readers should all hit the lock-free fast
   * path: the loader (which throws on invocation) must never run, and the {@code hits} counter must
   * increase by exactly the total invocation count.
   */
  @Test
  void concurrentFullHitDoesNotInvokeLoader() throws Exception {
    Path[] dirs = makeDirs(1);
    LanceExecutorCache cache = new LanceExecutorCache(dirs, Long.MAX_VALUE);

    LanceExecutorCacheKey key = keyFor(42);
    try (ArrowReader r =
        cache.getOrLoadColumns(key, Arrays.asList("v"), allocator, singleBatchLoader(8))) {
      assertTrue(r.loadNextBatch());
    }
    long hitsBefore = cache.hits();

    Function<List<String>, ArrowReader> loaderThatThrows =
        missCols -> {
          throw new AssertionError("loader must not be invoked on full hit; missCols=" + missCols);
        };

    int threads = 16;
    int perThread = 100;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    List<Future<?>> futures = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      futures.add(
          pool.submit(
              () -> {
                try {
                  start.await();
                  for (int i = 0; i < perThread; i++) {
                    try (ArrowReader r =
                        cache.getOrLoadColumns(
                            key, Arrays.asList("v"), allocator, loaderThatThrows)) {
                      assertTrue(r.loadNextBatch());
                    }
                  }
                } catch (Throwable e) {
                  firstFailure.compareAndSet(null, e);
                }
              }));
    }
    start.countDown();
    for (Future<?> f : futures) {
      f.get();
    }
    pool.shutdown();
    assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

    if (firstFailure.get() != null) {
      throw new AssertionError("worker thread failed", firstFailure.get());
    }
    assertEquals(hitsBefore + (long) threads * perThread, cache.hits());
  }

  /**
   * When part of the projection is cached and part is not, concurrent callers must converge: the
   * loader runs exactly once (under the per-fragment lock), {@code partialHits} increments, and the
   * lock-free index ends up containing every requested column.
   */
  @Test
  void partialHitFallsToSlowPath() throws Exception {
    Path[] dirs = makeDirs(1);
    LanceExecutorCache cache = new LanceExecutorCache(dirs, Long.MAX_VALUE);

    LanceExecutorCacheKey key = keyFor(7);
    try (ArrowReader r =
        cache.getOrLoadColumns(key, Arrays.asList("a", "b"), allocator, singleBatchLoader(4))) {
      assertTrue(r.loadNextBatch());
    }
    long partialBefore = cache.partialHits();

    AtomicInteger loaderInvocations = new AtomicInteger();
    Function<List<String>, ArrowReader> trackedLoader =
        missCols -> {
          loaderInvocations.incrementAndGet();
          return singleBatchLoader(4).apply(missCols);
        };

    int threads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    List<Future<?>> futures = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      futures.add(
          pool.submit(
              () -> {
                try {
                  start.await();
                  try (ArrowReader r =
                      cache.getOrLoadColumns(
                          key, Arrays.asList("a", "b", "c"), allocator, trackedLoader)) {
                    assertTrue(r.loadNextBatch());
                  }
                } catch (Throwable e) {
                  firstFailure.compareAndSet(null, e);
                }
              }));
    }
    start.countDown();
    for (Future<?> f : futures) {
      f.get();
    }
    pool.shutdown();
    assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

    if (firstFailure.get() != null) {
      throw new AssertionError("worker thread failed", firstFailure.get());
    }
    assertEquals(1, loaderInvocations.get(), "loader should run exactly once for col c");
    assertTrue(
        cache.partialHits() >= partialBefore + 1,
        "expected at least one partialHit increment, got " + (cache.partialHits() - partialBefore));
    Set<String> known = cache.presentColsForTest(key.fingerprint());
    assertTrue(
        known.containsAll(Arrays.asList("a", "b", "c")),
        "presentCols should include all written columns; got " + known);
  }

  /**
   * When a column file vanishes between fast-path probe and open (eviction race or external
   * cleanup), the fast path must self-heal: invalidate its index entry, drop into the slow path,
   * re-fetch the missing column, and serve correct data.
   */
  @Test
  void evictionRemovesColumnFileFastPathRecovers() throws Exception {
    Path[] dirs = makeDirs(1);
    LanceExecutorCache cache = new LanceExecutorCache(dirs, Long.MAX_VALUE);

    LanceExecutorCacheKey key = keyFor(11);
    try (ArrowReader r =
        cache.getOrLoadColumns(key, Arrays.asList("v"), allocator, singleBatchLoader(5))) {
      assertTrue(r.loadNextBatch());
    }
    Path victim = dirs[0].resolve(key.fingerprint()).resolve("v.arrow");
    assertTrue(Files.exists(victim));
    Files.delete(victim);

    AtomicInteger loaderInvocations = new AtomicInteger();
    Function<List<String>, ArrowReader> trackedLoader =
        missCols -> {
          loaderInvocations.incrementAndGet();
          return singleBatchLoader(5).apply(missCols);
        };

    try (ArrowReader r =
        cache.getOrLoadColumns(key, Arrays.asList("v"), allocator, trackedLoader)) {
      assertTrue(r.loadNextBatch());
      VectorSchemaRoot out = r.getVectorSchemaRoot();
      assertEquals(5, out.getRowCount());
      IntVector iv = (IntVector) out.getVector("v");
      assertEquals(1, iv.get(0));
      assertEquals(5, iv.get(4));
    }
    assertEquals(1, loaderInvocations.get(), "loader must re-fetch the deleted column");
    assertTrue(Files.exists(victim), "column file should be re-cached");
  }

  /**
   * A brand-new {@link LanceExecutorCache} instance pointing at populated cache directories must
   * behave as if the entries were already loaded: {@code rebuildIndex} should re-populate both the
   * LRU and the lock-free presentCols index, so subsequent reads do not invoke the loader.
   */
  @Test
  void restartedCacheServesFromDiskWithoutLoader() throws Exception {
    Path[] dirs = makeDirs(1);
    LanceExecutorCache cacheA = new LanceExecutorCache(dirs, Long.MAX_VALUE);

    int n = 10;
    for (int i = 0; i < n; i++) {
      LanceExecutorCacheKey key = keyFor(i);
      try (ArrowReader r =
          cacheA.getOrLoadColumns(key, Arrays.asList("v"), allocator, singleBatchLoader(3))) {
        assertTrue(r.loadNextBatch());
      }
    }

    // Simulate JVM restart: drop cacheA, build cacheB pointing at the same dirs.
    LanceExecutorCache cacheB = new LanceExecutorCache(dirs, Long.MAX_VALUE);
    assertEquals(n, cacheB.entryCount(), "rebuildIndex must re-populate LRU");
    assertEquals(
        n,
        cacheB.presentColsSizeForTest(),
        "rebuildIndex must hydrate presentCols for every fragment dir");
    for (int i = 0; i < n; i++) {
      Set<String> known = cacheB.presentColsForTest(keyFor(i).fingerprint());
      assertTrue(known.contains("v"), "presentCols missing column 'v' for frag " + i);
    }

    Function<List<String>, ArrowReader> loaderThatThrows =
        missCols -> {
          throw new AssertionError(
              "loader must not be invoked after restart for cached fragments; missCols="
                  + missCols);
        };
    long hitsBefore = cacheB.hits();
    for (int i = 0; i < n; i++) {
      LanceExecutorCacheKey key = keyFor(i);
      try (ArrowReader r =
          cacheB.getOrLoadColumns(key, Arrays.asList("v"), allocator, loaderThatThrows)) {
        assertTrue(r.loadNextBatch());
      }
    }
    assertEquals(hitsBefore + n, cacheB.hits());
  }

  /**
   * After enough writes to trigger eviction down to a small live set, the lock-free index and the
   * per-fragment lock map must mirror the LRU exactly: no stale presentCols entries pointing to
   * deleted fragment dirs, no unbounded keyLocks growth.
   */
  @Test
  void evictionClearsPresentColsAndKeyLocks() throws Exception {
    Path[] dirs = makeDirs(1);
    // tight per-disk limit forces eviction down to ~1 entry (the lru.size() > 1 guard).
    LanceExecutorCache cache = new LanceExecutorCache(dirs, 64L);

    int n = 30;
    for (int i = 0; i < n; i++) {
      LanceExecutorCacheKey key = keyFor(i);
      try (ArrowReader r =
          cache.getOrLoadColumns(key, Arrays.asList("v"), allocator, singleBatchLoader(64))) {
        assertTrue(r.loadNextBatch());
      }
    }

    int entries = cache.entryCount();
    assertTrue(
        entries < n, "tight limit should force eviction below " + n + " entries; got " + entries);
    assertEquals(
        entries,
        cache.presentColsSizeForTest(),
        "presentCols must equal entryCount after eviction");
    assertTrue(
        cache.keyLocksSizeForTest() <= entries,
        "keyLocks must not exceed live entries; got "
            + cache.keyLocksSizeForTest()
            + " for "
            + entries
            + " entries");
    assertNotNull(cache.getCacheDirs());
  }
}
