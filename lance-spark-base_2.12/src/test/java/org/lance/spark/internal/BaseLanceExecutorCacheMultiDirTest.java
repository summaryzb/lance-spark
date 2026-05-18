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
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Abstract base test for {@link LanceExecutorCache}'s multi-disk support: hash-based fragment
 * placement, per-disk LRU eviction, and read-after-write correctness across disks. Concrete
 * subclasses live in versioned modules per the project's "Base*" test-layout convention.
 */
public abstract class BaseLanceExecutorCacheMultiDirTest {

  @TempDir Path tmpRoot;

  private BufferAllocator allocator;
  private List<VectorSchemaRoot> rootsToClose;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    rootsToClose = new ArrayList<>();
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

  private int expectedBucket(LanceExecutorCacheKey key, int n) {
    if (n == 1) return 0;
    return Math.floorMod(key.fingerprint().hashCode(), n);
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

  @Test
  void fragmentsAreDistributedAcrossDisksByHash() throws Exception {
    Path[] dirs = makeDirs(3);
    LanceExecutorCache cache = new LanceExecutorCache(dirs, Long.MAX_VALUE);

    int n = 60;
    int[] perDiskCount = new int[3];
    for (int frag = 0; frag < n; frag++) {
      LanceExecutorCacheKey key = keyFor(frag);
      try (ArrowReader r =
          cache.getOrLoadColumns(key, Arrays.asList("v"), allocator, singleBatchLoader(8))) {
        assertTrue(r.loadNextBatch());
      }
      int bucket = expectedBucket(key, 3);
      perDiskCount[bucket]++;
      Path expectedFile = dirs[bucket].resolve(key.fingerprint()).resolve("v.arrow");
      assertTrue(Files.exists(expectedFile), "missing on bucket " + bucket + ": " + expectedFile);
      for (int j = 0; j < 3; j++) {
        if (j == bucket) continue;
        Path otherFile = dirs[j].resolve(key.fingerprint()).resolve("v.arrow");
        assertFalse(Files.exists(otherFile), "leaked to bucket " + j + ": " + otherFile);
      }
    }
    for (int i = 0; i < 3; i++) {
      assertTrue(
          perDiskCount[i] >= 5, "disk " + i + " count too low: " + Arrays.toString(perDiskCount));
    }
    assertEquals(n, cache.entryCount());
  }

  @Test
  void readReturnsCorrectDataFromAssignedDisk() throws Exception {
    Path[] dirs = makeDirs(2);
    LanceExecutorCache cache = new LanceExecutorCache(dirs, Long.MAX_VALUE);

    LanceExecutorCacheKey key = keyFor(7);
    try (ArrowReader r =
        cache.getOrLoadColumns(key, Arrays.asList("v"), allocator, singleBatchLoader(3))) {
      assertTrue(r.loadNextBatch());
    }

    Function<List<String>, ArrowReader> loaderThatThrows =
        missCols -> {
          throw new AssertionError("loader must not be invoked on a full cache hit");
        };
    try (ArrowReader r =
        cache.getOrLoadColumns(key, Arrays.asList("v"), allocator, loaderThatThrows)) {
      assertTrue(r.loadNextBatch());
      VectorSchemaRoot out = r.getVectorSchemaRoot();
      assertEquals(3, out.getRowCount());
      IntVector v = (IntVector) out.getVector("v");
      assertEquals(1, v.get(0));
      assertEquals(2, v.get(1));
      assertEquals(3, v.get(2));
    }

    int bucket = expectedBucket(key, 2);
    assertTrue(Files.exists(dirs[bucket].resolve(key.fingerprint())));
    int other = bucket == 0 ? 1 : 0;
    assertFalse(Files.exists(dirs[other].resolve(key.fingerprint())));
  }

  @Test
  void evictionIsIsolatedPerDisk() throws Exception {
    Path[] dirs = makeDirs(3);
    // tiny per-disk limit forces eviction down to 1 entry per disk (lru.size() > 1 guard).
    LanceExecutorCache cache = new LanceExecutorCache(dirs, 64L);

    int n = 30;
    long evictionsBefore = cache.evictions();
    for (int frag = 0; frag < n; frag++) {
      LanceExecutorCacheKey key = keyFor(frag);
      try (ArrowReader r =
          cache.getOrLoadColumns(key, Arrays.asList("v"), allocator, singleBatchLoader(64))) {
        assertTrue(r.loadNextBatch());
      }
    }

    int total = cache.entryCount();
    assertTrue(total <= 3, "expected entryCount<=3 after tight per-disk eviction, got " + total);
    long evictions = cache.evictions() - evictionsBefore;
    assertTrue(evictions >= n - 3, "expected >= " + (n - 3) + " evictions, got " + evictions);
  }
}
