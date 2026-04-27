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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public abstract class BaseLanceManifestCacheTest {

  @BeforeEach
  public void clearCache() {
    LanceManifestCache.invalidateAll();
  }

  @Test
  public void testPutThenGetHit() {
    byte[] bytes = new byte[] {1, 2, 3};
    LanceManifestCache.put("s3://bucket/t1", 5L, bytes);
    assertArrayEquals(bytes, LanceManifestCache.getIfPresent("s3://bucket/t1", 5L));
  }

  @Test
  public void testDifferentKeysAreIsolated() {
    LanceManifestCache.put("uri-a", 1L, new byte[] {1});
    LanceManifestCache.put("uri-b", 1L, new byte[] {2});
    LanceManifestCache.put("uri-a", 2L, new byte[] {3});
    assertArrayEquals(new byte[] {1}, LanceManifestCache.getIfPresent("uri-a", 1L));
    assertArrayEquals(new byte[] {2}, LanceManifestCache.getIfPresent("uri-b", 1L));
    assertArrayEquals(new byte[] {3}, LanceManifestCache.getIfPresent("uri-a", 2L));
  }

  @Test
  public void testKeyEqualsAndHashCode() {
    LanceManifestCache.Key a = new LanceManifestCache.Key("uri", 7L);
    LanceManifestCache.Key b = new LanceManifestCache.Key("uri", 7L);
    LanceManifestCache.Key c = new LanceManifestCache.Key("uri", 8L);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
  }

  @Test
  public void testInvalidateAllClears() {
    LanceManifestCache.put("uri", 1L, new byte[] {9});
    LanceManifestCache.invalidateAll();
    assertNull(LanceManifestCache.getIfPresent("uri", 1L));
  }

  @Test
  public void testConcurrentPutSameKey() throws Exception {
    int threads = 16;
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (int i = 0; i < threads; i++) {
        pool.submit(
            () -> {
              try {
                start.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              }
              LanceManifestCache.put("same-uri", 42L, new byte[] {(byte) 1});
            });
      }
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
    } finally {
      pool.shutdownNow();
    }
    assertNotNull(LanceManifestCache.getIfPresent("same-uri", 42L));
  }
}
