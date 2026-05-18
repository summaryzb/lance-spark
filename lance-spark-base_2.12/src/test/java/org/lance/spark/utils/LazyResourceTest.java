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
package org.lance.spark.utils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LazyResourceTest {

  @Test
  void uninitializedReportsFalseAndNullAccessor() {
    LazyResource<String> lazy = new LazyResource<>(() -> "value");
    assertFalse(lazy.isInitialized());
    assertNull(lazy.getIfInitialized());
  }

  @Test
  void getInvokesFactoryOnceAndMemoizes() {
    AtomicInteger calls = new AtomicInteger();
    LazyResource<String> lazy =
        new LazyResource<>(
            () -> {
              calls.incrementAndGet();
              return "value";
            });
    assertEquals("value", lazy.get());
    assertEquals("value", lazy.get());
    assertEquals(1, calls.get());
    assertTrue(lazy.isInitialized());
    assertEquals("value", lazy.getIfInitialized());
  }

  @Test
  void factoryExceptionLeavesUninitializedAndAllowsRetry() {
    AtomicInteger calls = new AtomicInteger();
    LazyResource<String> lazy =
        new LazyResource<>(
            () -> {
              if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("boom");
              }
              return "ok";
            });
    assertThrows(IllegalStateException.class, lazy::get);
    assertFalse(lazy.isInitialized());
    assertEquals("ok", lazy.get());
    assertEquals(2, calls.get());
  }

  @Test
  void concurrentGetsInvokeFactoryOnce() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    LazyResource<Object> lazy =
        new LazyResource<>(
            () -> {
              calls.incrementAndGet();
              return new Object();
            });
    int threads = 16;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    Object[] results = new Object[threads];
    try {
      for (int i = 0; i < threads; i++) {
        final int idx = i;
        pool.submit(
            () -> {
              start.await();
              results[idx] = lazy.get();
              return null;
            });
      }
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
    } finally {
      pool.shutdownNow();
    }
    assertEquals(1, calls.get());
    Object first = results[0];
    for (Object r : results) {
      assertSame(first, r);
    }
  }
}
