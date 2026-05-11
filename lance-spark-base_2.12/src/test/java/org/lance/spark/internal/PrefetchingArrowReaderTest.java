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

import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;
import org.lance.spark.read.LanceInputPartition;
import org.lance.spark.read.LanceSplit;
import org.lance.spark.utils.Optional;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the async prefetch wrapper via {@link LanceFragmentScanner#getArrowReader()}.
 *
 * <p>When {@code batch_prefetch_queue_depth > 0} the scanner wraps the Lance-provided {@link
 * ArrowReader} in a {@link PrefetchingArrowReader}. These tests assert the wrapper is transparent:
 * same row count, same column values, and orderly close under the non-trivial BG-thread lifecycle.
 * The default depth {@code 0} case continues to be covered by {@link
 * LanceFragmentColumnarBatchScannerTest}.
 */
public class PrefetchingArrowReaderTest {

  @Test
  public void loadsAllRowsWithQueueDepthOne() throws Exception {
    assertAllRowsMatch(1);
  }

  @Test
  public void loadsAllRowsWithQueueDepthFour() throws Exception {
    assertAllRowsMatch(4);
  }

  @Test
  public void queueDepthZeroReturnsDelegateDirectly() throws Exception {
    LanceInputPartition partition = partitionWithQueueDepth(0);
    try (LanceFragmentScanner scanner = LanceFragmentScanner.create(0, partition)) {
      try (ArrowReader reader = scanner.getArrowReader()) {
        assertFalse(
            reader instanceof PrefetchingArrowReader,
            "queue_depth=0 must return the raw Lance reader, not the prefetch wrapper");
      }
    }
  }

  @Test
  public void queueDepthOneReturnsPrefetchingWrapper() throws Exception {
    LanceInputPartition partition = partitionWithQueueDepth(1);
    try (LanceFragmentScanner scanner = LanceFragmentScanner.create(0, partition)) {
      try (ArrowReader reader = scanner.getArrowReader()) {
        assertTrue(
            reader instanceof PrefetchingArrowReader,
            "queue_depth=1 must return the async prefetch wrapper");
      }
    }
  }

  /**
   * Arrow's {@link ArrowReader} contract: {@code getVectorSchemaRoot()} returns a non-null root
   * even before the first {@code loadNextBatch()}. The wrapper must preserve that contract — the
   * pre-first-batch root is the empty schema placeholder created in the wrapper's constructor.
   */
  @Test
  public void getVectorSchemaRootBeforeFirstBatchReturnsEmptyRoot() throws Exception {
    LanceInputPartition partition = partitionWithQueueDepth(2);
    try (LanceFragmentScanner scanner = LanceFragmentScanner.create(0, partition);
        ArrowReader reader = scanner.getArrowReader()) {
      assertTrue(reader instanceof PrefetchingArrowReader, "sanity");
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertNotNull(root, "pre-first-batch root must be non-null");
      assertEquals(0, root.getRowCount(), "pre-first-batch root must be empty");
    }
  }

  /**
   * EOS must be sticky — once {@code loadNextBatch()} returns false, subsequent calls must also
   * return false without blocking or throwing. Reassures the Spark partition-reader contract of
   * end-of-stream idempotency.
   */
  @Test
  public void endOfStreamIsSticky() throws Exception {
    LanceInputPartition partition = partitionWithQueueDepth(2);
    try (LanceFragmentScanner scanner = LanceFragmentScanner.create(0, partition);
        ArrowReader reader = scanner.getArrowReader()) {
      assertTrue(reader instanceof PrefetchingArrowReader, "sanity");
      while (reader.loadNextBatch()) {
        // drain
      }
      assertFalse(reader.loadNextBatch(), "second EOS call must also return false");
      assertFalse(reader.loadNextBatch(), "third EOS call must also return false");
    }
  }

  /**
   * Close while the BG thread may still be in-flight: we open the wrapper, read one batch, then
   * close without draining. The BG thread is expected to observe the {@code closed} flag on its
   * next queue-offer attempt and exit cleanly; close() must not leak the prefetchAllocator. If it
   * did, Arrow's allocator would throw on the enclosing try-with-resources close of {@code
   * LanceRuntime.allocator()} at JVM shutdown — here we just assert the close path returns without
   * throwing.
   */
  @Test
  public void closeMidScanDoesNotLeak() throws Exception {
    LanceInputPartition partition = partitionWithQueueDepth(2);
    try (LanceFragmentScanner scanner = LanceFragmentScanner.create(0, partition);
        ArrowReader reader = scanner.getArrowReader()) {
      assertTrue(
          reader instanceof PrefetchingArrowReader, "sanity: wrapper active at queue_depth=2");
      assertTrue(reader.loadNextBatch(), "fragment 0 has at least one batch");
      assertNotNull(reader.getVectorSchemaRoot());
      // Intentionally do not drain the rest — try-with-resources closes the reader next.
    }
  }

  /**
   * Arrow's base {@code ArrowReader.close()} unconditionally dispatches to {@code
   * closeReadSource()}. Without the {@code closeReadSourceCalled} idempotency guard a second {@code
   * close()} call would double-close the delegate, the empty root, and the prefetch allocator — and
   * {@link org.apache.arrow.memory.BufferAllocator#close()} throws {@link IllegalStateException} on
   * a second invocation. This regression test locks that guard in.
   */
  @Test
  public void closeIsIdempotent() throws Exception {
    LanceInputPartition partition = partitionWithQueueDepth(2);
    LanceFragmentScanner scanner = LanceFragmentScanner.create(0, partition);
    try {
      ArrowReader reader = scanner.getArrowReader();
      assertTrue(reader instanceof PrefetchingArrowReader, "sanity");
      reader.close();
      // second close must not throw — idempotency guard on closeReadSource
      reader.close();
    } finally {
      scanner.close();
    }
  }

  private static void assertAllRowsMatch(int queueDepth) throws Exception {
    List<List<Long>> expected = TestUtils.TestTable1Config.expectedValues;
    List<List<Long>> actual = new ArrayList<>();
    for (int fragmentId = 0; fragmentId <= 1; fragmentId++) {
      LanceInputPartition partition = partitionWithQueueDepth(queueDepth);
      try (LanceFragmentScanner scanner = LanceFragmentScanner.create(fragmentId, partition);
          ArrowReader reader = scanner.getArrowReader()) {
        assertTrue(
            reader instanceof PrefetchingArrowReader,
            "queue_depth=" + queueDepth + " must wrap in PrefetchingArrowReader");
        while (reader.loadNextBatch()) {
          VectorSchemaRoot root = reader.getVectorSchemaRoot();
          int rows = root.getRowCount();
          List<FieldVector> vecs = root.getFieldVectors();
          for (int r = 0; r < rows; r++) {
            List<Long> row = new ArrayList<>(vecs.size());
            for (FieldVector v : vecs) {
              // TestTable1Config is all LongType
              row.add((Long) v.getObject(r));
            }
            actual.add(row);
          }
        }
      }
    }
    assertEquals(expected.size(), actual.size(), "total row count");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i), "row " + i + " at queue_depth=" + queueDepth);
    }
  }

  private static LanceInputPartition partitionWithQueueDepth(int queueDepth) {
    LanceSparkReadOptions readOptions =
        LanceSparkReadOptions.builder()
            .datasetUri(TestUtils.TestTable1Config.datasetUri)
            .batchPrefetchQueueDepth(queueDepth)
            .build();
    return new LanceInputPartition(
        TestUtils.TestTable1Config.schema,
        0,
        new LanceSplit(Arrays.asList(0, 1)),
        readOptions,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        "prefetch-test",
        null,
        null,
        null,
        null);
  }

  /**
   * The BG thread is the only thread that touches the delegate. If the delegate throws from {@code
   * loadNextBatch()}, that failure has to surface to the Spark executor thread via {@code
   * PrefetchingArrowReader.loadNextBatch()} — otherwise the Spark task would silently observe an
   * empty scan instead of a task failure. This test fabricates a delegate that throws on the very
   * first {@code loadNextBatch()} call and asserts the wrapper rethrows with the original cause
   * preserved.
   */
  @Test
  public void delegateErrorPropagatesAcrossPrefetchThread() throws Exception {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      IOException delegateError = new IOException("simulated delegate failure");
      ThrowingArrowReader delegate = new ThrowingArrowReader(allocator, delegateError);
      PrefetchingArrowReader wrapper = new PrefetchingArrowReader(delegate, 1, allocator);
      try {
        IOException thrown =
            assertThrows(IOException.class, wrapper::loadNextBatch, "must rethrow BG-thread error");
        // rethrow() passes IOException through untouched — assert we got the exact instance back.
        assertTrue(
            thrown == delegateError || thrown.getCause() == delegateError,
            "propagated error must wrap or equal the delegate's IOException");
      } finally {
        wrapper.close();
      }
    }
  }

  /**
   * Constructor contract: if reading the delegate's schema throws, the ctor must close the delegate
   * before propagating the failure. The caller has no reference to the delegate once the ctor
   * throws, so a leak here would only surface as an allocator-leak AssertionError at JVM shutdown.
   * This regression test locks in the try/catch-and-close path.
   */
  @Test
  public void constructorClosesDelegateOnSchemaFailure() throws Exception {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      IOException schemaError = new IOException("simulated schema-read failure");
      SchemaFailingArrowReader delegate = new SchemaFailingArrowReader(allocator, schemaError);
      RuntimeException thrown =
          assertThrows(
              RuntimeException.class,
              () -> new PrefetchingArrowReader(delegate, 1, allocator),
              "schema-read failure must propagate from ctor");
      // Ctor wraps IOException in RuntimeException — assert the original cause is preserved.
      assertTrue(
          thrown.getCause() == schemaError,
          "ctor must surface the original schema-read failure as cause");
      assertTrue(
          delegate.closeInvoked,
          "ctor must close the delegate to avoid leaking its native buffers");
    }
  }

  /**
   * Interrupting the consumer thread while it is blocked in {@code loadNextBatch()} must surface as
   * {@link InterruptedIOException} (preserving the cancellation signal so Spark's task-failure
   * paths can distinguish it from a generic I/O failure) AND re-arm the thread's interrupt flag (so
   * downstream code that polls {@code Thread.interrupted()} still sees it).
   */
  @Test
  public void consumerInterruptSurfacesAsInterruptedIOException() throws Exception {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      BlockingArrowReader delegate = new BlockingArrowReader(allocator);
      PrefetchingArrowReader wrapper = new PrefetchingArrowReader(delegate, 1, allocator);
      try {
        // Interrupt the consumer thread while it blocks inside loadNextBatch's poll loop.
        Thread current = Thread.currentThread();
        Thread interrupter =
            new Thread(
                () -> {
                  try {
                    delegate.blockStarted.await();
                    // Give the wrapper a moment to enter poll() so the interrupt lands while
                    // we're waiting on the queue rather than before we even poll.
                    Thread.sleep(100);
                    current.interrupt();
                  } catch (InterruptedException ignored) {
                    // nothing to do — test-only interrupter
                  }
                },
                "prefetch-test-interrupter");
        interrupter.setDaemon(true);
        interrupter.start();
        InterruptedIOException thrown =
            assertThrows(
                InterruptedIOException.class,
                wrapper::loadNextBatch,
                "consumer interrupt must surface as InterruptedIOException");
        assertNotNull(thrown);
        // loadNextBatch catches InterruptedException, re-arms the flag, then throws.
        // Interrupted() clears the flag in passing — it is fine if either that or a
        // follow-up isInterrupted() returns true; both mean the signal was preserved.
        assertTrue(
            Thread.interrupted(), "wrapper must preserve interrupt status on interrupt exit");
        interrupter.join(1000);
      } finally {
        delegate.unblock();
        wrapper.close();
      }
    }
  }

  /**
   * After {@code close()} has run, a stray {@code loadNextBatch()} call (e.g. defensive
   * double-drain, framework code unaware of the contract) must return {@code false} instead of
   * spinning forever in the poll loop. The BG thread has exited by then so no future batch or EOS
   * sentinel can arrive; polling indefinitely would wedge the consumer thread.
   */
  @Test
  public void loadNextBatchAfterCloseReturnsFalseInsteadOfHanging() throws Exception {
    LanceInputPartition partition = partitionWithQueueDepth(2);
    try (LanceFragmentScanner scanner = LanceFragmentScanner.create(0, partition)) {
      PrefetchingArrowReader reader = (PrefetchingArrowReader) scanner.getArrowReader();
      reader.close();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      // Fresh thread so an accidental spin-forever doesn't hang the test runner.
      Thread[] container = new Thread[1];
      boolean[] returnedFalse = {false};
      Thread worker =
          new Thread(
              () -> {
                try {
                  returnedFalse[0] = !reader.loadNextBatch();
                } catch (IOException e) {
                  // acceptable — we only care that the call terminates; any IOE also counts
                  returnedFalse[0] = true;
                }
              },
              "prefetch-test-post-close");
      container[0] = worker;
      worker.setDaemon(true);
      worker.start();
      worker.join(TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()));
      assertFalse(worker.isAlive(), "loadNextBatch after close must terminate, not spin");
      assertTrue(
          returnedFalse[0], "loadNextBatch after close must return false or throw, not hang");
    }
  }

  /**
   * A delegate whose {@code loadNextBatch()} blocks until unblocked by the test. Used to pin the
   * wrapper's consumer thread inside its poll loop so we can test interrupt handling.
   */
  private static final class BlockingArrowReader extends ArrowReader {
    private final Schema schema = new Schema(Collections.emptyList());
    final CountDownLatch blockStarted = new CountDownLatch(1);
    private final CountDownLatch unblock = new CountDownLatch(1);

    BlockingArrowReader(BufferAllocator allocator) {
      super(allocator);
    }

    void unblock() {
      unblock.countDown();
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      blockStarted.countDown();
      try {
        unblock.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return false;
    }

    @Override
    public long bytesRead() {
      return 0L;
    }

    @Override
    protected void closeReadSource() {
      unblock.countDown();
    }

    @Override
    protected Schema readSchema() {
      return schema;
    }
  }

  /**
   * Fake {@link ArrowReader} that throws a caller-supplied {@link IOException} from every {@code
   * loadNextBatch()}. Used to exercise cross-thread error propagation in the prefetch wrapper
   * without requiring a Lance-backed fragment.
   */
  private static final class ThrowingArrowReader extends ArrowReader {
    private final Schema schema = new Schema(Collections.emptyList());
    private final IOException error;

    ThrowingArrowReader(BufferAllocator allocator, IOException error) {
      super(allocator);
      this.error = error;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
      throw error;
    }

    @Override
    public long bytesRead() {
      return 0L;
    }

    @Override
    protected void closeReadSource() throws IOException {
      // nothing to close
    }

    @Override
    protected Schema readSchema() throws IOException {
      return schema;
    }
  }

  /**
   * Fake {@link ArrowReader} whose {@link #readSchema()} throws. Used to exercise the ctor's
   * schema-failure cleanup path — the wrapper's ctor calls {@code
   * delegate.getVectorSchemaRoot().getSchema()}, which in Arrow 15.x lazy-initializes by invoking
   * {@code readSchema()}. A failure there must trigger the ctor's delegate-close.
   */
  private static final class SchemaFailingArrowReader extends ArrowReader {
    private final IOException error;
    volatile boolean closeInvoked = false;

    SchemaFailingArrowReader(BufferAllocator allocator, IOException error) {
      super(allocator);
      this.error = error;
    }

    @Override
    public boolean loadNextBatch() {
      throw new AssertionError("loadNextBatch must not be reached — ctor failed before BG start");
    }

    @Override
    public long bytesRead() {
      return 0L;
    }

    @Override
    protected void closeReadSource() {
      closeInvoked = true;
    }

    @Override
    protected Schema readSchema() throws IOException {
      throw error;
    }
  }
}
