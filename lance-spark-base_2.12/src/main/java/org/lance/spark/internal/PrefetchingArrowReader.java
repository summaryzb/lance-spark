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

import com.google.common.base.Preconditions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Async prefetch wrapper around a Lance-provided {@link ArrowReader}.
 *
 * <p>Lance's Arrow reader already pipelines I/O inside Rust ({@code batch_readahead}). This wrapper
 * pipelines the JVM-side work — the JNI transition back into Java, the per-batch {@link
 * VectorSchemaRoot} rebuild inside the delegate reader, and our {@link
 * org.lance.spark.internal.LanceFragmentColumnarBatchScanner} wrapping — against the Spark
 * executor's consumption of the previous batch.
 *
 * <p>A single background thread drives the delegate reader. For each fetched batch it {@link
 * TransferPair#transfer() transfers} the buffers off the delegate's root into a fresh {@code
 * VectorSchemaRoot} backed by a per-batch child allocator and pushes it through a bounded queue.
 * This breaks the single-root buffer-reuse contract of {@link ArrowReader}: the delegate is free to
 * overwrite its root on the next {@code loadNextBatch()} without corrupting the caller's in-flight
 * batch.
 *
 * <p>Thread-safety: this class is not thread-safe; Spark's {@code PartitionReader} contract calls
 * {@code loadNextBatch()} / {@code getVectorSchemaRoot()} / {@code close()} from a single executor
 * task thread. The background thread drives the delegate's {@code loadNextBatch} / {@code
 * getVectorSchemaRoot} and owns its buffers. The sole cross-thread call into the delegate is {@link
 * #bytesRead()}, which the executor forwards as a plain field read — metric-only, no buffer access
 * — and which every known Arrow reader implementation services without blocking.
 */
public final class PrefetchingArrowReader extends ArrowReader {
  private static final Logger LOG = LoggerFactory.getLogger(PrefetchingArrowReader.class);

  /** Monotonic counter for thread-name disambiguation; concurrent wrappers get distinct ids. */
  private static final AtomicLong THREAD_SEQ = new AtomicLong();

  /** Sentinel signalling the background thread has reached EOS. */
  private static final BatchEntry EOS = new BatchEntry(null, null);

  /**
   * Upper bound on how long {@link #closeReadSource()} waits for the BG thread to exit before
   * giving up and leaking the delegate. Lance JNI calls are uninterruptible — during a typical scan
   * the BG thread finishes its in-flight {@code loadNextBatch} within a few seconds of {@code
   * close()} flipping the flag, but a hung object-store request could theoretically run longer. We
   * pick a window long enough to cover realistic worst-case remote reads yet short enough that a
   * stuck executor task still shuts down without wedging the JVM.
   */
  private static final long JOIN_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);

  private final ArrowReader delegate;
  private final Schema schema;
  private final BufferAllocator prefetchAllocator;
  private final BlockingQueue<BatchEntry> batchQueue;
  private final VectorSchemaRoot emptyRoot;
  private final Thread prefetchThread;

  private volatile Throwable prefetchError;
  private volatile boolean closed = false;
  private BatchEntry currentEntry;
  // Sticky EOS flag: once the EOS sentinel has been consumed, the BG thread has exited and the
  // queue will stay empty forever — subsequent loadNextBatch() calls must short-circuit to false
  // instead of polling indefinitely. Written and read only on the executor task thread.
  private boolean eosReached = false;
  // Idempotency guard for closeReadSource. Arrow's base ArrowReader.close(boolean) unconditionally
  // invokes closeReadSource() whenever closeReadChannel=true — and ArrowReader.close() always
  // passes true. A double-close (e.g. exception-suppression paths or framework re-close) would
  // otherwise double-close the delegate / emptyRoot / prefetchAllocator, and Arrow's
  // BufferAllocator.close() throws IllegalStateException on a second invocation.
  private boolean closeReadSourceCalled = false;

  /** Pairs a detached {@link VectorSchemaRoot} with the child allocator that owns its buffers. */
  private static final class BatchEntry {
    final VectorSchemaRoot root;
    final BufferAllocator allocator;

    BatchEntry(VectorSchemaRoot root, BufferAllocator allocator) {
      this.root = root;
      this.allocator = allocator;
    }

    // Aggregates exceptions from root.close() and allocator.close() so neither step's failure
    // masks the other. Without this, an exception from root.close() in the try would be
    // silently replaced by an exception from allocator.close() in the finally — Java language
    // semantics — making the original root-close failure invisible.
    void close() {
      if (root == null) {
        return;
      }
      Throwable primary = null;
      try {
        root.close();
      } catch (Throwable t) {
        primary = t;
      }
      if (allocator != null) {
        try {
          allocator.close();
        } catch (Throwable t) {
          if (primary != null) {
            primary.addSuppressed(t);
          } else {
            primary = t;
          }
        }
      }
      if (primary == null) {
        return;
      }
      if (primary instanceof RuntimeException) {
        throw (RuntimeException) primary;
      }
      if (primary instanceof Error) {
        throw (Error) primary;
      }
      throw new RuntimeException(primary);
    }
  }

  /**
   * Creates a prefetch wrapper around the given delegate reader.
   *
   * @param delegate the underlying Lance-provided reader; the wrapper takes ownership and will
   *     close it on {@link #close()}. Must not be {@code null}.
   * @param queueDepth maximum number of prefetched batches buffered ahead of the consumer. Must be
   *     strictly positive — {@code 0} is rejected; callers that want no prefetch should use the raw
   *     delegate directly rather than wrapping it in a zero-depth prefetcher.
   * @param parent parent {@link BufferAllocator} off which this wrapper creates its {@code
   *     lance-prefetch} child. Must not be {@code null}.
   */
  public PrefetchingArrowReader(ArrowReader delegate, int queueDepth, BufferAllocator parent) {
    super(Objects.requireNonNull(parent, "parent allocator must not be null"));
    Preconditions.checkNotNull(delegate, "delegate must not be null");
    Preconditions.checkArgument(queueDepth > 0, "queueDepth must be positive");

    // Single monotonic id shared between allocator name and thread name so a leak report
    // naming "lance-prefetch-17" can be correlated to the thread "lance-prefetch-17" that
    // produced it — critical when dozens of concurrent scans run in the same executor JVM.
    long instanceId = THREAD_SEQ.incrementAndGet();

    this.delegate = delegate;
    try {
      this.schema = delegate.getVectorSchemaRoot().getSchema();
    } catch (IOException | RuntimeException | Error e) {
      // Caller has no reference to `delegate` once the ctor throws, so the delegate's native
      // buffers would leak into the parent allocator and surface as an AssertionError only at
      // JVM shutdown. Close the delegate on the way out — original cause takes precedence.
      try {
        delegate.close();
      } catch (Throwable suppressed) {
        e.addSuppressed(suppressed);
      }
      // Preserve original throwable type — do NOT wrap Error in RuntimeException (OOM,
      // StackOverflowError, etc. need to reach the JVM/framework untouched).
      if (e instanceof Error) {
        throw (Error) e;
      }
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      // Remaining: IOException — wrap because ctor doesn't declare it.
      throw new RuntimeException("Failed to read schema from delegate ArrowReader", e);
    }

    // Allocator + empty root must be unwound if any mid-construction step throws, otherwise we
    // leak buffers into LanceRuntime.allocator() that only surface as AssertionError at JVM
    // shutdown. Assign to fields only after every allocation succeeds. The delegate was already
    // initialized successfully above — if *this* block throws, it must also close the delegate
    // so the caller (who has no reference to the wrapper on a failed ctor) doesn't leak it.
    BufferAllocator localAllocator;
    try {
      localAllocator = parent.newChildAllocator("lance-prefetch-" + instanceId, 0, Long.MAX_VALUE);
    } catch (Throwable t) {
      try {
        delegate.close();
      } catch (Throwable suppressed) {
        t.addSuppressed(suppressed);
      }
      throw t;
    }
    VectorSchemaRoot localEmptyRoot;
    try {
      localEmptyRoot = VectorSchemaRoot.create(schema, localAllocator);
    } catch (Throwable t) {
      try {
        localAllocator.close();
      } catch (Throwable suppressed) {
        t.addSuppressed(suppressed);
      }
      try {
        delegate.close();
      } catch (Throwable suppressed) {
        t.addSuppressed(suppressed);
      }
      throw t;
    }
    this.prefetchAllocator = localAllocator;
    this.emptyRoot = localEmptyRoot;
    // ArrayBlockingQueue<>(capacity) allocates an Object[capacity] backing array. For a
    // huge capacity (pathological Builder-path value that bypasses the parser's MAX check)
    // this throws OutOfMemoryError — at which point the already-owned delegate, emptyRoot
    // and prefetchAllocator would leak unless we unwind them here. Even though the Builder
    // now enforces MAX_BATCH_PREFETCH_QUEUE_DEPTH, keep the safety net: Thread ctor is the
    // next acquisition step and has the same unwind policy, so this mirrors that block.
    BlockingQueue<BatchEntry> localBatchQueue;
    try {
      localBatchQueue = new ArrayBlockingQueue<>(queueDepth);
    } catch (Throwable t) {
      try {
        localEmptyRoot.close();
      } catch (Throwable suppressed) {
        t.addSuppressed(suppressed);
      }
      try {
        localAllocator.close();
      } catch (Throwable suppressed) {
        t.addSuppressed(suppressed);
      }
      try {
        delegate.close();
      } catch (Throwable suppressed) {
        t.addSuppressed(suppressed);
      }
      throw t;
    }
    this.batchQueue = localBatchQueue;

    // Thread.start() establishes a happens-before relationship so the BG thread will observe
    // all writes above. Safe because this class is final — no subclass can observe a partially
    // initialized `this` via overridden methods invoked from runPrefetch(). If Thread
    // construction or start throws (OutOfMemoryError from Thread stack alloc, security
    // manager denying thread creation, etc.), we still own prefetchAllocator + emptyRoot +
    // delegate — close them on the way out to keep the "ctor owns cleanup on failure"
    // contract the caller (LanceFragmentScanner.getArrowReader) relies on.
    Thread localThread;
    try {
      localThread = new Thread(this::runPrefetch, "lance-prefetch-" + instanceId);
      localThread.setDaemon(true);
      localThread.start();
    } catch (Throwable t) {
      // Close in reverse construction order. emptyRoot first (its buffers belong to
      // prefetchAllocator, so closing the allocator before the root would make root.close
      // throw on an already-released parent).
      try {
        localEmptyRoot.close();
      } catch (Throwable suppressed) {
        t.addSuppressed(suppressed);
      }
      try {
        localAllocator.close();
      } catch (Throwable suppressed) {
        t.addSuppressed(suppressed);
      }
      try {
        delegate.close();
      } catch (Throwable suppressed) {
        t.addSuppressed(suppressed);
      }
      throw t;
    }
    this.prefetchThread = localThread;
  }

  private void runPrefetch() {
    // Each explicit exit path below offers EOS before returning; the finally block is a
    // safety net for unexpected Throwables (e.g. Error) that bypass the explicit paths.
    // Tracking `eosOffered` prevents a redundant finally-path offer from blocking up to
    // 100ms on a full queue after the primary exit already delivered EOS.
    boolean eosOffered = false;
    try {
      while (!closed) {
        boolean hasNext;
        try {
          hasNext = delegate.loadNextBatch();
        } catch (Throwable t) {
          prefetchError = t;
          offerOrDrop(EOS);
          eosOffered = true;
          return;
        }

        if (!hasNext) {
          offerOrDrop(EOS);
          eosOffered = true;
          return;
        }

        BatchEntry entry;
        try {
          entry = detachCurrentBatch();
        } catch (Throwable t) {
          prefetchError = t;
          offerOrDrop(EOS);
          eosOffered = true;
          return;
        }

        // Ownership: once `offered` is true, the entry belongs to the queue and eventually the
        // consumer (via loadNextBatch drain) or close() (via the closeReadSource drain loop).
        // The BG thread must NOT close an entry it has successfully handed off — otherwise the
        // drain path and this branch race and double-close the same allocator.
        boolean offered = false;
        try {
          while (!closed) {
            if (batchQueue.offer(entry, 100, TimeUnit.MILLISECONDS)) {
              offered = true;
              break;
            }
          }
          if (!offered) {
            entry.close();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          if (!offered) {
            entry.close();
          }
          prefetchError = e;
          offerOrDrop(EOS);
          eosOffered = true;
          return;
        }
      }
    } finally {
      // Safety net only — skip if an explicit exit path already delivered EOS, otherwise
      // we'd waste up to 100ms waiting to offer into a full bounded queue that nobody is
      // consuming anymore.
      if (!closed && !eosOffered) {
        offerOrDrop(EOS);
      }
    }
  }

  /**
   * Transfers every column of the delegate's current batch into a new {@link VectorSchemaRoot}
   * owned by a per-batch child allocator. O(1) per column — no data copy.
   */
  private BatchEntry detachCurrentBatch() throws IOException {
    VectorSchemaRoot source = delegate.getVectorSchemaRoot();
    // Allocate the holder list before the child allocator so a rare OOM on ArrayList creation
    // cannot leak a live child allocator into prefetchAllocator (which would only surface as an
    // IllegalStateException at prefetchAllocator.close() time).
    List<FieldVector> transferred = new ArrayList<>(source.getFieldVectors().size());
    BufferAllocator batchAllocator =
        prefetchAllocator.newChildAllocator("batch", 0, Long.MAX_VALUE);
    try {
      for (FieldVector srcVec : source.getFieldVectors()) {
        TransferPair tp = srcVec.getTransferPair(batchAllocator);
        tp.transfer();
        transferred.add((FieldVector) tp.getTo());
      }
      VectorSchemaRoot detached = new VectorSchemaRoot(transferred);
      detached.setRowCount(source.getRowCount());
      return new BatchEntry(detached, batchAllocator);
    } catch (Throwable t) {
      for (FieldVector v : transferred) {
        try {
          v.close();
        } catch (Throwable ignored) {
          // best-effort cleanup
        }
      }
      try {
        batchAllocator.close();
      } catch (Throwable ignored) {
        // best-effort cleanup
      }
      throw t;
    }
  }

  private void offerOrDrop(BatchEntry entry) {
    // Best-effort put; if queue is full and no consumer arrives, eventually consumer will drain.
    try {
      while (!closed && !batchQueue.offer(entry, 100, TimeUnit.MILLISECONDS)) {
        // keep trying
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    if (eosReached) {
      return false;
    }
    if (currentEntry != null) {
      // Null the field BEFORE calling close(). BatchEntry.close() uses try/finally to always
      // close the allocator — so if root.close() throws, the allocator is already closed but
      // the field would still reference it. A second close attempt would hit allocator.close()
      // again and throw IllegalStateException.
      BatchEntry toClose = currentEntry;
      currentEntry = null;
      toClose.close();
    }

    BatchEntry entry;
    try {
      while (true) {
        entry = batchQueue.poll(100, TimeUnit.MILLISECONDS);
        if (entry != null) {
          break;
        }
        if (prefetchError != null) {
          rethrow(prefetchError);
        }
        // Defensive termination: if close() was invoked out of contract, or if the BG thread
        // exited without delivering EOS (e.g. an interrupted offerOrDrop that dropped the
        // sentinel), promote to sticky EOS instead of polling forever. Once the BG thread is
        // not alive AND the queue is empty there is no future producer, so no further batch
        // can arrive — a second poll only wastes CPU.
        if (closed || (!prefetchThread.isAlive() && batchQueue.isEmpty())) {
          eosReached = true;
          return false;
        }
      }
    } catch (InterruptedException e) {
      // Surface as InterruptedIOException (IOException subtype) so callers that special-case
      // cancellation can distinguish it from a generic I/O failure while still satisfying the
      // ArrowReader.loadNextBatch signature. Also re-arm the interrupt flag so downstream code
      // that polls Thread.isInterrupted() still sees the signal.
      Thread.currentThread().interrupt();
      InterruptedIOException iio =
          new InterruptedIOException("Interrupted waiting for prefetched batch");
      iio.initCause(e);
      // If the BG thread failed before this cancellation and never got a chance to surface
      // through the poll-null path, attach its error as suppressed so task-failure telemetry
      // can still see the root cause rather than just the generic interrupt signal.
      Throwable bgError = prefetchError;
      if (bgError != null) {
        iio.addSuppressed(bgError);
      }
      throw iio;
    }

    if (entry == EOS) {
      eosReached = true;
      if (prefetchError != null) {
        rethrow(prefetchError);
      }
      return false;
    }

    currentEntry = entry;
    return true;
  }

  private static void rethrow(Throwable t) throws IOException {
    if (t instanceof IOException) {
      throw (IOException) t;
    }
    if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    }
    if (t instanceof Error) {
      throw (Error) t;
    }
    if (t instanceof InterruptedException) {
      // Preserve cancellation intent when surfacing a BG-thread interrupt to the caller:
      // re-arm the consumer's interrupt flag AND surface as InterruptedIOException so any
      // upstream handler that special-cases cancellation (e.g. Spark's TaskContext check
      // or a retry wrapper distinguishing interrupt from generic I/O failure) can see it.
      Thread.currentThread().interrupt();
      InterruptedIOException iio = new InterruptedIOException("Prefetch thread interrupted");
      iio.initCause(t);
      throw iio;
    }
    throw new IOException("Prefetch thread failed", t);
  }

  @Override
  public VectorSchemaRoot getVectorSchemaRoot() {
    if (currentEntry != null) {
      return currentEntry.root;
    }
    return emptyRoot;
  }

  @Override
  protected void prepareLoadNextBatch() {
    // No-op: batches are fully prepared by the background thread.
  }

  @Override
  public long bytesRead() {
    return delegate.bytesRead();
  }

  @Override
  protected Schema readSchema() {
    return schema;
  }

  @Override
  protected void closeReadSource() throws IOException {
    if (closeReadSourceCalled) {
      return;
    }
    closeReadSourceCalled = true;
    closed = true;
    prefetchThread.interrupt();
    // If the calling thread has already been interrupted (e.g. Spark task cancellation ran
    // through loadNextBatch → IOException → Spark invokes close() on the still-interrupted
    // task thread), Thread.join(timeout) — which delegates to Object.wait — would throw
    // InterruptedException immediately without waiting. That would flip bgStillAlive to true
    // and leak the delegate + prefetchAllocator on every cancelled task, accumulating native
    // buffers across the executor's lifetime. Clear the flag for the duration of join and
    // restore it afterwards so the BG thread gets its full JOIN_TIMEOUT_MS budget to drain
    // any in-flight JNI call.
    boolean callerInterrupted = Thread.interrupted();
    try {
      prefetchThread.join(JOIN_TIMEOUT_MS);
    } catch (InterruptedException e) {
      callerInterrupted = true;
    } finally {
      if (callerInterrupted) {
        Thread.currentThread().interrupt();
      }
    }

    // If the BG thread is still alive it is stuck inside an uninterruptible Lance JNI call,
    // and the delegate's native buffers are still in active use. Closing the delegate here
    // would race with the JNI call and typically SIGSEGV — leaking the delegate is strictly
    // safer than crashing the JVM. We also keep prefetchAllocator open so any child allocator
    // the BG thread may still create in {@link #detachCurrentBatch} remains parentable (Arrow
    // {@code BaseAllocator} is documented thread-safe, but a closed parent would reject
    // further {@code newChildAllocator} calls). Draining the queue + closing currentEntry IS
    // safe because BaseAllocator serializes child close/create on its own internal lock — only
    // the PARENT close is the hard race against BG's in-flight child creation.
    boolean bgStillAlive = prefetchThread.isAlive();
    if (bgStillAlive) {
      LOG.warn(
          "Prefetch BG thread did not exit within {}ms of close() — skipping delegate and"
              + " prefetchAllocator close to avoid racing with an in-flight Lance JNI call."
              + " The Lance native reader, the lance-prefetch child allocator, the empty"
              + " schema root, any batch the BG thread is holding pre-offer, and any batch"
              + " the BG thread produces after this call return will leak until the JVM exits."
              + " currentEntry + already-queued batches are drained here (safe: Arrow's"
              + " BaseAllocator thread-safety covers concurrent child close vs. BG's"
              + " newChildAllocator).",
          JOIN_TIMEOUT_MS);
    }

    // Close-path exception aggregation: every close() call below must be guarded so that a
    // throw from any one step does not skip the remaining steps and leak native buffers.
    // `primary` holds the first throw; later throws are chained via addSuppressed().
    Throwable primary = null;

    if (currentEntry != null) {
      // Null before close — symmetric with loadNextBatch(). If currentEntry.close() throws,
      // the closeReadSourceCalled guard already prevents re-entry into this block, but
      // nulling first keeps the invariant "a non-null currentEntry field always points to a
      // closeable entry" uniform across all code paths.
      BatchEntry toClose = currentEntry;
      currentEntry = null;
      try {
        toClose.close();
      } catch (Throwable t) {
        primary = t;
      }
    }

    BatchEntry drained;
    while ((drained = batchQueue.poll()) != null) {
      if (drained != EOS) {
        try {
          drained.close();
        } catch (Throwable t) {
          if (primary != null) {
            primary.addSuppressed(t);
          } else {
            primary = t;
          }
        }
      }
    }

    if (!bgStillAlive) {
      try {
        delegate.close();
      } catch (Throwable t) {
        if (primary != null) {
          primary.addSuppressed(t);
        } else {
          primary = t;
        }
      }
      try {
        emptyRoot.close();
      } catch (Throwable t) {
        if (primary != null) {
          primary.addSuppressed(t);
        } else {
          primary = t;
        }
      }
      // Only safe to close the prefetchAllocator if no BG thread may still be inside
      // detachCurrentBatch() allocating child allocators off it.
      try {
        prefetchAllocator.close();
      } catch (Throwable t) {
        if (primary != null) {
          primary.addSuppressed(t);
        } else {
          primary = t;
        }
      }
    }

    if (primary != null) {
      if (primary instanceof IOException) {
        throw (IOException) primary;
      }
      if (primary instanceof RuntimeException) {
        throw (RuntimeException) primary;
      }
      if (primary instanceof Error) {
        throw (Error) primary;
      }
      throw new IOException("Failed to close prefetching reader", primary);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("closed PrefetchingArrowReader");
    }
  }
}
