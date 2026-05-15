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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Assembles N single-column {@link ArrowReader}s into a multi-column view. Each call to {@link
 * #loadNextBatch()} advances all sub-readers in lockstep and presents their vectors as a single
 * {@link VectorSchemaRoot}.
 *
 * <p>Contract: all sub-readers must produce the same number of batches with the same row count per
 * batch (guaranteed when they originate from the same Lance fragment with the same batchSize).
 *
 * <p>Ownership: each {@code loadNextBatch()} transfers vector buffers from sub-readers into the
 * assembled root via {@code TransferPair.transfer()}. After transfer, the sub-reader's vectors are
 * empty shells; the assembled root owns the buffers. {@code dst.clear()} before each transfer
 * releases the previous batch's buffers. Closing this reader closes all sub-readers.
 */
public final class ColumnAssemblingArrowReader extends ArrowReader {

  private final List<ArrowReader> columnReaders;
  private final Schema assembledSchema;
  private long bytesRead = 0;

  public ColumnAssemblingArrowReader(BufferAllocator allocator, List<ArrowReader> columnReaders)
      throws IOException {
    super(allocator);
    if (columnReaders == null || columnReaders.isEmpty()) {
      throw new IllegalArgumentException("columnReaders must not be empty");
    }
    this.columnReaders = columnReaders;
    List<Field> fields = new ArrayList<>(columnReaders.size());
    for (ArrowReader r : columnReaders) {
      Schema s = r.getVectorSchemaRoot().getSchema();
      if (s.getFields().size() != 1) {
        throw new IllegalArgumentException(
            "Each column reader must have exactly 1 field, got " + s.getFields().size());
      }
      fields.add(s.getFields().get(0));
    }
    this.assembledSchema = new Schema(fields);
  }

  @Override
  protected Schema readSchema() {
    return assembledSchema;
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    ensureInitialized();
    boolean first = true;
    boolean hasMore = false;
    for (ArrowReader r : columnReaders) {
      boolean loaded = r.loadNextBatch();
      if (first) {
        hasMore = loaded;
        first = false;
      } else if (loaded != hasMore) {
        throw new IOException(
            "Column readers out of sync: some exhausted while others have more batches");
      }
    }
    if (!hasMore) {
      return false;
    }
    VectorSchemaRoot root = getVectorSchemaRoot();
    int rowCount = columnReaders.get(0).getVectorSchemaRoot().getRowCount();
    List<FieldVector> vectors = root.getFieldVectors();
    for (int i = 0; i < columnReaders.size(); i++) {
      VectorSchemaRoot colRoot = columnReaders.get(i).getVectorSchemaRoot();
      if (colRoot.getRowCount() != rowCount) {
        throw new IOException(
            "Row count mismatch: column 0 has "
                + rowCount
                + " rows but column "
                + i
                + " has "
                + colRoot.getRowCount());
      }
      FieldVector src = colRoot.getVector(0);
      FieldVector dst = vectors.get(i);
      dst.clear();
      long srcSize = src.getBufferSize();
      src.makeTransferPair(dst).transfer();
      bytesRead += srcSize;
    }
    root.setRowCount(rowCount);
    return true;
  }

  @Override
  public long bytesRead() {
    return bytesRead;
  }

  @Override
  protected void closeReadSource() throws IOException {
    IOException first = null;
    for (ArrowReader r : columnReaders) {
      try {
        r.close();
      } catch (IOException e) {
        if (first == null) first = e;
        else first.addSuppressed(e);
      }
    }
    if (first != null) throw first;
  }
}
