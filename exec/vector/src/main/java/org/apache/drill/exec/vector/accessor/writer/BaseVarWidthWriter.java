/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;

/**
 * Base class for variable-width (VarChar, VarBinary, etc.) writers.
 * Handles the additional complexity that such writers work with
 * both an offset vector and a data vector. The offset vector is
 * written using a specialized offset vector writer. The last write
 * index is defined as the the last write position in the offset
 * vector; not the last write position in the variable-width
 * vector.
 * <p>
 * Most and value events are forwarded to the offset vector.
 */

public abstract class BaseVarWidthWriter extends BaseScalarWriter {
  protected final OffsetVectorWriter offsetsWriter;

  public BaseVarWidthWriter(UInt4Vector offsetVector) {
    offsetsWriter = new OffsetVectorWriter(offsetVector);
  }

  @Override
  public void bindIndex(final ColumnWriterIndex index) {
    offsetsWriter.bindIndex(index);
    super.bindIndex(index);
  }

  @Override
  public void startWrite() {
    setBuffer();
    offsetsWriter.startWrite();
  }

  @Override
  public void startRow() { offsetsWriter.startRow(); }

  protected final int writeIndex(final int width) {

    // This is performance critical code; every operation counts.
    // Please be thoughtful when changing the code.

    int writeOffset = offsetsWriter.nextOffset();
    if (writeOffset + width < capacity) {
      return writeOffset;
    }
    resize(writeOffset + width);
    return offsetsWriter.nextOffset();
  }

  @Override
  protected final void setBuffer() {
    drillBuf = vector().getBuffer();
    capacity = drillBuf.capacity();
  }

  private void resize(int size) {
    if (size <= capacity) {
      return;
    }

    // Since some vectors start off as 0 length, set a
    // minimum size to avoid silly thrashing on early rows.

    if (size < MIN_BUFFER_SIZE) {
      size = MIN_BUFFER_SIZE;
    }

    // Grow the vector -- or overflow if the growth would make the batch
    // consume too much memory. The idea is that we grow vectors as they
    // fit the available memory budget, then we fill those vectors until
    // one of them needs more space. At that point we trigger overflow to
    // a new set of vectors. Internal fragmentation will result, but this
    // approach (along with proper initial vector sizing), minimizes that
    // fragmentation.

    size = BaseAllocator.nextPowerOfTwo(size);

    // Two cases: grow this vector or allocate a new one.

    if (size <= ValueVector.MAX_BUFFER_SIZE && canExpand(size - capacity)) {

      // Optimized form of reAlloc() which does not zero memory, does not do
      // bounds checks (since they were already done above). The write index
      // and offset remain unchanged.

      realloc(size);
    } else {

      // Allocate a new vector, or throw an exception if overflow is not
      // supported. If overflow is supported, the callback will call
      // endWrite(), which will set the final writer index for the current
      // vector. Then, bindVector() will be called to provide the new vector.
      // The write index changes with the new vector.

      overflowed();
    }
  }

  @Override
  public void skipNulls() { }

  @Override
  public void restartRow() { offsetsWriter.restartRow(); }

  @Override
  public int lastWriteIndex() { return offsetsWriter.lastWriteIndex(); }

  @Override
  public final void preRollover() {
    vector().getBuffer().writerIndex(offsetsWriter.rowStartOffset());
    offsetsWriter.preRollover();
  }

  @Override
  public void postRollover() {
    setBuffer();
    offsetsWriter.postRollover();
  }

  @Override
  public final void endWrite() {
    vector().getBuffer().writerIndex(offsetsWriter.nextOffset());
    offsetsWriter.endWrite();
  }

  @Override
  public void dump(HierarchicalFormatter format) {
    format.extend();
    super.dump(format);
    format.attribute("offsetsWriter");
    offsetsWriter.dump(format);
    format.endObject();
  }
}
