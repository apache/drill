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
import org.apache.drill.exec.vector.accessor.ValueType;

import io.netty.buffer.DrillBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Specialized column writer for the (hidden) offset vector used
 * with variable-length or repeated vectors.
 */

public class OffsetVectorWriter extends BaseScalarWriter {
  private static final int VALUE_WIDTH = UInt4Vector.VALUE_WIDTH;
  private static final int MIN_VECTOR_SIZE = VALUE_WIDTH * 1024;
  private UInt4Vector vector;
  private int writeOffset;

  @Override
  public final void bindVector(final ValueVector vector) {
    this.vector = (UInt4Vector) vector;
    setAddr(this.vector.getBuffer());

    // Special handling for first value. Alloc vector if needed.
    // Offset vectors require a 0 at position 0. The (end) offset
    // for row 0 starts at position 1, which is handled in
    // writeOffset() below.

    writeOffset = 0;
    lastWriteIndex = 0;
    if (capacity < MIN_VECTOR_SIZE) {
      setAddr(this.vector.reallocRaw(MIN_VECTOR_SIZE));
    }
    PlatformDependent.putInt(bufAddr, writeOffset);
  }

  private final void setAddr(final DrillBuf buf) {
    bufAddr = buf.addr();
    capacity = buf.capacity() / VALUE_WIDTH;
  }

  public int writeOffset() { return writeOffset; }

  @Override
  public ValueType valueType() {
    return ValueType.INTEGER;
  }

  private final int writeIndex() {
    int writeIndex = vectorIndex.vectorIndex() + 1;
    if (lastWriteIndex + 1 == writeIndex && writeIndex < capacity) {
      lastWriteIndex = writeIndex;
      return writeIndex;
    }
    if (writeIndex >= capacity) {
      int size = (writeIndex + 1) * VALUE_WIDTH;
      if (size > ValueVector.MAX_BUFFER_SIZE) {
        throw new IllegalStateException("Offset vectors should not overflow");
      } else {
        setAddr(vector.reallocRaw(Math.min(4096, BaseAllocator.nextPowerOfTwo(size))));
      }
    }
    while (lastWriteIndex < writeIndex - 1) {
      PlatformDependent.putInt(bufAddr + ++lastWriteIndex * VALUE_WIDTH, writeOffset);
    }
    lastWriteIndex = writeIndex;
    return writeIndex;
  }

  public final void setOffset(final int curOffset) {
    final int writeIndex = writeIndex();
    PlatformDependent.putInt(bufAddr + writeIndex * VALUE_WIDTH, curOffset);
    writeOffset = curOffset;
  }

  @Override
  public final void finish() {
    final int finalIndex = writeIndex();
    vector.getBuffer().writerIndex(finalIndex * VALUE_WIDTH);
  }
}
