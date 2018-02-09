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
package org.apache.drill.exec.vector;

import com.google.common.base.Preconditions;
import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.exception.OversizedAllocationException;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.vector.complex.impl.BitReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

/**
 * Bit implements a vector of bit-width values. Elements in the vector are accessed by position from the logical start
 * of the vector. The width of each element is 1 bit. The equivalent Java primitive is an int containing the value '0'
 * or '1'.
 */
public final class BitVector extends BaseDataValueVector implements FixedWidthVector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitVector.class);

  /**
   * Width of each fixed-width value.
   */

  public static final int VALUE_WIDTH = 1;

  /**
   * Maximum number of values that this fixed-width vector can hold
   * and stay below the maximum vector size limit. This is the limit
   * enforced when the vector is used to hold values in a repeated
   * vector.
   */

  public static final int MAX_CAPACITY = MAX_BUFFER_SIZE / VALUE_WIDTH;

  /**
   * Maximum number of values that this fixed-width vector can hold
   * and stay below the maximum vector size limit and/or stay below
   * the maximum item count. This lis the limit enforced when the
   * vector is used to hold required or nullable values.
   */

  public static final int MAX_COUNT = Math.min(MAX_ROW_COUNT, MAX_CAPACITY);

  /**
   * Actual maximum vector size, in bytes, given the number of fixed-width
   * values that either fit in the maximum overall vector size, or that
   * is no larger than the maximum vector item count.
   */

  public static final int NET_MAX_SIZE = VALUE_WIDTH * MAX_COUNT;

  private final FieldReader reader = new BitReaderImpl(BitVector.this);
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();

  private int valueCount;
  private int allocationSizeInBytes = INITIAL_VALUE_ALLOCATION;
  private int allocationMonitor = 0;

  public BitVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  @Override
  public FieldReader getReader() {
    return reader;
  }

  @Override
  public int getBufferSize() {
    return getSizeFromCount(valueCount);
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    return getSizeFromCount(valueCount);
  }

  private int getSizeFromCount(int valueCount) {
    return (int) Math.ceil(valueCount / 8.0);
  }

  @Override
  public int getValueCapacity() {
    return (int) Math.min((long)Integer.MAX_VALUE, data.capacity() * 8L);
  }

  private int getByteIndex(int index) {
    return (int) Math.floor(index / 8.0);
  }

  @Override
  public void setInitialCapacity(final int valueCount) {
    allocationSizeInBytes = getSizeFromCount(valueCount);
  }

  @Override
  public void allocateNew() {
    if (!allocateNewSafe()) {
      throw new OutOfMemoryException();
    }
  }

  @Override
  public boolean allocateNewSafe() {
    long curAllocationSize = allocationSizeInBytes;
    if (allocationMonitor > 10) {
      curAllocationSize = Math.max(8, allocationSizeInBytes / 2);
      allocationMonitor = 0;
    } else if (allocationMonitor < -2) {
      curAllocationSize = allocationSizeInBytes * 2L;
      allocationMonitor = 0;
    }

    try {
      allocateBytes(curAllocationSize);
    } catch (OutOfMemoryException ex) {
      return false;
    }
    return true;
  }

  @Override
  public void reset() {
    valueCount = 0;
    allocationSizeInBytes = INITIAL_VALUE_ALLOCATION;
    allocationMonitor = 0;
    zeroVector();
    super.reset();
  }

  /**
   * Allocate a new memory space for this vector. Must be called prior to using the ValueVector.
   *
   * @param valueCount
   *          The number of values which can be contained within this vector.
   */
  @Override
  public void allocateNew(int valueCount) {
    final int size = getSizeFromCount(valueCount);
    allocateBytes(size);
  }

  private void allocateBytes(final long size) {
    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
    }

    final int curSize = (int) size;
    clear();
    data = allocator.buffer(curSize);
    zeroVector();
    allocationSizeInBytes = curSize;
  }

  /**
   * Allocate new buffer with double capacity, and copy data into the new buffer. Replace vector's buffer with new buffer, and release old one
   */
  public void reAlloc() {
    final long newAllocationSize = allocationSizeInBytes * 2L;
    if (newAllocationSize > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
    }

    final int curSize = (int)newAllocationSize;
    final DrillBuf newBuf = allocator.buffer(curSize);
    newBuf.setZero(0, newBuf.capacity());
    newBuf.setBytes(0, data, 0, data.capacity());
    data.release();
    data = newBuf;
    allocationSizeInBytes = curSize;
  }

  // This version uses the base version because this vector appears to not be
  // used, so not worth the effort to avoid zero-fill.

  public DrillBuf reallocRaw(int newAllocationSize) {
    while (allocationSizeInBytes < newAllocationSize) {
      reAlloc();
    }
    return data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void zeroVector() {
    data.setZero(0, data.capacity());
  }

  @Override
  public int getValueWidth() {
    return VALUE_WIDTH;
  }

  public void copyFrom(int inIndex, int outIndex, BitVector from) {
    this.mutator.set(outIndex, from.accessor.get(inIndex));
  }

  public boolean copyFromSafe(int inIndex, int outIndex, BitVector from) {
    if (outIndex >= this.getValueCapacity()) {
      decrementAllocationMonitor();
      return false;
    }
    copyFrom(inIndex, outIndex, from);
    return true;
  }

  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    copyFrom(fromIndex, toIndex, (BitVector) from);
  }

  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
    Preconditions.checkArgument(this.field.getName().equals(metadata.getNamePart().getName()),
                                "The field %s doesn't match the provided metadata %s.", this.field, metadata);
    final int valueCount = metadata.getValueCount();
    final int expectedLength = getSizeFromCount(valueCount);
    final int actualLength = metadata.getBufferLength();
    assert expectedLength == actualLength: "expected and actual buffer sizes do not match";

    clear();
    data = buffer.slice(0, actualLength);
    data.retain();
    this.valueCount = valueCount;
  }

  @Override
  public Mutator getMutator() {
    return new Mutator();
  }

  @Override
  public Accessor getAccessor() {
    return new Accessor();
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new TransferImpl(getField(), allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(getField().withPath(ref), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((BitVector) to);
  }

  public void transferTo(BitVector target) {
    target.clear();
    if (target.data != null) {
      target.data.release();
    }
    target.data = data;
    target.data.retain(1);
    target.valueCount = valueCount;
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, BitVector target) {
    assert startIndex + length <= valueCount;
    int firstByte = getByteIndex(startIndex);
    int byteSize = getSizeFromCount(length);
    int offset = startIndex % 8;
    if (offset == 0) {
      target.clear();
      // slice
      if (target.data != null) {
        target.data.release();
      }
      target.data = data.slice(firstByte, byteSize);
      target.data.retain(1);
    } else {
      // Copy data
      // When the first bit starts from the middle of a byte (offset != 0), copy data from src BitVector.
      // Each byte in the target is composed by a part in i-th byte, another part in (i+1)-th byte.
      // The last byte copied to target is a bit tricky :
      //   1) if length requires partly byte (length % 8 !=0), copy the remaining bits only.
      //   2) otherwise, copy the last byte in the same way as to the prior bytes.
      target.clear();
      target.allocateNew(length);
      // TODO maybe do this one word at a time, rather than byte?
      for(int i = 0; i < byteSize - 1; i++) {
        target.data.setByte(i, (((this.data.getByte(firstByte + i) & 0xFF) >>> offset) + (this.data.getByte(firstByte + i + 1) <<  (8 - offset))));
      }
      if (length % 8 != 0) {
        target.data.setByte(byteSize - 1, ((this.data.getByte(firstByte + byteSize - 1) & 0xFF) >>> offset));
      } else {
        target.data.setByte(byteSize - 1,
            (((this.data.getByte(firstByte + byteSize - 1) & 0xFF) >>> offset) + (this.data.getByte(firstByte + byteSize) <<  (8 - offset))));
      }
    }
    target.getMutator().setValueCount(length);
  }

  private class TransferImpl implements TransferPair {
    BitVector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator) {
      this.to = new BitVector(field, allocator);
    }

    public TransferImpl(BitVector to) {
      this.to = to;
    }

    @Override
    public BitVector getTo() {
      return to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, BitVector.this);
    }
  }

  private void decrementAllocationMonitor() {
    if (allocationMonitor > 0) {
      allocationMonitor = 0;
    }
    --allocationMonitor;
  }

  private void incrementAllocationMonitor() {
    ++allocationMonitor;
  }

  public class Accessor extends BaseAccessor {

    /**
     * Get the byte holding the desired bit, then mask all other bits. Iff the result is 0, the bit was not set.
     *
     * @param index
     *          position of the bit in the vector
     * @return 1 if set, otherwise 0
     */
    public final int get(int index) {
      int byteIndex = index >> 3;
      byte b = data.getByte(byteIndex);
      int bitIndex = index & 7;
      return Long.bitCount(b &  (1L << bitIndex));
    }

    @Override
    public boolean isNull(int index) {
      return false;
    }

    @Override
    public final Boolean getObject(int index) {
      return Boolean.valueOf(get(index) != 0);
    }

    @Override
    public final int getValueCount() {
      return valueCount;
    }

    public final void get(int index, BitHolder holder) {
      holder.value = get(index);
    }

    public final void get(int index, NullableBitHolder holder) {
      holder.isSet = 1;
      holder.value = get(index);
    }
  }

  /**
   * MutableBit implements a vector of bit-width values. Elements in the vector are accessed by position from the
   * logical start of the vector. Values should be pushed onto the vector sequentially, but may be randomly accessed.
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public class Mutator extends BaseMutator {

    private Mutator() {
    }

    /**
     * Set the bit at the given index to the specified value.
     *
     * @param index
     *          position of the bit to set
     * @param value
     *          value to set (either 1 or 0)
     */
    public final void set(int index, int value) {
      int byteIndex = index >> 3;
      int bitIndex = index & 7;
      byte currentByte = data.getByte(byteIndex);
      byte bitMask = (byte) (1L << bitIndex);
      if (value != 0) {
        currentByte |= bitMask;
      } else {
        currentByte -= (bitMask & currentByte);
      }

      data.setByte(byteIndex, currentByte);
    }

    public final void set(int index, BitHolder holder) {
      set(index, holder.value);
    }

    final void set(int index, NullableBitHolder holder) {
      set(index, holder.value);
    }

    public void setSafe(int index, int value) {
      while(index >= getValueCapacity()) {
        reAlloc();
      }
      set(index, value);
    }

    public void setSafe(int index, BitHolder holder) {
      while(index >= getValueCapacity()) {
        reAlloc();
      }
      set(index, holder.value);
    }

    public void setSafe(int index, NullableBitHolder holder) {
      while(index >= getValueCapacity()) {
        reAlloc();
      }
      set(index, holder.value);
    }

    @Override
    public final void setValueCount(int valueCount) {
      int currentValueCapacity = getValueCapacity();
      BitVector.this.valueCount = valueCount;
      int idx = getSizeFromCount(valueCount);
      while(valueCount > getValueCapacity()) {
        reAlloc();
      }
      if (valueCount > 0 && currentValueCapacity > valueCount * 2) {
        incrementAllocationMonitor();
      } else if (allocationMonitor > 0) {
        allocationMonitor = 0;
      }
      VectorTrimmer.trim(data, idx);
    }

    @Override
    public final void generateTestData(int values) {
      boolean even = true;
      for(int i = 0; i < values; i++, even = !even) {
        if (even) {
          set(i, 1);
        }
      }
      setValueCount(values);
    }
  }

  @Override
  public void clear() {
    this.valueCount = 0;
    super.clear();
  }

  @Override
  public int getPayloadByteCount(int valueCount) {
    return getSizeFromCount(valueCount);
  }

  @Override
  public void toNullable(ValueVector nullableVector) {
    NullableBitVector dest = (NullableBitVector) nullableVector;
    dest.getMutator().fromNotNullable(this);
  }
}
