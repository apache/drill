/**
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

import io.netty.buffer.ByteBuf;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.memory.AccountingByteBuf;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
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

  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();

  private int allocationValueCount = 4096;
  private int allocationMonitor = 0;

  private int valueCapacity;

  public BitVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  @Override
  public SerializedField getMetadata() {
    return field.getAsBuilder() //
        .setValueCount(valueCount) //
        .setBufferLength( (int) Math.ceil(valueCount / 8.0)) //
        .build();
  }

  private int getSizeFromCount(int valueCount) {
    return (int) Math.ceil(valueCount / 8.0);
  }

  private int getByteIndex(int index) {
    return (int) Math.floor(index / 8.0);
  }

  public void allocateNew() {
    if(!allocateNewSafe()) throw new OutOfMemoryRuntimeException();
  }

  public boolean allocateNewSafe() {
    clear();
    if (allocationMonitor > 10) {
      allocationValueCount = Math.max(8, (int) (allocationValueCount / 2));
      allocationMonitor = 0;
    } else if (allocationMonitor < -2) {
      allocationValueCount = (int) (allocationValueCount * 2);
      allocationMonitor = 0;
    }

    clear();
    valueCapacity = allocationValueCount;
    int valueSize = getSizeFromCount(allocationValueCount);
    data = allocator.buffer(valueSize);
    if(data == null) return false;
    zeroVector();
    return true;
  }

  /**
   * Allocate a new memory space for this vector. Must be called prior to using the ValueVector.
   *
   * @param valueCount
   *          The number of values which can be contained within this vector.
   */
  public void allocateNew(int valueCount) {
    clear();
    valueCapacity = valueCount;
    int valueSize = getSizeFromCount(valueCount);
    data = allocator.buffer(valueSize);
    zeroVector();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void zeroVector() {
    data.setZero(0, data.capacity());
  }

  @Override
  public int load(int valueCount, ByteBuf buf) {
    clear();
    this.valueCount = valueCount;
    int len = getSizeFromCount(valueCount);
    data = buf.slice(0, len);
    data.retain();
    return len;
  }

  public void copyFrom(int inIndex, int outIndex, BitVector from) {
    this.mutator.set(outIndex, from.accessor.get(inIndex));
  }

  public boolean copyFromSafe(int inIndex, int outIndex, BitVector from){
    if(outIndex >= this.getValueCapacity()) {
      decrementAllocationMonitor();
      return false;
    }
    copyFrom(inIndex, outIndex, from);
    return true;
  }

  @Override
  public void load(SerializedField metadata, ByteBuf buffer) {
    assert this.field.matches(metadata);
    int loaded = load(metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded;
  }

  @Override
  public int getValueCapacity() {
    return valueCapacity;
  }

  public Mutator getMutator() {
    return new Mutator();
  }

  public Accessor getAccessor() {
    return new Accessor();
  }

  public TransferPair getTransferPair(){
    return new TransferImpl(getField());
  }
  public TransferPair getTransferPair(FieldReference ref){
    return new TransferImpl(getField().clone(ref));
  }

  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((BitVector) to);
  }


  public void transferTo(BitVector target) {
    target.data = data;
    target.data.retain();
    target.valueCount = valueCount;
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, BitVector target) {
    assert startIndex + length <= valueCount;
    int firstByte = getByteIndex(startIndex);
    int byteSize = getSizeFromCount(length);
    int offset = startIndex % 8;
    if (offset == 0) {
      // slice
      target.data = this.data.slice(firstByte, byteSize);
      target.data.retain();
    } else {
      // Copy data
      // When the first bit starts from the middle of a byte (offset != 0), copy data from src BitVector.
      // Each byte in the target is composed by a part in i-th byte, another part in (i+1)-th byte.
      // The last byte copied to target is a bit tricky :
      //   1) if length requires partly byte ( length % 8 !=0), copy the remaining bits only.
      //   2) otherwise, copy the last byte in the same way as to the prior bytes.
      target.clear();
      target.allocateNew(length);
      // TODO maybe do this one word at a time, rather than byte?
      for (int i = 0; i < byteSize - 1; i++) {
        target.data.setByte(i, (((this.data.getByte(firstByte + i) & 0xFF) >>> offset) + (this.data.getByte(firstByte + i + 1) <<  (8 - offset))));
      }
      if (length % 8 != 0)
        target.data.setByte(byteSize - 1, ((this.data.getByte(firstByte + byteSize - 1) & 0xFF) >>> offset));
      else
        target.data.setByte(byteSize - 1,
            (((this.data.getByte(firstByte + byteSize - 1) & 0xFF) >>> offset) + (this.data.getByte(firstByte + byteSize) <<  (8 - offset))));
    }
  }


  private class TransferImpl implements TransferPair {
    BitVector to;

    public TransferImpl(MaterializedField field) {
      this.to = new BitVector(field, allocator);
    }

    public TransferImpl(BitVector to) {
      this.to = to;
    }

    public BitVector getTo() {
      return to;
    }

    public void transfer() {
      transferTo(to);
    }

    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public boolean copyValueSafe(int fromIndex, int toIndex) {
      return to.copyFromSafe(fromIndex, toIndex, BitVector.this);
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

    public boolean isNull(int index){
      return false;
    }

    @Override
    public final Boolean getObject(int index) {
      return new Boolean(get(index) != 0);
    }

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

    @Override
    public FieldReader getReader() {
      return new BitReaderImpl(BitVector.this);
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

    public boolean setSafe(int index, int value) {
      if(index >= getValueCapacity()) {
        decrementAllocationMonitor();
        return false;
      }
      set(index, value);
      return true;
    }

    public boolean setSafe(int index, BitHolder holder) {
      if(index >= getValueCapacity()) {
        decrementAllocationMonitor();
        return false;
      }
      set(index, holder.value);
      return true;
    }

    public boolean setSafe(int index, NullableBitHolder holder) {
      if(index >= getValueCapacity()) {
        decrementAllocationMonitor();
        return false;
      }
      set(index, holder.value);
      return true;
    }

    public final void setValueCount(int valueCount) {
      int currentValueCapacity = getValueCapacity();
      BitVector.this.valueCount = valueCount;
      int idx = getSizeFromCount(valueCount);
      if (valueCount > 0 && currentValueCapacity > valueCount * 2) {
        incrementAllocationMonitor();
      } else if (allocationMonitor > 0) {
        allocationMonitor = 0;
      }
      data.writerIndex(idx);
      if (data instanceof AccountingByteBuf) {
        data.capacity(idx);
        data.writerIndex(idx);
      }
    }

    @Override
    public final void generateTestData(int values) {
      boolean even = true;
      for (int i = 0; i < values; i++, even = !even) {
        if (even) {
          set(i, 1);
        }
      }
      setValueCount(values);
    }

  }
}