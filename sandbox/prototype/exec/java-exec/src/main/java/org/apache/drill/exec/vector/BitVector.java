package org.apache.drill.exec.vector;

import io.netty.buffer.ByteBuf;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;

/**
 * Bit implements a vector of bit-width values. Elements in the vector are accessed by position from the logical start
 * of the vector. The width of each element is 1 bit. The equivalent Java primitive is an int containing the value '0'
 * or '1'.
 */
public final class BitVector extends BaseDataValueVector implements FixedWidthVector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitVector.class);

  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();

  private int valueCapacity;

  public BitVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  @Override
  public FieldMetadata getMetadata() {
    return FieldMetadata.newBuilder()
        .setDef(getField().getDef())
        .setValueCount(valueCount)
        .setBufferLength( (int) Math.ceil(valueCount / 8.0))
        .build();
  }

  private int getSizeFromCount(int valueCount) {
    return (int) Math.ceil((float)valueCount / 8.0);
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
    this.data.retain();
    for (int i = 0; i < valueSize; i++) {
      data.setByte(i, 0);
    }
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
    if(outIndex >= this.getValueCapacity()) return false;
    copyFrom(inIndex, outIndex, from);
    return true;
  }

  @Override
  public void load(FieldMetadata metadata, ByteBuf buffer) {
    assert this.field.getDef().equals(metadata.getDef());
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


  public void transferTo(BitVector target) {
    target.data = data;
    target.data.retain();
    target.valueCount = valueCount;
    clear();
  }

  private class TransferImpl implements TransferPair {
    BitVector to;

    public TransferImpl(MaterializedField field) {
      this.to = new BitVector(field, allocator);
    }

    public BitVector getTo() {
      return to;
    }

    public void transfer() {
      transferTo(to);
    }

    @Override
    public void copyValue(int fromIndex, int toIndex) {
      to.copyFrom(fromIndex, toIndex, BitVector.this);
    }
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
      // logger.debug("BIT GET: index: {}, byte: {}, mask: {}, masked byte: {}",
      // index,
      // data.getByte((int)Math.floor(index/8)),
      // (int)Math.pow(2, (index % 8)),
      // data.getByte((int)Math.floor(index/8)) & (int)Math.pow(2, (index % 8)));
      return ((data.getByte((int) Math.floor(index / 8)) & (int) Math.pow(2, (index % 8))) == 0) ? 0 : 1;
    }

    @Override
    public final Object getObject(int index) {
      return new Boolean(get(index) != 0);
    }

    public final int getValueCount() {
      return valueCount;
    }

    public final void get(int index, BitHolder holder) {
      holder.value = get(index);
    }

    final void get(int index, NullableBitHolder holder) {
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
      byte currentByte = data.getByte((int) Math.floor(index / 8));
      if (value != 0) {
        // true
        currentByte |= (byte) Math.pow(2, (index % 8));
      } else if ((currentByte & (byte) Math.pow(2, (index % 8))) == (byte) Math.pow(2, (index % 8))) {
        // false, and bit was previously set
        currentByte -= (byte) Math.pow(2, (index % 8));
      }
      data.setByte((int) Math.floor(index / 8), currentByte);
    }

    public final void set(int index, BitHolder holder) {
      set(index, holder.value);
    }

    final void set(int index, NullableBitHolder holder) {
      set(index, holder.value);
    }
    
    public boolean setSafe(int index, int value) {
      if(index >= getValueCapacity()) return false;
      set(index, value);
      return true;
    }

    public final void setValueCount(int valueCount) {
      BitVector.this.valueCount = valueCount;
      data.writerIndex(getSizeFromCount(valueCount));
    }

    @Override
    public final void generateTestData() {
      boolean even = true;
      for (int i = 0; i < valueCapacity; i++, even = !even) {
        if (even) {
          set(i, 1);
        }
      }
    }

  }
}