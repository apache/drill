<@pp.dropOutputFile />
<#list types as type>
<#list type.minor as minor>
<@pp.changeOutputFile name="Nullable${minor.class}Vector.java" />
package org.apache.drill.exec.vector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.util.Random;
import java.util.Vector;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.UInt2Vector;
import org.apache.drill.exec.vector.UInt4Vector;

/**
 * Nullable${minor.class} implements a vector of values which could be null.  Elements in the vector
 * are first checked against a fixed length vector of boolean values.  Then the element is retrieved
 * from the base class (if not null).
 *
 * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
 */
@SuppressWarnings("unused")
public final class Nullable${minor.class}Vector extends BaseValueVector implements <#if type.major == "VarLen">VariableWidth<#else>FixedWidth</#if>Vector {

  private int valueCount;
  private final BitVector bits;
  private final ${minor.class}Vector values;
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();

  public Nullable${minor.class}Vector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    this.bits = new BitVector(null, allocator);
    this.values = new ${minor.class}Vector(null, allocator);
  }
  
  public int getValueCapacity(){
    return bits.getValueCapacity();
  }
  
  @Override
  public ByteBuf[] getBuffers() {
    return new ByteBuf[]{bits.data, values.data};
  }

  @Override
  public void clear() {
    valueCount = 0;
    bits.clear();
    values.clear();
  }
  
  int getBufferSize(){
    return values.getBufferSize() + bits.getBufferSize();
  }

  <#if type.major == "VarLen">
  @Override
  public FieldMetadata getMetadata() {
    return FieldMetadata.newBuilder()
             .setDef(getField().getDef())
             .setValueCount(valueCount)
             .setVarByteLength(values.getVarByteLength())
             .setBufferLength(getBufferSize())
             .build();
  }
  
  @Override
  public void allocateNew(int totalBytes, int valueCount) {
    values.allocateNew(totalBytes, valueCount);
    bits.allocateNew(valueCount);
    mutator.reset();
    accessor.reset();
  }

  @Override
  public int load(int dataBytes, int valueCount, ByteBuf buf){
    clear();
    this.valueCount = valueCount;
    int loaded = bits.load(valueCount, buf);
    
    // remove bits part of buffer.
    buf = buf.slice(loaded, buf.capacity() - loaded);
    loaded += values.load(dataBytes, valueCount, buf);
    return loaded;
  }
  
  @Override
  public void load(FieldMetadata metadata, ByteBuf buffer) {
    assert this.field.getDef().equals(metadata.getDef());
    int loaded = load(metadata.getVarByteLength(), metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded;
  }
  
  @Override
  public int getByteCapacity(){
    return values.getByteCapacity();
  }

  <#else>
  @Override
  public FieldMetadata getMetadata() {
    return FieldMetadata.newBuilder()
             .setDef(getField().getDef())
             .setValueCount(valueCount)
             .setBufferLength(getBufferSize())
             .build();
  }
  
  @Override
  public void allocateNew(int valueCount) {
    values.allocateNew(valueCount);
    bits.allocateNew(valueCount);
    mutator.reset();
    accessor.reset();
  }
  
  @Override
  public int load(int valueCount, ByteBuf buf){
    clear();
    this.valueCount = valueCount;
    int loaded = bits.load(valueCount, buf);
    
    // remove bits part of buffer.
    buf = buf.slice(loaded, buf.capacity() - loaded);
    loaded += values.load(valueCount, buf);
    return loaded;
  }
  
  @Override
  public void load(FieldMetadata metadata, ByteBuf buffer) {
    assert this.field.getDef().equals(metadata.getDef());
    int loaded = load(metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded;
  }
  
  </#if>
  
  public TransferPair getTransferPair(){
    return new TransferImpl();
  }
  
  public void transferTo(Nullable${minor.class}Vector target){
    bits.transferTo(target.bits);
    values.transferTo(target.values);
    target.valueCount = valueCount;
    clear();
  }
  
  private class TransferImpl implements TransferPair{
    Nullable${minor.class}Vector to;
    
    public TransferImpl(){
      this.to = new Nullable${minor.class}Vector(getField(), allocator);
    }
    
    public Nullable${minor.class}Vector getTo(){
      return to;
    }
    
    public void transfer(){
      transferTo(to);
    }
  }
  
  public Accessor getAccessor(){
    return accessor;
  }
  
  public Mutator getMutator(){
    return mutator;
  }
  
  public ${minor.class}Vector convertToRequiredVector(){
    ${minor.class}Vector v = new ${minor.class}Vector(getField().getOtherNullableVersion(), allocator);
    v.data = values.data;
    v.valueCount = this.valueCount;
    v.data.retain();
    clear();
    return v;
  }

  
  public void copyValue(int inIndex, int outIndex, Nullable${minor.class}Vector v){
    bits.copyValue(inIndex, outIndex, v.bits);
    values.copyValue(inIndex, outIndex, v.values);
  }
  
  public final class Accessor implements ValueVector.Accessor{

    /**
     * Get the element at the specified position.
     *
     * @param   index   position of the value
     * @return  value of the element, if not null
     * @throws  NullValueException if the value is null
     */
    public <#if type.major == "VarLen">byte[]<#else>${minor.javaType!type.javaType}</#if> get(int index) {
      assert !isNull(index);
      return values.getAccessor().get(index);
    }

    public boolean isNull(int index) {
      return isSet(index) == 0;
    }

    public int isSet(int index){
      return bits.getAccessor().get(index);
    }
    
    @Override
    public Object getObject(int index) {
      return isNull(index) ? null : values.getAccessor().getObject(index);
    }
    
    public int getValueCount(){
      return valueCount;
    }
    
    public void reset(){}
  }
  
  public final class Mutator implements NonRepeatedMutator{
    
    private int setCount;
    
    private Mutator(){
    }

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param bytes   array of bytes to write
     */
    public void set(int index, <#if type.major == "VarLen">byte[]<#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value) {
      setCount++;
      bits.getMutator().set(index, 1);
      values.getMutator().set(index, value);
    }

    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      Nullable${minor.class}Vector.this.valueCount = valueCount;
      values.getMutator().setValueCount(valueCount);
      bits.getMutator().setValueCount(valueCount);
    }
    
    public boolean noNulls(){
      return valueCount == setCount;
    }
    
    public void randomizeData(){
      throw new UnsupportedOperationException();
    }
    
    public void reset(){
      setCount = 0;
    }
    
  }
}
</#list>
</#list>