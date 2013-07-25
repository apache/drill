<@pp.dropOutputFile />
<#list types as type>
<#list type.minor as minor>

<#if type.major == "VarLen">
<@pp.changeOutputFile name="${minor.class}Vector.java" />
package org.apache.drill.exec.vector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.vector.ByteHolder;

import antlr.collections.impl.Vector;

/**
 * ${minor.class}Vector implements a vector of variable width values.  Elements in the vector
 * are accessed by position from the logical start of the vector.  A fixed width offsetVector
 * is used to convert an element's position to it's offset from the start of the (0-based)
 * ByteBuf.  Size is inferred by adjacent elements.
 *   The width of each element is ${type.width} byte(s)
 *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
 *
 * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
 */
@SuppressWarnings("unused")
public final class ${minor.class}Vector extends BaseDataValueVector implements VariableWidthVector{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}Vector.class);

  private final UInt${type.width}Vector offsetVector;
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  
  public ${minor.class}Vector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    this.offsetVector = new UInt${type.width}Vector(null, allocator);
  }


  int getSizeFromCount(int valueCount) {
    return valueCount * ${type.width};
  }
  
  public int getValueCapacity(){
    return offsetVector.getValueCapacity();
  }
  
  public int getByteCapacity(){
    return data.capacity(); 
  }
  
  /**
   * Return the number of bytes contained in the current var len byte vector.
   * @return
   */
  public int getVarByteLength(){
    return offsetVector.getAccessor().get(valueCount); 
  }
  
  @Override
  public FieldMetadata getMetadata() {
    int len = valueCount * ${type.width} + getVarByteLength();
    return FieldMetadata.newBuilder()
             .setDef(getField().getDef())
             .setValueCount(valueCount)
             .setVarByteLength(getVarByteLength())
             .setBufferLength(len)
             .build();
  }

  public int load(int dataBytes, int valueCount, ByteBuf buf){
    this.valueCount = valueCount;
    int loaded = offsetVector.load(valueCount+1, buf);
    data = buf.slice(loaded, dataBytes);
    data.retain();
    return loaded + dataBytes;
  }
  
  @Override
  public void load(FieldMetadata metadata, ByteBuf buffer) {
    assert this.field.getDef().equals(metadata.getDef());
    int loaded = load(metadata.getVarByteLength(), metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded;
  }
  
  @Override
  public void clear() {
    super.clear();
    offsetVector.clear();
  }

  @Override
  public ByteBuf[] getBuffers() {
    return new ByteBuf[]{offsetVector.data, this.data};
  }
  
  public TransferPair getTransferPair(){
    return new TransferImpl();
  }
  
  public void transferTo(${minor.class}Vector target){
    this.offsetVector.transferTo(target.offsetVector);
    target.data = data;
    target.data.retain();
    target.valueCount = valueCount;
    clear();
  }
  
  public void copyValue(int inIndex, int outIndex, ${minor.class}Vector v){
    int start = offsetVector.data.get${(minor.javaType!type.javaType)?cap_first}(inIndex);
    int end =   offsetVector.data.get${(minor.javaType!type.javaType)?cap_first}(inIndex+1);
    int len = end - start;
    
    int outputStart = outIndex == 0 ? 0 : v.offsetVector.data.get${(minor.javaType!type.javaType)?cap_first}(outIndex * ${type.width});
    data.getBytes(start, v.data, outputStart, len);
    v.offsetVector.data.set${(minor.javaType!type.javaType)?cap_first}( (outIndex+1) * ${type.width}, len);
  }
  
  private class TransferImpl implements TransferPair{
    ${minor.class}Vector to;
    
    public TransferImpl(){
      this.to = new ${minor.class}Vector(getField(), allocator);
    }
    
    public ${minor.class}Vector getTo(){
      return to;
    }
    
    public void transfer(){
      transferTo(to);
    }
  }
  
  public void allocateNew(int totalBytes, int valueCount) {
    clear();
    assert totalBytes >= 0;
    data = allocator.buffer(totalBytes);
    data.retain();
    data.readerIndex(0);
    offsetVector.allocateNew(valueCount+1);
  }

  public Accessor getAccessor(){
    return accessor;
  }
  
  public Mutator getMutator() {
    return mutator;
  }
  
  public final class Accessor extends BaseValueVector.BaseAccessor{
    
    public byte[] get(int index) {
      assert index >= 0;
      int startIdx = offsetVector.getAccessor().get(index);
      int length = offsetVector.getAccessor().get(index + 1) - startIdx;
      assert length >= 0;
      byte[] dst = new byte[length];
      data.getBytes(startIdx, dst, 0, length);
      return dst;
    }
    
    public void get(int index, ByteHolder holder){
      assert index >= 0;
      holder.start = offsetVector.getAccessor().get(index);
      holder.length = offsetVector.getAccessor().get(index + 1) - holder.start;
      assert holder.length >= 0;
      holder.buffer = offsetVector.data;
    }
    
    public Object getObject(int index) {
      return get(index);
    }
    
    public int getValueCount() {
      return valueCount;
    }
  }
  
  /**
   * Mutable${minor.class} implements a vector of variable width values.  Elements in the vector
   * are accessed by position from the logical start of the vector.  A fixed width offsetVector
   * is used to convert an element's position to it's offset from the start of the (0-based)
   * ByteBuf.  Size is inferred by adjacent elements.
   *   The width of each element is ${type.width} byte(s)
   *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public final class Mutator extends BaseValueVector.BaseMutator{

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param bytes   array of bytes to write
     */
    public void set(int index, byte[] bytes) {
      assert index >= 0;
      int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + bytes.length);
      data.setBytes(currentOffset, bytes);
    }

    public void set(int index, int start, int length, ByteBuf buffer){
      assert index >= 0;
      int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + length);
      ByteBuf bb = buffer.slice(start, length);
      data.setBytes(currentOffset, bb);
    }

    public void setValueCount(int valueCount) {
      ${minor.class}Vector.this.valueCount = valueCount;
      data.writerIndex(valueCount * ${type.width});
      offsetVector.getMutator().setValueCount(valueCount+1);
    }

    @Override
    public void randomizeData(){}
  }
  
}


</#if> <#-- type.major -->
</#list>
</#list>