<@pp.dropOutputFile />
<#list types as type>
<#list type.minor as minor>

<#if type.major == "Fixed">
<@pp.changeOutputFile name="${minor.class}Vector.java" />
package org.apache.drill.exec.vector;

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.MsgPack2Vector;

import java.util.Random;

/**
 * ${minor.class} implements a vector of fixed width values.  Elements in the vector are accessed
 * by position, starting from the logical start of the vector.  Values should be pushed onto the
 * vector sequentially, but may be randomly accessed.
 *   The width of each element is ${type.width} byte(s)
 *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
 *
 * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
 */
@SuppressWarnings("unused")
public final class ${minor.class}Vector extends BaseDataValueVector implements FixedWidthVector{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}Vector.class);

 
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  
  public ${minor.class}Vector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  public int getValueCapacity(){
    return (int) (data.capacity() *1.0 / ${type.width});
  }

  public Accessor getAccessor(){
    return accessor;
  }
  
  public Mutator getMutator(){
    return mutator;
  }
  
  

  /**
   * Allocate a new buffer that supports setting at least the provided number of values.  May actually be sized bigger depending on underlying buffer rounding size. Must be called prior to using the ValueVector.
   * @param valueCount
   */
  public void allocateNew(int valueCount) {
    clear();
    this.data = allocator.buffer(valueCount * ${type.width});
    this.data.retain();
    this.data.readerIndex(0);
  }
  
  @Override
  public FieldMetadata getMetadata() {
    return FieldMetadata.newBuilder()
             .setDef(getField().getDef())
             .setValueCount(valueCount)
             .setBufferLength(valueCount * ${type.width})
             .build();
  }

  @Override
  public int load(int valueCount, ByteBuf buf){
    clear();
    this.valueCount = valueCount;
    int len = valueCount * ${type.width};
    data = buf.slice(0, len);
    data.retain();
    return len;
  }
  
  @Override
  public void load(FieldMetadata metadata, ByteBuf buffer) {
    assert this.field.getDef().equals(metadata.getDef());
    int loaded = load(metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded;
  }
  
  public TransferPair getTransferPair(){
    return new TransferImpl();
  }
  
  public void transferTo(${minor.class}Vector target){
    target.data = data;
    target.data.retain();
    target.valueCount = valueCount;
    clear();
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
  
  public void copyValue(int inIndex, int outIndex, ${minor.class}Vector v){
    <#if (type.width > 8)>
    data.getBytes(inIndex * ${type.width}, v.data, outIndex * ${type.width}, ${type.width});
    <#else> <#-- type.width <= 8 -->
    data.set${(minor.javaType!type.javaType)?cap_first}(outIndex * ${type.width}, 
        data.get${(minor.javaType!type.javaType)?cap_first}(inIndex * ${type.width})
    );
    </#if> <#-- type.width -->
  }
  
  public final class Accessor extends BaseValueVector.BaseAccessor{

    public int getValueCount() {
      return valueCount;
    }
    
    <#if (type.width > 8)>

    public ${minor.javaType!type.javaType} get(int index) {
      ByteBuf dst = allocator.buffer(${type.width});
      data.getBytes(index * ${type.width}, dst, 0, ${type.width});
      return dst;
    }

    @Override
    public Object getObject(int index) {
      ByteBuf dst = allocator.buffer(${type.width});
      data.getBytes(index, dst, 0, ${type.width});
      return dst;
    }

    <#else> <#-- type.width <= 8 -->

    public ${minor.javaType!type.javaType} get(int index) {
      return data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});
    }

    public Object getObject(int index) {
      return get(index);
    }


   </#if> <#-- type.width -->
 }
 
 /**
  * ${minor.class}.Mutator implements a mutable vector of fixed width values.  Elements in the
  * vector are accessed by position from the logical start of the vector.  Values should be pushed
  * onto the vector sequentially, but may be randomly accessed.
  *   The width of each element is ${type.width} byte(s)
  *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
  *
  * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
  */
  public final class Mutator extends BaseValueVector.BaseMutator{

    private Mutator(){};
   /**
    * Set the element at the given index to the given value.  Note that widths smaller than
    * 32 bits are handled by the ByteBuf interface.
    *
    * @param index   position of the bit to set
    * @param value   value to set
    */
  <#if (type.width > 8)>
   public void set(int index, <#if (type.width > 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
     data.setBytes(index * ${type.width}, value);
   }
   
   @Override
   public void randomizeData() {
     if (data != DeadBuf.DEAD_BUFFER) {
       Random r = new Random();
       for(int i =0; i < data.capacity()-${type.width}; i += ${type.width}){
         byte[] bytes = new byte[${type.width}];
         r.nextBytes(bytes);
         data.setByte(i, bytes[0]);
       }
     }
   }
   
  <#else> <#-- type.width <= 8 -->
   public void set(int index, <#if (type.width >= 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
     data.set${(minor.javaType!type.javaType)?cap_first}(index * ${type.width}, value);
   }
   
   @Override
   public void randomizeData() {
     if (data != DeadBuf.DEAD_BUFFER) {
       Random r = new Random();
       for(int i =0; i < data.capacity()-${type.width}; i += ${type.width}){
         data.set${(minor.javaType!type.javaType)?cap_first}(i,
             r.next<#if (type.width >= 4)>${(minor.javaType!type.javaType)?cap_first}
                   <#else>Int
                   </#if>());
       }
     }
   }
  </#if> <#-- type.width -->
  
   public void setValueCount(int valueCount) {
     ${minor.class}Vector.this.valueCount = valueCount;
     data.writerIndex(${type.width} * valueCount);
   }




  
 }
}

</#if> <#-- type.major -->
</#list>
</#list>