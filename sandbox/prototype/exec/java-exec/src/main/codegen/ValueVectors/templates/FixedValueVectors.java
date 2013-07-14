<@pp.dropOutputFile />
<#list types as type>
<#list type.minor as minor>

<#if type.major == "Fixed">
<@pp.changeOutputFile name="${minor.class}Vector.java" />
package org.apache.drill.exec.vector;

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;
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
public final class ${minor.class}Vector extends ValueVector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}Vector.class);

  public ${minor.class}Vector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  /**
   * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
   *
   * @param valueCount
   *          The number of values which can be contained within this vector.
   */
  public void allocateNew(int valueCount) {
    totalBytes = valueCount * ${type.width};
    allocateNew(totalBytes, allocator.buffer(totalBytes), valueCount);
  }

  @Override
  public int getAllocatedSize() {
    return (int) Math.ceil(totalBytes);
  }

  /**
   * Get the size requirement (in bytes) for the given number of values.  Only accurate
   * for fixed width value vectors.
   */
  @Override
  public int getSizeFromCount(int valueCount) {
    return valueCount * ${type.width};
  }

  public Mutator getMutator() {
    return new Mutator();
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
 
 
 /**
  * ${minor.class}.Mutator implements a mutable vector of fixed width values.  Elements in the
  * vector are accessed by position from the logical start of the vector.  Values should be pushed
  * onto the vector sequentially, but may be randomly accessed.
  *   The width of each element is ${type.width} byte(s)
  *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
  *
  * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
  */
  public class Mutator implements ValueVector.Mutator{

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
  
   @Override
   public void setRecordCount(int recordCount) {
     ${minor.class}Vector.this.setRecordCount(recordCount);
   }



  
 }
}

</#if> <#-- type.major -->
</#list>
</#list>