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

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;
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
public final class Nullable${minor.class}Vector extends ValueVector {

  private final BitVector bits;
  private final ${minor.class}Vector values;

  public Nullable${minor.class}Vector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    bits = new BitVector(null, allocator);
    values = new ${minor.class}Vector(null, allocator);
  }

  /**
   * Get the element at the specified position.
   *
   * @param   index   position of the value
   * @return  value of the element, if not null
   * @throws  NullValueException if the value is null
   */
  public <#if type.major == "VarLen">byte[]<#else>${minor.javaType!type.javaType}</#if> get(int index) {
    assert !isNull(index);
    return values.get(index);
  }


  public boolean isNull(int index) {
    return bits.get(index) == 0;
  }

  public int isSet(int index){
    return bits.get(index);
  }
  
  /**
   * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
   *
   * @param valueCount   The number of values which may be contained by this vector.
   */
  public void allocateNew(int totalBytes, ByteBuf sourceBuffer, int valueCount) {
    values.allocateNew(totalBytes, sourceBuffer, valueCount);
    bits.allocateNew(valueCount);
  }

  @Override
  public int getAllocatedSize() {
    return bits.getAllocatedSize() + values.getAllocatedSize();
  }

  /**
   * Get the size requirement (in bytes) for the given number of values.  Only accurate
   * for fixed width value vectors.
   */
  public int getTotalSizeFromCount(int valueCount) {
    return values.getSizeFromCount(valueCount) + bits.getSizeFromCount(valueCount);
  }
  
  public int getSizeFromCount(int valueCount){
    return getTotalSizeFromCount(valueCount);
  }

  @Override
  public MaterializedField getField() {
    return field;
  }

  @Override
  public ByteBuf[] getBuffers() {
    return new ByteBuf[]{bits.data, values.data};
  }


  @Override
  public Object getObject(int index) {
    return isNull(index) ? null : values.getObject(index);
  }
  
  public Mutator getMutator(){
    return new Mutator();
  }
  
  public class Mutator implements ValueVector.Mutator{

    private final BitVector.Mutator bitMutator;
    private final ${minor.class}Vector.Mutator valueMutator;
    
    private Mutator(){
      bitMutator = bits.getMutator();
      valueMutator = values.getMutator();
    }

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param bytes   array of bytes to write
     */
    public void set(int index, <#if type.major == "VarLen">byte[]<#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value) {
      setNotNull(index);
      valueMutator.set(index, value);
    }

    public void setNull(int index) {
      bitMutator.set(index, 0);
    }

    private void setNotNull(int index) {
      bitMutator.set(index, 1);
    }
    
    @Override
    public void setRecordCount(int recordCount) {
      Nullable${minor.class}Vector.this.setRecordCount(recordCount);
      bits.setRecordCount(recordCount);
    }
    
    public void randomizeData(){
      throw new UnsupportedOperationException();
    }
    
  }
}
</#list>
</#list>