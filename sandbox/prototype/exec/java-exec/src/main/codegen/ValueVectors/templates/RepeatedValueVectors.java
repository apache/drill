import org.apache.drill.exec.vector.UInt2Vector;
import org.apache.drill.exec.vector.UInt4Vector;

<@pp.dropOutputFile />
<#list types as type>
<#list type.minor as minor>
<@pp.changeOutputFile name="Repeated${minor.class}Vector.java" />
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

@SuppressWarnings("unused")
/**
 * Repeated${minor.class} implements a vector with multple values per row (e.g. JSON array or
 * repeated protobuf field).  The implementation uses two additional value vectors; one to convert
 * the index offset to the underlying element offset, and another to store the number of values
 * in the vector.
 *
 * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
 */

 public final class Repeated${minor.class}Vector extends ValueVector {

  private final UInt2Vector countVector;    // number of repeated elements in each record
  private final UInt4Vector offsetVector;   // offsets to start of each record
  private final ${minor.class}Vector valuesVector;

  public Repeated${minor.class}Vector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    this.countVector = new UInt2Vector(null, allocator);
    this.offsetVector = new UInt4Vector(null, allocator);
    this.valuesVector = new ${minor.class}Vector(null, allocator);
  }

  public void allocateNew(int totalBytes, ByteBuf sourceBuffer, int valueCount) {
    super.allocateNew(totalBytes, sourceBuffer, valueCount);
    countVector.allocateNew(valueCount);
    offsetVector.allocateNew(valueCount);
  }


  /**
   * Get a value for the given record.  Each element in the repeated field is accessed by
   * the positionIndex param.
   *
   * @param  index           record containing the repeated field
   * @param  positionIndex   position within the repeated field
   * @return element at the given position in the given record
   */
  public <#if type.major == "VarLen">byte[]
         <#else>${minor.javaType!type.javaType}
         </#if> get(int index, int positionIndex) {

    assert positionIndex < countVector.get(index);
    return valuesVector.get(offsetVector.get(index) + positionIndex);
  }

  public MaterializedField getField() {
    return field;
  }

  /**
   * Get the size requirement (in bytes) for the given number of values.  Only accurate
   * for fixed width value vectors.
   */
  public int getTotalSizeFromCount(int valueCount) {
    return valuesVector.getSizeFromCount(valueCount) +
           countVector.getSizeFromCount(valueCount) +
           offsetVector.getSizeFromCount(valueCount);
  }
  
  public int getSizeFromCount(int valueCount){
    return getTotalSizeFromCount(valueCount);
  }

  /**
   * Get the explicitly specified size of the allocated buffer, if available.  Otherwise
   * calculate the size based on width and record count.
   */
  public int getAllocatedSize() {
    return valuesVector.getAllocatedSize() +
           countVector.getAllocatedSize() +
           offsetVector.getAllocatedSize();
  }

  /**
   * Get the elements at the given index.
   */
  public int getCount(int index) {
    return countVector.get(index);
  }

  public ByteBuf[] getBuffers() {
    return new ByteBuf[]{countVector.data, offsetVector.data, data};
  }

  public Object getObject(int index) {
    return data.slice(index, getSizeFromCount(countVector.get(index)));
  }

  public Mutator getMutator(){
    return new Mutator();
  }
  
  public class Mutator implements ValueVector.Mutator{

    
    private final UInt2Vector.Mutator countMutator;
    private final ${minor.class}Vector.Mutator valuesMutator;
    private final UInt4Vector.Mutator offsetMutator;
    
    private Mutator(){
      this.countMutator = countVector.getMutator();
      this.offsetMutator = offsetVector.getMutator();
      this.valuesMutator = valuesVector.getMutator();
    }

    /**
     * Add an element to the given record index.  This is similar to the set() method in other
     * value vectors, except that it permits setting multiple values for a single record.
     *
     * @param index   record of the element to add
     * @param value   value to add to the given row
     */
    public void add(int index, <#if (type.width > 4)> ${minor.javaType!type.javaType}
                               <#elseif type.major == "VarLen"> byte[]
                               <#else> int
                               </#if> value) {
      countMutator.set(index, countVector.get(index) + 1);
      offsetMutator.set(index, offsetVector.get(index - 1) + countVector.get(index-1));
      valuesMutator.set(offsetVector.get(index), value);
    }
    
    public void setRecordCount(int recordCount) {
      valuesMutator.setRecordCount(recordCount);
      offsetMutator.setRecordCount(recordCount);
      countMutator.setRecordCount(recordCount);
    }
    
    public void randomizeData(){
      throw new UnsupportedOperationException();
    }
    
  }
}
</#list>
</#list>