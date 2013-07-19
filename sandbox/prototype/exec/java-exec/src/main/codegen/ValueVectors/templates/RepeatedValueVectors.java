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
import java.util.Vector;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;

@SuppressWarnings("unused")
/**
 * Repeated${minor.class} implements a vector with multple values per row (e.g. JSON array or
 * repeated protobuf field).  The implementation uses two additional value vectors; one to convert
 * the index offset to the underlying element offset, and another to store the number of values
 * in the vector.
 *
 * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
 */

 public final class Repeated${minor.class}Vector extends BaseValueVector implements Repeated<#if type.major == "VarLen">VariableWidth<#else>FixedWidth</#if>Vector {

  private MaterializedField field;
  
  private int parentValueCount;
  private int childValueCount;
  
  private final UInt2Vector counts;    // number of repeated elements in each record
  private final UInt4Vector offsets;   // offsets to start of each record
  private final ${minor.class}Vector values;
  private final Mutator mutator = new Mutator();
  private final Accessor accessor = new Accessor();
  
  
  public Repeated${minor.class}Vector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    this.counts = new UInt2Vector(null, allocator);
    this.offsets = new UInt4Vector(null, allocator);
    this.values = new ${minor.class}Vector(null, allocator);
  }

  public int getValueCapacity(){
    return values.getValueCapacity();
  }
  
  int getBufferSize(){
    return counts.getBufferSize() + offsets.getBufferSize() + values.getBufferSize();
  }
  
  public TransferPair getTransferPair(){
    return new TransferImpl();
  }
  
  public void transferTo(Repeated${minor.class}Vector target){
    counts.transferTo(target.counts);
    offsets.transferTo(target.offsets);
    values.transferTo(target.values);
    target.parentValueCount = parentValueCount;
    target.childValueCount = childValueCount;
    clear();
  }
  
  private class TransferImpl implements TransferPair{
    Repeated${minor.class}Vector to;
    
    public TransferImpl(){
      this.to = new Repeated${minor.class}Vector(getField(), allocator);
    }
    
    public Repeated${minor.class}Vector getTo(){
      return to;
    }
    
    public void transfer(){
      transferTo(to);
    }
  }
  
  public void copyValue(int inIndex, int outIndex, Repeated${minor.class}Vector v){
    throw new UnsupportedOperationException();
  }

  
  <#if type.major == "VarLen">
  @Override
  public FieldMetadata getMetadata() {
    return FieldMetadata.newBuilder()
             .setDef(getField().getDef())
             .setGroupCount(this.parentValueCount)
             .setValueCount(this.childValueCount)
             .setVarByteLength(values.getVarByteLength())
             .setBufferLength(getBufferSize())
             .build();
  }
  
  public void allocateNew(int totalBytes, int parentValueCount, int childValueCount) {
    counts.allocateNew(parentValueCount);
    offsets.allocateNew(parentValueCount);
    values.allocateNew(totalBytes, childValueCount);
    mutator.reset();
    accessor.reset();
  }
  
  @Override
  public int load(int dataBytes, int parentValueCount, int childValueCount, ByteBuf buf){
    clear();
    this.parentValueCount = parentValueCount;
    this.childValueCount = childValueCount;
    int loaded = 0;
    loaded += counts.load(parentValueCount, buf);
    loaded += offsets.load(parentValueCount, buf.slice(loaded, buf.capacity() - loaded));
    loaded += values.load(dataBytes, childValueCount, buf.slice(loaded, buf.capacity() - loaded));
    return loaded;
  }
  
  @Override
  public void load(FieldMetadata metadata, ByteBuf buffer) {
    assert this.field.getDef().equals(metadata.getDef());
    int loaded = load(metadata.getVarByteLength(), metadata.getGroupCount(), metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded;
  }
  
  public int getByteCapacity(){
    return values.getByteCapacity();
  }

  <#else>
  
  @Override
  public FieldMetadata getMetadata() {
    return FieldMetadata.newBuilder()
             .setDef(getField().getDef())
             .setGroupCount(this.parentValueCount)
             .setValueCount(this.childValueCount)
             .setBufferLength(getBufferSize())
             .build();
  }
  
  public void allocateNew(int parentValueCount, int childValueCount) {
    clear();
    values.allocateNew(childValueCount);
    counts.allocateNew(parentValueCount);
    offsets.allocateNew(parentValueCount);
    mutator.reset();
    accessor.reset();
  }
  
  public int load(int parentValueCount, int childValueCount, ByteBuf buf){
    clear();
    this.parentValueCount = parentValueCount;
    this.childValueCount = childValueCount;
    int loaded = 0;
    loaded += counts.load(parentValueCount, buf);
    loaded += offsets.load(parentValueCount, buf.slice(loaded, buf.capacity() - loaded));
    loaded += values.load(childValueCount, buf.slice(loaded, buf.capacity() - loaded));
    return loaded;
  }
  
  @Override
  public void load(FieldMetadata metadata, ByteBuf buffer) {
    assert this.field.getDef().equals(metadata.getDef());
    int loaded = load(metadata.getGroupCount(), metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded;
  }
  </#if>
  
//  /**
//   * Get the size requirement (in bytes) for the given number of values.  Only accurate
//   * for fixed width value vectors.
//   */
//  public int getTotalSizeFromCount(int valueCount) {
//    return values.getSizeFromCount(valueCount) +
//           counts.getSizeFromCount(valueCount) +
//           offsets.getSizeFromCount(valueCount);
//  }
//  
//  public int getSizeFromCount(int valueCount){
//    return getTotalSizeFromCount(valueCount);
//  }

//  /**
//   * Get the explicitly specified size of the allocated buffer, if available.  Otherwise
//   * calculate the size based on width and record count.
//   */
//  public int getAllocatedSize() {
//    return values.getAllocatedSize() +
//           counts.getAllocatedSize() +
//           offsets.getAllocatedSize();
//  }



  public ByteBuf[] getBuffers() {
    return new ByteBuf[]{counts.data, offsets.data, values.data};
  }

  public void clear(){
    counts.clear();
    offsets.clear();
    values.clear();
    parentValueCount = 0;
    childValueCount = 0;
  }

  public Mutator getMutator(){
    return mutator;
  }
  
  public Accessor getAccessor(){
    return accessor;
  }
  
  public final class Accessor implements ValueVector.Accessor{
    /**
     * Get the elements at the given index.
     */
    public int getCount(int index) {
      return counts.getAccessor().get(index);
    }
    
    public Object getObject(int index) {
      throw new UnsupportedOperationException();
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

      assert positionIndex < counts.getAccessor().get(index);
      return values.getAccessor().get(offsets.getAccessor().get(index) + positionIndex);
    }

    public MaterializedField getField() {
      return field;
    }
    
    public int getGroupCount(){
      return parentValueCount;
    }
    
    public int getValueCount(){
      return childValueCount;
    }
    
    public void reset(){
      
    }
  }
  
  public final class Mutator implements ValueVector.Mutator{

    
    private Mutator(){
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
      counts.getMutator().set(index, counts.getAccessor().get(index) + 1);
      offsets.getMutator().set(index, offsets.getAccessor().get(index - 1) + counts.getAccessor().get(index-1));
      values.getMutator().set(offsets.getAccessor().get(index), value);
    }

    
    public void setGroupAndValueCount(int groupCount, int valueCount) {
      parentValueCount = groupCount;
      childValueCount = valueCount;
      counts.getMutator().setValueCount(groupCount);
      offsets.getMutator().setValueCount(groupCount);
      values.getMutator().setValueCount(valueCount);
    }
    
    public void randomizeData(){
      throw new UnsupportedOperationException();
    }
    
    public void reset(){
      
    }
    
  }
}
</#list>
</#list>