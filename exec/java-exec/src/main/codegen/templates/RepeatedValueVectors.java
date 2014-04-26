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
<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/Repeated${minor.class}Vector.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector;

<#include "/@includes/vv_imports.ftl" />


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

  private int parentValueCount;
  private int childValueCount;
  
  private final UInt4Vector offsets;   // offsets to start of each record
  private final ${minor.class}Vector values;
  private final Mutator mutator = new Mutator();
  private final Accessor accessor = new Accessor();
  
  
  public Repeated${minor.class}Vector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    this.offsets = new UInt4Vector(null, allocator);
    this.values = new ${minor.class}Vector(null, allocator);
  }

  public int getValueCapacity(){
    return Math.min(values.getValueCapacity(), offsets.getValueCapacity());
  }
  
  public int getBufferSize(){
    return offsets.getBufferSize() + values.getBufferSize();
  }

  public ByteBuf getData(){
      return values.getData();
  }
  
  public TransferPair getTransferPair(){
    return new TransferImpl(getField());
  }
  public TransferPair getTransferPair(FieldReference ref){
    return new TransferImpl(getField().clone(ref));
  }

  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((Repeated${minor.class}Vector) to);
  }
  
  public void transferTo(Repeated${minor.class}Vector target){
    offsets.transferTo(target.offsets);
    values.transferTo(target.values);
    target.parentValueCount = parentValueCount;
    target.childValueCount = childValueCount;
    clear();
  }
  
  private class TransferImpl implements TransferPair{
    Repeated${minor.class}Vector to;
    
    public TransferImpl(MaterializedField field){
      this.to = new Repeated${minor.class}Vector(field, allocator);
    }

    public TransferImpl(Repeated${minor.class}Vector to){
      this.to = to;
    }

    public Repeated${minor.class}Vector getTo(){
      return to;
    }
    
    public void transfer(){
      transferTo(to);
    }
    
    @Override
    public void copyValue(int fromIndex, int toIndex) {
      to.copyFrom(fromIndex, toIndex, Repeated${minor.class}Vector.this);
    }
  }

<#if type.major == "VarLen">
    public void copyFrom(int inIndex, int outIndex, Repeated${minor.class}Vector v){
      int count = v.getAccessor().getCount(inIndex);
      getMutator().startNewGroup(outIndex);
      for (int i = 0; i < count; i++) {
        getMutator().add(outIndex, v.getAccessor().get(inIndex, i));
      }
    }

    public boolean copyFromSafe(int inIndex, int outIndex, Repeated${minor.class}Vector v){
      int count = v.getAccessor().getCount(inIndex);
      getMutator().startNewGroup(outIndex);
      for (int i = 0; i < count; i++) {
        if (!getMutator().addSafe(outIndex, v.getAccessor().get(inIndex, i))) {
          return false;
        }
      }
      return true;
    }
<#else>

    public void copyFrom(int inIndex, int outIndex, Repeated${minor.class}Vector v){
        throw new UnsupportedOperationException();
    }

    public boolean copyFromSafe(int inIndex, int outIndex, Repeated${minor.class}Vector v){
        throw new UnsupportedOperationException();
    }
</#if>

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
    offsets.allocateNew(parentValueCount+1);
    offsets.getMutator().set(0,0);
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
    loaded += offsets.load(parentValueCount+1, buf.slice(loaded, buf.capacity() - loaded));
    loaded += values.load(dataBytes + 4*(childValueCount + 1), childValueCount, buf.slice(loaded, buf.capacity() - loaded));
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
    offsets.allocateNew(parentValueCount+1);
    values.allocateNew(childValueCount);
    mutator.reset();
    accessor.reset();
  }
  
  public int load(int parentValueCount, int childValueCount, ByteBuf buf){
    clear();
    this.parentValueCount = parentValueCount;
    this.childValueCount = childValueCount;
    int loaded = 0;
    loaded += offsets.load(parentValueCount+1, buf.slice(loaded, buf.capacity() - loaded));
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

  @Override
  public ByteBuf[] getBuffers() {
    ByteBuf[] buffers = ObjectArrays.concat(offsets.getBuffers(), values.getBuffers(), ByteBuf.class);
    clear();
    return buffers;
  }

  public void clear(){
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
      return offsets.getAccessor().get(index+1) - offsets.getAccessor().get(index);
    }
    
    public Object getObject(int index) {
      List<Object> vals = Lists.newArrayList();
      int start = offsets.getAccessor().get(index);
      int end = offsets.getAccessor().get(index+1);
      for(int i = start; i < end; i++){
        vals.add(values.getAccessor().getObject(i));
      }
      return vals;
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
      return values.getAccessor().get(offsets.getAccessor().get(index) + positionIndex);
    }
        
           
    public boolean isNull(int index){
      return false;
    }
    
    public void get(int index, Repeated${minor.class}Holder holder){
      holder.start = offsets.getAccessor().get(index);
      holder.end =  offsets.getAccessor().get(index+1);
      holder.vector = values;
    }

    public void get(int index, int positionIndex, ${minor.class}Holder holder) {
      int offset = offsets.getAccessor().get(index);
      assert offset >= 0;
      values.getAccessor().get(offset + positionIndex, holder);
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
  
  public final class Mutator implements RepeatedMutator {

    
    private Mutator(){
    }

    public void startNewGroup(int index) {
      offsets.getMutator().set(index+1, offsets.getAccessor().get(index));
    }

    /**
     * Add an element to the given record index.  This is similar to the set() method in other
     * value vectors, except that it permits setting multiple values for a single record.
     *
     * @param index   record of the element to add
     * @param value   value to add to the given row
     */
    public void add(int index, <#if type.major == "VarLen">byte[]<#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value) {
      int nextOffset = offsets.getAccessor().get(index+1);
      values.getMutator().set(nextOffset, value);
      offsets.getMutator().set(index+1, nextOffset+1);
    }

    <#if type.major == "VarLen">
    public boolean addSafe(int index, byte[] bytes) {
      return addSafe(index, bytes, 0, bytes.length);
    }

    public boolean addSafe(int index, byte[] bytes, int start, int length) {
      int nextOffset = offsets.getAccessor().get(index+1);
      boolean b1 = values.getMutator().setSafe(nextOffset, bytes, start, length);
      boolean b2 = offsets.getMutator().setSafe(index+1, nextOffset+1);
      return (b1 && b2);
    }
    </#if>

    public void add(int index, ${minor.class}Holder holder){
      int nextOffset = offsets.getAccessor().get(index+1);
      values.getMutator().set(nextOffset, holder);
      offsets.getMutator().set(index+1, nextOffset+1);
    }
    
    public void add(int index, Repeated${minor.class}Holder holder){
      
      ${minor.class}Vector.Accessor accessor = holder.vector.getAccessor();
      ${minor.class}Holder innerHolder = new ${minor.class}Holder();
      
      for(int i = holder.start; i < holder.end; i++){
        accessor.get(i, innerHolder);
        add(index, innerHolder);
      }
    }
    
    /**
     * Set the number of value groups in this repeated field.
     * @param groupCount Count of Value Groups.
     */
    public void setValueCount(int groupCount) {
      parentValueCount = groupCount;
      childValueCount = offsets.getAccessor().get(groupCount);
      offsets.getMutator().setValueCount(groupCount+1);
      values.getMutator().setValueCount(childValueCount);
    }
    
    public void generateTestData(final int valCount){
      int[] sizes = {1,2,0,6};
      int size = 0;
      int runningOffset = 0;
      for(int i =1; i < valCount+1; i++, size++){
        runningOffset += sizes[size % sizes.length];
        offsets.getMutator().set(i, runningOffset);  
      }
      values.getMutator().generateTestData(valCount*9);
      setValueCount(size);
    }
    
    public void reset(){
      
    }
    
  }
}
</#list>
</#list>