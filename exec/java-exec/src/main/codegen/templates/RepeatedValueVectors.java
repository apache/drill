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

import java.lang.Override;

import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.vector.RepeatedFixedWidthVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.mortbay.jetty.servlet.Holder;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />
<#assign fields = minor.fields!type.fields />

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
    MaterializedField mf = MaterializedField.create(field.getPath(), Types.required(field.getType().getMinorType()));
    this.values = new ${minor.class}Vector(mf, allocator);
  }

  public int getValueCapacity(){
    return Math.min(values.getValueCapacity(), offsets.getValueCapacity() - 1);
  }

  public int getCurrentValueCount() {
    return values.getCurrentValueCount();
  }

  public void setCurrentValueCount(int count) {
    values.setCurrentValueCount(offsets.getAccessor().get(count));
  }
  
  public int getBufferSize(){
    if(accessor.getGroupCount() == 0){
      return 0;
    }
    return offsets.getBufferSize() + values.getBufferSize();
  }

  public DrillBuf getData(){
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
    target.clear();
    offsets.transferTo(target.offsets);
    values.transferTo(target.values);
    target.parentValueCount = parentValueCount;
    target.childValueCount = childValueCount;
    clear();
  }

  public void splitAndTransferTo(final int startIndex, final int groups, Repeated${minor.class}Vector to) {
    final UInt4Vector.Accessor a = offsets.getAccessor();
    final UInt4Vector.Mutator m = to.offsets.getMutator();
    
    final int startPos = offsets.getAccessor().get(startIndex);
    final int endPos = offsets.getAccessor().get(startIndex + groups);
    final int valuesToCopy = endPos - startPos;
    
    values.splitAndTransferTo(startPos, valuesToCopy, to.values);
    to.offsets.clear();
    to.offsets.allocateNew(valuesToCopy + 1);
    int normalizedPos = 0;
    for (int i=0; i < groups + 1;i++ ) {
      normalizedPos = a.get(startIndex+i) - startPos;
      m.set(i, normalizedPos);
    }
    to.parentValueCount = groups;
    to.childValueCount  = valuesToCopy;
    m.setValueCount(groups == 0 ? 0 : groups + 1);
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

    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }
    
    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, Repeated${minor.class}Vector.this);
    }
  }

    public void copyFrom(int inIndex, int outIndex, Repeated${minor.class}Vector v){
      int count = v.getAccessor().getCount(inIndex);
      getMutator().startNewGroup(outIndex);
      for (int i = 0; i < count; i++) {
        getMutator().add(outIndex, v.getAccessor().get(inIndex, i));
      }
    }

    public void copyFromSafe(int inIndex, int outIndex, Repeated${minor.class}Vector v){
      int count = v.getAccessor().getCount(inIndex);
      getMutator().startNewGroup(outIndex);
      for (int i = 0; i < count; i++) {
        getMutator().addSafe(outIndex, v.getAccessor().get(inIndex, i));
      }
    }

  @Override
  public void setInitialCapacity(int numRecords) {
    offsets.setInitialCapacity(numRecords + 1);
    values.setInitialCapacity(numRecords * DEFAULT_REPEAT_PER_RECORD);
  }

  public boolean allocateNewSafe(){
    if(!offsets.allocateNewSafe()) return false;
    offsets.zeroVector();
    if(!values.allocateNewSafe()) return false;
    mutator.reset();
    accessor.reset();
    return true;
  }
  
  public void allocateNew() {
    offsets.allocateNew();
    offsets.zeroVector();
    values.allocateNew();
    mutator.reset();
    accessor.reset();
  }

  <#if type.major == "VarLen">
  @Override
  public SerializedField getMetadata() {
    return getMetadataBuilder() //
             .setGroupCount(this.parentValueCount) //
             .setValueCount(this.childValueCount) //
             .setVarByteLength(values.getVarByteLength()) //
             .setBufferLength(getBufferSize()) //
             .build();
  }
  
  public void allocateNew(int totalBytes, int parentValueCount, int childValueCount) {
    offsets.allocateNew(parentValueCount+1);
    offsets.zeroVector();
    values.allocateNew(totalBytes, childValueCount);
    mutator.reset();
    accessor.reset();
  }
  
  @Override
  public int load(int dataBytes, int parentValueCount, int childValueCount, DrillBuf buf){
    clear();
    this.parentValueCount = parentValueCount;
    this.childValueCount = childValueCount;
    int loaded = 0;
    loaded += offsets.load(parentValueCount+1, buf.slice(loaded, buf.capacity() - loaded));
    loaded += values.load(dataBytes + 4*(childValueCount + 1), childValueCount, buf.slice(loaded, buf.capacity() - loaded));
    return loaded;
  }
  
  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
    assert this.field.matches(metadata) : String.format("The field %s doesn't match the provided metadata %s.", this.field, metadata);
    int loaded = load(metadata.getVarByteLength(), metadata.getGroupCount(), metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded : String.format("Expected to load %d bytes but actually loaded %d bytes", metadata.getBufferLength(), loaded);
  }
  
  public int getByteCapacity(){
    return values.getByteCapacity();
  }

  <#else>
  
  @Override
  public SerializedField getMetadata() {
    return getMetadataBuilder()
             .setGroupCount(this.parentValueCount)
             .setValueCount(this.childValueCount)
             .setBufferLength(getBufferSize())
             .build();
  }
  
  public void allocateNew(int parentValueCount, int childValueCount) {
    clear();
    offsets.allocateNew(parentValueCount+1);
    offsets.zeroVector();
    values.allocateNew(childValueCount);
    mutator.reset();
    accessor.reset();
  }
  
  public int load(int parentValueCount, int childValueCount, DrillBuf buf){
    clear();
    this.parentValueCount = parentValueCount;
    this.childValueCount = childValueCount;
    int loaded = 0;
    loaded += offsets.load(parentValueCount+1, buf.slice(loaded, buf.capacity() - loaded));
    loaded += values.load(childValueCount, buf.slice(loaded, buf.capacity() - loaded));
    return loaded;
  }
  
  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
    assert this.field.matches(metadata);
    int loaded = load(metadata.getGroupCount(), metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded;
  }
  </#if>

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    DrillBuf[] buffers = ObjectArrays.concat(offsets.getBuffers(clear), values.getBuffers(clear), DrillBuf.class);
    if (clear) {
      clear();
    }
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

  // This is declared a subclass of the accessor declared inside of FixedWidthVector, this is also used for
  // variable length vectors, as they should ahve consistent interface as much as possible, if they need to diverge
  // in the future, the interface shold be declared in the respective value vector superclasses for fixed and variable
  // and we should refer to each in the generation template
  public final class Accessor implements RepeatedFixedWidthVector.RepeatedAccessor{
    
    final FieldReader reader = new Repeated${minor.class}ReaderImpl(Repeated${minor.class}Vector.this);
    
    public FieldReader getReader(){
      return reader;
    }
    
    /**
     * Get the elements at the given index.
     */
    public int getCount(int index) {
      return offsets.getAccessor().get(index+1) - offsets.getAccessor().get(index);
    }

    public ValueVector getAllChildValues() {
      return values;
    }

    public List<${friendlyType}> getObject(int index) {
      List<${friendlyType}> vals = new JsonStringArrayList();
      int start = offsets.getAccessor().get(index);
      int end = offsets.getAccessor().get(index+1);
      for(int i = start; i < end; i++){
        vals.add(values.getAccessor().getObject(i));
      }
      return vals;
    }

    public int getGroupSizeAtIndex(int index){
      return offsets.getAccessor().get(index+1) - offsets.getAccessor().get(index);
    }
    
    public ${friendlyType} getSingleObject(int index, int arrayIndex){
      int start = offsets.getAccessor().get(index);
      return values.getAccessor().getObject(start + arrayIndex);
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
      assert positionIndex < getCount(index);
      values.getAccessor().get(offset + positionIndex, holder);
    }
    
    public void get(int index, int positionIndex, Nullable${minor.class}Holder holder) {
      int offset = offsets.getAccessor().get(index);
      assert offset >= 0;
      if (positionIndex >= getCount(index)) {
        holder.isSet = 0;
        return;
      }
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

    public void setRepetitionAtIndexSafe(int index, int repetitionCount) {
      offsets.getMutator().setSafe(index+1, offsets.getAccessor().get(index) + repetitionCount);
    }

    public BaseDataValueVector getDataVector() {
      return values;
    }

    public void setValueCounts(int parentValueCount, int childValueCount){
      Repeated${minor.class}Vector.this.parentValueCount = parentValueCount;
      Repeated${minor.class}Vector.this.childValueCount = childValueCount;
      values.getMutator().setValueCount(childValueCount);
      offsets.getMutator().setValueCount(parentValueCount == 0 ? 0 : parentValueCount + 1);
    }

    public void startNewGroup(int index) {
      offsets.getMutator().setSafe(index+1, offsets.getAccessor().get(index));
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
    public void addSafe(int index, byte[] bytes) {
      addSafe(index, bytes, 0, bytes.length);
    }

    public void addSafe(int index, byte[] bytes, int start, int length) {
      int nextOffset = offsets.getAccessor().get(index+1);
      values.getMutator().setSafe(nextOffset, bytes, start, length);
      offsets.getMutator().setSafe(index+1, nextOffset+1);
    }

    <#else>

    public void addSafe(int index, ${minor.javaType!type.javaType} srcValue) {
      int nextOffset = offsets.getAccessor().get(index+1);
      values.getMutator().setSafe(nextOffset, srcValue);
      offsets.getMutator().setSafe(index+1, nextOffset+1);
    }
        
    </#if>

    
    public void setSafe(int index, Repeated${minor.class}Holder h){
      ${minor.class}Holder ih = new ${minor.class}Holder();
      getMutator().startNewGroup(index);
      for(int i = h.start; i < h.end; i++){
        h.vector.getAccessor().get(i, ih);
        getMutator().addSafe(index, ih);
      }
    }
    
    public void addSafe(int index, ${minor.class}Holder holder){
      int nextOffset = offsets.getAccessor().get(index+1);
      values.getMutator().setSafe(nextOffset, holder);
      offsets.getMutator().setSafe(index+1, nextOffset+1);
    }
    
    public void addSafe(int index, Nullable${minor.class}Holder holder){
      int nextOffset = offsets.getAccessor().get(index+1);
      values.getMutator().setSafe(nextOffset, holder);
      offsets.getMutator().setSafe(index+1, nextOffset+1);
    }
    
    <#if (fields?size > 1) && !(minor.class == "Decimal9" || minor.class == "Decimal18" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse" || minor.class == "Decimal28Dense" || minor.class == "Decimal38Dense")>
    public void addSafe(int arrayIndex, <#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>){
      int nextOffset = offsets.getAccessor().get(arrayIndex+1);
      values.getMutator().setSafe(nextOffset, <#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
      offsets.getMutator().setSafe(arrayIndex+1, nextOffset+1);
    }
    </#if>
    
    protected void add(int index, ${minor.class}Holder holder){
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
      offsets.getMutator().setValueCount(groupCount == 0 ? 0 : groupCount+1);
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
