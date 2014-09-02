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
import org.apache.drill.exec.vector.NullableVectorDefinitionSetter;

import java.lang.Override;
import java.lang.UnsupportedOperationException;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>

<#assign className = "Nullable${minor.class}Vector" />
<#assign valuesName = "${minor.class}Vector" />
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />

<@pp.changeOutputFile name="/org/apache/drill/exec/vector/${className}.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector;

<#include "/@includes/vv_imports.ftl" />

/**
 * Nullable${minor.class} implements a vector of values which could be null.  Elements in the vector
 * are first checked against a fixed length vector of boolean values.  Then the element is retrieved
 * from the base class (if not null).
 *
 * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
 */
@SuppressWarnings("unused")
public final class ${className} extends BaseValueVector implements <#if type.major == "VarLen">VariableWidth<#else>FixedWidth</#if>Vector, NullableVector{

  private int valueCount;
  final UInt1Vector bits;
  final ${valuesName} values;
  private final Accessor accessor;
  private final Mutator mutator;

  public ${className}(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    this.bits = new UInt1Vector(null, allocator);
    this.values = new ${minor.class}Vector(field, allocator);
    this.accessor = new Accessor();
    this.mutator = new Mutator();
  }
  
  public int getValueCapacity(){
    return Math.min(bits.getValueCapacity(), values.getValueCapacity());
  }

  public int getCurrentValueCount() {
    return values.getCurrentValueCount();
  }

  public void setCurrentValueCount(int count) {
    values.setCurrentValueCount(count);
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    DrillBuf[] buffers = ObjectArrays.concat(bits.getBuffers(clear), values.getBuffers(clear), DrillBuf.class);
    if (clear) {
      clear();
    }
    return buffers;
  }
  
  @Override
  public void clear() {
    valueCount = 0;
    bits.clear();
    values.clear();
  }
  
  public int getBufferSize(){
    return values.getBufferSize() + bits.getBufferSize();
  }

  public DrillBuf getData(){
    return values.getData();
  }

  public ${valuesName} getValuesVector() {
    return values;
  }

  <#if type.major == "VarLen">
  @Override
  public SerializedField getMetadata() {
    return getMetadataBuilder()
             .setValueCount(valueCount)
             .setVarByteLength(values.getVarByteLength())
             .setBufferLength(getBufferSize())
             .build();
  }

  public void allocateNew() {
    if(!allocateNewSafe()){
      throw new OutOfMemoryRuntimeException("Failure while allocating buffer.");
    }
  }
  
  @Override
  public boolean allocateNewSafe() {
    if(!values.allocateNewSafe()) return false;
    if(!bits.allocateNewSafe()) return false;
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    return true;
  }

  @Override
  public void allocateNew(int totalBytes, int valueCount) {
    values.allocateNew(totalBytes, valueCount);
    bits.allocateNew(valueCount);
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
  }

  @Override
  public int load(int dataBytes, int valueCount, DrillBuf buf){
    clear();
    this.valueCount = valueCount;
    int loaded = bits.load(valueCount, buf);
    
    // remove bits part of buffer.
    buf = buf.slice(loaded, buf.capacity() - loaded);
    dataBytes -= loaded;
    loaded += values.load(dataBytes, valueCount, buf);
    this.mutator.lastSet = valueCount;
    return loaded;
  }
  
  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
    assert this.field.matches(metadata) : String.format("The field %s doesn't match the provided metadata %s.", this.field, metadata);
    int loaded = load(metadata.getBufferLength(), metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded : String.format("Expected to load %d bytes but actually loaded %d bytes", metadata.getBufferLength(), loaded);
  }
  
  @Override
  public int getByteCapacity(){
    return values.getByteCapacity();
  }

  @Override
  public int getCurrentSizeInBytes(){
    return values.getCurrentSizeInBytes();
  }

  <#else>
  @Override
  public SerializedField getMetadata() {
    return getMetadataBuilder()
             .setValueCount(valueCount)
             .setBufferLength(getBufferSize())
             .build();
  }

  @Override
  public void allocateNew() {
    values.allocateNew();
    bits.allocateNew();
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
  }
  

  @Override
  public boolean allocateNewSafe() {
    if(!values.allocateNewSafe()) return false;
    if(!bits.allocateNewSafe()) return false;
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    return true;
  }

  @Override
  public void allocateNew(int valueCount) {
    values.allocateNew(valueCount);
    bits.allocateNew(valueCount);
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
  }

  /**
   * {@inheritDoc}
   */
  public void zeroVector() {
    this.values.zeroVector();
    this.bits.zeroVector();
  }

  @Override
  public int load(int valueCount, DrillBuf buf){
    clear();
    this.valueCount = valueCount;
    int loaded = bits.load(valueCount, buf);
    
    // remove bits part of buffer.
    buf = buf.slice(loaded, buf.capacity() - loaded);
    loaded += values.load(valueCount, buf);
    return loaded;
  }
  
  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
    assert this.field.matches(metadata);
    int loaded = load(metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded;
  }
  
  </#if>
  
  public TransferPair getTransferPair(){
    return new TransferImpl(getField());
  }
  public TransferPair getTransferPair(FieldReference ref){
    return new TransferImpl(getField().clone(ref));
  }

  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((Nullable${minor.class}Vector) to);
  }

  
  public void transferTo(Nullable${minor.class}Vector target){
    bits.transferTo(target.bits);
    values.transferTo(target.values);
    target.valueCount = valueCount;
    <#if type.major == "VarLen">
    target.mutator.lastSet = mutator.lastSet;
    </#if>
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, Nullable${minor.class}Vector target) {
    bits.splitAndTransferTo(startIndex, length, target.bits);
    values.splitAndTransferTo(startIndex, length, target.values);
    <#if type.major == "VarLen">
    target.mutator.lastSet = length - 1;
    </#if>
  }
  
  private class TransferImpl implements TransferPair{
    Nullable${minor.class}Vector to;
    
    public TransferImpl(MaterializedField field){
      this.to = new Nullable${minor.class}Vector(field, allocator);
    }

    public TransferImpl(Nullable${minor.class}Vector to){
      this.to = to;
    }

    public Nullable${minor.class}Vector getTo(){
      return to;
    }
    
    public void transfer(){
      transferTo(to);
    }

    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public boolean copyValueSafe(int fromIndex, int toIndex) {
      return to.copyFromSafe(fromIndex, toIndex, Nullable${minor.class}Vector.this);
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

  
  protected void copyFrom(int fromIndex, int thisIndex, Nullable${minor.class}Vector from){
    if (!from.getAccessor().isNull(fromIndex)) {
      mutator.set(thisIndex, from.getAccessor().get(fromIndex));
    }
  }

  
  public boolean copyFromSafe(int fromIndex, int thisIndex, ${minor.class}Vector from){
    <#if type.major == "VarLen">
    if(!mutator.fillEmpties(thisIndex)) return false;
    </#if>
    boolean success = values.copyFromSafe(fromIndex, thisIndex, from);
    success = success && bits.getMutator().setSafe(thisIndex, 1);
    return success;    
  }
  
  public boolean copyFromSafe(int fromIndex, int thisIndex, Nullable${minor.class}Vector from){
    <#if type.major == "VarLen">
    if(!mutator.fillEmpties(thisIndex)) return false;
    </#if>
    boolean b1 = bits.copyFromSafe(fromIndex, thisIndex, from.bits);
    boolean b2 = values.copyFromSafe(fromIndex, thisIndex, from.values);
    return b1 && b2;
  }

  public long getDataAddr(){
    return values.getDataAddr();
  }
  
  public long getBitAddr(){
    return bits.getDataAddr();
  }
  
  <#if type.major == "VarLen">
  public long getOffsetAddr(){
    return values.getOffsetAddr();
  }
  </#if>
  
  public final class Accessor implements ValueVector.Accessor<#if type.major = "VarLen">, VariableWidthVector.VariableWidthAccessor</#if>{

    final FieldReader reader = new Nullable${minor.class}ReaderImpl(Nullable${minor.class}Vector.this);
    final UInt1Vector.Accessor bAccessor = bits.getAccessor();
    final ${valuesName}.Accessor vAccessor = values.getAccessor();
    
    public FieldReader getReader(){
      return reader;
    }
    
    /**
     * Get the element at the specified position.
     *
     * @param   index   position of the value
     * @return  value of the element, if not null
     * @throws  NullValueException if the value is null
     */
    public <#if type.major == "VarLen">byte[]<#else>${minor.javaType!type.javaType}</#if> get(int index) {
      assert !isNull(index) : "Tried to get null value";
      return vAccessor.get(index);
    }

    public boolean isNull(int index) {
      return isSet(index) == 0;
    }

    public int isSet(int index){
      return bAccessor.get(index);
    }
    
    <#if type.major == "VarLen">
    public long getStartEnd(int index){
      return vAccessor.getStartEnd(index);
    }
    
    public long getOffsetAddr(){
      return values.getOffsetAddr();
    }
    </#if>
    

    


    <#if type.major == "VarLen">
    public int getValueLength(int index) {
      return values.getAccessor().getValueLength(index);
    }
    </#if>

    public void get(int index, Nullable${minor.class}Holder holder){
      vAccessor.get(index, holder);
      holder.isSet = bAccessor.get(index);

      <#if minor.class.startsWith("Decimal")>
      holder.scale = getField().getScale();
      holder.precision = getField().getPrecision();
      </#if>
    }

    @Override
    public ${friendlyType} getObject(int index) {
      if (isNull(index)) {
          return null;
      }else{
        return vAccessor.getObject(index);
      }
    }

    <#if minor.class == "Interval" || minor.class == "IntervalDay" || minor.class == "IntervalYear">
    public StringBuilder getAsStringBuilder(int index) {
      if (isNull(index)) {
          return null;
      }else{
        return vAccessor.getAsStringBuilder(index);
      }
    }
    </#if>

    public int getValueCount(){
      return valueCount;
    }
    
    public void reset(){}
  }
  
  public final class Mutator implements ValueVector.Mutator, NullableVectorDefinitionSetter<#if type.major = "VarLen">, VariableWidthVector.VariableWidthMutator</#if> {
    
    private int setCount;
    <#if type.major = "VarLen"> private int lastSet = -1;</#if>

    private Mutator(){
    }

    public ${valuesName} getVectorWithValues(){
      return values;
    }

    public void setIndexDefined(int index){
      bits.getMutator().set(index, 1);
    }

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param bytes   array of bytes to write
     */
    public void set(int index, <#if type.major == "VarLen">byte[]<#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value) {
      setCount++;
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        values.getMutator().set(i, new byte[]{});
      }
      </#if>
      bits.getMutator().set(index, 1);
      values.getMutator().set(index, value);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    <#if type.major == "VarLen">
    private boolean fillEmpties(int index){
      for (int i = lastSet + 1; i < index; i++) {
        if(!values.getMutator().setSafe(i, new byte[]{})) return false;
      }
      lastSet = index;

      return true;
    }

    public boolean setValueLengthSafe(int index, int length) {
      return values.getMutator().setValueLengthSafe(index, length);
    }
    </#if>
    
    public boolean setSafe(int index, byte[] value, int start, int length) {
      <#if type.major != "VarLen">
      throw new UnsupportedOperationException();
      <#else>
      if(!fillEmpties(index)) return false;

      boolean b1 = bits.getMutator().setSafe(index, 1);
      boolean b2 = values.getMutator().setSafe(index, value, start, length);
      if(b1 && b2){
        setCount++;
        <#if type.major == "VarLen">lastSet = index;</#if>
        return true;
      }else{
        return false;
      }
      </#if>
    }

    public void setSkipNull(int index, ${minor.class}Holder holder){
      values.getMutator().set(index, holder);
    }

    public void setSkipNull(int index, Nullable${minor.class}Holder holder){
      values.getMutator().set(index, holder);
    }
    
    
    public void set(int index, Nullable${minor.class}Holder holder){
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        values.getMutator().set(i, new byte[]{});
      }
      </#if>
      bits.getMutator().set(index, holder.isSet);
      values.getMutator().set(index, holder);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    public void set(int index, ${minor.class}Holder holder){
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        values.getMutator().set(i, new byte[]{});
      }
      </#if>
      bits.getMutator().set(index, 1);
      values.getMutator().set(index, holder);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }
    
    public boolean isSafe(int outIndex) {
      return outIndex < Nullable${minor.class}Vector.this.getValueCapacity();
    }

    //public boolean setSafe(int index, int isSet<#if type.major == "VarLen" || minor.class == "TimeStampTZ" || minor.class == "Interval" || minor.class == "IntervalDay">Nullable${minor.class}Holder <#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value){

    <#assign fields = minor.fields!type.fields />
    public void set(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ){
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        values.getMutator().set(i, new byte[]{});
      }
      </#if>
      bits.getMutator().set(index, isSet);
      values.getMutator().set(index<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }
    
    public boolean setSafe(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) {
      <#if type.major == "VarLen">
      if(!fillEmpties(index)) return false;
      </#if>
      
      boolean b1 = bits.getMutator().setSafe(index, isSet);
      boolean b2 = values.getMutator().setSafe(index<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
      if(b1 && b2){
        setCount++;
        <#if type.major == "VarLen">lastSet = index;</#if>
        return true;
      }else{
        return false;
      }

    }


    public boolean setSafe(int index, Nullable${minor.class}Holder value) {

      <#if type.major == "VarLen">
      if(!fillEmpties(index)) return false;
      </#if>
      boolean b1 = bits.getMutator().setSafe(index, value.isSet);
      boolean b2 = values.getMutator().setSafe(index, value);
      if(b1 && b2){
        setCount++;
        <#if type.major == "VarLen">lastSet = index;</#if>
        return true;
      }else{
        return false;
      }

    }

    public boolean setSafe(int index, ${minor.class}Holder value) {

      <#if type.major == "VarLen">
      if(!fillEmpties(index)) return false;
      </#if>
      boolean b1 = bits.getMutator().setSafe(index, 1);
      boolean b2 = values.getMutator().setSafe(index, value);
      if(b1 && b2){
        setCount++;
        <#if type.major == "VarLen">lastSet = index;</#if>
        return true;
      }else{
        return false;
      }

    }
    
    <#if !(type.major == "VarLen" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse" || minor.class == "Decimal28Dense" || minor.class == "Decimal38Dense" || minor.class == "TimeStampTZ" || minor.class == "Interval" || minor.class == "IntervalDay")>
      public boolean setSafe(int index, ${minor.javaType!type.javaType} value) {
        <#if type.major == "VarLen">
        if(!fillEmpties(index)) return false;
        </#if>
        boolean b1 = bits.getMutator().setSafe(index, 1);
        boolean b2 = values.getMutator().setSafe(index, value);
        if(b1 && b2){
          setCount++;
          return true;
        }else{
          return false;
        }
      }

    </#if>

    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      <#if type.major == "VarLen">
      fillEmpties(valueCount);
      </#if>
      Nullable${minor.class}Vector.this.valueCount = valueCount;
      values.getMutator().setValueCount(valueCount);
      bits.getMutator().setValueCount(valueCount);
    }
    
    public boolean noNulls(){
      return valueCount == setCount;
    }
    
    public void generateTestData(int valueCount){
      bits.getMutator().generateTestDataAlt(valueCount);
      values.getMutator().generateTestData(valueCount);
      <#if type.major = "VarLen">lastSet = valueCount;</#if>
      setValueCount(valueCount);
    }
    
    public void reset(){
      setCount = 0;
      <#if type.major = "VarLen">lastSet = -1;</#if>
    }
    
  }
}
</#list>
</#list>
