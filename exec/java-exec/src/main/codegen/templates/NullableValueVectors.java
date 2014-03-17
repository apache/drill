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

import java.lang.UnsupportedOperationException;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>

<#assign className = "Nullable${minor.class}Vector" />
<#assign valuesName = "${minor.class}Vector" />
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
public final class ${className} extends BaseValueVector implements <#if type.major == "VarLen">VariableWidth<#else>FixedWidth</#if>Vector{

  private int valueCount;
  final BitVector bits;
  final ${valuesName} values;
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();

  public ${className}(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    this.bits = new BitVector(null, allocator);
    this.values = new ${minor.class}Vector(null, allocator);
  }
  
  public int getValueCapacity(){
    return bits.getValueCapacity();
  }
  
  @Override
  public ByteBuf[] getBuffers() {
    ByteBuf[] buffers = ObjectArrays.concat(bits.getBuffers(), values.getBuffers(), ByteBuf.class);
    clear();
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

  public ByteBuf getData(){
    return values.getData();
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
    dataBytes -= loaded;
    loaded += values.load(dataBytes, valueCount, buf);
    return loaded;
  }
  
  @Override
  public void load(FieldMetadata metadata, ByteBuf buffer) {
    assert this.field.getDef().equals(metadata.getDef());
    int loaded = load(metadata.getBufferLength(), metadata.getValueCount(), buffer);
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
    return new TransferImpl(getField());
  }
  public TransferPair getTransferPair(FieldReference ref){
    return new TransferImpl(getField().clone(ref));
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
  
  private class TransferImpl implements TransferPair{
    Nullable${minor.class}Vector to;
    
    public TransferImpl(MaterializedField field){
      this.to = new Nullable${minor.class}Vector(field, allocator);
    }
    
    public Nullable${minor.class}Vector getTo(){
      return to;
    }
    
    public void transfer(){
      transferTo(to);
    }
    
    @Override
    public void copyValue(int fromIndex, int toIndex) {
      to.copyFrom(fromIndex, toIndex, Nullable${minor.class}Vector.this);
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

  
  public void copyFrom(int fromIndex, int thisIndex, Nullable${minor.class}Vector from){
    if (!from.getAccessor().isNull(fromIndex)) {
    mutator.set(thisIndex, from.getAccessor().get(fromIndex));
}
  }
  
  public boolean copyFromSafe(int fromIndex, int thisIndex, Nullable${minor.class}Vector from){
    return bits.copyFromSafe(fromIndex, thisIndex, from.bits) && values.copyFromSafe(fromIndex, thisIndex, from.values);
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
    
    public void get(int index, Nullable${minor.class}Holder holder){
      holder.isSet = bits.getAccessor().get(index);
      values.getAccessor().get(index, holder);
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
  
  public final class Mutator implements ValueVector.Mutator, NullableVectorDefinitionSetter{
    
    private int setCount;
    <#if type.major = "VarLen"> private int lastSet;</#if>

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

    public boolean setSafe(int index, byte[] value, int start, int length) {
      <#if type.major != "VarLen">
      throw new UnsupportedOperationException();
      <#else>
      for (int i = lastSet + 1; i < index; i++) {
        values.getMutator().set(i, new byte[]{});
      }
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
    
    public boolean setSafe(int index, <#if type.major == "VarLen">Nullable${minor.class}Holder <#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value){
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        values.getMutator().set(i, new byte[]{});
      }
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

    
    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < valueCount; i++) {
        values.getMutator().set(i, new byte[]{});
      }
      </#if>
      Nullable${minor.class}Vector.this.valueCount = valueCount;
      values.getMutator().setValueCount(valueCount);
      bits.getMutator().setValueCount(valueCount);
    }
    
    public boolean noNulls(){
      return valueCount == setCount;
    }
    
    public void generateTestData(){
      bits.getMutator().generateTestData();
      values.getMutator().generateTestData();
    }
    
    public void reset(){
      setCount = 0;
    }
    
  }
}
</#list>
</#list>
