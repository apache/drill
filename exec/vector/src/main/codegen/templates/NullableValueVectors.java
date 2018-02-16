/*
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
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.memory.AllocationManager.BufferLedger;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.NullableVectorDefinitionSetter;

import java.lang.Override;
import java.lang.UnsupportedOperationException;
import java.util.Set;

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
 * NB: this class is automatically generated from ${.template_name} and ValueVectorTypes.tdd using FreeMarker.
 */

public final class ${className} extends BaseDataValueVector implements <#if type.major == "VarLen">VariableWidth<#else>FixedWidth</#if>Vector, NullableVector {

  private final FieldReader reader = new Nullable${minor.class}ReaderImpl(Nullable${minor.class}Vector.this);

  /**
   * Set value flag. Meaning:
   * <ul>
   * <li>0: value is not set (value is null).</li>
   * <li>1: value is set (value is not null).</li>
   * </ul>
   * That is, a 1 means that the values vector has a value. 0
   * means that the vector is null. Thus, all values start as
   * not set (null) and must be explicitly set (made not null).
   */

  private final UInt1Vector bits = new UInt1Vector(bitsField, allocator);

  private final ${valuesName} values;

  private final Mutator mutator = new Mutator();
  private final Accessor accessor;

  public ${className}(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    
    // The values vector has its own name, and has the same type and attributes
    // as the nullable vector. This ensures that
    // things like scale and precision are preserved in the values vector.
    
    values = new ${minor.class}Vector(
        MaterializedField.create(VALUES_VECTOR_NAME, field.getType()),
        allocator);
    
    field.addChild(bits.getField());
    field.addChild(values.getField());
    accessor = new Accessor();
  }

  @Override
  public FieldReader getReader(){
    return reader;
  }

  @Override
  public int getValueCapacity(){
    return Math.min(bits.getValueCapacity(), values.getValueCapacity());
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    final DrillBuf[] buffers = ObjectArrays.concat(bits.getBuffers(false), values.getBuffers(false), DrillBuf.class);
    if (clear) {
      for (final DrillBuf buffer:buffers) {
        buffer.retain(1);
      }
      clear();
    }
    return buffers;
  }

  @Override
  public void close() {
    bits.close();
    values.close();
    super.close();
  }

  @Override
  public void clear() {
    bits.clear();
    values.clear();
    super.clear();
  }

  @Override
  public int getBufferSize(){
    return values.getBufferSize() + bits.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    return values.getBufferSizeFor(valueCount) +
           bits.getBufferSizeFor(valueCount);
  }

  @Override
  public int getAllocatedSize(){
    return bits.getAllocatedSize() + values.getAllocatedSize();
  }

  @Override
  public DrillBuf getBuffer() {
    return values.getBuffer();
  }

  @Override
  public ${valuesName} getValuesVector() { return values; }

  @Override
  public UInt1Vector getBitsVector() { return bits; }

  <#if type.major == "VarLen">
  @Override
  public UInt4Vector getOffsetVector() {
    return ((VariableWidthVector) values).getOffsetVector();
  }

  </#if>
  @Override
  public void setInitialCapacity(int numRecords) {
    bits.setInitialCapacity(numRecords);
    values.setInitialCapacity(numRecords);
  }

  @Override
  public SerializedField.Builder getMetadataBuilder() {
    return super.getMetadataBuilder()
      .addChild(bits.getMetadata())
      .addChild(values.getMetadata());
  }

  @Override
  public void allocateNew() {
    if(!allocateNewSafe()){
      throw new OutOfMemoryException("Failure while allocating buffer.");
    }
  }

  @Override
  public boolean allocateNewSafe() {
    /* Boolean to keep track if all the memory allocations were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      success = values.allocateNewSafe() && bits.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    return success;
  }

  @Override
  public DrillBuf reallocRaw(int newAllocationSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void collectLedgers(Set<BufferLedger> ledgers) {
    bits.collectLedgers(ledgers);
    values.collectLedgers(ledgers);
  }

  @Override
  public int getPayloadByteCount(int valueCount) {
    // For nullable, we include all values, null or not, in computing
    // the value length.
    return bits.getPayloadByteCount(valueCount) + values.getPayloadByteCount(valueCount);
  }

  <#if type.major != "VarLen">
  @Override
  public int getValueWidth(){
    return bits.getValueWidth() + ${type.width};
  }
  </#if>

  <#if type.major == "VarLen">
  @Override
  public void allocateNew(int totalBytes, int valueCount) {
    try {
      values.allocateNew(totalBytes, valueCount);
      bits.allocateNew(valueCount);
    } catch(RuntimeException e) {
      clear();
      throw e;
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
  }

  @Override
  public void reset() {
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    super.reset();
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
  public void allocateNew(int valueCount) {
    try {
      values.allocateNew(valueCount);
      bits.allocateNew(valueCount);
    } catch(OutOfMemoryException e) {
      clear();
      throw e;
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
  }

  @Override
  public void reset() {
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    super.reset();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void zeroVector() {
    bits.zeroVector();
    values.zeroVector();
  }

  </#if>
  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
    clear();
    // the bits vector is the first child (the order in which the children are added in getMetadataBuilder is significant)
    final SerializedField bitsField = metadata.getChild(0);
    bits.load(bitsField, buffer);

    final int capacity = buffer.capacity();
    final int bitsLength = bitsField.getBufferLength();
    final SerializedField valuesField = metadata.getChild(1);
    values.load(valuesField, buffer.slice(bitsLength, capacity - bitsLength));
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator){
    return new TransferImpl(getField(), allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator){
    return new TransferImpl(getField().withPath(ref), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((Nullable${minor.class}Vector) to);
  }

  public void transferTo(Nullable${minor.class}Vector target){
    bits.transferTo(target.bits);
    values.transferTo(target.values);
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

  private class TransferImpl implements TransferPair {
    private final Nullable${minor.class}Vector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator){
      to = new Nullable${minor.class}Vector(field, allocator);
    }

    public TransferImpl(Nullable${minor.class}Vector to){
      this.to = to;
    }

    @Override
    public Nullable${minor.class}Vector getTo(){
      return to;
    }

    @Override
    public void transfer(){
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, Nullable${minor.class}Vector.this);
    }
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  public ${minor.class}Vector convertToRequiredVector(){
    ${minor.class}Vector v = new ${minor.class}Vector(getField().getOtherNullableVersion(), allocator);
    if (v.data != null) {
      v.data.release(1);
    }
    v.data = values.data;
    v.data.retain(1);
    clear();
    return v;
  }

  public void copyFrom(int fromIndex, int thisIndex, Nullable${minor.class}Vector from){
    final Accessor fromAccessor = from.getAccessor();
    if (!fromAccessor.isNull(fromIndex)) {
      mutator.set(thisIndex, fromAccessor.get(fromIndex));
    }
  }

  public void copyFromSafe(int fromIndex, int thisIndex, ${minor.class}Vector from){
    <#if type.major == "VarLen">
    mutator.fillEmpties(thisIndex);
    </#if>
    values.copyFromSafe(fromIndex, thisIndex, from);
    bits.getMutator().setSafe(thisIndex, 1);
  }

  public void copyFromSafe(int fromIndex, int thisIndex, Nullable${minor.class}Vector from){
    <#if type.major == "VarLen">
    mutator.fillEmpties(thisIndex);
    </#if>
    bits.copyFromSafe(fromIndex, thisIndex, from.bits);
    values.copyFromSafe(fromIndex, thisIndex, from.values);
  }

  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    <#if type.major == "VarLen">
    mutator.fillEmpties(toIndex);
    </#if>

    // Handle the case of not-nullable copied into a nullable
    if (from instanceof ${minor.class}Vector) {
      bits.getMutator().set(toIndex,1);
      values.copyFromSafe(fromIndex,toIndex,(${minor.class}Vector)from);
      return;
    }

    Nullable${minor.class}Vector fromVector = (Nullable${minor.class}Vector) from;
    <#if type.major == "VarLen">

    // This method is to be called only for loading the vector
    // sequentially, so there should be no empties to fill.

    </#if>
    bits.copyFromSafe(fromIndex, toIndex, fromVector.bits);
    values.copyFromSafe(fromIndex, toIndex, fromVector.values);
  }

  @Override
  public void exchange(ValueVector other) {
    ${className} target = (${className}) other;
    bits.exchange(target.bits);
    values.exchange(target.values);
    mutator.exchange(other.getMutator());
  }

  <#if type.major != "VarLen">
  @Override
  public void toNullable(ValueVector nullableVector) {
    exchange(nullableVector);
    clear();
  }

  </#if>
  public final class Accessor extends BaseDataValueVector.BaseAccessor <#if type.major = "VarLen">implements VariableWidthVector.VariableWidthAccessor</#if> {
    final UInt1Vector.Accessor bAccessor = bits.getAccessor();
    final ${valuesName}.Accessor vAccessor = values.getAccessor();

    /**
     * Get the element at the specified position.
     *
     * @param   index   position of the value
     * @return  value of the element, if not null
     * @throws  IllegalStateException if the value is null
     */
    public <#if type.major == "VarLen">byte[]<#else>${minor.javaType!type.javaType}</#if> get(int index) {
      if (isNull(index)) {
          throw new IllegalStateException("Can't get a null value");
      }
      return vAccessor.get(index);
    }

    @Override
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

    @Override
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
      } else {
        return vAccessor.getObject(index);
      }
    }

    <#if minor.class == "Interval" || minor.class == "IntervalDay" || minor.class == "IntervalYear">
    public StringBuilder getAsStringBuilder(int index) {
      if (isNull(index)) {
        return null;
      } else {
        return vAccessor.getAsStringBuilder(index);
      }
    }

    </#if>
    @Override
    public int getValueCount() {
      return bits.getAccessor().getValueCount();
    }

    public void reset() {}
  }

  public final class Mutator extends BaseDataValueVector.BaseMutator implements NullableVectorDefinitionSetter<#if type.major = "VarLen">, VariableWidthVector.VariableWidthMutator</#if> {
    private int setCount;
    <#if type.major = "VarLen">private int lastSet = -1;</#if>

    private Mutator() { }

    public ${valuesName} getVectorWithValues(){
      return values;
    }

    @Override
    public void setIndexDefined(int index){
      bits.getMutator().set(index, 1);
    }

    /**
     * Set the variable length element at the specified index to the supplied value.
     *
     * @param index   position of the bit to set
     * @param value   value to write
     */

    public void set(int index, <#if type.major == "VarLen">byte[]<#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value) {
      setCount++;
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      final UInt1Vector.Mutator bitsMutator = bits.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bitsMutator.set(index, 1);
      valuesMutator.set(index, value);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    <#if type.major == "VarLen">
    private void fillEmpties(int index) {
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      for (int i = lastSet; i < index; i++) {
        valuesMutator.setSafe(i + 1, emptyByteArray);
      }
      while(index > bits.getValueCapacity()) {
        bits.reAlloc();
      }
      lastSet = index;
    }

    @Override
    public void setValueLengthSafe(int index, int length) {
      values.getMutator().setValueLengthSafe(index, length);
      lastSet = index;
    }

    public void setSafe(int index, byte[] value, int start, int length) {
       if (index > lastSet + 1) {
        fillEmpties(index);
      }

      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value, start, length);
      setCount++;
      lastSet = index;
    }

    public void setSafe(int index, ByteBuffer value, int start, int length) {
      if (index > lastSet + 1) {
        fillEmpties(index);
      }

      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value, start, length);
      setCount++;
      lastSet = index;
    }

    </#if>
    public void setNull(int index) {
      bits.getMutator().setSafe(index, 0);
    }

    public void setSkipNull(int index, ${minor.class}Holder holder) {
      values.getMutator().set(index, holder);
    }

    public void setSkipNull(int index, Nullable${minor.class}Holder holder) {
      values.getMutator().set(index, holder);
    }

    public void set(int index, Nullable${minor.class}Holder holder) {
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bits.getMutator().set(index, holder.isSet);
      valuesMutator.set(index, holder);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    public void set(int index, ${minor.class}Holder holder) {
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bits.getMutator().set(index, 1);
      valuesMutator.set(index, holder);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    public boolean isSafe(int outIndex) {
      return outIndex < Nullable${minor.class}Vector.this.getValueCapacity();
    }

    <#assign fields = minor.fields!type.fields />
    public void set(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) {
      final ${valuesName}.Mutator valuesMutator = values.getMutator();
      <#if type.major == "VarLen">
      for (int i = lastSet + 1; i < index; i++) {
        valuesMutator.set(i, emptyByteArray);
      }
      </#if>
      bits.getMutator().set(index, isSet);
      valuesMutator.set(index<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    public void setSafe(int index, int isSet<#list fields as field><#if field.include!true >, ${field.type} ${field.name}Field</#if></#list> ) {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      bits.getMutator().setSafe(index, isSet);
      values.getMutator().setSafe(index<#list fields as field><#if field.include!true >, ${field.name}Field</#if></#list>);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    public void setSafe(int index, Nullable${minor.class}Holder value) {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      bits.getMutator().setSafe(index, value.isSet);
      values.getMutator().setSafe(index, value);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    public void setSafe(int index, ${minor.class}Holder value) {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value);
      setCount++;
      <#if type.major == "VarLen">lastSet = index;</#if>
    }

    <#if !(type.major == "VarLen" || minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse" || minor.class == "Decimal28Dense" || minor.class == "Decimal38Dense" || minor.class == "Interval" || minor.class == "IntervalDay")>
    public void setSafe(int index, ${minor.javaType!type.javaType} value) {
      <#if type.major == "VarLen">
      if (index > lastSet + 1) {
        fillEmpties(index);
      }
      </#if>
      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value);
      setCount++;
    }

    </#if>
    <#if minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse">
    public void set(int index, BigDecimal value) {
      bits.getMutator().set(index, 1);
      values.getMutator().set(index, value);
      setCount++;
    }

    public void setSafe(int index, BigDecimal value) {
      bits.getMutator().setSafe(index, 1);
      values.getMutator().setSafe(index, value);
      setCount++;
    }

    </#if>
    @Override
    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      <#if type.major == "VarLen">
      fillEmpties(valueCount);
      </#if>
      values.getMutator().setValueCount(valueCount);
      bits.getMutator().setValueCount(valueCount);
    }

    @Override
    public void generateTestData(int valueCount){
      bits.getMutator().generateTestDataAlt(valueCount);
      values.getMutator().generateTestData(valueCount);
      <#if type.major = "VarLen">lastSet = valueCount;</#if>
      setValueCount(valueCount);
    }

    @Override
    public void reset(){
      setCount = 0;
      <#if type.major = "VarLen">lastSet = -1;</#if>
    }

    // For nullable vectors, exchanging buffers (done elsewhere)
    // requires also exchanging mutator state (done here.)

    @Override
    public void exchange(ValueVector.Mutator other) {
      final Mutator target = (Mutator) other;
      int temp = setCount;
      setCount = target.setCount;
      target.setCount = temp;
    }

    public void fromNotNullable(${minor.class}Vector srce) {
      clear();
      final int valueCount = srce.getAccessor().getValueCount();

      // Create a new bits vector, all values non-null

      fillBitsVector(getBitsVector(), valueCount);

      // Swap the data portion

      getValuesVector().exchange(srce);
      <#if type.major = "VarLen">lastSet = valueCount;</#if>
      setValueCount(valueCount);
    }
  }
}
</#list>
</#list>
