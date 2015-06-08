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

import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>

<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />


<#if type.major == "VarLen">
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/${minor.class}Vector.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector;

<#include "/@includes/vv_imports.ftl" />

/**
 * ${minor.class}Vector implements a vector of variable width values.  Elements in the vector
 * are accessed by position from the logical start of the vector.  A fixed width offsetVector
 * is used to convert an element's position to it's offset from the start of the (0-based)
 * DrillBuf.  Size is inferred by adjacent elements.
 *   The width of each element is ${type.width} byte(s)
 *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
 *
 * Source code generated using FreeMarker template ${.template_name}
 */
@SuppressWarnings("unused")
public final class ${minor.class}Vector extends BaseDataValueVector implements VariableWidthVector{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}Vector.class);

  private static final int DEFAULT_RECORD_BYTE_COUNT = 8;
  private static final int INITIAL_BYTE_COUNT = 4096 * DEFAULT_RECORD_BYTE_COUNT;
  private static final int MIN_BYTE_COUNT = 4096;

  public final static String OFFSETS_VECTOR_NAME = "offsets";
  private final static MaterializedField offsetsField =
    MaterializedField.create(OFFSETS_VECTOR_NAME, Types.required(MinorType.UINT4));
  private final UInt${type.width}Vector offsetVector;
  private final FieldReader reader = new ${minor.class}ReaderImpl(${minor.class}Vector.this);

  private final Accessor accessor;
  private final Mutator mutator;

  private final UInt${type.width}Vector.Accessor oAccessor;


  private int allocationTotalByteCount = INITIAL_BYTE_COUNT;
  private int allocationMonitor = 0;

  public ${minor.class}Vector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    this.offsetVector = new UInt${type.width}Vector(offsetsField, allocator);
    this.oAccessor = offsetVector.getAccessor();
    this.accessor = new Accessor();
    this.mutator = new Mutator();
  }

  @Override
  public FieldReader getReader(){
    return reader;
  }

  public int getBufferSize(){
    if (getAccessor().getValueCount() == 0) return 0;
    return offsetVector.getBufferSize() + data.writerIndex();
  }

  int getSizeFromCount(int valueCount) {
    return valueCount * ${type.width};
  }

  public int getValueCapacity(){
    return offsetVector.getValueCapacity() - 1;
  }

  public int getByteCapacity(){
    return data.capacity();
  }

  public int getCurrentSizeInBytes() {
    return offsetVector.getAccessor().get(getAccessor().getValueCount());
  }

  /**
   * Return the number of bytes contained in the current var len byte vector.
   * @return
   */
  public int getVarByteLength(){
    final int valueCount = getAccessor().getValueCount();
    if(valueCount == 0) return 0;
    return offsetVector.getAccessor().get(valueCount);
  }

  @Override
  public SerializedField getMetadata() {
    return getMetadataBuilder() //
             .setValueCount(getAccessor().getValueCount()) //
             .setVarByteLength(getVarByteLength()) //
             .setBufferLength(getBufferSize()) //
             .build();
  }

  public int load(int dataBytes, int valueCount, DrillBuf buf){
    if(valueCount == 0){
      allocateNew(0,0);
      return 0;
    }
    clear();
    int loaded = offsetVector.load(valueCount+1, buf);
    data = buf.slice(loaded, dataBytes - loaded);
    data.retain();
    return  dataBytes;
  }

  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
    assert this.field.matches(metadata) : String.format("The field %s doesn't match the provided metadata %s.", this.field, metadata);
    int loaded = load(metadata.getBufferLength(), metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded : String.format("Expected to load %d bytes but actually loaded %d bytes", metadata.getBufferLength(), loaded);
  }

  @Override
  public void clear() {
    super.clear();
    offsetVector.clear();
  }


  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    DrillBuf[] buffers = ObjectArrays.concat(offsetVector.getBuffers(false), super.getBuffers(false), DrillBuf.class);
    if (clear) {
      // does not make much sense but we have to retain buffers even when clear is set. refactor this interface.
      for (DrillBuf buffer:buffers) {
        buffer.retain();
      }
      clear();
    }
    return buffers;
  }

  public long getOffsetAddr(){
    return offsetVector.getBuffer().memoryAddress();
  }

  public UInt${type.width}Vector getOffsetVector(){
    return offsetVector;
  }

  public TransferPair getTransferPair(){
    return new TransferImpl(getField());
  }
  public TransferPair getTransferPair(FieldReference ref){
    return new TransferImpl(getField().withPath(ref));
  }

  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((${minor.class}Vector) to);
  }

  public void transferTo(${minor.class}Vector target){
    target.clear();
    this.offsetVector.transferTo(target.offsetVector);
    target.data = data;
    target.data.retain();
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, ${minor.class}Vector target) {
    int startPoint = this.offsetVector.getAccessor().get(startIndex);
    int sliceLength = this.offsetVector.getAccessor().get(startIndex + length) - startPoint;
    target.offsetVector.clear();
    target.offsetVector.allocateNew(length + 1);
    for (int i = 0; i < length + 1; i++) {
      target.offsetVector.getMutator().set(i, this.offsetVector.getAccessor().get(startIndex + i) - startPoint);
    }
    target.data = this.data.slice(startPoint, sliceLength);
    target.data.retain();
    target.getMutator().setValueCount(length);
}

  protected void copyFrom(int fromIndex, int thisIndex, ${minor.class}Vector from){
    int start = from.offsetVector.getAccessor().get(fromIndex);
    int end =   from.offsetVector.getAccessor().get(fromIndex+1);
    int len = end - start;

    int outputStart = offsetVector.data.get${(minor.javaType!type.javaType)?cap_first}(thisIndex * ${type.width});
    from.data.getBytes(start, data, outputStart, len);
    offsetVector.data.set${(minor.javaType!type.javaType)?cap_first}( (thisIndex+1) * ${type.width}, outputStart + len);
  }

  public boolean copyFromSafe(int fromIndex, int thisIndex, ${minor.class}Vector from){

    int start = from.offsetVector.getAccessor().get(fromIndex);
    int end =   from.offsetVector.getAccessor().get(fromIndex+1);
    int len = end - start;

    int outputStart = offsetVector.data.get${(minor.javaType!type.javaType)?cap_first}(thisIndex * ${type.width});

    while(data.capacity() < outputStart + len) {
        reAlloc();
    }

    offsetVector.getMutator().setSafe(thisIndex + 1, outputStart + len);

    from.data.getBytes(start, data, outputStart, len);

    return true;
  }


  private class TransferImpl implements TransferPair{
    ${minor.class}Vector to;

    public TransferImpl(MaterializedField field){
      this.to = new ${minor.class}Vector(field, allocator);
    }

    public TransferImpl(${minor.class}Vector to){
      this.to = to;
    }

    public ${minor.class}Vector getTo(){
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
      to.copyFromSafe(fromIndex, toIndex, ${minor.class}Vector.this);
    }
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    allocationTotalByteCount = numRecords * DEFAULT_RECORD_BYTE_COUNT;
    offsetVector.setInitialCapacity(numRecords + 1);
  }

  public void allocateNew() {
    if(!allocateNewSafe()){
      throw new OutOfMemoryRuntimeException("Failure while allocating buffer.");
    }
  }

  @Override
  public boolean allocateNewSafe() {
    clear();
    if (allocationMonitor > 10) {
      allocationTotalByteCount = Math.max(MIN_BYTE_COUNT, (int) (allocationTotalByteCount / 2));
      allocationMonitor = 0;
    } else if (allocationMonitor < -2) {
      allocationTotalByteCount = (int) (allocationTotalByteCount * 2);
      allocationMonitor = 0;
    }

    /* Boolean to keep track if all the memory allocations were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      DrillBuf newBuf = allocator.buffer(allocationTotalByteCount);
      if (newBuf == null) {
        return false;
      }
      this.data = newBuf;
      if (!offsetVector.allocateNewSafe()) {
        return false;
      }
      success = true;
    } finally {
      if (!success) {
        clear();
      }
    }
    data.readerIndex(0);
    offsetVector.zeroVector();
    return true;
  }

  public void allocateNew(int totalBytes, int valueCount) {
    clear();
    assert totalBytes >= 0;
    try {
      DrillBuf newBuf = allocator.buffer(totalBytes);
      if (newBuf == null) {
        throw new OutOfMemoryRuntimeException(String.format("Failure while allocating buffer of %d bytes", totalBytes));
      }
      this.data = newBuf;
      offsetVector.allocateNew(valueCount + 1);
    } catch (OutOfMemoryRuntimeException e) {
      clear();
      throw e;
    }
    data.readerIndex(0);
    allocationTotalByteCount = totalBytes;
    offsetVector.zeroVector();
  }

    public void reAlloc() {
      allocationTotalByteCount *= 2;
      DrillBuf newBuf = allocator.buffer(allocationTotalByteCount);
      if(newBuf == null){
        throw new OutOfMemoryRuntimeException(
          String.format("Failure while reallocating buffer of %d bytes", allocationTotalByteCount));
      }

      newBuf.setBytes(0, data, 0, data.capacity());
      data.release();
      data = newBuf;
    }

  public void decrementAllocationMonitor() {
    if (allocationMonitor > 0) {
      allocationMonitor = 0;
    }
    --allocationMonitor;
  }

  private void incrementAllocationMonitor() {
    ++allocationMonitor;
  }

  public Accessor getAccessor(){
    return accessor;
  }

  public Mutator getMutator() {
    return mutator;
  }

  public final class Accessor extends BaseValueVector.BaseAccessor implements VariableWidthAccessor {
    final UInt${type.width}Vector.Accessor oAccessor = offsetVector.getAccessor();

    public long getStartEnd(int index){
      return oAccessor.getTwoAsLong(index);
    }

    public byte[] get(int index) {
      assert index >= 0;
      int startIdx = oAccessor.get(index);
      int length = oAccessor.get(index + 1) - startIdx;
      assert length >= 0;
      byte[] dst = new byte[length];
      data.getBytes(startIdx, dst, 0, length);
      return dst;
    }

    public int getValueLength(int index) {
      return offsetVector.getAccessor().get(index + 1) - offsetVector.getAccessor().get(index);
    }

    public void get(int index, ${minor.class}Holder holder){
      holder.start = oAccessor.get(index);
      holder.end = oAccessor.get(index + 1);
      holder.buffer = data;
    }

    public void get(int index, Nullable${minor.class}Holder holder){
      holder.isSet = 1;
      holder.start = oAccessor.get(index);
      holder.end = oAccessor.get(index + 1);
      holder.buffer = data;
    }


    <#switch minor.class>
    <#case "VarChar">
    public ${friendlyType} getObject(int index) {
      Text text = new Text();
      text.set(get(index));
      return text;
    }
    <#break>
    <#case "Var16Char">
    public ${friendlyType} getObject(int index) {
      return new String(get(index), Charsets.UTF_16);
    }
    <#break>
    <#default>
    public ${friendlyType} getObject(int index) {
      return get(index);
    }

    </#switch>



    public int getValueCount() {
      return Math.max(offsetVector.getAccessor().getValueCount()-1, 0);
    }

    public boolean isNull(int index){
      return false;
    }

    public UInt${type.width}Vector getOffsetVector(){
      return offsetVector;
    }
  }

  /**
   * Mutable${minor.class} implements a vector of variable width values.  Elements in the vector
   * are accessed by position from the logical start of the vector.  A fixed width offsetVector
   * is used to convert an element's position to it's offset from the start of the (0-based)
   * DrillBuf.  Size is inferred by adjacent elements.
   *   The width of each element is ${type.width} byte(s)
   *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public final class Mutator extends BaseValueVector.BaseMutator implements VariableWidthVector.VariableWidthMutator {

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param bytes   array of bytes to write
     */
    protected void set(int index, byte[] bytes) {
      assert index >= 0;
      int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + bytes.length);
      data.setBytes(currentOffset, bytes, 0, bytes.length);
    }

    public void setSafe(int index, byte[] bytes) {
      assert index >= 0;

      int currentOffset = offsetVector.getAccessor().get(index);
      while (data.capacity() < currentOffset + bytes.length) {
        reAlloc();
      }
      offsetVector.getMutator().setSafe(index + 1, currentOffset + bytes.length);
      offsetVector.getMutator().set(index + 1, currentOffset + bytes.length);
      data.setBytes(currentOffset, bytes, 0, bytes.length);
    }

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param bytes   array of bytes to write
     * @param start   start index of bytes to write
     * @param length  length of bytes to write
     */
    protected void set(int index, byte[] bytes, int start, int length) {
      assert index >= 0;
      int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + length);
      data.setBytes(currentOffset, bytes, start, length);
    }

    public void setSafe(int index, ByteBuffer bytes, int start, int length) {
      assert index >= 0;

      int currentOffset = offsetVector.getAccessor().get(index);

      while (data.capacity() < currentOffset + length) {
        reAlloc();
      }
      offsetVector.getMutator().setSafe(index + 1, currentOffset + length);
      data.setBytes(currentOffset, bytes, start, length);
    }

    public void setSafe(int index, byte[] bytes, int start, int length) {
      assert index >= 0;

      int currentOffset = offsetVector.getAccessor().get(index);

      while (data.capacity() < currentOffset + length) {
        reAlloc();
      }
      offsetVector.getMutator().setSafe(index + 1, currentOffset + length);
      data.setBytes(currentOffset, bytes, start, length);
    }

    public void setValueLengthSafe(int index, int length) {
      int offset = offsetVector.getAccessor().get(index);
      while(data.capacity() < offset + length ) {
        reAlloc();
      }
      offsetVector.getMutator().setSafe(index + 1, offsetVector.getAccessor().get(index) + length);
    }


    public void setSafe(int index, int start, int end, DrillBuf buffer){
      int len = end - start;

      int outputStart = offsetVector.data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});

      while(data.capacity() < outputStart + len) {
        reAlloc();
      }

      offsetVector.getMutator().setSafe( index+1,  outputStart + len);
      buffer.getBytes(start, data, outputStart, len);
    }


    public void setSafe(int index, Nullable${minor.class}Holder holder){
      assert holder.isSet == 1;

      int start = holder.start;
      int end =   holder.end;
      int len = end - start;

      int outputStart = offsetVector.data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});

      while(data.capacity() < outputStart + len) {
        reAlloc();
      }

      holder.buffer.getBytes(start, data, outputStart, len);
      offsetVector.getMutator().setSafe( index+1,  outputStart + len);
    }

    public void setSafe(int index, ${minor.class}Holder holder){

      int start = holder.start;
      int end =   holder.end;
      int len = end - start;

      int outputStart = offsetVector.data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});

      while(data.capacity() < outputStart + len) {
        reAlloc();
      }

      holder.buffer.getBytes(start, data, outputStart, len);
      offsetVector.getMutator().setSafe( index+1,  outputStart + len);
    }

    protected void set(int index, int start, int length, DrillBuf buffer){
      assert index >= 0;
      int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + length);
      DrillBuf bb = buffer.slice(start, length);
      data.setBytes(currentOffset, bb);
    }

    protected void set(int index, Nullable${minor.class}Holder holder){
      int length = holder.end - holder.start;
      int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + length);
      data.setBytes(currentOffset, holder.buffer, holder.start, length);
    }

    protected void set(int index, ${minor.class}Holder holder){
      int length = holder.end - holder.start;
      int currentOffset = offsetVector.getAccessor().get(index);
      offsetVector.getMutator().set(index + 1, currentOffset + length);
      data.setBytes(currentOffset, holder.buffer, holder.start, length);
    }

    public void setValueCount(int valueCount) {
      int currentByteCapacity = getByteCapacity();
      int idx = offsetVector.getAccessor().get(valueCount);
      data.writerIndex(idx);
      if (valueCount > 0 && currentByteCapacity > idx * 2) {
        incrementAllocationMonitor();
      } else if (allocationMonitor > 0) {
        allocationMonitor = 0;
      }
      VectorTrimmer.trim(data, idx);
      offsetVector.getMutator().setValueCount(valueCount == 0 ? 0 : valueCount+1);
    }

    @Override
    public void generateTestData(int size){
      boolean even = true;
      <#switch minor.class>
      <#case "Var16Char">
      java.nio.charset.Charset charset = Charsets.UTF_16;
      <#break>
      <#case "VarChar">
      <#default>
      java.nio.charset.Charset charset = Charsets.UTF_8;
      </#switch>
      for(int i =0; i < size; i++, even = !even){
        if(even){
          set(i, new String("aaaaa").getBytes(charset));
        }else{
          set(i, new String("bbbbbbbbbb").getBytes(charset));
        }
      }
      setValueCount(size);
    }
  }

}


</#if> <#-- type.major -->
</#list>
</#list>
