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

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />

<#if type.major == "Fixed">
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/${minor.class}Vector.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector;

<#include "/@includes/vv_imports.ftl" />

/**
 * ${minor.class} implements a vector of fixed width values.  Elements in the vector are accessed
 * by position, starting from the logical start of the vector.  Values should be pushed onto the
 * vector sequentially, but may be randomly accessed.
 *   The width of each element is ${type.width} byte(s)
 *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
 *
 * Source code generated using FreeMarker template ${.template_name}
 */
@SuppressWarnings("unused")
public final class ${minor.class}Vector extends BaseDataValueVector implements FixedWidthVector{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}Vector.class);

  private final FieldReader reader = new ${minor.class}ReaderImpl(${minor.class}Vector.this);
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();

  private int allocationSizeInBytes = INITIAL_VALUE_ALLOCATION * ${type.width};
  private int allocationMonitor = 0;

  public ${minor.class}Vector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  @Override
  public FieldReader getReader(){
    return reader;
  }

  public int getValueCapacity(){
    return (int) (data.capacity() *1.0 / ${type.width});
  }

  public Accessor getAccessor(){
    return accessor;
  }

  public Mutator getMutator(){
    return mutator;
  }

  @Override
  public void setInitialCapacity(final int valueCount) {
    final long size = 1L * valueCount * ${type.width};
    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
    }
    allocationSizeInBytes = (int)size;
  }

  public void allocateNew() {
    if(!allocateNewSafe()){
      throw new OutOfMemoryRuntimeException("Failure while allocating buffer.");
    }
  }

  public boolean allocateNewSafe() {
    long curAllocationSize = allocationSizeInBytes;
    if (allocationMonitor > 10) {
      curAllocationSize = Math.max(8, curAllocationSize / 2);
      allocationMonitor = 0;
    } else if (allocationMonitor < -2) {
      curAllocationSize = allocationSizeInBytes * 2L;
      allocationMonitor = 0;
    }

    try{
      allocateBytes(curAllocationSize);
    } catch (DrillRuntimeException ex) {
      return false;
    }
    return true;
  }

  /**
   * Allocate a new buffer that supports setting at least the provided number of values. May actually be sized bigger
   * depending on underlying buffer rounding size. Must be called prior to using the ValueVector.
   *
   * Note that the maximum number of values a vector can allocate is Integer.MAX_VALUE / value width.
   *
   * @param valueCount
   * @throws org.apache.drill.exec.memory.OutOfMemoryRuntimeException if it can't allocate the new buffer
   */
  public void allocateNew(final int valueCount) {
    allocateBytes(valueCount * ${type.width});
  }

  private void allocateBytes(final long size) {
    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
    }

    final int curSize = (int)size;
    clear();
    final DrillBuf newBuf = allocator.buffer(curSize);
    if (newBuf == null) {
      throw new OutOfMemoryRuntimeException(String.format("Failure while allocating buffer of %d bytes", size));
    }
    data = newBuf;
    data.readerIndex(0);
    allocationSizeInBytes = curSize;
  }

/**
 * Allocate new buffer with double capacity, and copy data into the new buffer. Replace vector's buffer with new buffer, and release old one
 *
 * @throws org.apache.drill.exec.memory.OutOfMemoryRuntimeException if it can't allocate the new buffer
 */
  public void reAlloc() {
    final long newAllocationSize = allocationSizeInBytes * 2L;
    if (newAllocationSize > MAX_ALLOCATION_SIZE)  {
      throw new OversizedAllocationException("Unable to expand the buffer. Max allowed buffer size is reached.");
    }

    logger.debug("Reallocating vector [{}]. # of bytes: [{}] -> [{}]", field, allocationSizeInBytes, newAllocationSize);
    final DrillBuf newBuf = allocator.buffer((int)newAllocationSize);
    if (newBuf == null) {
      throw new OutOfMemoryRuntimeException(String.format("Failure while reallocating buffer to %d bytes", newAllocationSize));
    }

    newBuf.setBytes(0, data, 0, data.capacity());
    newBuf.setZero(newBuf.capacity() / 2, newBuf.capacity() / 2);
    newBuf.writerIndex(data.writerIndex());
    data.release();
    data = newBuf;
    allocationSizeInBytes = (int)newAllocationSize;
  }

  public void zeroVector() {
    data.setZero(0, data.capacity());
  }

  @Override
  public int load(int valueCount, DrillBuf buf){
    clear();
    int len = valueCount * ${type.width};
    data = buf.slice(0, len);
    data.retain();
    data.writerIndex(len);
    return len;
  }

  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
    assert this.field.matches(metadata) : String.format("The field %s doesn't match the provided metadata %s.", this.field, metadata);
    int loaded = load(metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded : String.format("Expected to load %d bytes but actually loaded %d bytes", metadata.getBufferLength(), loaded);
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
    target.data = data;
    target.data.retain();
    target.data.writerIndex(data.writerIndex());
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, ${minor.class}Vector target) {
    int currentWriterIndex = data.writerIndex();
    int startPoint = startIndex * ${type.width};
    int sliceLength = length * ${type.width};
    target.clear();
    target.data = this.data.slice(startPoint, sliceLength);
    target.data.writerIndex(sliceLength);
    target.data.retain();
  }

  private class TransferImpl implements TransferPair{
    ${minor.class}Vector to;

    public TransferImpl(MaterializedField field){
      this.to = new ${minor.class}Vector(field, allocator);
    }

    public TransferImpl(${minor.class}Vector to) {
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

  public void copyFrom(int fromIndex, int thisIndex, ${minor.class}Vector from){
    <#if (type.width > 8)>
    from.data.getBytes(fromIndex * ${type.width}, data, thisIndex * ${type.width}, ${type.width});
    <#else> <#-- type.width <= 8 -->
    data.set${(minor.javaType!type.javaType)?cap_first}(thisIndex * ${type.width},
        from.data.get${(minor.javaType!type.javaType)?cap_first}(fromIndex * ${type.width})
    );
    </#if> <#-- type.width -->
  }

  public void copyFromSafe(int fromIndex, int thisIndex, ${minor.class}Vector from){
    while(thisIndex >= getValueCapacity()) {
        reAlloc();
    }
    copyFrom(fromIndex, thisIndex, from);
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

  public final class Accessor extends BaseDataValueVector.BaseAccessor {

    public int getValueCount() {
      return data.writerIndex() / ${type.width};
    }

    public boolean isNull(int index){
      return false;
    }

    <#if (type.width > 8)>

    public ${minor.javaType!type.javaType} get(int index) {
      return data.slice(index * ${type.width}, ${type.width});
    }

    <#if (minor.class == "Interval")>
    public void get(int index, ${minor.class}Holder holder){

      int offsetIndex = index * ${type.width};
      holder.months = data.getInt(offsetIndex);
      holder.days = data.getInt(offsetIndex + ${minor.daysOffset});
      holder.milliseconds = data.getInt(offsetIndex + ${minor.millisecondsOffset});
    }

    public void get(int index, Nullable${minor.class}Holder holder){
      int offsetIndex = index * ${type.width};
      holder.isSet = 1;
      holder.months = data.getInt(offsetIndex);
      holder.days = data.getInt(offsetIndex + ${minor.daysOffset});
      holder.milliseconds = data.getInt(offsetIndex + ${minor.millisecondsOffset});
    }

    @Override
    public ${friendlyType} getObject(int index) {
      int offsetIndex = index * ${type.width};
      int months  = data.getInt(offsetIndex);
      int days    = data.getInt(offsetIndex + ${minor.daysOffset});
      int millis = data.getInt(offsetIndex + ${minor.millisecondsOffset});
      Period p = new Period();
      return p.plusMonths(months).plusDays(days).plusMillis(millis);
    }

    public StringBuilder getAsStringBuilder(int index) {

      int offsetIndex = index * ${type.width};

      int months  = data.getInt(offsetIndex);
      int days    = data.getInt(offsetIndex + ${minor.daysOffset});
      int millis = data.getInt(offsetIndex + ${minor.millisecondsOffset});

      int years  = (months / org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);
      months = (months % org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);

      int hours  = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis);
      millis     = millis % (org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis);

      int minutes = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis);
      millis      = millis % (org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis);

      long seconds = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis);
      millis      = millis % (org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis);

      String yearString = (Math.abs(years) == 1) ? " year " : " years ";
      String monthString = (Math.abs(months) == 1) ? " month " : " months ";
      String dayString = (Math.abs(days) == 1) ? " day " : " days ";


      return(new StringBuilder().
             append(years).append(yearString).
             append(months).append(monthString).
             append(days).append(dayString).
             append(hours).append(":").
             append(minutes).append(":").
             append(seconds).append(".").
             append(millis));
    }

    <#elseif (minor.class == "IntervalDay")>
    public void get(int index, ${minor.class}Holder holder){

      int offsetIndex = index * ${type.width};
      holder.days = data.getInt(offsetIndex);
      holder.milliseconds = data.getInt(offsetIndex + ${minor.millisecondsOffset});
    }

    public void get(int index, Nullable${minor.class}Holder holder){
      int offsetIndex = index * ${type.width};
      holder.isSet = 1;
      holder.days = data.getInt(offsetIndex);
      holder.milliseconds = data.getInt(offsetIndex + ${minor.millisecondsOffset});
    }

    @Override
    public ${friendlyType} getObject(int index) {
      int offsetIndex = index * ${type.width};
      int millis = data.getInt(offsetIndex + ${minor.millisecondsOffset});
      int  days   = data.getInt(offsetIndex);
      Period p = new Period();
      return p.plusDays(days).plusMillis(millis);
    }


    public StringBuilder getAsStringBuilder(int index) {
      int offsetIndex = index * ${type.width};

      int millis = data.getInt(offsetIndex + ${minor.millisecondsOffset});
      int  days   = data.getInt(offsetIndex);

      int hours  = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis);
      millis     = millis % (org.apache.drill.exec.expr.fn.impl.DateUtility.hoursToMillis);

      int minutes = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis);
      millis      = millis % (org.apache.drill.exec.expr.fn.impl.DateUtility.minutesToMillis);

      int seconds = millis / (org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis);
      millis      = millis % (org.apache.drill.exec.expr.fn.impl.DateUtility.secondsToMillis);

      String dayString = (Math.abs(days) == 1) ? " day " : " days ";

      return(new StringBuilder().
              append(days).append(dayString).
              append(hours).append(":").
              append(minutes).append(":").
              append(seconds).append(".").
              append(millis));
    }

    <#elseif (minor.class == "Decimal28Sparse") || (minor.class == "Decimal38Sparse") || (minor.class == "Decimal28Dense") || (minor.class == "Decimal38Dense")>

    public void get(int index, ${minor.class}Holder holder) {
        holder.start = index * ${type.width};
        holder.buffer = data;
        holder.scale = getField().getScale();
        holder.precision = getField().getPrecision();
    }

    public void get(int index, Nullable${minor.class}Holder holder) {
        holder.isSet = 1;
        holder.start = index * ${type.width};
        holder.buffer = data;
        holder.scale = getField().getScale();
        holder.precision = getField().getPrecision();
    }

      @Override
      public ${friendlyType} getObject(int index) {
      <#if (minor.class == "Decimal28Sparse") || (minor.class == "Decimal38Sparse")>
      // Get the BigDecimal object
      return org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(data, index * ${type.width}, ${minor.nDecimalDigits}, getField().getScale());
      <#else>
      return org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromDense(data, index * ${type.width}, ${minor.nDecimalDigits}, getField().getScale(), ${minor.maxPrecisionDigits}, ${type.width});
      </#if>
    }

    <#else>
    public void get(int index, ${minor.class}Holder holder){
      holder.buffer = data;
      holder.start = index * ${type.width};
    }

    public void get(int index, Nullable${minor.class}Holder holder){
      holder.isSet = 1;
      holder.buffer = data;
      holder.start = index * ${type.width};
    }

    @Override
    public ${friendlyType} getObject(int index) {
      return data.slice(index * ${type.width}, ${type.width})
    }

    </#if>
    <#else> <#-- type.width <= 8 -->

    public ${minor.javaType!type.javaType} get(int index) {
      return data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});
    }

    <#if type.width == 4>
    public long getTwoAsLong(int index) {
      return data.getLong(index * ${type.width});
    }

    </#if>

    <#if minor.class == "Date">
    public ${friendlyType} getObject(int index) {
        org.joda.time.DateTime date = new org.joda.time.DateTime(get(index), org.joda.time.DateTimeZone.UTC);
        date = date.withZoneRetainFields(org.joda.time.DateTimeZone.getDefault());
        return date;
    }

    <#elseif minor.class == "TimeStamp">
    public ${friendlyType} getObject(int index) {
        org.joda.time.DateTime date = new org.joda.time.DateTime(get(index), org.joda.time.DateTimeZone.UTC);
        date = date.withZoneRetainFields(org.joda.time.DateTimeZone.getDefault());
        return date;
    }

    <#elseif minor.class == "IntervalYear">
    public ${friendlyType} getObject(int index) {

      int value = get(index);

      int years  = (value / org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);
      int months = (value % org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);
      Period p = new Period();
      return p.plusYears(years).plusMonths(months);
    }

    public StringBuilder getAsStringBuilder(int index) {

      int months  = data.getInt(index);

      int years  = (months / org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);
      months = (months % org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);

      String yearString = (Math.abs(years) == 1) ? " year " : " years ";
      String monthString = (Math.abs(months) == 1) ? " month " : " months ";

      return(new StringBuilder().
             append(years).append(yearString).
             append(months).append(monthString));
    }

    <#elseif minor.class == "Time">
    @Override
    public DateTime getObject(int index) {

        org.joda.time.DateTime time = new org.joda.time.DateTime(get(index), org.joda.time.DateTimeZone.UTC);
        time = time.withZoneRetainFields(org.joda.time.DateTimeZone.getDefault());
        return time;
    }



    <#elseif minor.class == "Decimal9" || minor.class == "Decimal18">
    @Override
    public ${friendlyType} getObject(int index) {

        BigInteger value = BigInteger.valueOf(((${type.boxedType})get(index)).${type.javaType}Value());
        return new BigDecimal(value, getField().getScale());
    }

    <#else>
    public ${friendlyType} getObject(int index) {
      return get(index);
    }
    public ${minor.javaType!type.javaType} getPrimitiveObject(int index) {
      return get(index);
    }
    </#if>

    public void get(int index, ${minor.class}Holder holder){
      <#if minor.class.startsWith("Decimal")>
      holder.scale = getField().getScale();
      holder.precision = getField().getPrecision();
      </#if>

      holder.value = data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});
    }

    public void get(int index, Nullable${minor.class}Holder holder){
      holder.isSet = 1;
      holder.value = data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});
    }


   </#if> <#-- type.width -->
 }

 /**
  * ${minor.class}.Mutator implements a mutable vector of fixed width values.  Elements in the
  * vector are accessed by position from the logical start of the vector.  Values should be pushed
  * onto the vector sequentially, but may be randomly accessed.
  *   The width of each element is ${type.width} byte(s)
  *   The equivalent Java primitive is '${minor.javaType!type.javaType}'
  *
  * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
  */
  public final class Mutator extends BaseDataValueVector.BaseMutator {

    private Mutator(){};
   /**
    * Set the element at the given index to the given value.  Note that widths smaller than
    * 32 bits are handled by the DrillBuf interface.
    *
    * @param index   position of the bit to set
    * @param value   value to set
    */
  <#if (type.width > 8)>
   public void set(int index, <#if (type.width > 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
     data.setBytes(index * ${type.width}, value, 0, ${type.width});
   }

   public void setSafe(int index, <#if (type.width > 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     data.setBytes(index * ${type.width}, value, 0, ${type.width});
   }

  <#if (minor.class == "Interval")>
   public void set(int index, int months, int days, int milliseconds){
     int offsetIndex = index * ${type.width};
     data.setInt(offsetIndex, months);
     data.setInt((offsetIndex + ${minor.daysOffset}), days);
     data.setInt((offsetIndex + ${minor.millisecondsOffset}), milliseconds);
   }

   protected void set(int index, ${minor.class}Holder holder){
     set(index, holder.months, holder.days, holder.milliseconds);
   }

   protected void set(int index, Nullable${minor.class}Holder holder){
     set(index, holder.months, holder.days, holder.milliseconds);
   }

   public void setSafe(int index, int months, int days, int milliseconds){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, months, days, milliseconds);
   }

   public void setSafe(int index, Nullable${minor.class}Holder holder){
     setSafe(index, holder.months, holder.days, holder.milliseconds);
   }

   public void setSafe(int index, ${minor.class}Holder holder){
     setSafe(index, holder.months, holder.days, holder.milliseconds);
   }

   <#elseif (minor.class == "IntervalDay")>
   public void set(int index, int days, int milliseconds){
     int offsetIndex = index * ${type.width};
     data.setInt(offsetIndex, days);
     data.setInt((offsetIndex + ${minor.millisecondsOffset}), milliseconds);
   }

   protected void set(int index, ${minor.class}Holder holder){
     set(index, holder.days, holder.milliseconds);
   }
   protected void set(int index, Nullable${minor.class}Holder holder){
     set(index, holder.days, holder.milliseconds);
   }

   public void setSafe(int index, int days, int milliseconds){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, days, milliseconds);
   }

   public void setSafe(int index, ${minor.class}Holder holder){
     setSafe(index, holder.days, holder.milliseconds);
   }

   public void setSafe(int index, Nullable${minor.class}Holder holder){
     setSafe(index, holder.days, holder.milliseconds);
   }

   <#elseif (minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse") || (minor.class == "Decimal28Dense") || (minor.class == "Decimal38Dense")>

   public void set(int index, ${minor.class}Holder holder){
     set(index, holder.start, holder.buffer);
   }

   void set(int index, Nullable${minor.class}Holder holder){
     set(index, holder.start, holder.buffer);
   }

   public void setSafe(int index,  Nullable${minor.class}Holder holder){
     setSafe(index, holder.start, holder.buffer);
   }
   public void setSafe(int index,  ${minor.class}Holder holder){
     setSafe(index, holder.start, holder.buffer);
   }

   public void setSafe(int index, int start, DrillBuf buffer){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, start, buffer);
   }

   public void set(int index, int start, DrillBuf buffer){
     data.setBytes(index * ${type.width}, buffer, start, ${type.width});
   }

   <#else>

   protected void set(int index, ${minor.class}Holder holder){
     set(index, holder.start, holder.buffer);
   }

   public void set(int index, Nullable${minor.class}Holder holder){
     set(index, holder.start, holder.buffer);
   }

   public void set(int index, int start, DrillBuf buffer){
     data.setBytes(index * ${type.width}, buffer, start, ${type.width});
   }

   public void setSafe(int index, ${minor.class}Holder holder){
     setSafe(index, holder.start, holder.buffer);
   }
   public void setSafe(int index, Nullable${minor.class}Holder holder){
     setSafe(index, holder.start, holder.buffer);
   }

   public void setSafe(int index, int start, DrillBuf buffer){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, holder);
   }

   public void set(int index, Nullable${minor.class}Holder holder){
     data.setBytes(index * ${type.width}, holder.buffer, holder.start, ${type.width});
   }
   </#if>

   @Override
   public void generateTestData(int count) {
     setValueCount(count);
     boolean even = true;
     for(int i =0; i < getAccessor().getValueCount(); i++, even = !even){
       byte b = even ? Byte.MIN_VALUE : Byte.MAX_VALUE;
       for(int w = 0; w < ${type.width}; w++){
         data.setByte(i + w, b);
       }
     }
   }




   <#else> <#-- type.width <= 8 -->
   public void set(int index, <#if (type.width >= 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
     data.set${(minor.javaType!type.javaType)?cap_first}(index * ${type.width}, value);
   }

   public void setSafe(int index, <#if (type.width >= 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, value);
   }

   protected void set(int index, ${minor.class}Holder holder){
     data.set${(minor.javaType!type.javaType)?cap_first}(index * ${type.width}, holder.value);
   }

   public void setSafe(int index, ${minor.class}Holder holder){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, holder);
   }

   protected void set(int index, Nullable${minor.class}Holder holder){
     data.set${(minor.javaType!type.javaType)?cap_first}(index * ${type.width}, holder.value);
   }

   public void setSafe(int index, Nullable${minor.class}Holder holder){
     while(index >= getValueCapacity()) {
       reAlloc();
     }
     set(index, holder);
   }

   @Override
   public void generateTestData(int size) {
     setValueCount(size);
     boolean even = true;
     for(int i =0; i < getAccessor().getValueCount(); i++, even = !even){
       if(even){
         set(i, ${minor.boxedType!type.boxedType}.MIN_VALUE);
       }else{
         set(i, ${minor.boxedType!type.boxedType}.MAX_VALUE);
       }
     }
   }


   public void generateTestDataAlt(int size) {
     setValueCount(size);
     boolean even = true;
     for(int i =0; i < getAccessor().getValueCount(); i++, even = !even){
       if(even){
         set(i, (${(minor.javaType!type.javaType)}) 1);
       }else{
         set(i, (${(minor.javaType!type.javaType)}) 0);
       }
     }
   }

  </#if> <#-- type.width -->



   public void setValueCount(int valueCount) {
     int currentValueCapacity = getValueCapacity();
     int idx = (${type.width} * valueCount);
     while(valueCount > getValueCapacity()) {
       reAlloc();
     }
     if (valueCount > 0 && currentValueCapacity > valueCount * 2) {
       incrementAllocationMonitor();
     } else if (allocationMonitor > 0) {
       allocationMonitor = 0;
     }
     VectorTrimmer.trim(data, idx);
     data.writerIndex(valueCount * ${type.width});
   }





 }
}

</#if> <#-- type.major -->
</#list>
</#list>
