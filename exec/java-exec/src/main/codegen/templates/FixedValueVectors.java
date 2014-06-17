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

import java.lang.Long;
import java.lang.Override;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.joda.time.Period;

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
 * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
 */
@SuppressWarnings("unused")
public final class ${minor.class}Vector extends BaseDataValueVector implements FixedWidthVector{

  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();

  private int allocationValueCount = 4096;
  private int allocationMonitor = 0;
  
  public ${minor.class}Vector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
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


  public void allocateNew() {
    if(!allocateNewSafe()){
      throw new OutOfMemoryRuntimeException("Failure while allocating buffer.");
    }
  }
  
  public boolean allocateNewSafe() {
    clear();
    if (allocationMonitor > 10) {
      allocationValueCount = Math.max(8, (int) (allocationValueCount / 2));
      allocationMonitor = 0;
    } else if (allocationMonitor < -2) {
      allocationValueCount = (int) (allocationValueCount * 2);
      allocationMonitor = 0;
    }
    this.data = allocator.buffer(allocationValueCount * ${type.width});
    if(data == null) return false;
    this.data.readerIndex(0);
    return true;
  }

  /**
   * Allocate a new buffer that supports setting at least the provided number of values.  May actually be sized bigger depending on underlying buffer rounding size. Must be called prior to using the ValueVector.
   * @param valueCount
   */
  public void allocateNew(int valueCount) {
    clear();
    this.data = allocator.buffer(valueCount * ${type.width});
    this.data.readerIndex(0);
    this.allocationValueCount = valueCount;
  }

  /**
   * {@inheritDoc}
   */
  public void zeroVector() {
    data.setZero(0, data.capacity());
  }

  @Override
  public SerializedField getMetadata() {
    return getMetadataBuilder()
             .setValueCount(valueCount)
             .setBufferLength(getBufferSize())
             .build();
  }

  public int getBufferSize() {
    if(valueCount == 0) return 0;
    return valueCount * ${type.width};
  }

  @Override
  public int load(int valueCount, ByteBuf buf){
    clear();
    this.valueCount = valueCount;
    int len = valueCount * ${type.width};
    data = buf.slice(0, len);
    data.retain();
    data.writerIndex(len);
    return len;
  }
  
  @Override
  public void load(SerializedField metadata, ByteBuf buffer) {
    assert this.field.matches(metadata);
    int loaded = load(metadata.getValueCount(), buffer);
    assert metadata.getBufferLength() == loaded;
  }
  
  public TransferPair getTransferPair(){
    return new TransferImpl(getField());
  }
  public TransferPair getTransferPair(FieldReference ref){
    return new TransferImpl(getField().clone(ref));
  }

  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((${minor.class}Vector) to);
  }

  public void transferTo(${minor.class}Vector target){
    target.data = data;
    target.data.retain();
    target.data.writerIndex(data.writerIndex());
    target.valueCount = valueCount;
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, ${minor.class}Vector target) {
    int currentWriterIndex = data.writerIndex();
    int startPoint = startIndex * ${type.width};
    int sliceLength = length * ${type.width};
    target.valueCount = length;
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
    public boolean copyValueSafe(int fromIndex, int toIndex) {
      return to.copyFromSafe(fromIndex, toIndex, ${minor.class}Vector.this);
    }
  }
  
  protected void copyFrom(int fromIndex, int thisIndex, ${minor.class}Vector from){
    <#if (type.width > 8)>
    from.data.getBytes(fromIndex * ${type.width}, data, thisIndex * ${type.width}, ${type.width});
    <#else> <#-- type.width <= 8 -->
    data.set${(minor.javaType!type.javaType)?cap_first}(thisIndex * ${type.width}, 
        from.data.get${(minor.javaType!type.javaType)?cap_first}(fromIndex * ${type.width})
    );
    </#if> <#-- type.width -->
  }
  
  public boolean copyFromSafe(int fromIndex, int thisIndex, ${minor.class}Vector from){
    if(thisIndex >= getValueCapacity()) {
      decrementAllocationMonitor();
      return false;
    }
    copyFrom(fromIndex, thisIndex, from);
    return true;
  }

  private void decrementAllocationMonitor() {
    if (allocationMonitor > 0) {
      allocationMonitor = 0;
    }
    --allocationMonitor;
  }

  private void incrementAllocationMonitor() {
    ++allocationMonitor;
  }

  public final class Accessor extends BaseValueVector.BaseAccessor{

    final FieldReader reader = new ${minor.class}ReaderImpl(${minor.class}Vector.this);
    
    public FieldReader getReader(){
      return reader;
    }
    
    public int getValueCount() {
      return valueCount;
    }
    
    public boolean isNull(int index){
      return false;
    }
    
    <#if (type.width > 8)>

    public ${minor.javaType!type.javaType} get(int index) {
      ByteBuf dst = io.netty.buffer.Unpooled.wrappedBuffer(new byte[${type.width}]);
      //dst = new io.netty.buffer.SwappedByteBuf(dst);
      data.getBytes(index * ${type.width}, dst, 0, ${type.width});

      return dst;
    }

    <#if (minor.class == "TimeStampTZ")>
    public void get(int index, ${minor.class}Holder holder){
      holder.value = data.getLong(index * ${type.width});
      holder.index = data.getInt((index * ${type.width})+ ${minor.milliSecondsSize});
    }

    public void get(int index, Nullable${minor.class}Holder holder){
      holder.isSet = 1;
      holder.value = data.getLong(index * ${type.width});
      holder.index = data.getInt((index * ${type.width})+ ${minor.milliSecondsSize});
    }

    @Override
    public ${friendlyType} getObject(int index) {
      long l = data.getLong(index * ${type.width});
      DateTime t = new DateTime(l);
      return t;
    }

    <#elseif (minor.class == "Interval")>
    public void get(int index, ${minor.class}Holder holder){

      int offsetIndex = index * ${type.width};
      holder.months = data.getInt(offsetIndex);
      holder.days = data.getInt(offsetIndex + ${minor.daysOffset});
      holder.milliSeconds = data.getInt(offsetIndex + ${minor.milliSecondsOffset});
    }

    public void get(int index, Nullable${minor.class}Holder holder){
      int offsetIndex = index * ${type.width};
      holder.isSet = 1;
      holder.months = data.getInt(offsetIndex);
      holder.days = data.getInt(offsetIndex + ${minor.daysOffset});
      holder.milliSeconds = data.getInt(offsetIndex + ${minor.milliSecondsOffset});
    }

    @Override
    public ${friendlyType} getObject(int index) {
      int offsetIndex = index * ${type.width};
      int months  = data.getInt(offsetIndex);
      int days    = data.getInt(offsetIndex + ${minor.daysOffset});
      int millis = data.getInt(offsetIndex + ${minor.milliSecondsOffset});
      Period p = new Period();
      return p.plusMonths(months).plusDays(days).plusMillis(millis);
    }
    
    public StringBuilder getAsStringBuilder(int index) {

      int offsetIndex = index * ${type.width};

      int months  = data.getInt(offsetIndex);
      int days    = data.getInt(offsetIndex + ${minor.daysOffset});
      int millis = data.getInt(offsetIndex + ${minor.milliSecondsOffset});

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
      holder.milliSeconds = data.getInt(offsetIndex + ${minor.milliSecondsOffset});
    }

    public void get(int index, Nullable${minor.class}Holder holder){
      int offsetIndex = index * ${type.width};
      holder.isSet = 1;
      holder.days = data.getInt(offsetIndex);
      holder.milliSeconds = data.getInt(offsetIndex + ${minor.milliSecondsOffset});
    }

    @Override
    public ${friendlyType} getObject(int index) {
      int offsetIndex = index * ${type.width};
      int millis = data.getInt(offsetIndex + ${minor.milliSecondsOffset});
      int  days   = data.getInt(offsetIndex);
      Period p = new Period();
      return p.plusDays(days).plusMillis(millis);
    }

    
    public StringBuilder getAsStringBuilder(int index) {
      int offsetIndex = index * ${type.width};

      int millis = data.getInt(offsetIndex + ${minor.milliSecondsOffset});
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

        /* The buffer within the value vector is little endian.
         * For the dense representation though, we use big endian
         * byte ordering (internally). This is because we shift bits to the right and
         * big endian ordering makes sense for this purpose.  So we have to deal with
         * the sign bit for the two representation in a slightly different fashion
         */

        // Get the sign of the decimal
          <#if minor.class.endsWith("Sparse")>
          if ((holder.buffer.getInt(holder.start) & 0x80000000) != 0) {
          <#elseif minor.class.endsWith("Dense")>
          if ((holder.buffer.getInt(holder.start) & 0x00000080) != 0) {
          </#if>
            holder.sign = true;
        }

        holder.scale = getField().getScale();
        holder.precision = getField().getPrecision();


    }

    public void get(int index, Nullable${minor.class}Holder holder) {
        holder.isSet = 1;
        holder.start = index * ${type.width};

        holder.buffer = data;

          // Get the sign the of the decimal
          <#if minor.class.endsWith("Sparse")>
          if ((holder.buffer.getInt(holder.start) & 0x80000000) != 0) {
          <#elseif minor.class.endsWith("Dense")>
          if ((holder.buffer.getInt(holder.start) & 0x00000080) != 0) {
          </#if>
            holder.sign = true;
        }
    }

      @Override
      public ${friendlyType} getObject(int index) {
      <#if (minor.class == "Decimal28Sparse") || (minor.class == "Decimal38Sparse")>
      // Get the BigDecimal object
      return org.apache.drill.common.util.DecimalUtility.getBigDecimalFromSparse(data, index * ${type.width}, ${minor.nDecimalDigits}, getField().getScale());
      <#else>
      return org.apache.drill.common.util.DecimalUtility.getBigDecimalFromDense(data, index * ${type.width}, ${minor.nDecimalDigits}, getField().getScale(), ${minor.maxPrecisionDigits}, ${type.width});
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

      ByteBuf dst = io.netty.buffer.Unpooled.wrappedBuffer(new byte[${type.width}]);
      //dst = new io.netty.buffer.SwappedByteBuf(dst);
      data.getBytes(index * ${type.width}, dst, 0, ${type.width});

      return dst;



    }

    </#if>
    <#else> <#-- type.width <= 8 -->

    public ${minor.javaType!type.javaType} get(int index) {
      return data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});
    }

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
  public final class Mutator extends BaseValueVector.BaseMutator{

    private Mutator(){};
   /**
    * Set the element at the given index to the given value.  Note that widths smaller than
    * 32 bits are handled by the ByteBuf interface.
    *
    * @param index   position of the bit to set
    * @param value   value to set
    */
  <#if (type.width > 8)>
   public void set(int index, <#if (type.width > 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
     data.setBytes(index * ${type.width}, value, 0, ${type.width});
   }

   public boolean setSafe(int index, <#if (type.width > 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
     if(index >= getValueCapacity()) {
       decrementAllocationMonitor();
       return false;
     }
     data.setBytes(index * ${type.width}, value, 0, ${type.width});
     return true;
   }

   <#if (minor.class == "TimeStampTZ")>
   protected void set(int index, ${minor.class}Holder holder){
     data.setLong((index * ${type.width}), holder.value);
     data.setInt(((index * ${type.width}) + ${minor.milliSecondsSize}), holder.index);

   }

   protected void set(int index, Nullable${minor.class}Holder holder){
     data.setLong((index * ${type.width}), holder.value);
     data.setInt(((index * ${type.width}) + ${minor.milliSecondsSize}), holder.index);
   }

   public boolean setSafe(int index, ${minor.class}Holder holder){
     if(index >= getValueCapacity()) {
       decrementAllocationMonitor();
       return false;
     }
     set(index, holder);
     return true;
   }

   public boolean setSafe(int index, Nullable${minor.class}Holder holder){
     if(index >= getValueCapacity()) {
       decrementAllocationMonitor();
       return false;
     }
     set(index, holder);
     return true;
   }
   <#elseif (minor.class == "Interval")>
   protected void set(int index, ${minor.class}Holder holder){
     int offsetIndex = index * ${type.width};
     data.setInt(offsetIndex, holder.months);
     data.setInt((offsetIndex + ${minor.daysOffset}), holder.days);
     data.setInt((offsetIndex + ${minor.milliSecondsOffset}), holder.milliSeconds);
   }

   protected void set(int index, Nullable${minor.class}Holder holder){
     int offsetIndex = index * ${type.width};
     data.setInt(offsetIndex, holder.months);
     data.setInt((offsetIndex + ${minor.daysOffset}), holder.days);
     data.setInt((offsetIndex + ${minor.milliSecondsOffset}), holder.milliSeconds);
   }

   public boolean setSafe(int index, ${minor.class}Holder holder){
     if(index >= getValueCapacity()) {
       decrementAllocationMonitor();
       return false;
     }
     set(index, holder);
     return true;
   }

   public boolean setSafe(int index, Nullable${minor.class}Holder holder){
     if(index >= getValueCapacity()) {
       decrementAllocationMonitor();
       return false;
     }
     set(index, holder);
     return true;
   }
   <#elseif (minor.class == "IntervalDay")>
   protected void set(int index, ${minor.class}Holder holder){
     int offsetIndex = index * ${type.width};
     data.setInt(offsetIndex, holder.days);
     data.setInt((offsetIndex + ${minor.milliSecondsOffset}), holder.milliSeconds);
   }

   protected void set(int index, Nullable${minor.class}Holder holder){
     int offsetIndex = index * ${type.width};
     data.setInt(offsetIndex, holder.days);
     data.setInt((offsetIndex + ${minor.milliSecondsOffset}), holder.milliSeconds);
   }

   public boolean setSafe(int index, ${minor.class}Holder holder){
     if(index >= getValueCapacity()) {
       decrementAllocationMonitor();
       return false;
     }
     set(index, holder);
     return true;
   }

   public boolean setSafe(int index, Nullable${minor.class}Holder holder){
     if(index >= getValueCapacity()) {
       decrementAllocationMonitor();
       return false;
     }
     set(index, holder);
     return true;
   }

   <#elseif (minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse") || (minor.class == "Decimal28Dense") || (minor.class == "Decimal38Dense")>

   public void set(int index, ${minor.class}Holder holder){
      data.setBytes(index * ${type.width}, holder.buffer, holder.start, ${type.width});

      // Set the sign of the decimal
      if (holder.sign == true) {
          int value = data.getInt(index * ${type.width});
          <#if minor.class.endsWith("Sparse")>
          data.setInt(index * ${type.width}, (value | 0x80000000));
          <#elseif minor.class.endsWith("Dense")>
          data.setInt(index * ${type.width}, (value | 0x00000080));
          </#if>

      }
   }

   void set(int index, Nullable${minor.class}Holder holder){
       data.setBytes(index * ${type.width}, holder.buffer, holder.start, ${type.width});

      // Set the sign of the decimal
      if (holder.sign == true) {
          int value = data.getInt(index * ${type.width});
          <#if minor.class.endsWith("Sparse")>
          data.setInt(index * ${type.width}, (value | 0x80000000));
          <#elseif minor.class.endsWith("Dense")>
          data.setInt(index * ${type.width}, (value | 0x00000080));
          </#if>
      }
   }

   public boolean setSafe(int index,  Nullable${minor.class}Holder holder){
       if(index >= getValueCapacity()) {
         decrementAllocationMonitor();
         return false;
       }
       set(index, holder);
       return true;
   }

   public boolean setSafe(int index,  ${minor.class}Holder holder){
       if(index >= getValueCapacity()) {
         decrementAllocationMonitor();
         return false;
       }
       set(index, holder);
       return true;
   }

   <#else>
   protected void set(int index, ${minor.class}Holder holder){
     data.setBytes(index * ${type.width}, holder.buffer, holder.start, ${type.width});
   }
   
   public boolean setSafe(int index, ${minor.class}Holder holder){
     if(index >= getValueCapacity()) {
       decrementAllocationMonitor();
       return false;
     }
     set(index, holder);
     return true;
   }

   public void set(int index, Nullable${minor.class}Holder holder){
     data.setBytes(index * ${type.width}, holder.buffer, holder.start, ${type.width});
   }
   </#if>

   @Override
   public void generateTestData(int count) {
     setValueCount(count);
     boolean even = true;
     for(int i =0; i < valueCount; i++, even = !even){
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

   public boolean setSafe(int index, <#if (type.width >= 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
     if(index >= getValueCapacity()) {
       decrementAllocationMonitor();
       return false;
     }
     set(index, value);
     return true;
   }

   protected void set(int index, ${minor.class}Holder holder){
     data.set${(minor.javaType!type.javaType)?cap_first}(index * ${type.width}, holder.value);
   }

   public boolean setSafe(int index, ${minor.class}Holder holder){
     if(index >= getValueCapacity()) {
       decrementAllocationMonitor();
       return false;
     }
     set(index, holder);
     return true;
   }

   protected void set(int index, Nullable${minor.class}Holder holder){
     data.set${(minor.javaType!type.javaType)?cap_first}(index * ${type.width}, holder.value);
   }

   public boolean setSafe(int index, Nullable${minor.class}Holder holder){
     if(index >= getValueCapacity()) {
       decrementAllocationMonitor();
       return false;
     }
     set(index, holder);
     return true;
   }

   @Override
   public void generateTestData(int size) {
     setValueCount(size);
     boolean even = true;
     for(int i =0; i < valueCount; i++, even = !even){
       if(even){
         set(i, ${minor.boxedType!type.boxedType}.MIN_VALUE);
       }else{
         set(i, ${minor.boxedType!type.boxedType}.MAX_VALUE);
       }
     }
   }

  </#if> <#-- type.width -->
  

  
   public void setValueCount(int valueCount) {
     int currentValueCapacity = getValueCapacity();
     ${minor.class}Vector.this.valueCount = valueCount;
     int idx = (${type.width} * valueCount);
     if (valueCount > 0 && currentValueCapacity > valueCount * 2) {
       incrementAllocationMonitor();
     } else if (allocationMonitor > 0) {
       allocationMonitor = 0;
     }
     data.writerIndex(idx);
     if (data instanceof AccountingByteBuf) {
       data.capacity(idx);
       data.writerIndex(idx);
     }
   }




  
 }
}

</#if> <#-- type.major -->
</#list>
</#list>