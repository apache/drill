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

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>

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
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}Vector.class);

 
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();
  
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
  
  

  /**
   * Allocate a new buffer that supports setting at least the provided number of values.  May actually be sized bigger depending on underlying buffer rounding size. Must be called prior to using the ValueVector.
   * @param valueCount
   */
  public void allocateNew(int valueCount) {
    clear();
    this.data = allocator.buffer(valueCount * ${type.width});
    this.data.readerIndex(0);
  }
  
  @Override
  public FieldMetadata getMetadata() {
    return FieldMetadata.newBuilder()
             .setDef(getField().getDef())
             .setValueCount(valueCount)
             .setBufferLength(valueCount * ${type.width})
             .build();
  }

  @Override
  public int load(int valueCount, ByteBuf buf){
    clear();
    this.valueCount = valueCount;
    int len = valueCount * ${type.width};
    data = buf.slice(0, len);
    data.retain();
    return len;
  }
  
  @Override
  public void load(FieldMetadata metadata, ByteBuf buffer) {
    assert this.field.getDef().equals(metadata.getDef());
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
    target.valueCount = valueCount;
    clear();
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
    
    @Override
    public void copyValue(int fromIndex, int toIndex) {
      to.copyFrom(fromIndex, toIndex, ${minor.class}Vector.this);
    }
  }
  
  public void copyFrom(int fromIndex, int thisIndex, ${minor.class}Vector from){
    <#if (type.width > 8)>
    data.getBytes(fromIndex * ${type.width}, from.data, thisIndex * ${type.width}, ${type.width});
    <#else> <#-- type.width <= 8 -->
    data.set${(minor.javaType!type.javaType)?cap_first}(thisIndex * ${type.width}, 
        from.data.get${(minor.javaType!type.javaType)?cap_first}(fromIndex * ${type.width})
    );
    </#if> <#-- type.width -->
  }
  
  public boolean copyFromSafe(int fromIndex, int thisIndex, ${minor.class}Vector from){
    if(thisIndex >= getValueCapacity()) return false;
    copyFrom(fromIndex, thisIndex, from);
    return true;
  }

  public final class Accessor extends BaseValueVector.BaseAccessor{

    public int getValueCount() {
      return valueCount;
    }
    
    public boolean isNull(int index){
      return false;
    }
    
    <#if (type.width > 8)>

    public ${minor.javaType!type.javaType} get(int index) {
      ByteBuf dst = allocator.buffer(${type.width});
      data.getBytes(index * ${type.width}, dst, 0, ${type.width});
      return dst;
    }

    <#if (minor.class == "TimeStampTZ")>
    public void get(int index, ${minor.class}Holder holder){
      holder.value = data.getLong(index * ${type.width});
      holder.index = data.getInt((index * ${type.width})+ ${minor.milliSecondsSize});
    }

    void get(int index, Nullable${minor.class}Holder holder){
      holder.value = data.getLong(index * ${type.width});
      holder.index = data.getInt((index * ${type.width})+ ${minor.milliSecondsSize});
    }

    @Override
    public Object getObject(int index) {

        return new Timestamp(data.getLong(index * ${type.width}));
    }

    <#elseif (minor.class == "Interval")>
    public void get(int index, ${minor.class}Holder holder){

      int offsetIndex = index * ${type.width};
      holder.months = data.getInt(offsetIndex);
      holder.days = data.getInt(offsetIndex + ${minor.daysOffset});
      holder.milliSeconds = data.getInt(offsetIndex + ${minor.milliSecondsOffset});
    }

    void get(int index, Nullable${minor.class}Holder holder){
      int offsetIndex = index * ${type.width};
      holder.months = data.getInt(offsetIndex);
      holder.days = data.getInt(offsetIndex + ${minor.daysOffset});
      holder.milliSeconds = data.getInt(offsetIndex + ${minor.milliSecondsOffset});
    }

    @Override
    public Object getObject(int index) {

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

    void get(int index, Nullable${minor.class}Holder holder){
      int offsetIndex = index * ${type.width};
      holder.days = data.getInt(offsetIndex);
      holder.milliSeconds = data.getInt(offsetIndex + ${minor.milliSecondsOffset});
    }

    @Override
    public Object getObject(int index) {
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

    <#else>

    public void get(int index, ${minor.class}Holder holder){
      holder.buffer = data;
      holder.start = index * ${type.width};
    }

    void get(int index, Nullable${minor.class}Holder holder){
      holder.buffer = data;
      holder.start = index * ${type.width};
    }

    @Override
    public Object getObject(int index) {
      ByteBuf dst = allocator.buffer(${type.width});
      data.getBytes(index, dst, 0, ${type.width});
      return dst;
    }

    </#if>

    <#else> <#-- type.width <= 8 -->

    public ${minor.javaType!type.javaType} get(int index) {
      return data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});
    }

    <#if minor.class == "Date">
    public Object getObject(int index) {
        org.joda.time.DateTime date = new org.joda.time.DateTime(get(index), org.joda.time.DateTimeZone.UTC);
        date = date.withZoneRetainFields(org.joda.time.DateTimeZone.getDefault());

        return new Date(date.getMillis());

    }
    <#elseif minor.class == "TimeStamp">
    public Object getObject(int index) {
        org.joda.time.DateTime date = new org.joda.time.DateTime(get(index), org.joda.time.DateTimeZone.UTC);
        date = date.withZoneRetainFields(org.joda.time.DateTimeZone.getDefault());

        return new Date(date.getMillis());
    }
    <#elseif minor.class == "IntervalYear">
    public Object getObject(int index) {

      int value = get(index);

      int years  = (value / org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);
      int months = (value % org.apache.drill.exec.expr.fn.impl.DateUtility.yearsToMonths);

      String yearString = (Math.abs(years) == 1) ? " year " : " years ";
      String monthString = (Math.abs(months) == 1) ? " month " : " months ";


      return(new StringBuilder().append(years).append(yearString).append(months).append(monthString));


    }
    <#elseif minor.class == "Time">
    @Override
    public Object getObject(int index) {

        org.joda.time.DateTime time = new org.joda.time.DateTime(get(index), org.joda.time.DateTimeZone.UTC);
        time = time.withZoneRetainFields(org.joda.time.DateTimeZone.getDefault());

        return new Time(time.getMillis());
    }

    <#else>
    public Object getObject(int index) {
      return get(index);
    }
    </#if>
    
    public void get(int index, ${minor.class}Holder holder){
      holder.value = data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});
    }

    void get(int index, Nullable${minor.class}Holder holder){
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
     data.setBytes(index * ${type.width}, value);
   }

   <#if (minor.class == "TimeStampTZ")>
   public void set(int index, ${minor.class}Holder holder){
     data.setLong((index * ${type.width}), holder.value);
     data.setInt(((index * ${type.width}) + ${minor.milliSecondsSize}), holder.index);

   }

   void set(int index, Nullable${minor.class}Holder holder){
     data.setLong((index * ${type.width}), holder.value);
     data.setInt(((index * ${type.width}) + ${minor.milliSecondsSize}), holder.index);
   }

   public boolean setSafe(int index, ${minor.class}Holder holder){
     if(index >= getValueCapacity()) return false;
     set(index, holder);
     return true;
   }

   public boolean setSafe(int index, Nullable${minor.class}Holder holder){
     if(index >= getValueCapacity()) return false;
     set(index, holder);
     return true;
   }
   <#elseif (minor.class == "Interval")>
   public void set(int index, ${minor.class}Holder holder){
     int offsetIndex = index * ${type.width};
     data.setInt(offsetIndex, holder.months);
     data.setInt((offsetIndex + ${minor.daysOffset}), holder.days);
     data.setInt((offsetIndex + ${minor.milliSecondsOffset}), holder.milliSeconds);
   }

   void set(int index, Nullable${minor.class}Holder holder){
     int offsetIndex = index * ${type.width};
     data.setInt(offsetIndex, holder.months);
     data.setInt((offsetIndex + ${minor.daysOffset}), holder.days);
     data.setInt((offsetIndex + ${minor.milliSecondsOffset}), holder.milliSeconds);
   }

   public boolean setSafe(int index, ${minor.class}Holder holder){
     if(index >= getValueCapacity()) return false;
     set(index, holder);
     return true;
   }

   public boolean setSafe(int index, Nullable${minor.class}Holder holder){
     if(index >= getValueCapacity()) return false;
     set(index, holder);
     return true;
   }
   <#elseif (minor.class == "IntervalDay")>
   public void set(int index, ${minor.class}Holder holder){
     int offsetIndex = index * ${type.width};
     data.setInt(offsetIndex, holder.days);
     data.setInt((offsetIndex + ${minor.milliSecondsOffset}), holder.milliSeconds);
   }

   void set(int index, Nullable${minor.class}Holder holder){
     int offsetIndex = index * ${type.width};
     data.setInt(offsetIndex, holder.days);
     data.setInt((offsetIndex + ${minor.milliSecondsOffset}), holder.milliSeconds);
   }

   public boolean setSafe(int index, ${minor.class}Holder holder){
     if(index >= getValueCapacity()) return false;
     set(index, holder);
     return true;
   }

   public boolean setSafe(int index, Nullable${minor.class}Holder holder){
     if(index >= getValueCapacity()) return false;
     set(index, holder);
     return true;
   }
   <#else>
   public void set(int index, ${minor.class}Holder holder){
     data.setBytes(index * ${type.width}, holder.buffer, holder.start, ${type.width});
   }
   
   public boolean setSafe(int index, ${minor.class}Holder holder){
     if(index >= getValueCapacity()) return false;
     set(index, holder);
     return true;
   }

   void set(int index, Nullable${minor.class}Holder holder){
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
     if(index >= getValueCapacity()) return false;
     set(index, value);
     return true;
   }

   public void set(int index, ${minor.class}Holder holder){
     data.set${(minor.javaType!type.javaType)?cap_first}(index * ${type.width}, holder.value);
   }

   void set(int index, Nullable${minor.class}Holder holder){
     data.set${(minor.javaType!type.javaType)?cap_first}(index * ${type.width}, holder.value);
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
     ${minor.class}Vector.this.valueCount = valueCount;
     data.writerIndex(${type.width} * valueCount);
   }




  
 }
}

</#if> <#-- type.major -->
</#list>
</#list>