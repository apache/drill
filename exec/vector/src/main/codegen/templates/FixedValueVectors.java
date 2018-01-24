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

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>
<#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />

<#if type.major == "Fixed">
  <@pp.changeOutputFile name="/org/apache/drill/exec/vector/${minor.class}Vector.java" />
  <#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector;

<#include "/@includes/vv_imports.ftl" />
import org.apache.drill.exec.util.DecimalUtility;

/**
 * ${minor.class} implements a vector of fixed width values. Elements in the
 * vector are accessed by position, starting from the logical start of the
 * vector. Values should be pushed onto the vector sequentially, but may be
 * accessed randomly.
 * <ul>
 * <li>The width of each element is {@link #VALUE_WIDTH} (= ${type.width})
 * byte<#if type.width != 1>s</#if>.</li>
 * <li>The equivalent Java primitive is '${minor.javaType!type.javaType}'.</li>
 * </ul>
 *
 * NB: this class is automatically generated from ${.template_name} and
 * ValueVectorTypes.tdd using FreeMarker.
 */
public final class ${minor.class}Vector extends BaseDataValueVector implements FixedWidthVector {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}Vector.class);

  /**
   * Width of each fixed-width value.
   */

  public static final int VALUE_WIDTH = ${type.width};

  /**
   * Maximum number of values that this fixed-width vector can hold
   * and stay below the maximum vector size limit. This is the limit
   * enforced when the vector is used to hold values in a repeated
   * vector.
   */

  public static final int MAX_VALUE_COUNT = MAX_BUFFER_SIZE / VALUE_WIDTH;

  /**
   * Maximum number of values that this fixed-width vector can hold
   * and stay below the maximum vector size limit and/or stay below
   * the maximum row count. This is the limit enforced when the
   * vector is used to hold scalar (required or nullable) values.
   * <p>
   * Note: <tt>MAX_ROW_COUNT</tt> is defined in the parent <tt>ValueVector</tt>
   * class as the maximum number of rows in a record batch (64K). Use this
   * in place of the <tt>Character.MAX_SIZE</tt> value previously used.
   */

  public static final int MAX_SCALAR_COUNT = Math.min(MAX_ROW_COUNT, MAX_VALUE_COUNT);

  /**
   * Actual maximum vector size, in bytes, given the number of fixed-width
   * values that either fit in the maximum overall vector size, or that
   * is no larger than the maximum vector item count.
   */

  public static final int NET_MAX_SCALAR_SIZE = VALUE_WIDTH * MAX_SCALAR_COUNT;

  private final FieldReader reader = new ${minor.class}ReaderImpl(${minor.class}Vector.this);
  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();

  private int allocationSizeInBytes = Math.min(INITIAL_VALUE_ALLOCATION * VALUE_WIDTH, MAX_BUFFER_SIZE);
  private int allocationMonitor = 0;

  public ${minor.class}Vector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  @Override
  public FieldReader getReader() { return reader; }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }
    return valueCount * VALUE_WIDTH;
  }

  @Override
  public int getValueCapacity() {
    return data.capacity() / VALUE_WIDTH;
  }

  @Override
  public Accessor getAccessor() { return accessor; }

  @Override
  public Mutator getMutator() { return mutator; }

  @Override
  public void setInitialCapacity(final int valueCount) {
    final long size = (long) valueCount * VALUE_WIDTH;
    // TODO: Replace this with MAX_BUFFER_SIZE once all
    // code is aware of the maximum vector size.
    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
    }
    allocationSizeInBytes = (int) size;
  }

  @Override
  public void allocateNew() {
    if (!allocateNewSafe()) {
      throw new OutOfMemoryException("Failure while allocating buffer.");
    }
  }

  @Override
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
   * Allocate a new buffer that supports setting at least the provided number of
   * values. May actually be sized bigger depending on underlying buffer
   * rounding size. Must be called prior to using the ValueVector.
   *
   * Note that the maximum number of values a vector can allocate is
   * Integer.MAX_VALUE / value width.
   *
   * @param valueCount
   * @throws OutOfMemoryException
   *           if it can't allocate the new buffer
   */
  @Override
  public void allocateNew(final int valueCount) {
    allocateBytes(valueCount * VALUE_WIDTH);
  }

  @Override
  public void reset() {
    allocationSizeInBytes = INITIAL_VALUE_ALLOCATION;
    allocationMonitor = 0;
    zeroVector();
    super.reset();
  }

  private void allocateBytes(final long size) {
    // TODO: Replace this with MAX_BUFFER_SIZE once all
    // code is aware of the maximum vector size.
    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
    }

    final int curSize = (int)size;
    clear();
    data = allocator.buffer(curSize);
    data.readerIndex(0);
    allocationSizeInBytes = curSize;
  }

  /**
   * Allocate new buffer with double capacity, and copy data into the new
   * buffer. Replace vector's buffer with new buffer, and release old one
   *
   * @throws org.apache.drill.exec.memory.OutOfMemoryException
   *           if it can't allocate the new buffer
   */

  public void reAlloc() {

    // Avoid an infinite loop if we try to double the size of
    // a zero-length buffer. Instead, just allocate a 256 byte
    // buffer if we start at 0.

    final long newAllocationSize = allocationSizeInBytes == 0
        ? 256
        : allocationSizeInBytes * 2L;

    // TODO: Replace this with MAX_BUFFER_SIZE once all
    // code is aware of the maximum vector size.

    if (newAllocationSize > MAX_ALLOCATION_SIZE)  {
      throw new OversizedAllocationException("Unable to expand the buffer. Max allowed buffer size is reached.");
    }

    reallocRaw((int) newAllocationSize);
    final int halfNewCapacity = data.capacity() / 2;
    data.setZero(halfNewCapacity, halfNewCapacity);
  }

  @Override
  public DrillBuf reallocRaw(int newAllocationSize) {
    logger.debug("Reallocating vector [{}]. # of bytes: [{}] -> [{}]", field, allocationSizeInBytes, newAllocationSize);
    if (newAllocationSize == 0) {
      throw new IllegalStateException("Attempt to reAlloc a zero-sized vector");
    }
    final DrillBuf newBuf = allocator.buffer(newAllocationSize);
    newBuf.setBytes(0, data, 0, data.capacity());
    newBuf.writerIndex(data.writerIndex());
    data.release(1);
    data = newBuf;
    allocationSizeInBytes = newAllocationSize;
    return newBuf;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void zeroVector() {
    data.setZero(0, data.capacity());
  }

  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
    Preconditions.checkArgument(this.field.getName().equals(metadata.getNamePart().getName()),
                                "The field %s doesn't match the provided metadata %s.", this.field, metadata);
    final int actualLength = metadata.getBufferLength();
    final int valueCount = metadata.getValueCount();
    final int expectedLength = valueCount * VALUE_WIDTH;
    assert actualLength == expectedLength : String.format("Expected to load %d bytes but actually loaded %d bytes", expectedLength, actualLength);

    clear();
    if (data != null) {
      data.release(1);
    }
    data = buffer.slice(0, actualLength);
    data.retain(1);
    data.writerIndex(actualLength);
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new TransferImpl(getField(), allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(getField().withPath(ref), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((${minor.class}Vector) to);
  }

  public void transferTo(${minor.class}Vector target) {
    target.clear();
    target.data = data.transferOwnership(target.allocator).buffer;
    target.data.writerIndex(data.writerIndex());
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, ${minor.class}Vector target) {
    final int startPoint = startIndex * VALUE_WIDTH;
    final int sliceLength = length * VALUE_WIDTH;
    target.clear();
    target.data = data.slice(startPoint, sliceLength).transferOwnership(target.allocator).buffer;
    target.data.writerIndex(sliceLength);
  }

  @Override
  public int getPayloadByteCount(int valueCount) {
    return valueCount * ${type.width};
  }

  private class TransferImpl implements TransferPair {
    private ${minor.class}Vector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator) {
      to = new ${minor.class}Vector(field, allocator);
    }

    public TransferImpl(${minor.class}Vector to) {
      this.to = to;
    }

    @Override
    public ${minor.class}Vector getTo() {
      return to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, ${minor.class}Vector.this);
    }
  }

  public void copyFrom(int fromIndex, int thisIndex, ${minor.class}Vector from) {
    <#if (type.width > 8)>
    from.data.getBytes(fromIndex * VALUE_WIDTH, data, thisIndex * VALUE_WIDTH, VALUE_WIDTH);
    <#else> <#-- type.width <= 8 -->
    data.set${(minor.javaType!type.javaType)?cap_first}(thisIndex * VALUE_WIDTH,
        from.data.get${(minor.javaType!type.javaType)?cap_first}(fromIndex * VALUE_WIDTH)
    );
    </#if> <#-- type.width -->
  }

  public void copyFromSafe(int fromIndex, int thisIndex, ${minor.class}Vector from) {
    while(thisIndex >= getValueCapacity()) {
        reAlloc();
    }
    copyFrom(fromIndex, thisIndex, from);
  }

  @Override
  public void copyEntry(int toIndex, ValueVector from, int fromIndex) {
    ((${minor.class}Vector) from).data.getBytes(fromIndex * VALUE_WIDTH, data, toIndex * VALUE_WIDTH, VALUE_WIDTH);
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

  @Override
  public void toNullable(ValueVector nullableVector) {
    Nullable${minor.class}Vector dest = (Nullable${minor.class}Vector) nullableVector;
    dest.getMutator().fromNotNullable(this);
  }

  public final class Accessor extends BaseDataValueVector.BaseAccessor {
    @Override
    public int getValueCount() {
      return data.writerIndex() / VALUE_WIDTH;
    }

    @Override
    public boolean isNull(int index) {
      return false;
    }
    <#if (type.width > 8)>
    
    public ${minor.javaType!type.javaType} get(int index) {
      return data.slice(index * VALUE_WIDTH, VALUE_WIDTH);
    }
    <#if (minor.class == "Interval")>
    
    public void get(int index, ${minor.class}Holder holder) {
      final int offsetIndex = index * VALUE_WIDTH;
      holder.months = data.getInt(offsetIndex);
      holder.days = data.getInt(offsetIndex + ${minor.daysOffset});
      holder.milliseconds = data.getInt(offsetIndex + ${minor.millisecondsOffset});
    }

    public void get(int index, Nullable${minor.class}Holder holder) {
      final int offsetIndex = index * VALUE_WIDTH;
      holder.isSet = 1;
      holder.months = data.getInt(offsetIndex);
      holder.days = data.getInt(offsetIndex + ${minor.daysOffset});
      holder.milliseconds = data.getInt(offsetIndex + ${minor.millisecondsOffset});
    }

    @Override
    public ${friendlyType} getObject(int index) {
      final int offsetIndex = index * VALUE_WIDTH;
      final int months  = data.getInt(offsetIndex);
      final int days    = data.getInt(offsetIndex + ${minor.daysOffset});
      final int millis = data.getInt(offsetIndex + ${minor.millisecondsOffset});
      return DateUtilities.fromInterval(months, days, millis);
    }

    public StringBuilder getAsStringBuilder(int index) {
      final int offsetIndex = index * VALUE_WIDTH;
      final int months = data.getInt(offsetIndex);
      final int days   = data.getInt(offsetIndex + ${minor.daysOffset});
      final int millis = data.getInt(offsetIndex + ${minor.millisecondsOffset});      
      return DateUtilities.intervalStringBuilder(months, days, millis);
    }
    <#elseif (minor.class == "IntervalDay")>
    
    public void get(int index, ${minor.class}Holder holder) {
      final int offsetIndex = index * VALUE_WIDTH;
      holder.days = data.getInt(offsetIndex);
      holder.milliseconds = data.getInt(offsetIndex + ${minor.millisecondsOffset});
    }

    public void get(int index, Nullable${minor.class}Holder holder) {
      final int offsetIndex = index * VALUE_WIDTH;
      holder.isSet = 1;
      holder.days = data.getInt(offsetIndex);
      holder.milliseconds = data.getInt(offsetIndex + ${minor.millisecondsOffset});
    }

    @Override
    public ${friendlyType} getObject(int index) {
      final int offsetIndex = index * VALUE_WIDTH;
      final int days   = data.getInt(offsetIndex);
      final int millis = data.getInt(offsetIndex + ${minor.millisecondsOffset});
      return DateUtilities.fromIntervalDay(days, millis);
    }

    public StringBuilder getAsStringBuilder(int index) {
      final int offsetIndex = index * VALUE_WIDTH;
      final int days   = data.getInt(offsetIndex);
      final int millis = data.getInt(offsetIndex + ${minor.millisecondsOffset});     
      return DateUtilities.intervalDayStringBuilder(days, millis);
    }
    <#elseif minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse" || minor.class == "Decimal28Dense" || minor.class == "Decimal38Dense">

    public void get(int index, ${minor.class}Holder holder) {
      holder.start = index * VALUE_WIDTH;
      holder.buffer = data;
      holder.scale = getField().getScale();
      holder.precision = getField().getPrecision();
    }

    public void get(int index, Nullable${minor.class}Holder holder) {
      holder.isSet = 1;
      holder.start = index * VALUE_WIDTH;
      holder.buffer = data;
      holder.scale = getField().getScale();
      holder.precision = getField().getPrecision();
    }

    @Override
    public ${friendlyType} getObject(int index) {
      <#if (minor.class == "Decimal28Sparse") || (minor.class == "Decimal38Sparse")>
      // Get the BigDecimal object
      return DecimalUtility.getBigDecimalFromSparse(data, index * VALUE_WIDTH, ${minor.nDecimalDigits}, getField().getScale());
      <#else>
      return DecimalUtility.getBigDecimalFromDense(data, index * VALUE_WIDTH,
          ${minor.nDecimalDigits}, getField().getScale(),
          ${minor.maxPrecisionDigits}, VALUE_WIDTH);
      </#if>
    }
    <#else>

    public void get(int index, ${minor.class}Holder holder) {
      holder.buffer = data;
      holder.start = index * VALUE_WIDTH;
    }

    public void get(int index, Nullable${minor.class}Holder holder) {
      holder.isSet = 1;
      holder.buffer = data;
      holder.start = index * VALUE_WIDTH;
    }

    @Override
    public ${friendlyType} getObject(int index) {
      return data.slice(index * VALUE_WIDTH, VALUE_WIDTH)
    }
    </#if>
    <#else> <#-- type.width <= 8 -->
    
    public ${minor.javaType!type.javaType} get(int index) {
      return data.get${(minor.javaType!type.javaType)?cap_first}(index * VALUE_WIDTH);
    }
    <#if type.width == 4>
    
    public long getTwoAsLong(int index) {
      return data.getLong(index * VALUE_WIDTH);
    }
    </#if>
    <#if minor.class == "Date">

    @Override
    public ${friendlyType} getObject(int index) {
      org.joda.time.DateTime date = new org.joda.time.DateTime(get(index), org.joda.time.DateTimeZone.UTC);
      date = date.withZoneRetainFields(org.joda.time.DateTimeZone.getDefault());
      return date;
    }
    <#elseif minor.class == "TimeStamp">

    @Override
    public ${friendlyType} getObject(int index) {
      org.joda.time.DateTime date = new org.joda.time.DateTime(get(index), org.joda.time.DateTimeZone.UTC);
      date = date.withZoneRetainFields(org.joda.time.DateTimeZone.getDefault());
      return date;
    }
    <#elseif minor.class == "IntervalYear">

    @Override
    public ${friendlyType} getObject(int index) {
      return DateUtilities.fromIntervalYear(get(index));
    }

    public StringBuilder getAsStringBuilder(int index) {
      return DateUtilities.intervalYearStringBuilder(data.getInt(index));
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
      final BigInteger value = BigInteger.valueOf(((${type.boxedType})get(index)).${type.javaType}Value());
      return new BigDecimal(value, getField().getScale());
    }
    <#else>

    @Override
    public ${friendlyType} getObject(int index) {
      return get(index);
    }

    public ${minor.javaType!type.javaType} getPrimitiveObject(int index) {
      return get(index);
    }
    </#if>

    public void get(int index, ${minor.class}Holder holder) {
      <#if minor.class.startsWith("Decimal")>
      holder.scale = getField().getScale();
      holder.precision = getField().getPrecision();
      </#if>

      holder.value = data.get${(minor.javaType!type.javaType)?cap_first}(index * VALUE_WIDTH);
    }

    public void get(int index, Nullable${minor.class}Holder holder) {
      holder.isSet = 1;
      holder.value = data.get${(minor.javaType!type.javaType)?cap_first}(index * VALUE_WIDTH);
    }
    </#if> <#-- type.width -->
  }

  /**
   * ${minor.class}.Mutator implements a mutable vector of fixed width values.
   * Elements in the vector are accessed by position from the logical start of
   * the vector. Values should be pushed onto the vector sequentially, but may
   * be randomly accessed.
   * <ul>
   * <li>The width of each element is {@link #VALUE_WIDTH} (= ${type.width})
   * byte(s).</li>
   * <li>The equivalent Java primitive is '${minor.javaType!type.javaType}'</li>
   * </ul>
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using
   * FreeMarker.
   */
   public final class Mutator extends BaseDataValueVector.BaseMutator {

    private Mutator() {};

    /**
     * Set the element at the given index to the given value. Note that widths
     * smaller than 32 bits are handled by the DrillBuf interface.
     *
     * @param index
     *          position of the bit to set
     * @param value
     *          value to set
     */
  <#if (type.width > 8)>
  
    public void set(int index, <#if (type.width > 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
      data.setBytes(index * VALUE_WIDTH, value, 0, VALUE_WIDTH);
    }

    public void setSafe(int index, <#if (type.width > 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
      while(index >= getValueCapacity()) {
        reAlloc();
      }
      data.setBytes(index * VALUE_WIDTH, value, 0, VALUE_WIDTH);
    }
    <#if minor.class == "Interval">
    
    public void set(int index, int months, int days, int milliseconds) {
      final int offsetIndex = index * VALUE_WIDTH;
      data.setInt(offsetIndex, months);
      data.setInt((offsetIndex + ${minor.daysOffset}), days);
      data.setInt((offsetIndex + ${minor.millisecondsOffset}), milliseconds);
    }

    public void setSafe(int index, int months, int days, int milliseconds) {
      while(index >= getValueCapacity()) {
        reAlloc();
      }
      set(index, months, days, milliseconds);
    }

    protected void set(int index, ${minor.class}Holder holder) {
      set(index, holder.months, holder.days, holder.milliseconds);
    }

    public void setSafe(int index, ${minor.class}Holder holder) {
      setSafe(index, holder.months, holder.days, holder.milliseconds);
    }

    protected void set(int index, Nullable${minor.class}Holder holder) {
      set(index, holder.months, holder.days, holder.milliseconds);
    }

    public void setSafe(int index, Nullable${minor.class}Holder holder) {
      setSafe(index, holder.months, holder.days, holder.milliseconds);
    }
    <#elseif minor.class == "IntervalDay">
    
    public void set(int index, int days, int milliseconds) {
      final int offsetIndex = index * VALUE_WIDTH;
      data.setInt(offsetIndex, days);
      data.setInt((offsetIndex + ${minor.millisecondsOffset}), milliseconds);
    }

    public void setSafe(int index, int days, int milliseconds) {
      while(index >= getValueCapacity()) {
        reAlloc();
      }
      set(index, days, milliseconds);
    }

    protected void set(int index, ${minor.class}Holder holder) {
      set(index, holder.days, holder.milliseconds);
    }

    public void setSafe(int index, ${minor.class}Holder holder) {
      setSafe(index, holder.days, holder.milliseconds);
    }

    protected void set(int index, Nullable${minor.class}Holder holder) {
      set(index, holder.days, holder.milliseconds);
    }

    public void setSafe(int index, Nullable${minor.class}Holder holder) {
      setSafe(index, holder.days, holder.milliseconds);
    }
    <#elseif minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse" || minor.class == "Decimal28Dense" || minor.class == "Decimal38Dense">
    
    public void setSafe(int index, int start, DrillBuf buffer) {
      while(index >= getValueCapacity()) {
        reAlloc();
      }
      set(index, start, buffer);
    }

    public void set(int index, ${minor.class}Holder holder) {
      set(index, holder.start, holder.buffer);
    }

    public void setSafe(int index, ${minor.class}Holder holder) {
      setSafe(index, holder.start, holder.buffer);
    }

    void set(int index, Nullable${minor.class}Holder holder) {
      set(index, holder.start, holder.buffer);
    }

    public void setSafe(int index, Nullable${minor.class}Holder holder) {
      setSafe(index, holder.start, holder.buffer);
    }
      <#if minor.class == "Decimal28Sparse" || minor.class == "Decimal38Sparse">
      
    public void set(int index, BigDecimal value) {
      DecimalUtility.getSparseFromBigDecimal(value, data, index * VALUE_WIDTH,
           field.getScale(), field.getPrecision(), ${minor.nDecimalDigits});
    }

    public void setSafe(int index, BigDecimal value) {
      while(index >= getValueCapacity()) {
        reAlloc();
      }
      set(index, value);
    }
      </#if>
      
    public void set(int index, int start, DrillBuf buffer) {
      data.setBytes(index * VALUE_WIDTH, buffer, start, VALUE_WIDTH);
    }
    </#if>
    
    @Override
    public void generateTestData(int count) {
      setValueCount(count);
      boolean even = true;
      final int valueCount = getAccessor().getValueCount();
      for(int i = 0; i < valueCount; i++, even = !even) {
        final byte b = even ? Byte.MIN_VALUE : Byte.MAX_VALUE;
        for(int w = 0; w < VALUE_WIDTH; w++) {
          data.setByte(i + w, b);
        }
      }
    }
  <#else> <#-- type.width <= 8 -->
  
    public void set(int index, <#if (type.width >= 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
      data.set${(minor.javaType!type.javaType)?cap_first}(index * VALUE_WIDTH, value);
    }

    /**
     * Set the value of a required or nullable vector. Grows the vector as needed.
     * Does not enforce size limits; scalar fixed-width types can never overflow
     * a vector.
     * @param index item to write
     */

    public void setSafe(int index, <#if (type.width >= 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
      while(index >= getValueCapacity()) {
        reAlloc();
      }
      set(index, value);
    }

    protected void set(int index, ${minor.class}Holder holder) {
      data.set${(minor.javaType!type.javaType)?cap_first}(index * VALUE_WIDTH, holder.value);
    }

    public void setSafe(int index, ${minor.class}Holder holder) {
      while(index >= getValueCapacity()) {
        reAlloc();
      }
      set(index, holder);
    }

    protected void set(int index, Nullable${minor.class}Holder holder) {
      data.set${(minor.javaType!type.javaType)?cap_first}(index * VALUE_WIDTH, holder.value);
    }

    public void setSafe(int index, Nullable${minor.class}Holder holder) {
      while(index >= getValueCapacity()) {
        reAlloc();
      }
      set(index, holder);
    }

    @Override
    public void generateTestData(int size) {
      setValueCount(size);
      boolean even = true;
      final int valueCount = getAccessor().getValueCount();
      for(int i = 0; i < valueCount; i++, even = !even) {
        if(even) {
          set(i, ${minor.boxedType!type.boxedType}.MIN_VALUE);
        } else {
          set(i, ${minor.boxedType!type.boxedType}.MAX_VALUE);
        }
      }
    }

    public void generateTestDataAlt(int size) {
      setValueCount(size);
      boolean even = true;
      final int valueCount = getAccessor().getValueCount();
      for(int i = 0; i < valueCount; i++, even = !even) {
        if(even) {
          set(i, (${(minor.javaType!type.javaType)}) 1);
        } else {
          set(i, (${(minor.javaType!type.javaType)}) 0);
        }
      }
    }
  </#if> <#-- type.width -->
  
    @Override
    public void setValueCount(int valueCount) {
      final int currentValueCapacity = getValueCapacity();
      final int idx = (VALUE_WIDTH * valueCount);
      while(valueCount > getValueCapacity()) {
        reAlloc();
      }
      if (valueCount > 0 && currentValueCapacity > valueCount * 2) {
        incrementAllocationMonitor();
      } else if (allocationMonitor > 0) {
        allocationMonitor = 0;
      }
      VectorTrimmer.trim(data, idx);
      data.writerIndex(valueCount * VALUE_WIDTH);
    }
  }
}
</#if> <#-- type.major -->
</#list>
</#list>
