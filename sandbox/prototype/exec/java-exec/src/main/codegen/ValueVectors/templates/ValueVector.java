/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.record.vector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import io.netty.buffer.ByteBuf;
import java.io.Closeable;
import java.util.Random;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;

/**
 * ValueVectorTypes defines a set of template-generated classes which implement type-specific
 * value vectors.  The template approach was chosen due to the lack of multiple inheritence.  It
 * is also important that all related logic be as efficient as possible.
 */
public class ValueVector {

  /**
   * ValueVector.Base implements common logic for all immutable value vectors.
   */
  public abstract static class Base implements Closeable {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Base.class);

    protected final BufferAllocator allocator;
    protected ByteBuf data = DeadBuf.DEAD_BUFFER;
    protected MaterializedField field;
    protected int recordCount;
    protected int totalBytes;

    public Base(MaterializedField field, BufferAllocator allocator) {
      this.allocator = allocator;
      this.field = field;
    }

    /**
     * Get the explicitly specified size of the allocated buffer, if available.  Otherwise
     * calculate the size based on width and record count.
     */
    public abstract int getAllocatedSize();

    /**
     * Get the size requirement (in bytes) for the given number of values.  Takes derived
     * type specs into account.
     */
    public abstract int getSizeFromCount(int valueCount);

    /**
     * Get the Java Object representation of the element at the specified position
     *
     * @param index   Index of the value to get
     */
    public abstract Object getObject(int index);

    /**
     * Return the underlying buffers associated with this vector. Note that this doesn't impact the
     * reference counts for this buffer so it only should be used for in-context access. Also note
     * that this buffer changes regularly thus external classes shouldn't hold a reference to
     * it (unless they change it).
     *
     * @return The underlying ByteBuf.
     */
    public ByteBuf[] getBuffers() {
      return new ByteBuf[]{data};
    }

    /**
     * Returns the maximum number of values contained within this vector.
     * @return Vector size
     */
    public int capacity() {
      return getRecordCount();
    }

    /**
     * Release supporting resources.
     */
    @Override
    public void close() {
      clear();
    }

    /**
     * Get information about how this field is materialized.
     * @return
     */
    public MaterializedField getField() {
      return field;
    }

    /**
     * Get the number of records allocated for this value vector.
     * @return number of allocated records
     */
    public int getRecordCount() {
      return recordCount;
    }

    /**
     * Get the metadata for this field.
     * @return
     */
    public FieldMetadata getMetadata() {
      int len = 0;
      for(ByteBuf b : getBuffers()){
        len += b.writerIndex();
      }
      return FieldMetadata.newBuilder()
               .setDef(getField().getDef())
               .setValueCount(getRecordCount())
               .setBufferLength(len)
               .build();
    }

    /**
     * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
     *
     * @param totalBytes   Optional desired size of the underlying buffer.  Specifying 0 will
     *                     estimate the size based on valueCount.
     * @param sourceBuffer Optional ByteBuf to use for storage (null will allocate automatically).
     * @param valueCount   Number of values in the vector.
     */
    public void allocateNew(int totalBytes, ByteBuf sourceBuffer, int valueCount) {
      clear();
      this.recordCount = valueCount;
      this.totalBytes = totalBytes > 0 ? totalBytes : getSizeFromCount(valueCount);
      this.data = (sourceBuffer != null) ? sourceBuffer : allocator.buffer(this.totalBytes);
      this.data.retain();
      data.readerIndex(0);
    }

    /**
     * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
     *
     * @param valueCount
     *          The number of elements which can be contained within this vector.
     */
    public void allocateNew(int valueCount) {
      allocateNew(0, null, valueCount);
    }

    /**
     * Release the underlying ByteBuf and reset the ValueVector
     */
    protected void clear() {
      if (data != DeadBuf.DEAD_BUFFER) {
        data.release();
        data = DeadBuf.DEAD_BUFFER;
        recordCount = 0;
        totalBytes = 0;
      }
    }

    /**
     * Define the number of records that are in this value vector.
     * @param recordCount Number of records active in this vector.
     */
    public void setRecordCount(int recordCount) {
      data.writerIndex(getSizeFromCount(recordCount));
      this.recordCount = recordCount;
    }

    /**
     * For testing only -- randomize the buffer contents
     */
    public void randomizeData() { }

  }

  /**
   * Bit implements a vector of bit-width values.  Elements in the vector are accessed
   * by position from the logical start of the vector.
   *   The width of each element is 1 bit.
   *   The equivilent Java primitive is an int containing the value '0' or '1'.
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public static class Bit extends Base {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Bit.class);

    public Bit(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
    }

    /**
     * Get the byte holding the desired bit, then mask all other bits.  Iff the result is 0, the
     * bit was not set.
     *
     * @param  index   position of the bit in the vector
     * @return 1 if set, otherwise 0
     */
    public int get(int index) {
      // logger.debug("BIT GET: index: {}, byte: {}, mask: {}, masked byte: {}",
      //             index,
      //             data.getByte((int)Math.floor(index/8)),
      //             (int)Math.pow(2, (index % 8)),
      //             data.getByte((int)Math.floor(index/8)) & (int)Math.pow(2, (index % 8)));
      return ((data.getByte((int)Math.floor(index/8)) & (int)Math.pow(2, (index % 8))) == 0) ? 0 : 1;
    }

    @Override
    public Object getObject(int index) {
      return new Boolean(get(index) != 0);
    }

    /**
     * Get the size requirement (in bytes) for the given number of values.
     */
    @Override
    public int getSizeFromCount(int valueCount) {
      return (int) Math.ceil(valueCount / 8);
    }

    @Override
    public int getAllocatedSize() {
      return totalBytes;
    }

    public MutableBit getMutable() {
      return (MutableBit)this;
    }

    /**
     * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
     *
     * @param valueCount  The number of values which can be contained within this vector.
     */
    @Override
    public void allocateNew(int valueCount) {
      allocateNew(getSizeFromCount(valueCount), null, valueCount);
      for (int i = 0; i < getSizeFromCount(valueCount); i++) {
        data.setByte(i, 0);
      }
    }

  }

  /**
   * MutableBit implements a vector of bit-width values.  Elements in the vector are accessed
   * by position from the logical start of the vector.  Values should be pushed onto the vector
   * sequentially, but may be randomly accessed.
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public static class MutableBit extends Bit {

    public MutableBit(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
    }

    /**
     * Set the bit at the given index to the specified value.
     *
     * @param index   position of the bit to set
     * @param value   value to set (either 1 or 0)
     */
    public void set(int index, int value) {
      byte currentByte = data.getByte((int)Math.floor(index/8));
      if (value != 0) {
        // true
        currentByte |= (byte) Math.pow(2, (index % 8));
      }
      else if ((currentByte & (byte) Math.pow(2, (index % 8))) == (byte) Math.pow(2, (index % 8))) {
        // false, and bit was previously set
        currentByte -= (byte) Math.pow(2, (index % 8));
      }
      data.setByte((int) Math.floor(index/8), currentByte);
    }

    @Override
    public void randomizeData() {
      if (this.data != DeadBuf.DEAD_BUFFER) {
        Random r = new Random();
        for (int i = 0; i < data.capacity() - 1; i++) {
          byte[] bytes = new byte[1];
          r.nextBytes(bytes);
          data.setByte(i, bytes[0]);
        }
      }
    }
  }

<#list types as type>
 <#list type.minor as minor>
  <#if type.major == "Fixed">

  /**
   * ${minor.class} implements a vector of fixed width values.  Elements in the vector are accessed
   * by position, starting from the logical start of the vector.  Values should be pushed onto the
   * vector sequentially, but may be randomly accessed.
   *   The width of each element is ${type.width} byte(s)
   *   The equivilent Java primitive is '${minor.javaType!type.javaType}'
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public static class ${minor.class} extends Base {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}.class);

    public ${minor.class}(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
    }

    /**
     * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
     *
     * @param valueCount
     *          The number of values which can be contained within this vector.
     */
    public void allocateNew(int valueCount) {
      totalBytes = valueCount * ${type.width};
      allocateNew(totalBytes, allocator.buffer(totalBytes), valueCount);
    }

    @Override
    public int getAllocatedSize() {
      return (int) Math.ceil(totalBytes);
    }

    /**
     * Get the size requirement (in bytes) for the given number of values.  Only accurate
     * for fixed width value vectors.
     */
    @Override
    public int getSizeFromCount(int valueCount) {
      return valueCount * ${type.width};
    }

    public Mutable${minor.class} getMutable() {
      return (Mutable${minor.class})this;
    }

   <#if (type.width > 8)>

    public ${minor.javaType!type.javaType} get(int index) {
      ByteBuf dst = allocator.buffer(${type.width});
      data.getBytes(index * ${type.width}, dst, 0, ${type.width});
      return dst;
    }

    @Override
    public Object getObject(int index) {
      ByteBuf dst = allocator.buffer(${type.width});
      data.getBytes(index, dst, 0, ${type.width});
      return dst;
    }

    @Override
    public void randomizeData() {
      if (this.data != DeadBuf.DEAD_BUFFER) {
        Random r = new Random();
        for(int i =0; i < data.capacity()-${type.width}; i += ${type.width}){
          byte[] bytes = new byte[${type.width}];
          r.nextBytes(bytes);
          data.setByte(i, bytes[0]);
        }
      }
    }

   <#else> <#-- type.width <= 8 -->

    public ${minor.javaType!type.javaType} get(int index) {
      return data.get${(minor.javaType!type.javaType)?cap_first}(index * ${type.width});
    }

    public Object getObject(int index) {
      return get(index);
    }

    @Override
    public void randomizeData() {
      if (this.data != DeadBuf.DEAD_BUFFER) {
        Random r = new Random();
        for(int i =0; i < data.capacity()-${type.width}; i += ${type.width}){
          data.set${(minor.javaType!type.javaType)?cap_first}(i,
              r.next<#if (type.width >= 4)>${(minor.javaType!type.javaType)?cap_first}
                    <#else>Int
                    </#if>());
        }
      }
    }

   </#if> <#-- type.width -->
  }

  /**
   * Mutable${minor.class} implements a mutable vector of fixed width values.  Elements in the
   * vector are accessed by position from the logical start of the vector.  Values should be pushed
   * onto the vector sequentially, but may be randomly accessed.
   *   The width of each element is ${type.width} byte(s)
   *   The equivilent Java primitive is '${minor.javaType!type.javaType}'
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
   public static class Mutable${minor.class} extends ${minor.class} {

    public Mutable${minor.class}(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
    }

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
   <#else> <#-- type.width <= 8 -->
    public void set(int index, <#if (type.width >= 4)>${minor.javaType!type.javaType}<#else>int</#if> value) {
      data.set${(minor.javaType!type.javaType)?cap_first}(index * ${type.width}, value);
   </#if> <#-- type.width -->
    }
  }

  <#elseif type.major == "VarLen">

  /**
   * ${minor.class} implements a vector of variable width values.  Elements in the vector
   * are accessed by position from the logical start of the vector.  A fixed width lengthVector
   * is used to convert an element's position to it's offset from the start of the (0-based)
   * ByteBuf.  Size is inferred by adjacent elements.
   *   The width of each element is ${type.width} byte(s)
   *   The equivilent Java primitive is '${minor.javaType!type.javaType}'
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public static class ${minor.class} extends Base {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${minor.class}.class);

    protected final MutableUInt${type.width} lengthVector;

    public ${minor.class}(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
      this.lengthVector = new MutableUInt${type.width}(null, allocator);
    }

    public byte[] get(int index) {
      checkArgument(index >= 0);
      int startIdx = 0;
      int size = 0;
      if (index == 0) {
        size = lengthVector.get(1);
      } else {
        startIdx = lengthVector.get(index);
        size = lengthVector.get(index + 1) - startIdx;
      }
      checkState(size >= 0);
      byte[] dst = new byte[size];
      data.getBytes(startIdx, dst, 0, size);
      return dst;
    }

    @Override
    public int getAllocatedSize() {
      return lengthVector.getAllocatedSize() + totalBytes;
    }

    /**
     * Get the size requirement (in bytes) for the given number of values.  Only accurate
     * for fixed width value vectors.
     */
    public int getSizeFromCount(int valueCount) {
      return valueCount * ${type.width};
    }

    @Override
    protected void clear() {
      super.clear();
      lengthVector.clear();
    }

    /**
     * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
     *
     * @param valueCount
     *          The number of values which can be contained within this vector.
     */
    public void allocateNew(int totalBytes, ByteBuf sourceBuffer, int valueCount) {
      super.allocateNew(totalBytes, sourceBuffer, valueCount);
      lengthVector.allocateNew(valueCount);
    }

    @Override
    public ByteBuf[] getBuffers() {
      return new ByteBuf[]{lengthVector.data, data};
    }

    public Object getObject(int index) {
      return get(index);
    }

    public Mutable${minor.class} getMutable() {
      return (Mutable${minor.class})this;
    }
  }

  /**
   * Mutable${minor.class} implements a vector of variable width values.  Elements in the vector
   * are accessed by position from the logical start of the vector.  A fixed width lengthVector
   * is used to convert an element's position to it's offset from the start of the (0-based)
   * ByteBuf.  Size is inferred by adjacent elements.
   *   The width of each element is ${type.width} byte(s)
   *   The equivilent Java primitive is '${minor.javaType!type.javaType}'
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public static class Mutable${minor.class} extends ${minor.class} {

    public Mutable${minor.class}(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
    }

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param bytes   array of bytes to write
     */
    public void set(int index, byte[] bytes) {
      checkArgument(index >= 0);
      if (index == 0) {
        lengthVector.set(0, 0);
        lengthVector.set(1, bytes.length);
        data.setBytes(0, bytes);
      }
      else {
        int currentOffset = lengthVector.get(index);
        // set the end offset of the buffer
        lengthVector.set(index + 1, currentOffset + bytes.length);
        data.setBytes(currentOffset, bytes);
      }
    }

    @Override
    public void setRecordCount(int recordCount) {
      super.setRecordCount(recordCount);
      lengthVector.setRecordCount(recordCount);
    }

  }

  </#if> <#-- type.major -->

  /**
   * Nullable${minor.class} implements a vector of values which could be null.  Elements in the vector
   * are first checked against a fixed length vector of boolean values.  Then the element is retrieved
   * from the base class (if not null).
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public static class Nullable${minor.class} extends Mutable${minor.class} {

    protected MutableBit bits;

    public Nullable${minor.class}(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
      bits = new MutableBit(null, allocator);
    }

    /**
     * Set the variable length element at the specified index to the supplied byte array.
     *
     * @param index   position of the bit to set
     * @param bytes   array of bytes to write
     */
    public void set(int index, <#if type.major == "VarLen">byte[]<#elseif (type.width < 4)>int<#else>${minor.javaType!type.javaType}</#if> value) {
      setNotNull(index);
      super.set(index, value);
    }

    /**
     * Get the element at the specified position.
     *
     * @param   index   position of the value
     * @return  value of the element, if not null
     * @throws  NullValueException if the value is null
     */
    public <#if type.major == "VarLen">byte[]<#else>${minor.javaType!type.javaType}</#if> get(int index) {
      if (isNull(index))
        throw new NullValueException(index);
      return super.get(index);
    }

    public void setNull(int index) {
      bits.set(index, 0);
    }

    private void setNotNull(int index) {
      bits.set(index, 1);
    }

    public boolean isNull(int index) {
      return bits.get(index) == 0;
    }

    /**
     * Allocate a new memory space for this vector.  Must be called prior to using the ValueVector.
     *
     * @param valueCount   The number of values which may be contained by this vector.
     */
    public void allocateNew(int totalBytes, ByteBuf sourceBuffer, int valueCount) {
      super.allocateNew(totalBytes, sourceBuffer, valueCount);
      bits.allocateNew(valueCount);
    }

    @Override
    public int getAllocatedSize() {
      return bits.getAllocatedSize() + super.getAllocatedSize();
    }

    /**
     * Get the size requirement (in bytes) for the given number of values.  Only accurate
     * for fixed width value vectors.
     */
    public int getTotalSizeFromCount(int valueCount) {
      return getSizeFromCount(valueCount) + bits.getSizeFromCount(valueCount);
    }

    @Override
    public MaterializedField getField() {
      return field;
    }

    @Override
    public ByteBuf[] getBuffers() {
      return new ByteBuf[]{bits.data, super.data};
    }

    @Override
    public void setRecordCount(int recordCount) {
      super.setRecordCount(recordCount);
      bits.setRecordCount(recordCount);
    }

    @Override
    public Object getObject(int index) {
      return isNull(index) ? null : super.getObject(index);
    }
  }

  /**
   * Repeated${minor.class} implements a vector with multple values per row (e.g. JSON array or
   * repeated protobuf field).  The implementation uses two additional value vectors; one to convert
   * the index offset to the underlying element offset, and another to store the number of values
   * in the vector.
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
   public static class Repeated${minor.class} extends Mutable${minor.class} {

    private MutableUInt4 countVector;    // number of repeated elements in each record
    private MutableUInt4 offsetVector;   // offsets to start of each record

    public Repeated${minor.class}(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
      countVector = new MutableUInt4(null, allocator);
      offsetVector = new MutableUInt4(null, allocator);
    }

    public void allocateNew(int totalBytes, ByteBuf sourceBuffer, int valueCount) {
      super.allocateNew(totalBytes, sourceBuffer, valueCount);
      countVector.allocateNew(valueCount);
      offsetVector.allocateNew(valueCount);
    }

    /**
     * Add an element to the given record index.  This is similar to the set() method in other
     * value vectors, except that it permits setting multiple values for a single record.
     *
     * @param index   record of the element to add
     * @param value   value to add to the given row
     */
    public void add(int index, <#if (type.width > 4)> ${minor.javaType!type.javaType}
                               <#elseif type.major == "VarLen"> byte[]
                               <#else> int
                               </#if> value) {
      countVector.set(index, countVector.get(index) + 1);
      offsetVector.set(index, offsetVector.get(index - 1) + countVector.get(index-1));
      super.set(offsetVector.get(index), value);
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

      assert positionIndex < countVector.get(index);
      return super.get(offsetVector.get(index) + positionIndex);
    }

    public MaterializedField getField() {
      return field;
    }

    /**
     * Get the size requirement (in bytes) for the given number of values.  Only accurate
     * for fixed width value vectors.
     */
    public int getTotalSizeFromCount(int valueCount) {
      return getSizeFromCount(valueCount) +
             countVector.getSizeFromCount(valueCount) +
             offsetVector.getSizeFromCount(valueCount);
    }

    /**
     * Get the explicitly specified size of the allocated buffer, if available.  Otherwise
     * calculate the size based on width and record count.
     */
    public int getAllocatedSize() {
      return super.getAllocatedSize() +
             countVector.getAllocatedSize() +
             offsetVector.getAllocatedSize();
    }

    /**
     * Get the elements at the given index.
     */
    public int getCount(int index) {
      return countVector.get(index);
    }

    public void setRecordCount(int recordCount) {
      super.setRecordCount(recordCount);
      offsetVector.setRecordCount(recordCount);
      countVector.setRecordCount(recordCount);
    }

    public ByteBuf[] getBuffers() {
      return new ByteBuf[]{countVector.data, offsetVector.data, data};
    }

    public Object getObject(int index) {
      return data.slice(index, getSizeFromCount(countVector.get(index)));
    }

  }
 </#list>
</#list>
}

