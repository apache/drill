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
package org.apache.drill.exec.vector;

import io.netty.buffer.ByteBuf;

import java.io.Closeable;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;

/**
 * ValueVectorTypes defines a set of template-generated classes which implement type-specific
 * value vectors.  The template approach was chosen due to the lack of multiple inheritence.  It
 * is also important that all related logic be as efficient as possible.
 */
public abstract class ValueVector implements Closeable {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueVector.class);

  protected final BufferAllocator allocator;
  protected ByteBuf data = DeadBuf.DEAD_BUFFER;
  protected MaterializedField field;
  protected int recordCount;
  protected int totalBytes;

  ValueVector(MaterializedField field, BufferAllocator allocator) {
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

  
  public abstract Mutator getMutator();
  
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

  //public abstract <T extends Mutator> T getMutator();
  
  /**
   * Define the number of records that are in this value vector.
   * @param recordCount Number of records active in this vector.
   */
  void setRecordCount(int recordCount) {
    data.writerIndex(getSizeFromCount(recordCount));
    this.recordCount = recordCount;
  }

  /**
   * For testing only -- randomize the buffer contents
   */
  public void randomizeData() { }

  
  public static interface Mutator{
    public void randomizeData();
    public void setRecordCount(int recordCount);
  }
}

