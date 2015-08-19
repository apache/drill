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
package org.apache.drill.exec.vector;

import java.io.Closeable;

import io.netty.buffer.DrillBuf;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

/**
 * An abstraction that is used to store a sequence of values in an individual column.
 *
 * A {@link ValueVector value vector} stores underlying data in-memory in a columnar fashion that is compact and
 * efficient. The column whose data is stored, is referred by {@link #getField()}.
 *
 * A vector when instantiated, relies on a {@link org.apache.drill.exec.record.DeadBuf dead buffer}. It is important
 * that vector is allocated before attempting to read or write.
 *
 */
public interface ValueVector extends Closeable, Iterable<ValueVector> {

  /**
   * Allocate new buffers. ValueVector implements logic to determine how much to allocate.
   * @throws OutOfMemoryRuntimeException Thrown if no memory can be allocated.
   */
  void allocateNew() throws OutOfMemoryRuntimeException;

  /**
   * Allocates new buffers. ValueVector implements logic to determine how much to allocate.
   * @return Returns true if allocation was succesful.
   */
  boolean allocateNewSafe();

  /**
   * Set the initial record capacity
   * @param numRecords
   */
  void setInitialCapacity(int numRecords);

  /**
   * Returns the maximum number of values that can be stored in this vector instance.
   */
  int getValueCapacity();

  /**
   * Alternative to clear(). Allows use as an AutoCloseable in try-with-resources.
   */
  @Override
  void close();

  /**
   * Release the underlying DrillBuf and reset the ValueVector to empty.
   */
  void clear();

  /**
   * Get information about how this field is materialized.
   */
  MaterializedField getField();

  /**
   * Returns a {@link org.apache.drill.exec.record.TransferPair transfer pair}, creating a new target vector of
   * the same type.
   */
  TransferPair getTransferPair();

  TransferPair getTransferPair(FieldReference ref);

  /**
   * Returns a new {@link org.apache.drill.exec.record.TransferPair transfer pair} that is used to transfer underlying
   * buffers into the target vector.
   */
  TransferPair makeTransferPair(ValueVector target);

  /**
   * Returns an {@link org.apache.drill.exec.vector.ValueVector.Accessor accessor} that is used to read from this vector
   * instance.
   */
  Accessor getAccessor();

  /**
   * Returns an {@link org.apache.drill.exec.vector.ValueVector.Mutator mutator} that is used to write to this vector
   * instance.
   */
  Mutator getMutator();

  /**
   * Returns a {@link org.apache.drill.exec.vector.complex.reader.FieldReader field reader} that supports reading values
   * from this vector.
   */
  FieldReader getReader();

  /**
   * Get the metadata for this field. Used in serialization
   *
   * @return FieldMetadata for this field.
   */
  SerializedField getMetadata();

  /**
   * Returns the number of bytes that is used by this vector instance.
   */
  int getBufferSize();

  /**
   * Return the underlying buffers associated with this vector. Note that this doesn't impact the reference counts for
   * this buffer so it only should be used for in-context access. Also note that this buffer changes regularly thus
   * external classes shouldn't hold a reference to it (unless they change it).
   * @param clear Whether to clear vector before returning; the buffers will still be refcounted;
   *   but the returned array will be the only reference to them
   *
   * @return The underlying {@link io.netty.buffer.DrillBuf buffers} that is used by this vector instance.
   */
  DrillBuf[] getBuffers(boolean clear);

  /**
   * Load the data provided in the buffer. Typically used when deserializing from the wire.
   *
   * @param metadata
   *          Metadata used to decode the incoming buffer.
   * @param buffer
   *          The buffer that contains the ValueVector.
   */
  void load(SerializedField metadata, DrillBuf buffer);

  /**
   * An abstraction that is used to read from this vector instance.
   */
  interface Accessor {
    /**
     * Get the Java Object representation of the element at the specified position. Useful for testing.
     *
     * @param index
     *          Index of the value to get
     */
    Object getObject(int index);

    /**
     * Returns the number of values that is stored in this vector.
     */
    int getValueCount();

    /**
     * Returns true if the value at the given index is null, false otherwise.
     */
    boolean isNull(int index);
  }

  /**
   * An abstractiong that is used to write into this vector instance.
   */
  interface Mutator {
    /**
     * Sets the number of values that is stored in this vector to the given value count.
     *
     * @param valueCount  value count to set.
     */
    void setValueCount(int valueCount);

    /**
     * Resets the mutator to pristine state.
     */
    void reset();

    /**
     * @deprecated  this has nothing to do with value vector abstraction and should be removed.
     */
    @Deprecated
    void generateTestData(int values);
  }
}
