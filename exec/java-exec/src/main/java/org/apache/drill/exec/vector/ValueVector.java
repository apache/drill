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

import io.netty.buffer.DrillBuf;

import java.io.Closeable;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

/**
 * ValueVectorTypes defines a set of template-generated classes which implement type-specific value vectors. The
 * template approach was chosen due to the lack of multiple inheritence. It is also important that all related logic be
 * as efficient as possible.
 */
public interface ValueVector extends Closeable, Iterable<ValueVector> {


  /**
   * Allocate new buffers. ValueVector implements logic to determine how much to allocate.
   * @throws OutOfMemoryRuntimeException Thrown if no memory can be allocated.
   */
  public void allocateNew() throws OutOfMemoryRuntimeException;

  /**
   * Allocates new buffers. ValueVector implements logic to determine how much to allocate.
   * @return Returns true if allocation was succesful.
   */
  public boolean allocateNewSafe();

  /**
   * Set the initial record capacity
   * @param numRecords
   */
  public void setInitialCapacity(int numRecords);

  public int getBufferSize();

  /**
   * Alternative to clear(). Allows use as closeable in try-with-resources.
   */
  public void close();

  /**
   * Release the underlying DrillBuf and reset the ValueVector to empty.
   */
  public void clear();

  /**
   * Get information about how this field is materialized.
   *
   * @return
   */
  public MaterializedField getField();

  /**
   * Get a transfer pair to allow transferring this vectors data between this vector and a destination vector of the
   * same type. Will also generate a second instance of this vector class that is connected through the TransferPair.
   *
   * @return
   */
  public TransferPair getTransferPair();

  public TransferPair makeTransferPair(ValueVector to);

  public TransferPair getTransferPair(FieldReference ref);

  /**
   * Given the current buffer allocation, return the maximum number of values that this buffer can contain.
   *
   * @return Maximum values buffer can contain. In the case of a Repeated field, this is the number of atoms, not
   *         repeated groups.
   */
  public int getValueCapacity();

  /**
   * Get Accessor to read value vector data.
   *
   * @return
   */
  public abstract Accessor getAccessor();

  /**
   * Return the underlying buffers associated with this vector. Note that this doesn't impact the reference counts for
   * this buffer so it only should be used for in-context access. Also note that this buffer changes regularly thus
   * external classes shouldn't hold a reference to it (unless they change it).
   *
   * @param clear
   *          Whether to clear vector
   *
   * @return The underlying ByteBuf.
   */
  public abstract DrillBuf[] getBuffers(boolean clear);

  /**
   * Load the data provided in the buffer. Typically used when deserializing from the wire.
   *
   * @param metadata
   *          Metadata used to decode the incoming buffer.
   * @param buffer
   *          The buffer that contains the ValueVector.
   */
  public void load(SerializedField metadata, DrillBuf buffer);

  /**
   * Get the metadata for this field. Used in serialization
   *
   * @return FieldMetadata for this field.
   */
  public SerializedField getMetadata();

  /**
   * Get a Mutator to update this vectors data.
   *
   * @return
   */
  public abstract Mutator getMutator();

  public interface Accessor {

    // /**
    // * Get the number of records allocated for this value vector.
    // * @return number of allocated records
    // */
    // public int getRecordCount();

    /**
     * Get the Java Object representation of the element at the specified position. Useful for testing.
     *
     * @param index
     *          Index of the value to get
     */
    public abstract Object getObject(int index);

    public int getValueCount();

    public boolean isNull(int index);

    public void reset();

    public FieldReader getReader();
  }

  public interface Mutator {
    /**
     * Set the top number values (optional/required) or number of value groupings (repeated) in this vector.
     *
     * @param valueCount
     */
    public void setValueCount(int valueCount);

    public void reset();

    public void generateTestData(int values);
  }

}
