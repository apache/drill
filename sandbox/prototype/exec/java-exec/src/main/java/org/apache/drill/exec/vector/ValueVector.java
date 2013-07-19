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

import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;

/**
 * ValueVectorTypes defines a set of template-generated classes which implement type-specific
 * value vectors.  The template approach was chosen due to the lack of multiple inheritence.  It
 * is also important that all related logic be as efficient as possible.
 */
public interface ValueVector extends Closeable {

//  /**
//   * Get the explicitly specified size of the allocated buffer, if available.  Otherwise
//   * calculate the size based on width and record count.
//   */
//  public abstract int getAllocatedSize();


  /**
   * Alternative to clear().  Allows use as closeable in try-with-resources.
   */
  public void close();
  
  /**
   * Release the underlying ByteBuf and reset the ValueVector to empty.
   */
  public void clear();
  
  
  public TransferPair getTransferPair();

  
  /**
   * Return the underlying buffers associated with this vector. Note that this doesn't impact the
   * reference counts for this buffer so it only should be used for in-context access. Also note
   * that this buffer changes regularly thus external classes shouldn't hold a reference to
   * it (unless they change it).
   *
   * @return The underlying ByteBuf.
   */
  public abstract ByteBuf[] getBuffers();
  
  /**
   * Load the data provided in the buffer.  Typically used when deserializing from the wire.
   * @param metadata Metadata used to decode the incoming buffer.
   * @param buffer The buffer that contains the ValueVector.
   */
  public void load(FieldMetadata metadata, ByteBuf buffer);


  /**
   * Given the current buffer allocation, return the maximum number of values that this buffer can contain.
   * @return Maximum values buffer can contain.  In the case of a Repeated field, this is the number of atoms, not repeated groups.
   */
  public int getValueCapacity();
  
  /**
   * Get information about how this field is materialized.
   * @return
   */
  public MaterializedField getField();
  
  /**
   * Get the metadata for this field.  Used in serialization
   * @return FieldMetadata for this field.
   */
  public FieldMetadata getMetadata();
  
  /**
   * Get Accessor to read value vector data.
   * @return 
   */
  public abstract Accessor getAccessor();
  
  /**
   * Get a Mutator to update this vectors data.
   * @return
   */
  public abstract Mutator getMutator();

  
  public interface Accessor{

//    /**
//     * Get the number of records allocated for this value vector.
//     * @return number of allocated records
//     */
//    public int getRecordCount();

    /**
     * Get the Java Object representation of the element at the specified position.  Useful for testing.
     *
     * @param index   Index of the value to get
     */
    public abstract Object getObject(int index);
    
    public int getValueCount();
    public void reset();
  }
  
  
    
  
  
  public interface Mutator{
    public void reset();
    public void randomizeData();
  }
}

