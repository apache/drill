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

import io.netty.buffer.ByteBuf;

import java.io.Closeable;

import org.apache.drill.exec.record.MaterializedField;

/**
 * A vector of values.  Acts a containing instance that may rotate its internal buffer depending on what it needs to hold.  Should be language agnostic so that it can be passed between Java and JNI without modification.
 */
public interface ValueVector<T extends ValueVector<T>> extends Closeable {

  /**
   * Copies the data from this vector into its pair.
   * 
   * @param vector
   */
  public abstract void cloneInto(T vector);

  /**
   * Allocate a new memory space for this vector.
   * 
   * @param valueCount
   *          The number of possible values which should be contained in this vector.
   */
  public abstract void allocateNew(int valueCount);

  /**
   * Zero copy move of data from this vector to the target vector. Any future access to this vector without being
   * populated by a new vector will cause problems.
   * 
   * @param vector
   */
  public abstract void transferTo(T vector);

  /**
   * Return the underlying buffer. Note that this doesn't impact the reference counts for this buffer so it only should be
   * used for in context access. Also note that this buffer changes regularly thus external classes shouldn't hold a
   * reference to it.
   * 
   * @return The underlying ByteBuf.
   */
  public abstract ByteBuf getBuffer();

  /**
   * Returns the number of value contained within this vector.
   * @return Vector size
   */
  public abstract int size();


  /**
   * Release supporting resources.
   */
  public abstract void close();

  /**
   * Get information about how this field is materialized.
   * 
   * @return
   */
  public abstract MaterializedField getField();

}
