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

import org.apache.drill.exec.BufferAllocator;

/**
 * Abstract class supports null versions.
 */
abstract class NullableValueVector<T extends NullableValueVector<T, E>, E extends BaseValueVector<E>> extends BaseValueVector<T> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NullableValueVector.class);

  protected BitVector bits;
  protected E value;

  public NullableValueVector(int fieldId, BufferAllocator allocator) {
    super(fieldId, allocator);
    bits = new BitVector(fieldId, allocator);
    value = getNewValueVector(fieldId, allocator);
  }
  
  protected abstract E getNewValueVector(int fieldId, BufferAllocator allocator);

  @Override
  protected int getAllocationSize(int valueCount) {
    return bits.getAllocationSize(valueCount) + value.getAllocationSize(valueCount);
  }
  
  
  @Override
  protected void childResetAllocation(int valueCount, ByteBuf buf) {
    super.resetAllocation(valueCount, buf);
    int firstSize = bits.getAllocationSize(valueCount);
    value.resetAllocation(valueCount, buf.slice(firstSize, value.getAllocationSize(valueCount)));
    bits.resetAllocation(valueCount, buf.slice(0, firstSize));
    bits.setAllFalse();
  }

  @Override
  protected void childCloneMetadata(T other) {
    bits.cloneMetadata(other.bits);
    value.cloneInto(value);
  }

  @Override
  protected void childClear() {
    bits.clear();
    value.clear();
  }

  
}

