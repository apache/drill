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

import org.apache.drill.exec.memory.BufferAllocator;

/**
 * Abstract class that fixed value vectors are derived from.
 */
abstract class AbstractFixedValueVector<T extends AbstractFixedValueVector<T>> extends BaseValueVector<T> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractFixedValueVector.class);

  private final int widthInBits;

  protected int longWords = 0;

  public AbstractFixedValueVector(int fieldId, BufferAllocator allocator, int widthInBits) {
    super(fieldId, allocator);
    this.widthInBits = widthInBits;
  }
  
  @Override
  protected int getAllocationSize(int valueCount) {
    return (int) Math.ceil(valueCount*widthInBits*1.0/8);
  }
  
  @Override
  protected void childResetAllocation(int valueCount, ByteBuf buf) {
    this.longWords = valueCount/8;
  }

  @Override
  protected void childCloneMetadata(T other) {
    other.longWords = this.longWords;
  }

  @Override
  protected void childClear() {
    longWords = 0;
  }

}

