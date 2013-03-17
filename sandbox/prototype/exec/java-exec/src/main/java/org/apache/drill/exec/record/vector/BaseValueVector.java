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
import org.apache.drill.exec.record.DeadBuf;

public abstract class BaseValueVector<T extends BaseValueVector<T>> implements ValueVector<T>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseValueVector.class);
  
  protected final BufferAllocator allocator;
  protected ByteBuf data = DeadBuf.DEAD_BUFFER;
  protected int valueCount = 0;
  protected final int fieldId;
  
  public BaseValueVector(int fieldId, BufferAllocator allocator) {
    this.allocator = allocator;
    this.fieldId = fieldId;
  }

  public final void allocateNew(int valueCount){
    int allocationSize = getAllocationSize(valueCount);
    resetAllocation(valueCount, allocator.buffer(allocationSize));
  }

  protected abstract int getAllocationSize(int valueCount);
  protected abstract void childResetAllocation(int valueCount, ByteBuf buf);
  protected abstract void childCloneMetadata(T other);
  protected abstract void childClear();
  
  protected final void resetAllocation(int valueCount, ByteBuf buf){
    clear();
    this.valueCount = valueCount;
    this.data = buf;
    childResetAllocation(valueCount, buf);
  }
  
  public final void cloneMetadata(T other){
    other.valueCount = this.valueCount;
  }
  
  @Override
  public final void cloneInto(T vector) {
    vector.allocateNew(valueCount);
    data.writeBytes(vector.data);
    cloneMetadata(vector);
    childResetAllocation(valueCount, vector.data);
  }
  
  @Override
  public final void transferTo(T vector) {
    vector.data = this.data;
    cloneMetadata(vector);
    childResetAllocation(valueCount, data);
    clear();
  }

  protected final void clear(){
    if(this.data != DeadBuf.DEAD_BUFFER){
      this.data.release();
      this.data = DeadBuf.DEAD_BUFFER;
      this.valueCount = 0;
    }
    childClear();
  }
  
  /**
   * Give the length of the value vector in bytes.
   * 
   * @return
   */
  public int size() {
    return valueCount;
  }
  
  @Override
  public void close() {
    clear();
  }

  @Override
  public ByteBuf getBuffer() {
    return data;
  }
  
  
}
