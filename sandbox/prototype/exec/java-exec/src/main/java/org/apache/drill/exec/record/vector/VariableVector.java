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
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/** 
 * A vector of variable length bytes.  Constructed as a vector of lengths or positions and a vector of values.  Random access is only possible if the variable vector stores positions as opposed to lengths.
 */
public abstract class VariableVector<T extends VariableVector<T, E>, E extends BaseValueVector<E>> extends BaseValueVector<T>{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VariableVector.class);
  
  protected final E lengthVector;
  protected int expectedValueLength;
  
  public VariableVector(MaterializedField field, BufferAllocator allocator, int expectedValueLength) {
    super(field, allocator);
    this.lengthVector = getNewLengthVector(allocator);
    this.expectedValueLength = expectedValueLength;
  }
  
  protected abstract E getNewLengthVector(BufferAllocator allocator);
  
  @Override
  protected int getAllocationSize(int valueCount) {
    return lengthVector.getAllocationSize(valueCount) + (expectedValueLength * valueCount);
  }
  
  @Override
  protected void childResetAllocation(int valueCount, ByteBuf buf) {
    int firstSize = lengthVector.getAllocationSize(valueCount);
    lengthVector.resetAllocation(valueCount, buf.slice(0, firstSize));
    data = buf.slice(firstSize, expectedValueLength * valueCount);
  }

  @Override
  protected void childCloneMetadata(T other) {
    lengthVector.cloneMetadata(other.lengthVector);
    other.expectedValueLength = expectedValueLength;
  }

  @Override
  protected void childClear() {
    lengthVector.clear();
    if(data != DeadBuf.DEAD_BUFFER){
      data.release();
      data = DeadBuf.DEAD_BUFFER;
    }
  }

  @Override
  public ByteBuf[] getBuffers() {
    return new ByteBuf[]{lengthVector.data, data};
  }

  @Override
  public void setRecordCount(int recordCount) {
    super.setRecordCount(recordCount);
    lengthVector.setRecordCount(recordCount);
  }  
  
  public void setTotalBytes(int totalBytes){
    data.writerIndex(totalBytes);
  }

  @Override
  public Object getObject(int index) {
      checkArgument(index >= 0);
      int startIdx = 0;
      if(index > 0) {
          startIdx = (int) lengthVector.getObject(index - 1);
      }
      int size = (int) lengthVector.getObject(index) - startIdx;
      checkState(size >= 0);
      byte[] dst = new byte[size];
      data.getBytes(startIdx, dst, 0, size);
      return dst;
  }
}
