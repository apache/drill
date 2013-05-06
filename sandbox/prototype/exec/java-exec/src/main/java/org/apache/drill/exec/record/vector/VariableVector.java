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

/** 
 * A vector of variable length bytes.  Constructed as a vector of lengths or positions and a vector of values.  Random access is only possible if the variable vector stores positions as opposed to lengths.
 */
public abstract class VariableVector<T extends VariableVector<T, E>, E extends BaseValueVector<E>> extends BaseValueVector<T>{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VariableVector.class);
  
  protected final E lengthVector;
  private ByteBuf values = DeadBuf.DEAD_BUFFER;
  protected int expectedValueLength;
  
  public VariableVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    this.lengthVector = getNewLengthVector(allocator);
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
    values = buf.slice(firstSize, expectedValueLength * valueCount);
  }

  @Override
  protected void childCloneMetadata(T other) {
    lengthVector.cloneMetadata(other.lengthVector);
    other.expectedValueLength = expectedValueLength;
  }

  @Override
  protected void childClear() {
    lengthVector.clear();
    if(values != DeadBuf.DEAD_BUFFER){
      values.release();
      values = DeadBuf.DEAD_BUFFER;
    }
  }

  
  @Override
  public ByteBuf[] getBuffers() {
    return new ByteBuf[]{lengthVector.data, values};
  }

  @Override
  public void setRecordCount(int recordCount) {
    super.setRecordCount(recordCount);
    lengthVector.setRecordCount(recordCount);
  }  
  
  public void setTotalBytes(int totalBytes){
    values.writerIndex(totalBytes);
  }

  @Override
  public Object getObject(int index) {
    return null;
  }
  
  
}
