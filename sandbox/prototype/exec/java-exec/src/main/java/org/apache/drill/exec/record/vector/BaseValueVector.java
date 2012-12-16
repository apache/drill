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

import java.util.Random;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;

public abstract class BaseValueVector<T extends BaseValueVector<T>> implements ValueVector<T>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseValueVector.class);
  
  protected final BufferAllocator allocator;
  protected ByteBuf data = DeadBuf.DEAD_BUFFER;
  protected int maxValueCount = 0;
  protected final MaterializedField field;
  private int recordCount;
  
  public BaseValueVector(MaterializedField field, BufferAllocator allocator) {
    this.allocator = allocator;
    this.field = field;
  }

  public final void allocateNew(int valueCount){
    int allocationSize = getAllocationSize(valueCount);
    ByteBuf newBuf = allocator.buffer(allocationSize);
    resetAllocation(valueCount, newBuf);
  }

  protected abstract int getAllocationSize(int maxValueCount);
  protected abstract void childResetAllocation(int valueCount, ByteBuf buf);
  protected abstract void childCloneMetadata(T other);
  protected abstract void childClear();
  
  /**
   * Update the current buffer allocation utilize the provided allocation.
   * @param maxValueCount
   * @param buf
   */
  protected final void resetAllocation(int maxValueCount, ByteBuf buf){
    clear();
    if(buf != DeadBuf.DEAD_BUFFER) {
       buf.retain();
    }
    this.maxValueCount = maxValueCount;
    this.data = buf;
    childResetAllocation(maxValueCount, buf);
  }
  
  public final void cloneMetadata(T other){
    other.maxValueCount = this.maxValueCount;
  }
  
  
  @Override
  public final void cloneInto(T vector) {
    vector.allocateNew(maxValueCount);
    data.writeBytes(vector.data);
    cloneMetadata(vector);
    childResetAllocation(maxValueCount, vector.data);
  }
  
  @Override
  public final void transferTo(T vector) {
    vector.data = this.data;
    cloneMetadata(vector);
    childResetAllocation(maxValueCount, data);
    clear();
  }

  protected final void clear(){
    if(this.data != DeadBuf.DEAD_BUFFER){
      this.data.release();
      this.data = DeadBuf.DEAD_BUFFER;
      this.maxValueCount = 0;
    }
    childClear();
  }
  
  /**
   * Give the length of the value vector in bytes.
   * 
   * @return
   */
  public int capacity() {
    return maxValueCount;
  }
  
  @Override
  public void close() {
    clear();
  }

  @Override
  public ByteBuf[] getBuffers() {
    return new ByteBuf[]{data};
  }
  
  public MaterializedField getField(){
    return field;
  }
  
  
  public int getRecordCount() {
    return recordCount;
  }

  public void setRecordCount(int recordCount) {
    this.recordCount = recordCount;
  }

  @Override
  public FieldMetadata getMetadata() {
    int len = 0;
    for(ByteBuf b : getBuffers()){
      len += b.writerIndex();
    }
    return FieldMetadata.newBuilder().setDef(getField().getDef()).setValueCount(getRecordCount()).setBufferLength(len).build();
  }
  
  @Override
  public void setTo(FieldMetadata metadata, ByteBuf data) {
//    logger.debug("Updating value vector to {}, {}", metadata, data);
    clear();
    resetAllocation(metadata.getValueCount(), data);
  }

  @Override
  public void randomizeData() {
    if(this.data != DeadBuf.DEAD_BUFFER){
      Random r = new Random();
      for(int i =0; i < data.capacity()-8; i+=8){
        data.setLong(i, r.nextLong());
      }
    }
    
  }
  
  
}
