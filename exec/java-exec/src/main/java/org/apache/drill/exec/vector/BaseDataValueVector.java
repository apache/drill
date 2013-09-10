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

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.DeadBuf;
import org.apache.drill.exec.record.MaterializedField;

public abstract class BaseDataValueVector extends BaseValueVector{

  protected ByteBuf data = DeadBuf.DEAD_BUFFER;
  protected int valueCount;
  
  public BaseDataValueVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
    
  }

  /**
   * Release the underlying ByteBuf and reset the ValueVector
   */
  @Override
  public void clear() {
    if (data != DeadBuf.DEAD_BUFFER) {
      data.release();
      data = DeadBuf.DEAD_BUFFER;
      valueCount = 0;
    }
  }

  
  @Override
  public ByteBuf[] getBuffers(){
    ByteBuf[] out;
    if(valueCount == 0){
      out = new ByteBuf[0];
    }else{
      out = new ByteBuf[]{data};
      data.readerIndex(0);
      data.retain();
    }
    clear();
    return out;
  }
  
  public int getBufferSize() {
    if(valueCount == 0) return 0;
    return data.writerIndex();
  }

  @Override
  public abstract FieldMetadata getMetadata();

  public ByteBuf getData(){
    return data;
  }
  
  
}
