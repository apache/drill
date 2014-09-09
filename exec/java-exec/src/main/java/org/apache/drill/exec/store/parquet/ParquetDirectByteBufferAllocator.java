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

package org.apache.drill.exec.store.parquet;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.drill.exec.ops.OperatorContext;

import parquet.bytes.ByteBufferAllocator;

public class ParquetDirectByteBufferAllocator implements ByteBufferAllocator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetDirectByteBufferAllocator.class);

  private OperatorContext oContext;
  private HashMap<Integer, ByteBuf> allocatedBuffers = new HashMap<Integer, ByteBuf>();

  public ParquetDirectByteBufferAllocator(OperatorContext o){
    oContext=o;
  }


  @Override
  public ByteBuffer allocate(int sz) {
    ByteBuf bb = oContext.getAllocator().buffer(sz);
    ByteBuffer b = bb.nioBuffer(0, sz);
    allocatedBuffers.put(System.identityHashCode(b), bb);
    logger.debug("ParquetDirectByteBufferAllocator: Allocated "+sz+" bytes. Allocated ByteBuffer id: "+System.identityHashCode(b));
    return b;
  }

  @Override
  public void release(ByteBuffer b) {
    Integer id = System.identityHashCode(b);
    ByteBuf bb = allocatedBuffers.get(id);
    // The ByteBuffer passed in may already have been freed or not allocated by this allocator.
    // If it is not found in the allocated buffers, do nothing
    if(bb!=null) {
      logger.debug("ParquetDirectByteBufferAllocator: Freed byte buffer. Allocated ByteBuffer id: "+System.identityHashCode(b));
      bb.release();
      allocatedBuffers.remove(id);
    }
  }

  public void clear(){
    Iterator it = allocatedBuffers.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry)it.next();
      Integer id = (Integer)pair.getKey();
      ByteBuf bb = allocatedBuffers.get(id);
      bb.release();
      it.remove();
    }
  }
}
