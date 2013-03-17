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
package org.apache.drill.exec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.Closeable;

import org.apache.drill.exec.server.DrillbitContext;

/**
 * Wrapper class to deal with byte buffer allocation. Ensures users only use designated methods.  Also allows inser 
 */
public abstract class BufferAllocator implements Closeable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BufferAllocator.class);
  
  /**
   * Allocate a new or reused buffer of the provided size.  Note that the buffer may technically be larger than the requested size for rounding purposes.  However, the buffers capacity will be set to the configured size.
   * @param size The size in bytes.
   * @return A new ByteBuf.
   */
  public abstract ByteBuf buffer(int size);
  
  public abstract ByteBufAllocator getUnderlyingAllocator();
  
  /**
   * Close and release all buffers generated from this buffer pool.
   */
  @Override
  public abstract void close(); 
  
  public static BufferAllocator getAllocator(DrillbitContext context){
    // TODO: support alternative allocators (including a debugging allocator that records all allocation locations for each buffer).
    return new DirectBufferAllocator();
  }
}
