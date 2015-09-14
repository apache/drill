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

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.OperatorContext;

import org.apache.parquet.bytes.ByteBufferAllocator;

/**
 * {@link ByteBufferAllocator} implementation that uses Drill's {@link BufferAllocator} to allocate and release
 * {@link ByteBuffer} objects.<br>
 * To properly release an allocated {@link ByteBuf}, this class keeps track of it's corresponding {@link ByteBuffer}
 * that was passed to the Parquet library.
 */
public class ParquetDirectByteBufferAllocator implements ByteBufferAllocator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetDirectByteBufferAllocator.class);

  private final BufferAllocator allocator;
  private final HashMap<Key, ByteBuf> allocatedBuffers = new HashMap<>();

  public ParquetDirectByteBufferAllocator(OperatorContext o){
    allocator = o.getAllocator();
  }

  public ParquetDirectByteBufferAllocator(BufferAllocator allocator) {
    this.allocator = allocator;
  }


  @Override
  public ByteBuffer allocate(int sz) {
    ByteBuf bb = allocator.buffer(sz);
    ByteBuffer b = bb.nioBuffer(0, sz);
    final Key key = new Key(b);
    allocatedBuffers.put(key, bb);
    logger.debug("ParquetDirectByteBufferAllocator: Allocated {} bytes. Allocated ByteBuffer id: {}", sz, key.hash);
    return b;
  }

  @Override
  public void release(ByteBuffer b) {
    final Key key = new Key(b);
    final ByteBuf bb = allocatedBuffers.get(key);
    // The ByteBuffer passed in may already have been freed or not allocated by this allocator.
    // If it is not found in the allocated buffers, do nothing
    if(bb != null) {
      logger.debug("ParquetDirectByteBufferAllocator: Freed byte buffer. Allocated ByteBuffer id: {}", key.hash);
      bb.release();
      allocatedBuffers.remove(key);
    }
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  /**
   * ByteBuffer wrapper that computes a fixed hashcode.
   * <br><br>
   * Parquet only handles {@link ByteBuffer} objects, so we need to use them as keys to keep track of their corresponding
   * {@link ByteBuf}, but {@link ByteBuffer} is mutable and it can't be used as a {@link HashMap} key as it is.<br>
   * This class solves this by providing a fixed hashcode for {@link ByteBuffer} and uses reference equality in case
   * of collisions (we don't need to compare the content of {@link ByteBuffer} because the object passed to
   * {@link #release(ByteBuffer)} will be the same object returned from a previous {@link #allocate(int)}.
   */
  private class Key {
    final int hash;
    final ByteBuffer buffer;

    Key(final ByteBuffer buffer) {
      this.buffer = buffer;
      // remember, we can't use buffer.hashCode()
      this.hash = System.identityHashCode(buffer);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof Key)) {
        return false;
      }
      final Key key = (Key) obj;
      return hash == key.hash && buffer == key.buffer;
    }
  }
}
