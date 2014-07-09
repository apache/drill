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
package io.netty.buffer;

import java.nio.ByteBuffer;

public class PooledByteBufAllocatorL extends PooledByteBufAllocator{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PooledByteBufAllocatorL.class);

  public static final PooledByteBufAllocatorL DEFAULT = new PooledByteBufAllocatorL();

//  public final UnsafeDirectLittleEndian emptyBuf;

  public PooledByteBufAllocatorL() {
    super(true);
//    emptyBuf = newDirectBuffer(0,0);
  }

  @Override
  protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
    throw new UnsupportedOperationException("Drill doesn't support using heap buffers.");
  }

  @Override
  protected UnsafeDirectLittleEndian newDirectBuffer(int initialCapacity, int maxCapacity) {
    PoolThreadCache cache = threadCache.get();
    PoolArena<ByteBuffer> directArena = cache.directArena;

    ByteBuf buf;
    if (directArena != null) {
        buf = directArena.allocate(cache, initialCapacity, maxCapacity);
    } else {
      throw new UnsupportedOperationException("Drill requries that the allocator operates in DirectBuffer mode.");
    }

    if(buf instanceof PooledUnsafeDirectByteBuf){
      return new UnsafeDirectLittleEndian( (PooledUnsafeDirectByteBuf) buf);
    }else{
      throw new UnsupportedOperationException("Drill requries that the JVM used supports access sun.misc.Unsafe.  This platform didn't provide that functionality.");
    }

  }


  @Override
  public UnsafeDirectLittleEndian directBuffer(int initialCapacity, int maxCapacity) {
      if (initialCapacity == 0 && maxCapacity == 0) {
          newDirectBuffer(initialCapacity, maxCapacity);
      }
      validate(initialCapacity, maxCapacity);
      return newDirectBuffer(initialCapacity, maxCapacity);
  }

  @Override
  public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
    throw new UnsupportedOperationException("Drill doesn't support using heap buffers.");
  }


  private static void validate(int initialCapacity, int maxCapacity) {
    if (initialCapacity < 0) {
        throw new IllegalArgumentException("initialCapacity: " + initialCapacity + " (expectd: 0+)");
    }
    if (initialCapacity > maxCapacity) {
        throw new IllegalArgumentException(String.format(
                "initialCapacity: %d (expected: not greater than maxCapacity(%d)",
                initialCapacity, maxCapacity));
    }
}
}
