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
package org.apache.drill.exec.ops;

import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.memory.BufferAllocator;

import com.carrotsearch.hppc.LongObjectOpenHashMap;

public class BufferManagerImpl implements BufferManager {
  private LongObjectOpenHashMap<DrillBuf> managedBuffers = new LongObjectOpenHashMap<>();
  private final BufferAllocator allocator;

  public BufferManagerImpl(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public void close() {
    final Object[] mbuffers = ((LongObjectOpenHashMap<Object>) (Object) managedBuffers).values;
    for (int i = 0; i < mbuffers.length; i++) {
      if (managedBuffers.allocated[i]) {
        ((DrillBuf) mbuffers[i]).release(1);
      }
    }
    managedBuffers.clear();
  }

  public DrillBuf replace(DrillBuf old, int newSize) {
    if (managedBuffers.remove(old.memoryAddress()) == null) {
      throw new IllegalStateException("Tried to remove unmanaged buffer.");
    }
    old.release(1);
    return getManagedBuffer(newSize);
  }

  public DrillBuf getManagedBuffer() {
    return getManagedBuffer(256);
  }

  public DrillBuf getManagedBuffer(int size) {
    DrillBuf newBuf = allocator.buffer(size, this);
    managedBuffers.put(newBuf.memoryAddress(), newBuf);
    return newBuf;
  }
}
