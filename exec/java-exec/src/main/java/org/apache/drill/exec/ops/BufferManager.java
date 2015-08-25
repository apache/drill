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
package org.apache.drill.exec.ops;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.memory.BufferAllocator;

/**
 * Manages a list of {@link DrillBuf}s that can be reallocated as needed. Upon
 * re-allocation the old buffer will be freed. Managing a list of these buffers
 * prevents some parts of the system from needing to define a correct location
 * to place the final call to free them.
 *
 * The current uses of these types of buffers are within the pluggable components of Drill.
 * In UDFs, memory management should not be a concern. We provide access to re-allocatable
 * DrillBufs to give UDF writers general purpose buffers we can account for. To prevent the need
 * for UDFs to contain boilerplate to close all of the buffers they request, this list
 * is tracked at a higher level and all of the buffers are freed once we are sure that
 * the code depending on them is done executing (currently {@link FragmentContext}
 * and {@link QueryContext}.
 */
public class BufferManager implements AutoCloseable {
  private LongObjectOpenHashMap<DrillBuf> managedBuffers = new LongObjectOpenHashMap<>();
  private final BufferAllocator allocator;

  // fragment context associated with this buffer manager, if the buffer
  // manager is owned by another type, this can be null
  private final FragmentContext fragmentContext;

  public BufferManager(BufferAllocator allocator, FragmentContext fragmentContext) {
    this.allocator = allocator;
    this.fragmentContext = fragmentContext;
  }

  @Override
  public void close() throws Exception {
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
    DrillBuf newBuf = allocator.buffer(size);
    managedBuffers.put(newBuf.memoryAddress(), newBuf);
    newBuf.setFragmentContext(fragmentContext);
    newBuf.setBufferManager(this);
    return newBuf;
  }
}
