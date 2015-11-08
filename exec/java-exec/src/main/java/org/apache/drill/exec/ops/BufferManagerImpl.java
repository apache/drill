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
    DrillBuf newBuf = allocator.buffer(size);
    managedBuffers.put(newBuf.memoryAddress(), newBuf);
    newBuf.setBufferManager(this);
    return newBuf;
  }
}
