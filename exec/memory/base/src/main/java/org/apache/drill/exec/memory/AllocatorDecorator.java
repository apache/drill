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
package org.apache.drill.exec.memory;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.ops.BufferManager;

/**
 * Decorator class that allows someone to wrap an Allocator implementation to enhance capabilities.
 */
public abstract class AllocatorDecorator implements BufferAllocator {

  private final BufferAllocator allocator;

  /**
   * Constructor to create a new Decorator.
   *
   * @param allocator
   *          The allocator that all calls will be passed to when using the default implementation.
   */
  public AllocatorDecorator(final BufferAllocator allocator) {
    super();
    this.allocator = allocator;
  }

  @Override
  public DrillBuf buffer(int size) {
    return allocator.buffer(size);
  }

  @Override
  public DrillBuf buffer(int size, BufferManager manager) {
    return allocator.buffer(size, manager);
  }

  @Override
  public ByteBufAllocator getAsByteBufAllocator() {
    return allocator.getAsByteBufAllocator();
  }

  @Override
  public BufferAllocator newChildAllocator(String name, long initReservation, long maxAllocation) {
    return allocator.newChildAllocator(name, initReservation, maxAllocation);
  }

  @Override
  public void close() {
    allocator.close();
  }

  @Override
  public long getAllocatedMemory() {
    return allocator.getAllocatedMemory();
  }

  @Override
  public void setLimit(long newLimit) {
    allocator.setLimit(newLimit);
  }

  @Override
  public long getLimit() {
    return allocator.getLimit();
  }

  @Override
  public long getPeakMemoryAllocation() {
    return allocator.getPeakMemoryAllocation();
  }

  @Override
  public AllocationReservation newReservation() {
    return allocator.newReservation();
  }

  @Override
  public DrillBuf getEmpty() {
    return allocator.getEmpty();
  }

  @Override
  public String getName() {
    return allocator.getName();
  }

  @Override
  public boolean isOverLimit() {
    return allocator.isOverLimit();
  }

  @Override
  public String toVerboseString() {
    return allocator.toVerboseString();
  }

  @Override
  public boolean isClosed() {
    return allocator.isClosed();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> c) {
    if (c == AllocatorDecorator.class) {
      return (T) this;
    } else {
      return allocator.unwrap(c);
    }
  }

}
