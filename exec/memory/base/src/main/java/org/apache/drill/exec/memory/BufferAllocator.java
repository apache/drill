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

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.util.Pointer;

/**
 * Wrapper class to deal with byte buffer allocation. Ensures users only use designated methods.
 */
public interface BufferAllocator extends AutoCloseable {
  /**
   * Allocate a new or reused buffer of the provided size. Note that the buffer may technically be larger than the
   * requested size for rounding purposes. However, the buffer's capacity will be set to the configured size.
   *
   * @param size The size in bytes.
   * @return a new DrillBuf, or null if the request can't be satisfied
   * @throws OutOfMemoryRuntimeException if buffer cannot be allocated
   */
  public DrillBuf buffer(int size);

  /**
   * Allocate a new or reused buffer within provided range. Note that the buffer may technically be larger than the
   * requested size for rounding purposes. However, the buffer's capacity will be set to the configured size.
   *
   * @param minSize The minimum size in bytes.
   * @param maxSize The maximum size in bytes.
   * @return a new DrillBuf, or null if the request can't be satisfied
   * @throws OutOfMemoryRuntimeException if buffer cannot be allocated
   */
  public DrillBuf buffer(int minSize, int maxSize);

  /**
   * Returns the allocator this allocator falls back to when it needs more memory.
   *
   * @return the underlying allocator used by this allocator
   */
  public ByteBufAllocator getUnderlyingAllocator();

  /**
   * Create a child allocator nested below this one.
   *
   * @param context - the owner or this allocator
   * @param initialReservation - specified in bytes
   * @param maximumReservation - specified in bytes
   * @param applyFragmentLimit - flag to conditionally enable fragment memory limits
   * @return - a new buffer allocator owned by the parent it was spawned from
   */
  @Deprecated
  public BufferAllocator getChildAllocator(FragmentContext context, long initialReservation,
      long maximumReservation, boolean applyFragmentLimit);

  /**
   * Flag: this allocator is a limiting sub-tree root, meaning that the maxAllocation for
   * it applies to all its descendant child allocators. In low memory situations, the limits
   * for sub-tree roots may be adjusted down so that they evenly share the total amount of
   * direct memory across all the sub-tree roots.
   */
  public final static int F_LIMITING_ROOT = 0x0001;

  /**
   * Create a new child allocator.
   *
   * @param allocatorOwner the allocator owner
   * @param initReservation the initial space reservation (obtained from this allocator)
   * @param maxAllocation maximum amount of space the new allocator can allocate
   * @param flags one or more of BufferAllocator.F_* flags
   * @return the new allocator, or null if it can't be created
   */
  public BufferAllocator newChildAllocator(AllocatorOwner allocatorOwner,
      long initReservation, long maxAllocation, int flags);

  /**
   * Take over ownership of the given buffer, adjusting accounting accordingly.
   * This allocator always takes over ownership.
   *
   * @param buf the buffer to take over
   * @return false if over allocation
   */
  public boolean takeOwnership(DrillBuf buf);

  /**
   * Share ownership of a buffer between allocators.
   *
   * @param buf the buffer
   * @param bufOut a new DrillBuf owned by this allocator, but sharing the same underlying buffer
   * @return false if over allocation.
   */
  public boolean shareOwnership(DrillBuf buf, Pointer<DrillBuf> bufOut);

  /**
   * Not thread safe.
   *
   * WARNING: unclaimed pre-allocations leak memory. If you call preAllocate(), you must
   * make sure to ultimately try to get the buffer and release it.
   *
   * For Child allocators to set their Fragment limits.
   *
   * @param fragmentLimit the new fragment limit
   */
  @Deprecated // happens automatically, and via allocation policies
  public void setFragmentLimit(long fragmentLimit);

  /**
   * Returns the current fragment limit.
   *
   * @return the current fragment limit
   */
  /*
   * TODO should be replaced with something more general because of
   * the availability of multiple allocation policies
   *
   * TODO We should also have a getRemainingMemory() so operators
   * can query how much more is left to allocate. That could be
   * tricky.
   */
  @Deprecated
  public long getFragmentLimit();

  /**
   * Return a unique Id for an allocator. Id's may be recycled after
   * a long period of time.
   *
   * <p>Primary use for this is for debugging output.</p>
   *
   * @return the allocator's id
   */
  public int getId();

  /**
   * Close and release all buffers generated from this buffer pool.
   *
   * <p>When assertions are on, complains if there are any outstanding buffers; to avoid
   * that, release all buffers before the allocator is closed.
   */
  @Override
  public void close() throws Exception;

  /**
   * Returns the amount of memory currently allocated from this allocator.
   *
   * @return the amount of memory currently allocated
   */
  public long getAllocatedMemory();

  /**
   * Returns the peak amount of memory allocated from this allocator.
   *
   * @return the peak amount of memory allocated
   */
  public long getPeakMemoryAllocation();

  /**
   * Returns an empty DrillBuf.
   *
   * @return an empty DrillBuf
   */
  public DrillBuf getEmpty();

  /**
   * Create an allocation reservation. A reservation is a way of building up
   * a request for a buffer whose size is not known in advance. See
   * {@see AllocationReservation}.
   *
   * @return the newly created reservation
   */
  public AllocationReservation newReservation();
}
