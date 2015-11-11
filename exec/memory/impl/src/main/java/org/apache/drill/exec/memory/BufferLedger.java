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

import org.apache.drill.exec.util.Pointer;

import io.netty.buffer.DrillBuf;
import io.netty.buffer.PooledByteBufAllocatorL;

/**
 * BufferLedger is an interface meant to facility the private
 * exchange of information between a DrillBuf and its owning
 * allocator. To that end, a number of DrillBuf constructors
 * and methods take a BufferLedger as an argument, yet there
 * are no public implementations of BufferLedger; they all
 * come from inner classes implemented by allocators, ensuring
 * that allocators can give DrillBufs the access they need when
 * they are created or asked to perform complex operations such
 * as ownership sharing or transfers.
 */
public interface BufferLedger {
  /**
   * Get the underlying pooled allocator used by this ledger's
   * allocator.
   *
   * <p>This is usually used to create the shared singleton
   * empty buffer. Don't use it to create random buffers, because
   * they won't be tracked, and we won't be able to find leaks.</p>
   *
   * @return the underlying pooled allocator
   */
  public PooledByteBufAllocatorL getUnderlyingAllocator();

  /**
   * Return a buffer's memory to the allocator.
   *
   * @param drillBuf the DrillBuf that was freed
   */
  public void release(DrillBuf drillBuf);

  /**
   * Share ownership of a buffer with another allocator. As far as reader
   * and writer index positions go, this acts like a new slice that is owned
   * by the target allocator, but which has it's own lifetime (i.e., it doesn't
   * share the fate of the original buffer, unlike real slices).
   *
   * @param pDrillBuf returns the new DrillBuf that is shared
   * @param otherLedger the ledger the new DrillBuf should use
   * @param otherAllocator the new allocator-owner
   * @param drillBuf the original DrillBuf to be shared
   * @param index the starting index to be shared (as for slicing)
   * @param length the length to be shared (as for slicing)
   * @param drillBufFlags private flags passed through from the allocator
   *   (this call originates with a call to BufferAllocator.shareOwnership()).
   * @return the ledger the calling DrillBuf must use from this point forward;
   *   this may not match it's original ledger, as allocators provide multiple
   *   implementations of ledgers to cope with sharing and slicing
   */
  public BufferLedger shareWith(Pointer<DrillBuf> pDrillBuf,
      BufferLedger otherLedger, BufferAllocator otherAllocator,
      DrillBuf drillBuf, int index, int length, int drillBufFlags);

  /**
   * Transfer the ownership of a buffer to another allocator. This doesn't change
   * any of the buffer's reader or writer positions or size, just which allocator
   * owns it. The reference count stays the same.
   *
   * @param newAlloc the new allocator (the one to transfer to)
   * @param pNewLedger a Pointer<> initialized with a candidate ledger; this
   *   may be used, or it may not, depending on the sharing state of the buffer.
   *   The caller is required to use whatever ledger is in pNewLedger on return
   * @param drillBuf the buffer to transfer
   * @return true if the transfer kept the target allocator within its maximum
   *   allocation limit; false if the allocator now owns more memory than its
   *   creation-time maximum
   */
  public boolean transferTo(final BufferAllocator newAlloc,
      final Pointer<BufferLedger> pNewLedger, final DrillBuf drillBuf);
}
