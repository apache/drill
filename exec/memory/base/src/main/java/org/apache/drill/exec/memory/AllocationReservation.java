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

import io.netty.buffer.DrillBuf;

import com.google.common.base.Preconditions;

/**
 * Supports cumulative allocation reservation. Clients may increase the size of the reservation repeatedly until they
 * call for an allocation of the current total size. The reservation can only be used once, and will throw an exception
 * if it is used more than once.
 * <p>
 * For the purposes of airtight memory accounting, the reservation must be close()d whether it is used or not.
 * This is not threadsafe.
 */
public abstract class AllocationReservation implements AutoCloseable {
  private int nBytes = 0;
  private boolean used = false;
  private boolean closed = false;

  /**
   * Constructor. Prevent construction except by derived classes.
   * <p>The expectation is that the derived class will be a non-static inner
   * class in an allocator.
   */
  AllocationReservation() {
  }

  /**
   * Add to the current reservation.
   *
   * <p>Adding may fail if the allocator is not allowed to consume any more space.
   *
   * @param nBytes the number of bytes to add
   * @return true if the addition is possible, false otherwise
   * @throws IllegalStateException if called after buffer() is used to allocate the reservation
   */
  public boolean add(final int nBytes) {
    Preconditions.checkArgument(nBytes >= 0, "nBytes(%d) < 0", nBytes);
    Preconditions.checkState(!closed, "Attempt to increase reservation after reservation has been closed");
    Preconditions.checkState(!used, "Attempt to increase reservation after reservation has been used");

    // we round up to next power of two since all reservations are done in powers of two. This may overestimate the
    // preallocation since someone may perceive additions to be power of two. If this becomes a problem, we can look at
    // modifying this behavior so that we maintain what we reserve and what the user asked for and make sure to only
    // round to power of two as necessary.
    final int nBytesTwo = BaseAllocator.nextPowerOfTwo(nBytes);
    if (!reserve(nBytesTwo)) {
      return false;
    }

    this.nBytes += nBytesTwo;
    return true;
  }

  /**
   * Requests a reservation of additional space.
   *
   * <p>The implementation of the allocator's inner class provides this.
   *
   * @param nBytes the amount to reserve
   * @return true if the reservation can be satisfied, false otherwise
   */
  abstract boolean reserve(int nBytes);

  /**
   * Allocate a buffer whose size is the total of all the add()s made.
   *
   * <p>The allocation request can still fail, even if the amount of space
   * requested is available, if the allocation cannot be made contiguously.
   *
   * @return the buffer, or null, if the request cannot be satisfied
   * @throws IllegalStateException if called called more than once
   */
  public DrillBuf buffer() {
    Preconditions.checkState(!closed, "Attempt to allocate after closed");
    Preconditions.checkState(!used, "Attempt to allocate more than once");

    final DrillBuf drillBuf = allocate(nBytes);
    used = true;
    return drillBuf;
  }

  /**
   * Allocate the a buffer of the requested size.
   *
   * <p>The implementation of the allocator's inner class provides this.
   *
   * @param nBytes the size of the buffer requested
   * @return the buffer, or null, if the request cannot be satisfied
   */
  abstract DrillBuf allocate(int nBytes);

  @Override
  public void close() {
    if (closed) {
      return;
    }
    if (!used) {
      releaseReservation(nBytes);
    }

    closed = true;
  }

  /**
   * Return the reservation back to the allocator without having used it.
   *
   * @param nBytes the size of the reservation
   */
  abstract void releaseReservation(int nBytes);

  /**
   * Get the current size of the reservation (the sum of all the add()s).
   *
   * @return size of the current reservation
   */
  public int getSize() {
    return nBytes;
  }

  /**
   * Return whether or not the reservation has been used.
   *
   * @return whether or not the reservation has been used
   */
  public boolean isUsed() {
    return used;
  }

  /**
   * Return whether or not the reservation has been closed.
   *
   * @return whether or not the reservation has been closed
   */
  public boolean isClosed() {
    return closed;
  }
}
