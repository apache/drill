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

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.drill.exec.exception.OutOfMemoryException;

import com.google.common.base.Preconditions;

/**
 * Provides a concurrent way to manage account for memory usage without locking. Used as basis for Allocators. All
 * operations are threadsafe (except for close).
 */
@ThreadSafe
class Accountant implements AutoCloseable {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Accountant.class);

  /**
   * The parent allocator
   */
  protected final Accountant parent;

  /**
   * The amount of memory reserved for this allocator. Releases below this amount of memory will not be returned to the
   * parent Accountant until this Accountant is closed.
   */
  protected final long reservation;

  private final AtomicLong peakAllocation = new AtomicLong();

  /**
   * Maximum local memory that can be held. This can be externally updated. Changing it won't cause past memory to
   * change but will change responses to future allocation efforts
   */
  private final AtomicLong allocationLimit = new AtomicLong();

  /**
   * Currently allocated amount of memory;
   */
  private final AtomicLong locallyHeldMemory = new AtomicLong();

  public Accountant(Accountant parent, long reservation, long maxAllocation) {
    Preconditions.checkArgument(reservation >= 0, "The initial reservation size must be non-negative.");
    Preconditions.checkArgument(maxAllocation >= 0, "The maximum allocation limit must be non-negative.");
    Preconditions.checkArgument(reservation <= maxAllocation,
        "The initial reservation size must be <= the maximum allocation.");
    Preconditions.checkArgument(reservation == 0 || parent != null, "The root accountant can't reserve memory.");

    this.parent = parent;
    this.reservation = reservation;
    this.allocationLimit.set(maxAllocation);

    if (reservation != 0) {
      // we will allocate a reservation from our parent.
      final AllocationOutcome outcome = parent.allocateBytes(reservation);
      if (!outcome.isOk()) {
        throw new OutOfMemoryException("Failure trying to allocate initial reservation for Allocator.");
      }
    }
  }

  /**
   * Attempt to allocate the requested amount of memory. Either completely succeeds or completely fails. Constructs a a
   * log of delta
   *
   * If it fails, no changes are made to accounting.
   *
   * @param size
   *          The amount of memory to reserve in bytes.
   * @return True if the allocation was successful, false if the allocation failed.
   */
  AllocationOutcome allocateBytes(long size) {
    final AllocationOutcome outcome = allocate(size, true, false);
    if (!outcome.isOk()) {
      releaseBytes(size);
    }
    return outcome;
  }

  private void updatePeak() {
    final long currentMemory = locallyHeldMemory.get();
    while (true) {
      final long previousPeak = peakAllocation.get();
      if (currentMemory > previousPeak) {
        if (peakAllocation.compareAndSet(previousPeak, currentMemory)) {
          // if we're able to update peak, finish.
          return;
        } else {
          // peak allocation changed underneath us. try again.
          continue;
        }
      } else {
        return;
      }
    }
  }

  /**
   * Increase the accounting. Returns whether the allocation fit within limits.
   *
   * @param size
   *          to increase
   * @return Whether the allocation fit within limits.
   */
  boolean forceAllocate(long size) {
    final AllocationOutcome outcome = allocate(size, true, true);
    return outcome.isOk();
  }

  /**
   * Internal method for allocation. This takes a forced approach to allocation to ensure that we manage reservation
   * boundary issues consistently. Allocation is always done through the entire tree. The two options that we influence
   * are whether the allocation should be forced and whether or not the peak memory allocation should be updated. If at
   * some point during allocation escalation we determine that the allocation is no longer possible, we will continue to
   * do a complete and consistent allocation but we will stop updating the peak allocation. We do this because we know
   * that we will be directly unwinding this allocation (and thus never actually making the allocation). If force
   * allocation is passed, then we continue to update the peak limits since we now know that this allocation will occur
   * despite our moving past one or more limits.
   *
   * @param size
   *          The size of the allocation.
   * @param incomingUpdatePeak
   *          Whether we should update the local peak for this allocation.
   * @param forceAllocation
   *          Whether we should force the allocation.
   * @return The outcome of the allocation.
   */
  private AllocationOutcome allocate(final long size, final boolean incomingUpdatePeak, final boolean forceAllocation) {
    final long newLocal = locallyHeldMemory.addAndGet(size);
    final long beyondReservation = newLocal - reservation;
    final boolean beyondLimit = newLocal > allocationLimit.get();
    final boolean updatePeak = forceAllocation || (incomingUpdatePeak && !beyondLimit);

    AllocationOutcome parentOutcome = AllocationOutcome.SUCCESS;
    if (beyondReservation > 0 && parent != null) {
      // we need to get memory from our parent.
      final long parentRequest = Math.min(beyondReservation, size);
      parentOutcome = parent.allocate(parentRequest, updatePeak, forceAllocation);
    }

    final AllocationOutcome finalOutcome = beyondLimit ? AllocationOutcome.FAILED_LOCAL :
        parentOutcome.ok ? AllocationOutcome.SUCCESS : AllocationOutcome.FAILED_PARENT;

    if (updatePeak) {
      updatePeak();
    }

    return finalOutcome;
  }

  public void releaseBytes(long size) {
    // reduce local memory. all memory released above reservation should be released up the tree.
    final long newSize = locallyHeldMemory.addAndGet(-size);

    Preconditions.checkArgument(newSize >= 0, "Accounted size went negative.");

    final long originalSize = newSize + size;
    if(originalSize > reservation && parent != null){
      // we deallocated memory that we should release to our parent.
      final long possibleAmountToReleaseToParent = originalSize - reservation;
      final long actualToReleaseToParent = Math.min(size, possibleAmountToReleaseToParent);
      parent.releaseBytes(actualToReleaseToParent);
    }

  }

  /**
   * Set the maximum amount of memory that can be allocated in the this Accountant before failing an allocation.
   *
   * @param newLimit
   *          The limit in bytes.
   */
  public void setLimit(long newLimit) {
    allocationLimit.set(newLimit);
  }

  public boolean isOverLimit() {
    return getAllocatedMemory() > getLimit() || (parent != null && parent.isOverLimit());
  }

  /**
   * Close this Accountant. This will release any reservation bytes back to a parent Accountant.
   */
  public void close() {
    // return memory reservation to parent allocator.
    if (parent != null) {
      parent.releaseBytes(reservation);
    }
  }

  /**
   * Return the current limit of this Accountant.
   *
   * @return Limit in bytes.
   */
  public long getLimit() {
    return allocationLimit.get();
  }

  /**
   * Return the current amount of allocated memory that this Accountant is managing accounting for. Note this does not
   * include reservation memory that hasn't been allocated.
   *
   * @return Currently allocate memory in bytes.
   */
  public long getAllocatedMemory() {
    return locallyHeldMemory.get();
  }

  /**
   * The peak memory allocated by this Accountant.
   *
   * @return The peak allocated memory in bytes.
   */
  public long getPeakMemoryAllocation() {
    return peakAllocation.get();
  }

  /**
   * Describes the type of outcome that occurred when trying to account for allocation of memory.
   */
  public static enum AllocationOutcome {

    /**
     * Allocation succeeded.
     */
    SUCCESS(true),

    /**
     * Allocation succeeded but only because the allocator was forced to move beyond a limit.
     */
    FORCED_SUCESS(true),

    /**
     * Allocation failed because the local allocator's limits were exceeded.
     */
    FAILED_LOCAL(false),

    /**
     * Allocation failed because a parent allocator's limits were exceeded.
     */
    FAILED_PARENT(false);

    private final boolean ok;

    AllocationOutcome(boolean ok) {
      this.ok = ok;
    }

    public boolean isOk() {
      return ok;
    }
  }
}
