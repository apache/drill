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

import javax.annotation.concurrent.NotThreadSafe;
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
    final DeltaLog mlog = new DeltaLog();
    final AllocationOutcome outcome = allocate(mlog, size, false);
    if (!outcome.isOk()) {
      mlog.rollback();
    } else {
      updatePeak();
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
    final DeltaLog mlog = new DeltaLog();
    boolean ok = allocate(mlog, size, true) == AllocationOutcome.SUCCESS;
    updatePeak();
    return ok;
  }

  private AllocationOutcome allocate(DeltaOperator mlog, long size, boolean forceAllocation) {
    final long remainder = mlog.tryAdd(locallyHeldMemory, reservation, size);

    if(remainder == 0){
      // we were able to allocate all required memory from our reservation.
      return AllocationOutcome.SUCCESS;
    }

    // check to see if we are allowed to allocate this much based on our max allocation.
    final long limitRemainder = mlog.tryAdd(locallyHeldMemory, allocationLimit, remainder);

    if (limitRemainder != 0) {
      // our limit disallowed us from allocating on the memory.
      if (forceAllocation) {
        // allocate locally anyway.
        mlog.tryAdd(locallyHeldMemory, Long.MAX_VALUE, limitRemainder);

        if(parent != null){
          parent.allocate(mlog, remainder, forceAllocation);
        }

        // we've blown past our local limit. What our potential parent says doesn't matter.
        return AllocationOutcome.FORCED_SUCESS;
      } else {
        // allocation isn't forced, the allocation failed.
        return AllocationOutcome.FAILED_LOCAL;
      }
    }

    assert limitRemainder == 0;
    assert remainder > 0;

    if (parent != null){
      // if the parent of this allocator isn't null, confirm that its limit also allow this allocation.
      AllocationOutcome parentOutcome = parent.allocate(mlog, remainder, forceAllocation);

      // rewrite a local failure since it is reported from our parent.
      if (parentOutcome == AllocationOutcome.FAILED_LOCAL) {
        return AllocationOutcome.FAILED_PARENT;
      }

      return parentOutcome;

    } else{
      // the allocation was successful and this is the root allocator.
      return AllocationOutcome.SUCCESS;
    }
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

  private interface DeltaOperator {
    public long tryAdd(final AtomicLong valueToChange, final AtomicLong maximumAllowed, final long deltaToAttempt);
    public long tryAdd(final AtomicLong valueToChange, final long maximumAllowed, final long deltaToAttempt);
  }

  /**
   * Private log of memory actions.
   */
  @NotThreadSafe
  private class DeltaLog implements DeltaOperator {

    private DeltaOperation op;

    private class DeltaOperation {
      final AtomicLong atomicLong;
      final long delta;
      final DeltaOperation next;

      public DeltaOperation(AtomicLong atomicLong, long delta, DeltaOperation next) {
        super();
        this.atomicLong = atomicLong;
        this.delta = delta;
        this.next = next;
      }

      private void rollback(){
        atomicLong.addAndGet(-delta);
      }
    }

    private void add(AtomicLong variable, long delta){
      final DeltaOperation newOp = new DeltaOperation(variable, delta, op);
      this.op = newOp;
    }

    /**
     * Rollback any memory changes that have taken place.
     */
    public void rollback(){
      DeltaOperation o = op;
      while(o != null){
        o.rollback();
        o = o.next;
      }
    }

    /**
     * Attempt to add a value to an atomic long. The result is guaranteed to be within [0..deltaToAttempt]
     *
     * This is a best-effort fail-fast approach to delta application. We attempt to apply the entire delta. If that
     * fails, we'll rewind to apply only the portion of delta that was possible when we did our initial application
     * (possibly nothing). We do not double-check maximumAllowed nor valueToChange to see if they might have changed
     * between our initial C & CAS and when we are considering rewinding.
     *
     * This is best effort because it is possible for multiple threads to interact with the underlying valueToChange and
     * maximumAllowed amounts while this function is operating. In those cases this function may return a different
     * value depending on when those were applied. Consumers of this interface should ensure that this is their desired
     * behavior. The ordering of operations here supports a consistent view of the world.
     *
     * @param valueToChange
     *          Value to change.
     * @param limit
     *          The maximum value allowed.
     * @param deltaToAttempt
     *          The amount you would like to attempt to add.
     * @return The remaining amount of delta that has been yet to apply [0..deltaToAttempt]. If 0, that means the
     *         complete delta was applied
     */
    public long tryAdd(final AtomicLong valueToChange, final AtomicLong limit, final long deltaToAttempt){
      return tryAdd(valueToChange, limit.get(), deltaToAttempt);
    }


    /**
     * Similar to {@link #tryAdd(AtomicLong, AtomicLong, deltaToAttempt) tryAdd} method except accepts long for maximum
     * value instead of AtomicLong.
     */
    public long tryAdd(final AtomicLong valueToChange, final long limit, final long deltaToAttempt){
      assert deltaToAttempt >= 0;

      // we check this first so we don't waste a bunch of time when we can't apply a change.
      if (valueToChange.get() >= limit) {
        // we're already beyond our limit, no reason to try anything else.
        return deltaToAttempt;
      }

      final long newValue = valueToChange.addAndGet(deltaToAttempt);
      final long overreach = newValue - limit;

      if(overreach <= 0){
        // we allocated everything and didn't overreach.
        add(valueToChange, deltaToAttempt);
        return 0;

      }

      final long originalValue = newValue - deltaToAttempt;
      final long desiredAllocation = limit - originalValue;

      if(desiredAllocation <= 0){
        // we were already over allocated before we started. unroll our allocation and return the entire delta.
        valueToChange.addAndGet(-deltaToAttempt);
        return deltaToAttempt;
      }else{
        final long excess = deltaToAttempt - desiredAllocation;
        valueToChange.addAndGet(-excess);
        add(valueToChange, desiredAllocation);
        return excess;
      }
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
