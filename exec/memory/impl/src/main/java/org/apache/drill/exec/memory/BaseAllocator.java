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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.DrillBuf;
import io.netty.buffer.PooledByteBufAllocatorL;
import io.netty.buffer.UnsafeDirectLittleEndian;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.DrillAutoCloseables;
import org.apache.drill.common.HistoricalLog;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.util.AssertionUtil;
import org.apache.drill.exec.util.Pointer;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;

// TODO(cwestin) javadoc
// TODO(cwestin) add allocator implementation options tried
public abstract class BaseAllocator implements BufferAllocator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseAllocator.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(BaseAllocator.class);

  private static final AtomicInteger idGenerator = new AtomicInteger(0);
  private static final Object ALLOCATOR_LOCK = new Object();

  public static final String CHILD_BUFFER_INJECTION_SITE = "child.buffer";

  static final boolean DEBUG = AssertionUtil.isAssertionsEnabled()
      || Boolean.getBoolean(ExecConstants.DEBUG_ALLOCATOR);
  private static final PooledByteBufAllocatorL INNER_ALLOCATOR = PooledByteBufAllocatorL.DEFAULT;

  private long allocated; // the amount of memory this allocator has given out to its clients (including children)
  private long owned; // the amount of memory this allocator has obtained from its parent
  private long peakAllocated; // the most memory this allocator has given out during its lifetime
  private long bufferAllocation; // the amount of memory used just for directly allocated buffers, not children

  private boolean isClosed = false; // the allocator has been closed

  private final long maxAllocation; // the maximum amount of memory this allocator will give out
  private final long chunkSize; // size of secondary chunks to allocate
  private final AllocationPolicyAgent policyAgent;
  private final BaseAllocator parentAllocator;
  private final AllocatorOwner allocatorOwner;
  protected final int id = idGenerator.incrementAndGet(); // unique ID assigned to each allocator
  private final DrillBuf empty;
  private final AllocationPolicy allocationPolicy;
  private final InnerBufferLedger bufferLedger = new InnerBufferLedger();

  // members used purely for debugging
  private int getsFromParent;
  private int putsToParent;
  private final IdentityHashMap<UnsafeDirectLittleEndian, BufferLedger> allocatedBuffers;
  private final IdentityHashMap<BaseAllocator, Object> childAllocators;
  private final IdentityHashMap<Reservation, Object> reservations;
  private long preallocSpace;

  private final HistoricalLog historicalLog;

  private static BaseAllocator getBaseAllocator(final BufferAllocator bufferAllocator) {
    if (!(bufferAllocator instanceof BaseAllocator)) {
      throw new IllegalArgumentException("expected a BaseAllocator instance, but got a "
          + bufferAllocator.getClass().getName());
    }
    return (BaseAllocator) bufferAllocator;
  }

  // TODO move allocation policy and agent to outside of allocator
  private static class PerFragmentAllocationPolicy implements AllocationPolicy {
    static class Globals {
      private long maxBufferAllocation = 0;
      private final AtomicInteger limitingRoots = new AtomicInteger(0);
    }

    private final Globals globals = new Globals();

    @Override
    public AllocationPolicyAgent newAgent() {
      return new PerFragmentAllocationPolicyAgent(globals);
    }
  }

  /**
   * AllocationPolicy that allows each fragment running on a drillbit to share an
   * equal amount of direct memory, regardless of whether or not those fragments
   * belong to the same query.
   */
  public static final AllocationPolicy POLICY_PER_FRAGMENT = new PerFragmentAllocationPolicy();

  /**
   * String name of {@link #POLICY_PER_FRAGMENT} policy.
   */
  public static final String POLICY_PER_FRAGMENT_NAME = "per-fragment";

  private static class PerFragmentAllocationPolicyAgent implements AllocationPolicyAgent {
    private final PerFragmentAllocationPolicy.Globals globals;
    private boolean limitingRoot; // this is a limiting root; see F_LIMITING_ROOT

    PerFragmentAllocationPolicyAgent(PerFragmentAllocationPolicy.Globals globals) {
      this.globals = globals;
    }

    @Override
    public void close() {
      if (limitingRoot) {
        // now there's one fewer active root
        final int rootCount = globals.limitingRoots.decrementAndGet();

        synchronized(globals) {
          /*
           * If the rootCount went to zero, we don't need to worry about setting the
           * maxBufferAllocation, because there aren't any allocators to reference it;
           * the next allocator to get created will set it appropriately.
           */
          if (rootCount != 0) {
            globals.maxBufferAllocation = RootAllocator.getMaxDirect() / rootCount;
          }
        }
      }
    }

    @Override
    public void checkNewAllocator(BufferAllocator parentAllocator,
        long initReservation, long maxAllocation, int flags) {
/*
      Preconditions.checkArgument(parentAllocator != null, "parent allocator can't be null");
      Preconditions.checkArgument(parentAllocator instanceof BaseAllocator, "Parent allocator must be a BaseAllocator");
*/

//      final BaseAllocator baseAllocator = (BaseAllocator) parentAllocator;

      // this is synchronized to protect maxBufferAllocation
      synchronized(POLICY_PER_FRAGMENT) {
        // initialize maxBufferAllocation the very first time we call this
        if (globals.maxBufferAllocation == 0) {
          globals.maxBufferAllocation = RootAllocator.getMaxDirect();
        }

        if (limitingRoot = ((flags & F_LIMITING_ROOT) != 0)) {
          // figure out the new current per-allocator limit
          globals.maxBufferAllocation = RootAllocator.getMaxDirect() / (globals.limitingRoots.get() + 1);
        }

        if (initReservation > 0) {
          if (initReservation > globals.maxBufferAllocation) {
            throw new OutOfMemoryRuntimeException(
                String.format("can't fulfill initReservation request at this time "
                    + "(initReservation = %d > maxBufferAllocation = %d)",
                initReservation, globals.maxBufferAllocation));
          }
        }
      }
    }

    @Override
    public long getMemoryLimit(BufferAllocator bufferAllocator) {
      synchronized(POLICY_PER_FRAGMENT) {
        return globals.maxBufferAllocation;
      }
    }

    @Override
    public void initializeAllocator(final BufferAllocator bufferAllocator) {
      final BaseAllocator baseAllocator = getBaseAllocator(bufferAllocator);

      if (limitingRoot) {
        globals.limitingRoots.incrementAndGet();
      }
    }

    @Override
    public boolean shouldReleaseToParent(final BufferAllocator bufferAllocator) {
      final BaseAllocator baseAllocator = getBaseAllocator(bufferAllocator);
      return (baseAllocator.owned + baseAllocator.chunkSize > globals.maxBufferAllocation);
    }
  }

  private static class LocalMaxAllocationPolicy implements AllocationPolicy {
    // this agent is stateless, so we can always use the same one
    private static final AllocationPolicyAgent AGENT = new LocalMaxAllocationPolicyAgent();

    @Override
    public AllocationPolicyAgent newAgent() {
      return AGENT;
    }
  }

  /**
   * AllocationPolicy that imposes no limits on how much direct memory fragments
   * may allocate. LOCAL_MAX refers to the only limit that is enforced, which is
   * the maxAllocation specified at allocators' creation.
   *
   * <p>This policy ignores the value of {@link BufferAllocator#F_LIMITING_ROOT}.</p>
   */
  public static final AllocationPolicy POLICY_LOCAL_MAX = new LocalMaxAllocationPolicy();

  /**
   * String name of {@link #POLICY_LOCAL_MAX} allocation policy.
   */
  public static final String POLICY_LOCAL_MAX_NAME = "local-max";

  private static class LocalMaxAllocationPolicyAgent implements AllocationPolicyAgent {
    @Override
    public void close() throws Exception {
    }

    @Override
    public void checkNewAllocator(BufferAllocator parentAllocator,
        long initReservation, long maxAllocation, int flags) {
    }

    @Override
    public long getMemoryLimit(BufferAllocator bufferAllocator) {
      final BaseAllocator baseAllocator = (BaseAllocator) bufferAllocator;
      return baseAllocator.maxAllocation;
    }

    @Override
    public void initializeAllocator(BufferAllocator bufferAllocator) {
    }

    @Override
    public boolean shouldReleaseToParent(BufferAllocator bufferAllocator) {
      // since there are no shared limits, release space whenever we can
      return true;
    }
  }

  // TODO(DRILL-2698) POLICY_PER_QUERY

  protected BaseAllocator(
      final BaseAllocator parentAllocator,
      final AllocatorOwner allocatorOwner,
      final AllocationPolicy allocationPolicy,
      final long initReservation,
      final long maxAllocation,
      final int flags) throws OutOfMemoryRuntimeException {
    Preconditions.checkArgument(allocatorOwner != null, "allocatorOwner must be non-null");
    Preconditions.checkArgument(initReservation >= 0,
        "the initial reservation size must be non-negative");
    Preconditions.checkArgument(maxAllocation >= 0,
        "the maximum allocation limit mjst be non-negative");
    Preconditions.checkArgument(initReservation <= maxAllocation,
        "the initial reservation size must be <= the maximum allocation");

    if (initReservation > 0) {
      if (parentAllocator == null) {
        throw new IllegalStateException(
            "can't reserve memory without a parent allocator");
      }
    }

    // check to see if we can create this new allocator (the check throws if it's not ok)
    final AllocationPolicyAgent policyAgent = allocationPolicy.newAgent();
    policyAgent.checkNewAllocator(parentAllocator, initReservation, maxAllocation, flags);

    if ((initReservation > 0) && !parentAllocator.reserve(this, initReservation, 0)) {
      throw new OutOfMemoryRuntimeException(
          "can't fulfill initial reservation of size (unavailable from parent)" + initReservation);
    }

    /*
     * Figure out how much more to ask for from our parent if we're out.
     * Secondary allocations from the parent will be done in integral multiples of
     * chunkSize. We also use this to determine when to hand back memory to our
     * parent in order to create hysteresis. This reduces contention on the parent's
     * lock
     */
    if (initReservation == 0) {
      chunkSize = maxAllocation / 8;
    } else {
      chunkSize = initReservation;
    }

    this.parentAllocator = parentAllocator;
    this.allocatorOwner = allocatorOwner;
    this.allocationPolicy = allocationPolicy;
    this.policyAgent = policyAgent;
    this.maxAllocation = maxAllocation;

    // the root allocator owns all of its memory; anything else just owns it's initial reservation
    owned = parentAllocator == null ? maxAllocation : initReservation;
    empty = DrillBuf.getEmpty(new EmptyLedger(), this);

    if (DEBUG) {
      allocatedBuffers = new IdentityHashMap<>();
      childAllocators = new IdentityHashMap<>();
      reservations = new IdentityHashMap<>();
      historicalLog = new HistoricalLog(4, "allocator[%d]", id);

      historicalLog.recordEvent("created by \"%s\", owned = %d", allocatorOwner.toString(), owned);
    } else {
      allocatedBuffers = null;
      childAllocators = null;
      reservations = null;
      historicalLog = null;
    }

    // now that we're not in danger of throwing an exception, we can take this step
    policyAgent.initializeAllocator(this);
  }

  /**
   * Allocators without a parent must provide an implementation of this so
   * that they may reserve additional space even though they don't have a
   * parent they can fall back to.
   *
   * <p>Prior to calling this, BaseAllocator has verified that this won't violate
   * the maxAllocation for this allocator.</p>
   *
   * @param nBytes the amount of space to reserve
   * @param ignoreMax ignore the maximum allocation limit;
   *   see {@link ChildLedger#reserve(long, boolean)}.
   * @return true if the request can be met, false otherwise
   */
  protected boolean canIncreaseOwned(final long nBytes, final int flags) {
    if (parentAllocator == null) {
      return false;
    }

    return parentAllocator.reserve(this, nBytes, flags);
  }

  /**
   * Reserve space for the child allocator from this allocator.
   *
   * @param childAllocator child allocator making the request, or null
   *  if this is not for a child
   * @param nBytes how much to reserve
   * @param flags one or more of RESERVE_F_* flags or'ed together
   * @return true if the reservation can be satisfied, false otherwise
   */
  private static final int RESERVE_F_IGNORE_MAX = 0x0001;
  private boolean reserve(final BaseAllocator childAllocator,
      final long nBytes, final int flags) {
    Preconditions.checkArgument(nBytes >= 0,
        "the number of bytes to reserve must be non-negative");

    // we can always fulfill an empty request
    if (nBytes == 0) {
      return true;
    }

    final boolean ignoreMax = (flags & RESERVE_F_IGNORE_MAX) != 0;

    synchronized(ALLOCATOR_LOCK) {
      if (isClosed) {
        throw new AllocatorClosedException(String.format("Attempt to use closed allocator[%d]", id));
      }

      final long ownAtLeast = allocated + nBytes;
      // Are we allowed to hand out this much?
      if (!ignoreMax && (ownAtLeast > maxAllocation)) {
        return false;
      }

      // do we need more from our parent first?
      if (ownAtLeast > owned) {
        /*
         * Allocate space in integral multiples of chunkSize, as long as it doesn't exceed
         * the maxAllocation.
         */
        final long needAdditional = ownAtLeast - owned;
        final long getInChunks = (1 + ((needAdditional - 1) / chunkSize)) * chunkSize;
        final long getFromParent;
        if (getInChunks + owned <= maxAllocation) {
          getFromParent = getInChunks;
        } else {
          getFromParent = needAdditional;
        }
        if (!canIncreaseOwned(getFromParent, flags)) {
          return false;
        }
        owned += getFromParent;

        if (DEBUG) {
          ++getsFromParent;
          historicalLog.recordEvent("increased owned by %d, now owned == %d", needAdditional, owned);
        }
      }

      if (DEBUG) {
        if (owned < ownAtLeast) {
          throw new IllegalStateException("don't own enough memory to satisfy request");
        }
        if (allocated > owned) {
          throw new IllegalStateException(
              String.format("more memory allocated (%d) than owned (%d)", allocated, owned));
        }

        historicalLog.recordEvent("allocator[%d] allocated increased by nBytes == %d to %d",
            id, nBytes, allocated + nBytes);
      }

      allocated += nBytes;

      if (allocated > peakAllocated) {
        peakAllocated = allocated;
      }

      return true;
    }
  }

  private void releaseBytes(final long nBytes) {
    Preconditions.checkArgument(nBytes >= 0,
        "the number of bytes being released must be non-negative");

    synchronized(ALLOCATOR_LOCK) {
      allocated -= nBytes;

      if (DEBUG) {
        historicalLog.recordEvent("allocator[%d] released nBytes == %d, allocated now %d",
            id, nBytes, allocated);
      }

      /*
       * Return space to our parent if our allocation is over the currently allowed amount.
       */
      final boolean releaseToParent = (parentAllocator != null)
          && (owned > maxAllocation) && policyAgent.shouldReleaseToParent(this);
      if (releaseToParent) {
        final long canFree = owned - maxAllocation;
        parentAllocator.releaseBytes(canFree);
        owned -= canFree;

        if (DEBUG) {
          ++putsToParent;
          historicalLog.recordEvent("returned %d to parent, now owned == %d", canFree, owned);
        }
      }
    }
  }

  private void releaseBuffer(final DrillBuf drillBuf) {
    Preconditions.checkArgument(drillBuf != null,
        "the DrillBuf being released can't be null");

    final ByteBuf byteBuf = drillBuf.unwrap();
    final int udleMaxCapacity = byteBuf.maxCapacity();

    synchronized(ALLOCATOR_LOCK) {
      bufferAllocation -= udleMaxCapacity;
      releaseBytes(udleMaxCapacity);

      if (DEBUG) {
        // make sure the buffer came from this allocator
        final Object object = allocatedBuffers.remove(byteBuf);
        if (object == null) {
          historicalLog.logHistory(logger);
          drillBuf.logHistory(logger);
          throw new IllegalStateException("Released buffer did not belong to this allocator");
        }
      }
    }
  }

  private void childClosed(final BaseAllocator childAllocator) {
    Preconditions.checkArgument(childAllocator != null, "child allocator can't be null");

    if (DEBUG) {
      synchronized(ALLOCATOR_LOCK) {
        final Object object = childAllocators.remove(childAllocator);
        if (object == null) {
          childAllocator.historicalLog.logHistory(logger);
          throw new IllegalStateException("Child allocator[" + childAllocator.id
              + "] not found in parent allocator[" + id + "]'s childAllocators");
        }

        try {
          verifyAllocator();
        } catch(Exception e) {
          /*
           * If there was a problem with verification, the history of the closed
           * child may also be useful.
           */
          logger.debug("allocator[" + id + "]: exception while closing the following child");
          childAllocator.historicalLog.logHistory(logger);

          // Continue with the verification exception throwing.
          throw e;
        }
      }
    }
  }

  /**
   * TODO(DRILL-2740) We use this to bypass the regular accounting for the
   * empty DrillBuf, because it is treated specially at this time. Once that
   * is remedied, this should be able to go away.
   */
  private class EmptyLedger implements BufferLedger {
    @Override
    public PooledByteBufAllocatorL getUnderlyingAllocator() {
      return INNER_ALLOCATOR;
    }

    @Override
    public void release(final DrillBuf drillBuf) {
      if (DEBUG) {
        if (drillBuf != empty) {
          throw new IllegalStateException("The empty buffer's ledger is being used to release something else");
        }
      }
    }

    @Override
    public BufferLedger shareWith(Pointer<DrillBuf> pDrillBuf,
        BufferLedger otherLedger, BufferAllocator otherAllocator, DrillBuf drillBuf,
        int index, int length, int drillBufFlags) {
      // As a special case, we allow sharing with the same allocator so that slicing works.
      if (otherAllocator != BaseAllocator.this) {
        throw new UnsupportedOperationException("The empty buffer can't be shared");
      }

      pDrillBuf.value = drillBuf;
      return otherLedger;
    }

    @Override
    public boolean transferTo(BufferAllocator newAlloc,
        Pointer<BufferLedger> pNewLedger, DrillBuf drillBuf) {
      throw new UnsupportedOperationException("The empty buffer's ownership can't be changed");
    }
  }

  private class InnerBufferLedger implements BufferLedger {
    @Override
    public PooledByteBufAllocatorL getUnderlyingAllocator() {
      return INNER_ALLOCATOR;
    }

    @Override
    public void release(final DrillBuf drillBuf) {
      releaseBuffer(drillBuf);
    }

    @Override
    public BufferLedger shareWith(final Pointer<DrillBuf> pDrillBuf,
        final BufferLedger otherLedger, final BufferAllocator otherAllocator,
        final DrillBuf drillBuf, final int index, final int length, final int drillBufFlags) {
      final BaseAllocator baseAllocator = (BaseAllocator) otherAllocator;
      synchronized(ALLOCATOR_LOCK) {
        if (baseAllocator.isClosed) {
          throw new AllocatorClosedException(
              String.format("Attempt to use closed allocator[%d]", baseAllocator.id));
        }

        /*
         * If this is called, then the buffer isn't yet shared, and should
         * become so.
         */
        final SharedBufferLedger sharedLedger = new SharedBufferLedger(drillBuf, BaseAllocator.this);

        // Create the new wrapping DrillBuf.
        final DrillBuf newBuf =
            new DrillBuf(sharedLedger, otherAllocator, drillBuf, index, length, drillBufFlags);
        sharedLedger.addMapping(newBuf, baseAllocator);

        if (DEBUG) {
          final UnsafeDirectLittleEndian udle = (UnsafeDirectLittleEndian) drillBuf.unwrap();
          historicalLog.recordEvent("InnerBufferLedger(allocator[%d]).shareWith(..., "
              + "otherAllocator[%d], drillBuf[%d]{UnsafeDirectLittleEndian[identityHashCode == %d]}, ...)",
              BaseAllocator.this.id, baseAllocator.id, drillBuf.getId(),
              System.identityHashCode(udle));

          final BaseAllocator drillBufAllocator = (BaseAllocator) drillBuf.getAllocator();
          if (BaseAllocator.this != drillBufAllocator) {
            historicalLog.logHistory(logger);
            drillBuf.logHistory(logger);
            throw new IllegalStateException(String.format(
                "DrillBuf's allocator([%d]) doesn't match this(allocator[%d])",
                drillBufAllocator.id, BaseAllocator.this.id));
          }

          // Replace the ledger for the existing buffer.
          final BufferLedger thisLedger = allocatedBuffers.put(udle, sharedLedger);

          // If we throw any of these exceptions, we need to clean up newBuf.
          if (thisLedger == null) {
            newBuf.release();
            historicalLog.logHistory(logger);
            drillBuf.logHistory(logger);
            throw new IllegalStateException("Buffer to be shared is unknown to the source allocator");
          }
          if (thisLedger != this) {
            newBuf.release();
            historicalLog.logHistory(logger);
            drillBuf.logHistory(logger);
            throw new IllegalStateException("Buffer's ledger was not the one it should be");
          }
        }

        pDrillBuf.value = newBuf;
        return sharedLedger;
      }
    }

    @Override
    public boolean transferTo(final BufferAllocator newAlloc,
        final Pointer<BufferLedger> pNewLedger, final DrillBuf drillBuf) {
      Preconditions.checkArgument(newAlloc != null, "New allocator cannot be null");
      Preconditions.checkArgument(newAlloc != BaseAllocator.this,
          "New allocator is same as current");
      Preconditions.checkArgument(newAlloc instanceof BaseAllocator,
          "New allocator isn't a BaseAllocator");
      Preconditions.checkArgument(pNewLedger.value != null, "Candidate new ledger can't be null");
      Preconditions.checkArgument(drillBuf != null, "DrillBuf can't be null");

      final BaseAllocator newAllocator = (BaseAllocator) newAlloc;
      synchronized(ALLOCATOR_LOCK) {
        if (newAllocator.isClosed) {
          throw new AllocatorClosedException(
              String.format("Attempt to use closed allocator[%d]", newAllocator.id));
        }

        return BaseAllocator.transferTo(newAllocator, pNewLedger.value, drillBuf);
      }
    }
  }

  /**
   * Transfer ownership of a buffer from one allocator to another.
   *
   * <p>Assumes the allocatorLock is held.</p>
   *
   * @param newAllocator the new allocator
   * @param newLedger the new ledger to use (which could be shared)
   * @param drillBuf the buffer
   * @return true if the buffer's transfer didn't exceed the new owner's maximum
   *   allocation limit
   */
  private static boolean transferTo(final BaseAllocator newAllocator,
      final BufferLedger newLedger, final DrillBuf drillBuf) {
    final UnsafeDirectLittleEndian udle = (UnsafeDirectLittleEndian) drillBuf.unwrap();
    final int udleMaxCapacity = udle.maxCapacity();

    synchronized(ALLOCATOR_LOCK) {
      // Account for the space and track the buffer.
      newAllocator.reserveForBuf(udleMaxCapacity);

      if (DEBUG) {
        final Object object = newAllocator.allocatedBuffers.put(udle, newLedger);
        if (object != null) {
          newAllocator.historicalLog.logHistory(logger);
          drillBuf.logHistory(logger);
          throw new IllegalStateException("Buffer unexpectedly found in new allocator");
        }
      }

      // Remove from the old allocator.
      final BaseAllocator oldAllocator = (BaseAllocator) drillBuf.getAllocator();
      oldAllocator.releaseBuffer(drillBuf);

      if (DEBUG) {
        final Object object = oldAllocator.allocatedBuffers.get(udle);
        if (object != null) {
          oldAllocator.historicalLog.logHistory(logger);
          drillBuf.logHistory(logger);
          throw new IllegalStateException("Buffer was not removed from old allocator");
        }

        oldAllocator.historicalLog.recordEvent("BaseAllocator.transferTo(otherAllocator[%d], ..., "
            + "drillBuf[%d]{UnsafeDirectLittleEndian[identityHashCode == %d]}) oldAllocator[%d]",
            newAllocator.id, drillBuf.getId(), System.identityHashCode(drillBuf.unwrap()),
            oldAllocator.id);
        newAllocator.historicalLog.recordEvent("BaseAllocator.transferTo(otherAllocator[%d], ..., "
            + "drillBuf[%d]{UnsafeDirectLittleEndian[identityHashCode == %d]}) oldAllocator[%d]",
            newAllocator.id, drillBuf.getId(), System.identityHashCode(drillBuf.unwrap()),
            oldAllocator.id);
      }

      return newAllocator.allocated < newAllocator.maxAllocation;
    }
  }

  private static class SharedBufferLedger implements BufferLedger {
    private volatile BaseAllocator owningAllocator;
    private final IdentityHashMap<DrillBuf, BaseAllocator> bufferMap = new IdentityHashMap<>();

    private final HistoricalLog historicalLog;

    public SharedBufferLedger(final DrillBuf drillBuf, final BaseAllocator baseAllocator) {
      if (DEBUG) {
        historicalLog = new HistoricalLog(4,
            "SharedBufferLedger for DrillBuf[%d]{UnsafeDirectLittleEndian[identityHashCode == %d]}",
            drillBuf.getId(), System.identityHashCode(drillBuf.unwrap()));
      } else {
        historicalLog = null;
      }
      addMapping(drillBuf, baseAllocator);
      owningAllocator = baseAllocator;

      if (DEBUG) {
        checkBufferMap();
      }
    }

    private synchronized void addMapping(final DrillBuf drillBuf, final BaseAllocator baseAllocator) {
      bufferMap.put(drillBuf, baseAllocator);

      if (DEBUG) {
        historicalLog.recordEvent("addMapping(DrillBuf[%d], allocator[%d])", drillBuf.getId(), baseAllocator.id);
      }
    }

    private synchronized void logBufferHistories(final Logger logger) {
      final Set<Map.Entry<DrillBuf, BaseAllocator>> bufsToCheck = bufferMap.entrySet();
      for(final Map.Entry<DrillBuf, BaseAllocator> mapEntry : bufsToCheck) {
        final DrillBuf drillBuf = mapEntry.getKey();
        drillBuf.logHistory(logger);
      }
    }

    private synchronized void checkBufferMap() {
      boolean foundOwner = false;
      final Set<Map.Entry<DrillBuf, BaseAllocator>> bufsToCheck = bufferMap.entrySet();
      for(final Map.Entry<DrillBuf, BaseAllocator> mapEntry : bufsToCheck) {
        final DrillBuf drillBuf = mapEntry.getKey();
        final BaseAllocator bufferAllocator = mapEntry.getValue();

        final Object object = bufferAllocator.allocatedBuffers.get(drillBuf.unwrap());
        if (bufferAllocator == owningAllocator) {
          foundOwner = true;
          if (object == null) {
            historicalLog.logHistory(logger);
            logBufferHistories(logger);
            throw new IllegalStateException(
                String.format("Shared buffer DrillBuf[%d] not found in owning allocator[%d]",
                    drillBuf.getId(), bufferAllocator.id));
          }
        } else {
          if (object != null) {
            historicalLog.logHistory(logger);
            logBufferHistories(logger);
            throw new IllegalStateException(
                String.format("Shared buffer DrillBuf[%d] not found in non-owning allocator[%d]",
                    drillBuf.getId(), bufferAllocator.id));

          }
        }
      }

      if (!foundOwner && !bufferMap.isEmpty()) {
        historicalLog.logHistory(logger);
        logBufferHistories(logger);
        owningAllocator.historicalLog.logHistory(logger);
        throw new IllegalStateException(
            String.format("Did not find owning allocator[%d] in bufferMap", owningAllocator.id));
      }
    }

    @Override
    public PooledByteBufAllocatorL getUnderlyingAllocator() {
      return INNER_ALLOCATOR;
    }

    @Override
    public void release(final DrillBuf drillBuf) {
      Preconditions.checkArgument(drillBuf != null, "drillBuf can't be null");

      /*
       * This is the only method on the shared ledger that can be entered without
       * having first come through an outside method on BaseAllocator (such
       * as takeOwnership() or shareOwnership()), all of which get the allocatorLock.
       * Operations in the below require the allocatorLock. We also need to synchronize
       * on this object to protect the bufferMap. In order to avoid a deadlock with other
       * methods, we have to get the allocatorLock first, as will be done in all the
       * other cases.
       */
      synchronized(ALLOCATOR_LOCK) {
        synchronized(this) {
          final Object bufferObject = bufferMap.remove(drillBuf);
          if (DEBUG) {
            if (bufferObject == null) {
              historicalLog.logHistory(logger, String.format("release(DrillBuf[%d])", drillBuf.getId()));
              drillBuf.logHistory(logger);
              throw new IllegalStateException("Buffer not found in SharedBufferLedger's buffer map");
            }
          }

          /*
           * If there are other buffers in the bufferMap that share this buffer's fate,
           * remove them, since they are also now invalid. As we examine buffers, take note
           * of any others that don't share this one's fate, but which belong to the same
           * allocator; if we find any such, then we can avoid transferring ownership at this
           * time.
           */
          final BaseAllocator bufferAllocator = (BaseAllocator) drillBuf.getAllocator();
          final List<DrillBuf> sameAllocatorSurvivors = new LinkedList<>();
          if (!bufferMap.isEmpty()) {
            /*
             * We're going to be modifying bufferMap (if we find any other related buffers);
             * in order to avoid getting a ConcurrentModificationException, we can't do it
             * on the same iteration we use to examine the buffers, so we use an intermediate
             * list to figure out which ones we have to remove.
             */
            final Set<Map.Entry<DrillBuf, BaseAllocator>> bufsToCheck = bufferMap.entrySet();
            final List<DrillBuf> sharedFateBuffers = new LinkedList<>();
            for(final Map.Entry<DrillBuf, BaseAllocator> mapEntry : bufsToCheck) {
              final DrillBuf otherBuf = mapEntry.getKey();
              if (otherBuf.hasSharedFate(drillBuf)) {
                sharedFateBuffers.add(otherBuf);
              } else {
                final BaseAllocator otherAllocator = mapEntry.getValue();
                if (otherAllocator == bufferAllocator) {
                  sameAllocatorSurvivors.add(otherBuf);
                }
              }
            }

            final int nSharedFate = sharedFateBuffers.size();
            if (nSharedFate > 0) {
              final int[] sharedIds = new int[nSharedFate];
              int index = 0;
              for(final DrillBuf bufToRemove : sharedFateBuffers) {
                sharedIds[index++] = bufToRemove.getId();
                bufferMap.remove(bufToRemove);
              }

              if (DEBUG) {
                final StringBuilder sb = new StringBuilder();
                for(final DrillBuf bufToRemove : sharedFateBuffers) {
                  sb.append(String.format("DrillBuf[%d], ", bufToRemove.getId()));
                }
                sb.setLength(sb.length() - 2); // Chop off the trailing comma and space.
                historicalLog.recordEvent("removed shared fate buffers " + sb.toString());
              }
            }
          }

          if (sameAllocatorSurvivors.isEmpty()) {
            /*
             * If that was the owning allocator, then we need to transfer ownership to
             * another allocator (any one) that is part of the sharing set.
             *
             * When we release the buffer back to the allocator, release the root buffer,
             */
            if (bufferAllocator == owningAllocator) {
              if (bufferMap.isEmpty()) {
                /*
                 * There are no other allocators available to transfer to, so
                 * release the space to the owner.
                 */
                bufferAllocator.releaseBuffer(drillBuf);
              } else {
                // Pick another allocator, and transfer ownership to that.
                final Collection<BaseAllocator> allocators = bufferMap.values();
                final Iterator<BaseAllocator> allocatorIter = allocators.iterator();
                if (!allocatorIter.hasNext()) {
                  historicalLog.logHistory(logger);
                  throw new IllegalStateException("Shared ledger buffer map is non-empty, but not iterable");
                }
                final BaseAllocator nextAllocator = allocatorIter.next();
                BaseAllocator.transferTo(nextAllocator, this, drillBuf);
                owningAllocator = nextAllocator;

                if (DEBUG) {
                  if (owningAllocator == bufferAllocator) {
                    historicalLog.logHistory(logger);
                    owningAllocator.historicalLog.logHistory(logger);
                    drillBuf.logHistory(logger);
                    throw new IllegalStateException("Shared buffer release transfer to same owner");
                  }

                  final UnsafeDirectLittleEndian udle = (UnsafeDirectLittleEndian) drillBuf.unwrap();
                  final Object oldObject = bufferAllocator.allocatedBuffers.get(udle);
                  if (oldObject != null) {
                    historicalLog.logHistory(logger);
                    bufferAllocator.historicalLog.logHistory(logger);
                    owningAllocator.historicalLog.logHistory(logger);
                    drillBuf.logHistory(logger);

                    throw new IllegalStateException("Inconsistent shared buffer release state (old owner)");
                  }

                  final Object newObject = owningAllocator.allocatedBuffers.get(udle);
                  if (newObject == null) {
                    historicalLog.logHistory(logger);
                    bufferAllocator.historicalLog.logHistory(logger);
                    owningAllocator.historicalLog.logHistory(logger);
                    drillBuf.logHistory(logger);

                    throw new IllegalStateException("Inconsistent shared buffer release state (new owner)");
                  }
                }
              }
            }
          }
        }
      }

      if (DEBUG) {
        checkBufferMap();
      }
    }

    @Override
    public BufferLedger shareWith(final Pointer<DrillBuf> pDrillBuf,
        final BufferLedger otherLedger, final BufferAllocator otherAllocator,
        final DrillBuf drillBuf, final int index, final int length, final int drillBufFlags) {
      final BaseAllocator baseAllocator = (BaseAllocator) otherAllocator;
      if (baseAllocator.isClosed) {
        throw new AllocatorClosedException(
            String.format("Attempt to use closed allocator[%d]", baseAllocator.id));
      }

      synchronized(ALLOCATOR_LOCK) {
        /*
         * This buffer is already shared, but we want to add more sharers.
         *
         * Create the new wrapper.
         */
        final DrillBuf newBuf = new DrillBuf(this, otherAllocator, drillBuf, index, length, drillBufFlags);
        if (DEBUG) {
          final UnsafeDirectLittleEndian udle = (UnsafeDirectLittleEndian) drillBuf.unwrap();
          baseAllocator.historicalLog.recordEvent("SharedBufferLedger.shareWith(..., otherAllocator[%d], "
              + "drillBuf[%d]{UnsafeDirectLittleEndian[identityHashCode == %d]}, ...)",
              baseAllocator.id, drillBuf.getId(), System.identityHashCode(udle));

          // Make sure the current ownership is still correct.
          final Object object = owningAllocator.allocatedBuffers.get(udle); // This may not be protectable w/o ALLOCATOR_LOCK.
          if (object == null) {
            newBuf.release();
            historicalLog.logHistory(logger);
            owningAllocator.historicalLog.logHistory(logger);
            drillBuf.logHistory(logger);
            throw new IllegalStateException("Buffer not found in owning allocator");
          }
        }

        addMapping(newBuf, baseAllocator);
        pDrillBuf.value = newBuf;

        if (DEBUG) {
          checkBufferMap();
        }

        return this;
      }
    }

    @Override
    public boolean transferTo(final BufferAllocator newAlloc,
        final Pointer<BufferLedger> pNewLedger, final DrillBuf drillBuf) {
      Preconditions.checkArgument(newAlloc != null, "New allocator cannot be null");
      Preconditions.checkArgument(newAlloc instanceof BaseAllocator,
          "New allocator isn't a BaseAllocator");
      Preconditions.checkArgument(pNewLedger.value != null, "Candidate new ledger can't be null");
      Preconditions.checkArgument(drillBuf != null, "DrillBuf can't be null");

      final BaseAllocator newAllocator = (BaseAllocator) newAlloc;
      if (newAllocator.isClosed) {
        throw new AllocatorClosedException(String.format(
            "Attempt to use closed allocator[%d]", newAllocator.id));
      }

      // This doesn't need the ALLOCATOR_LOCK, because it will already be held.
      synchronized(this) {
        try {
          // Modify the buffer mapping to reflect the virtual transfer.
          final BaseAllocator oldAllocator = bufferMap.put(drillBuf, newAllocator);
          if (oldAllocator == null) {
            final BaseAllocator bufAllocator = (BaseAllocator) drillBuf.getAllocator();
            historicalLog.logHistory(logger);
            bufAllocator.historicalLog.logHistory(logger);
            drillBuf.logHistory(logger);
            throw new IllegalStateException("No previous entry in SharedBufferLedger for drillBuf");
          }

          // Whatever happens, this is the new ledger.
          pNewLedger.value = this;

          /*
           * If the oldAllocator was the owner, then transfer ownership to the new allocator.
           */
          if (oldAllocator == owningAllocator) {
            owningAllocator = newAllocator;
            return BaseAllocator.transferTo(newAllocator, this, drillBuf);
          }

          // Even though we didn't do a real transfer, tell if it would have fit the limit.
          final int udleMaxCapacity = drillBuf.unwrap().maxCapacity();
          return newAllocator.allocated + udleMaxCapacity < newAllocator.maxAllocation;
        } finally {
          if (DEBUG) {
            checkBufferMap();
          }
        }
      }
    }
  }

  @Override
  public DrillBuf buffer(int size) {
    return buffer(size, size);
  }

  private static String createErrorMsg(final BufferAllocator allocator, final int size) {
    return String.format("Unable to allocate buffer of size %d due to memory limit. Current allocation: %d",
      size, allocator.getAllocatedMemory());
  }

  @Override
  public DrillBuf buffer(final int minSize, final int maxSize) {
    Preconditions.checkArgument(minSize >= 0,
        "the minimimum requested size must be non-negative");
    Preconditions.checkArgument(maxSize >= 0,
        "the maximum requested size must be non-negative");
    Preconditions.checkArgument(minSize <= maxSize,
        "the minimum requested size must be <= the maximum requested size");

    if (DEBUG) {
      injector.injectUnchecked(allocatorOwner.getExecutionControls(), CHILD_BUFFER_INJECTION_SITE);
    }

    // we can always return an empty buffer
    if (minSize == 0) {
      return getEmpty();
    }

    synchronized(ALLOCATOR_LOCK) {
      // Don't allow the allocation if it will take us over the limit.
      final long allocatedWas = allocated;
      if (!reserve(null, maxSize, 0)) {
        throw new OutOfMemoryRuntimeException(createErrorMsg(this, minSize));
      }

      final long reserved = allocated - allocatedWas;
      assert reserved == maxSize;

      final UnsafeDirectLittleEndian buffer = INNER_ALLOCATOR.directBuffer(minSize, maxSize);
      final int actualSize = buffer.maxCapacity();
      if (actualSize > maxSize) {
        final int extraSize = actualSize - maxSize;
        reserve(null, extraSize, RESERVE_F_IGNORE_MAX);
      }

      final DrillBuf wrapped = new DrillBuf(bufferLedger, this, buffer);
      buffer.release(); // Should have been retained by the DrillBuf constructor.
      assert buffer.refCnt() == 1 : "buffer was not retained by DrillBuf";
      assert allocated <= owned : "allocated more memory than owned";

      bufferAllocation += maxSize;
      if (allocated > peakAllocated) {
        peakAllocated = allocated;
      }

      if (allocatedBuffers != null) {
        allocatedBuffers.put(buffer, bufferLedger);
      }

      return wrapped;
    }
  }

  @Override
  public ByteBufAllocator getUnderlyingAllocator() {
    return INNER_ALLOCATOR;
  }

  @Override
  public BufferAllocator newChildAllocator(final AllocatorOwner allocatorOwner,
      final long initReservation, final long maxAllocation, final int flags) {
    synchronized(ALLOCATOR_LOCK) {
      final BaseAllocator childAllocator =
          new ChildAllocator(this, allocatorOwner, allocationPolicy,
              initReservation, maxAllocation, flags);

      if (DEBUG) {
        childAllocators.put(childAllocator, childAllocator);
        historicalLog.recordEvent("allocator[%d] created new child allocator[%d]",
            id, childAllocator.id);
      }

      return childAllocator;
    }
  }

  @Override
  public BufferAllocator getChildAllocator(FragmentContext fragmentContext,
      final long initialReservation, final long maximumAllocation,
      final boolean applyFragmentLimit) {
    return newChildAllocator(allocatorOwner, initialReservation, maximumAllocation,
        (applyFragmentLimit ? F_LIMITING_ROOT : 0));
  }

  /**
   * Reserve space for a DrillBuf for an ownership transfer.
   *
   * @param drillBuf the buffer to reserve space for
   */
  private void reserveForBuf(final int maxCapacity) {
    final boolean reserved = reserve(null, maxCapacity, RESERVE_F_IGNORE_MAX);
    if (DEBUG) {
      if (!reserved) {
        throw new IllegalStateException("reserveForBuf() failed");
      }
    }
  }

  @Override
  public boolean takeOwnership(final DrillBuf drillBuf) {
    // If already owned by this, there's nothing to do.
    if (this == drillBuf.getAllocator()) {
      return true;
    }

    synchronized(ALLOCATOR_LOCK) {
      return drillBuf.transferTo(this, bufferLedger);
    }
  }

  @Override
  public boolean shareOwnership(final DrillBuf drillBuf, final Pointer<DrillBuf> bufOut) {
    synchronized(ALLOCATOR_LOCK) {
      bufOut.value = drillBuf.shareWith(bufferLedger, this, 0, drillBuf.capacity());
      return allocated < maxAllocation;
    }
  }

  /*
   * It's not clear why we'd allow anyone to set their own limit, need to see why this is used;
   * this also doesn't make sense when the limits are constantly shifting, nor for other
   * allocation policies.
   */
  @Deprecated
  @Override
  public void setFragmentLimit(long fragmentLimit) {
    throw new UnsupportedOperationException("unimplemented:BaseAllocator.setFragmentLimit()");
  }

  /**
   * Get the fragment limit. This was originally meant to be the maximum amount
   * of memory the currently running fragment (which owns this allocator or
   * its ancestor) may use. Note that the value may vary up and down over time
   * as fragments come and go on the node.
   *
   * <p>This is deprecated because the concept is not entirely stable. This
   * only makes sense for one particular memory allocation policy, which is the
   * one that sets limits on what fragments on a node may use by dividing up all
   * the memory evenly between all the fragments (see {@see #POLICY_PER_FRAGMENT}).
   * Other allocation policies, such as the one that limits memory on a
   * per-query-per-node basis, wouldn't have a value for this. But we need to have
   * something until we figure out what to eplace this with because it is used by
   * some operators (such as ExternalSortBatch) to determine how much memory they
   * can use before they have to spill to disk.</p>
   *
   * @return the fragment limit
   */
  @Deprecated
  @Override
  public long getFragmentLimit() {
    return policyAgent.getMemoryLimit(this);
  }

  @Override
  public void close() {
    /*
     * Some owners may close more than once because of complex cleanup and shutdown
     * procedures.
     */
    if (isClosed) {
      return;
    }

    synchronized(ALLOCATOR_LOCK) {
      if (DEBUG) {
        verifyAllocator();

        // are there outstanding child allocators?
        if (!childAllocators.isEmpty()) {
          for(final BaseAllocator childAllocator : childAllocators.keySet()) {
            if (childAllocator.isClosed) {
              logger.debug(String.format(
                  "Closed child allocator[%d] on parent allocator[%d]'s child list",
                  childAllocator.id, id));
            }
          }

          historicalLog.logHistory(logger);
          logChildren();

          throw new IllegalStateException(
              String.format("Allocator[%d] closed with outstanding child allocators", id));
        }

        // are there outstanding buffers?
        final int allocatedCount = allocatedBuffers.size();
        if (allocatedCount > 0) {
          historicalLog.logHistory(logger);
          logBuffers();

          throw new IllegalStateException(
              String.format("Allocator[%d] closed with outstanding buffers allocated (%d)",
                  id, allocatedCount));
        }

        if (reservations.size() != 0) {
          historicalLog.logHistory(logger);
          logReservations(ReservationsLog.ALL);

          throw new IllegalStateException(
              String.format("Allocator closed with outstanding reservations (%d)", reservations.size()));
        }

        /* TODO(DRILL-2740)
        // We should be the only client holding a reference to empty now.
        final int emptyRefCnt = empty.refCnt();
        if (emptyRefCnt != 1) {
          final String msg = "empty buffer refCnt() == " + emptyRefCnt + " (!= 1)";
          final StringWriter stringWriter = new StringWriter();
          stringWriter.write(msg);
          stringWriter.write('\n');
          empty.writeState(stringWriter);
          logger.debug(stringWriter.toString());
          throw new IllegalStateException(msg);
        }
        */
      }

      // Is there unaccounted-for outstanding allocation?
      if (allocated > 0) {
        if (DEBUG) {
          historicalLog.logHistory(logger);
        }
        throw new IllegalStateException(
            String.format("Unaccounted for outstanding allocation (%d)", allocated));
      }

      // Any unclaimed reservations?
      if (preallocSpace > 0) {
        if (DEBUG) {
          historicalLog.logHistory(logger);
        }
        throw new IllegalStateException(
            String.format("Unclaimed preallocation space (%d)", preallocSpace));
      }

      /*
       * Let go of the empty buffer. If the allocator has been closed more than once,
       * this may not be necessary, so check to avoid illegal states.
       */
      final int emptyCount = empty.refCnt();
      if (emptyCount > 0) {
        empty.release(emptyCount);
      }

      DrillAutoCloseables.closeNoChecked(policyAgent);

      // Inform our parent allocator that we've closed.
      if (parentAllocator != null) {
        parentAllocator.releaseBytes(owned);
        owned = 0;
        parentAllocator.childClosed(this);
      }

      if (DEBUG) {
        historicalLog.recordEvent("closed");
        logger.debug(String.format(
            "closed allocator[%d]; getsFromParent == %d, putsToParent == %d",
            id, getsFromParent, putsToParent));
      }

      isClosed = true;
    }
  }

  /**
   * Log information about child allocators; only works if DEBUG
   */
  private void logChildren() {
    logger.debug(String.format("allocator[%d] open child allocators BEGIN", id));
    final Set<BaseAllocator> allocators = childAllocators.keySet();
    for(final BaseAllocator childAllocator : allocators) {
      childAllocator.historicalLog.logHistory(logger);
    }
    logger.debug(String.format("allocator[%d] open child allocators END", id));
  }

  private void logBuffers() {
    final StringBuilder sb = new StringBuilder();
    final Set<UnsafeDirectLittleEndian> udleSet = allocatedBuffers.keySet();

    sb.append("allocator[");
    sb.append(Integer.toString(id));
    sb.append("], ");
    sb.append(Integer.toString(udleSet.size()));
    sb.append(" allocated buffers\n");

    for(final UnsafeDirectLittleEndian udle : udleSet) {
      sb.append(udle.toString());
      sb.append("[identityHashCode == ");
      sb.append(Integer.toString(System.identityHashCode(udle)));
      sb.append("]\n");

      final Collection<DrillBuf> drillBufs = DrillBuf.unwrappedGet(udle);
      for(DrillBuf drillBuf : drillBufs) {
        drillBuf.logHistory(logger);
      }
    }

    logger.debug(sb.toString());
  }

  private enum ReservationsLog {
    ALL,
    UNUSED,
  }

  private void logReservations(final ReservationsLog reservationsLog) {
    final StringBuilder sb = new StringBuilder();
    sb.append(String.format("allocator[%d] reservations BEGIN", id));

    final Set<Reservation> reservations = this.reservations.keySet();
    for(final Reservation reservation : reservations) {
      if ((reservationsLog == ReservationsLog.ALL)
          || ((reservationsLog == ReservationsLog.UNUSED) && (!reservation.isUsed()))) {
        reservation.writeHistoryToBuilder(sb);
      }
    }

    sb.append(String.format("allocator[%d] reservations END", id));

    logger.debug(sb.toString());
  }

  @Override
  public long getAllocatedMemory() {
    return allocated;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public long getPeakMemoryAllocation() {
    return peakAllocated;
  }

  @Override
  public DrillBuf getEmpty() {
    empty.retain(1);
    // TODO(DRILL-2740) update allocatedBuffers
    return empty;
  }

  private class Reservation extends AllocationReservation {
    private final HistoricalLog historicalLog;

    public Reservation() {
      if (DEBUG) {
        historicalLog = new HistoricalLog("Reservation[allocator[%d], %d]", id, System.identityHashCode(this));
        historicalLog.recordEvent("created");
        synchronized(ALLOCATOR_LOCK) {
          reservations.put(this, this);
        }
      } else {
        historicalLog = null;
      }
    }

    @Override
    public void close() {
      if (DEBUG) {
        if (!isClosed()) {
          final Object object;
          synchronized(ALLOCATOR_LOCK) {
            object = reservations.remove(this);
          }
          if (object == null) {
            final StringBuilder sb = new StringBuilder();
            writeHistoryToBuilder(sb);

            logger.debug(sb.toString());
            throw new IllegalStateException(
                String.format("Didn't find closing reservation[%d]", System.identityHashCode(this)));
          }

          historicalLog.recordEvent("closed");
        }
      }

      super.close();
    }

    @Override
    protected boolean reserve(int nBytes) {
      final boolean reserved;
      synchronized(ALLOCATOR_LOCK) {
        reserved = BaseAllocator.this.reserve(null, nBytes, 0);
        if (reserved) {
          preallocSpace += nBytes;
        }
      }

      if (DEBUG) {
        historicalLog.recordEvent("reserve(%d) => %s", nBytes, Boolean.toString(reserved));
      }

      return reserved;
    }

    @Override
    protected DrillBuf allocate(int nBytes) {
      /*
       * The reservation already added the requested bytes to the
       * allocators owned and allocated bytes via reserve(). This
       * ensures that they can't go away. But when we ask for the buffer
       * here, that will add to the allocated bytes as well, so we need to
       * return the same number back to avoid double-counting them.
       */
      synchronized(ALLOCATOR_LOCK) {
        BaseAllocator.this.allocated -= nBytes;
        final DrillBuf drillBuf = BaseAllocator.this.buffer(nBytes);
        preallocSpace -= nBytes;

        if (DEBUG) {
          historicalLog.recordEvent("allocate() => %s",
              drillBuf == null ? "null" : String.format("DrillBuf[%d]", drillBuf.getId()));
        }

        return drillBuf;
      }
    }

    @Override
    protected void releaseReservation(int nBytes) {
      synchronized(ALLOCATOR_LOCK) {
        releaseBytes(nBytes);
        preallocSpace -= nBytes;
      }

      if (DEBUG) {
        historicalLog.recordEvent("releaseReservation(%d)", nBytes);
      }
    }

    private String getState() {
      return String.format("size == %d, isUsed == %s", getSize(), Boolean.toString(isUsed()));
    }

    private void writeToBuilder(final StringBuilder sb) {
      sb.append(String.format("reservation[%d]: ", System.identityHashCode(this)));
      sb.append(getState());
    }

    /**
     * Only works for DEBUG
     *
     * @param sb builder to write to
     */
    private void writeHistoryToBuilder(final StringBuilder sb) {
      historicalLog.buildHistory(sb, getState());
    }
  }

  @Override
  public AllocationReservation newReservation() {
    return new Reservation();
  }

  /**
   * Verifies the accounting state of the allocator. Only works for DEBUG.
   *
   * @throws IllegalStateException when any problems are found
   */
  protected void verifyAllocator() {
    final IdentityHashMap<UnsafeDirectLittleEndian, BaseAllocator> buffersSeen = new IdentityHashMap<>();
    verifyAllocator(buffersSeen);
  }

  /**
   * Verifies the accounting state of the allocator. Only works for DEBUG.
   *
   * <p>This overload is used for recursive calls, allowing for checking that DrillBufs are unique
   * across all allocators that are checked.</p>
   *
   * @param buffersSeen a map of buffers that have already been seen when walking a tree of allocators
   * @throws IllegalStateException when any problems are found
   */
  protected void verifyAllocator(
      final IdentityHashMap<UnsafeDirectLittleEndian, BaseAllocator> buffersSeen) {
    synchronized(ALLOCATOR_LOCK) {
      // verify purely local accounting
      if (allocated > owned) {
        historicalLog.logHistory(logger);
        throw new IllegalStateException("Allocator (id = " + id + ") has allocated more than it owns");
      }

      // the empty buffer should still be empty
      final long emptyCapacity = empty.maxCapacity();
      if (emptyCapacity != 0) {
        throw new IllegalStateException("empty buffer maxCapacity() == " + emptyCapacity + " (!= 0)");
      }

      // The remaining tests can only be performed if we're in debug mode.
      if (!DEBUG) {
        return;
      }

      // verify my direct descendants
      final Set<BaseAllocator> childSet = childAllocators.keySet();
      for(final BaseAllocator childAllocator : childSet) {
        childAllocator.verifyAllocator(buffersSeen);
      }

      /*
       * Verify my relationships with my descendants.
       *
       * The sum of direct child allocators' owned memory must be <= my allocated memory;
       * my allocated memory also includes DrillBuf's directly allocated by me.
       */
      long childTotal = 0;
      for(final BaseAllocator childAllocator : childSet) {
        childTotal += childAllocator.owned;
      }
      if (childTotal > allocated) {
        historicalLog.logHistory(logger);
        logger.debug("allocator[" + id + "] child event logs BEGIN");
        for(final BaseAllocator childAllocator : childSet) {
          childAllocator.historicalLog.logHistory(logger);
        }
        logger.debug("allocator[" + id + "] child event logs END");
        throw new IllegalStateException(
            "Child allocators own more memory (" + childTotal + ") than their parent (id = "
                + id + " ) has allocated (" + allocated + ')');
      }

      // Furthermore, the amount I've allocated should be that plus buffers I've allocated.
      long bufferTotal = 0;
      final Set<UnsafeDirectLittleEndian> udleSet = allocatedBuffers.keySet();
      for(final UnsafeDirectLittleEndian udle : udleSet) {
        /*
         * Even when shared, DrillBufs are rewrapped, so we should never see the same
         * instance twice.
         */
        final BaseAllocator otherOwner = buffersSeen.get(udle);
        if (otherOwner != null) {
          throw new IllegalStateException("This allocator's drillBuf already owned by another allocator");
        }
        buffersSeen.put(udle, this);

        bufferTotal += udle.maxCapacity();
      }

      // Preallocated space has to be accounted for
      final Set<Reservation> reservationSet = reservations.keySet();
      long reservedTotal = 0;
      for(final Reservation reservation : reservationSet) {
        if (!reservation.isUsed()) {
          reservedTotal += reservation.getSize();
        }
      }
      if (reservedTotal != preallocSpace) {
        logReservations(ReservationsLog.UNUSED);

        throw new IllegalStateException(
            String.format("This allocator's reservedTotal(%d) doesn't match preallocSpace (%d)",
                reservedTotal, preallocSpace));
      }

      if (bufferTotal + reservedTotal + childTotal != allocated) {
        final StringBuilder sb = new StringBuilder();
        sb.append("allocator[");
        sb.append(Integer.toString(id));
        sb.append("]\nallocated: ");
        sb.append(Long.toString(allocated));
        sb.append(" allocated - (bufferTotal + reservedTotal + childTotal): ");
        sb.append(Long.toString(allocated - (bufferTotal + reservedTotal + childTotal)));
        sb.append('\n');

        if (bufferTotal != 0) {
          sb.append("buffer total: ");
          sb.append(Long.toString(bufferTotal));
          sb.append('\n');
          dumpBuffers(sb, udleSet);
        }

        if (childTotal != 0) {
          sb.append("child total: ");
          sb.append(Long.toString(childTotal));
          sb.append('\n');

          for(final BaseAllocator childAllocator : childSet) {
            sb.append("child allocator[");
            sb.append(Integer.toString(childAllocator.id));
            sb.append("] owned ");
            sb.append(Long.toString(childAllocator.owned));
            sb.append('\n');
          }
        }

        if (reservedTotal != 0) {
          sb.append(String.format("reserved total : ", reservedTotal));
          for(final Reservation reservation : reservationSet) {
            reservation.writeToBuilder(sb);
            sb.append('\n');
          }
        }

        logger.debug(sb.toString());
        throw new IllegalStateException(String.format(
            "allocator[%d]: buffer space (%d) + prealloc space (%d) + child space (%d) != allocated (%d)",
            id, bufferTotal, reservedTotal, childTotal, allocated));
      }
    }
  }

  private void dumpBuffers(final StringBuilder sb, final Set<UnsafeDirectLittleEndian> udleSet) {
    for(final UnsafeDirectLittleEndian udle : udleSet) {
      sb.append("UnsafeDirectLittleEndian[dentityHashCode == ");
      sb.append(Integer.toString(System.identityHashCode(udle)));
      sb.append("] size ");
      sb.append(Integer.toString(udle.maxCapacity()));
      sb.append('\n');
    }
  }

  public static boolean isDebug() {
    return DEBUG;
  }
}
