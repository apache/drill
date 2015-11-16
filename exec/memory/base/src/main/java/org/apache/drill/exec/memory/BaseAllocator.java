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
import io.netty.buffer.UnsafeDirectLittleEndian;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.common.HistoricalLog;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.memory.AllocatorManager.BufferLedger;
import org.apache.drill.exec.ops.BufferManager;
import org.apache.drill.exec.util.AssertionUtil;

import com.google.common.base.Preconditions;

public abstract class BaseAllocator extends Accountant implements BufferAllocator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseAllocator.class);

  public static final String DEBUG_ALLOCATOR = "drill.memory.debug.allocator";

  private static final AtomicLong ID_GENERATOR = new AtomicLong(0);
  private static final int CHUNK_SIZE = AllocatorManager.INNER_ALLOCATOR.getChunkSize();

  static final boolean DEBUG = AssertionUtil.isAssertionsEnabled()
      || Boolean.parseBoolean(System.getProperty(DEBUG_ALLOCATOR, "false"));
  private final Object DEBUG_LOCK = DEBUG ? new Object() : null;

  private final BaseAllocator parentAllocator;
  private final ByteBufAllocator thisAsByteBufAllocator;
  private final IdentityHashMap<BaseAllocator, Object> childAllocators;
  private final DrillBuf empty;

  private volatile boolean isClosed = false; // the allocator has been closed

  // Package exposed for sharing between AllocatorManger and BaseAllocator objects
  final long id = ID_GENERATOR.incrementAndGet(); // unique ID assigned to each allocator
  final String name;
  final RootAllocator root;

  // members used purely for debugging
  private final IdentityHashMap<BufferLedger, Object> childLedgers;
  private final IdentityHashMap<Reservation, Object> reservations;
  private final HistoricalLog historicalLog;

  protected BaseAllocator(
      final BaseAllocator parentAllocator,
      final String name,
      final long initReservation,
      final long maxAllocation) throws OutOfMemoryException {
    super(parentAllocator, initReservation, maxAllocation);

    if (parentAllocator != null) {
      this.root = parentAllocator.root;
      empty = parentAllocator.empty;
    } else if (this instanceof RootAllocator) {
      this.root = (RootAllocator) this;
      empty = createEmpty();
    } else {
      throw new IllegalStateException("An parent allocator must either carry a root or be the root.");
    }

    this.parentAllocator = parentAllocator;
    this.name = name;

    // TODO: DRILL-4131
    // this.thisAsByteBufAllocator = new DrillByteBufAllocator(this);
    this.thisAsByteBufAllocator = AllocatorManager.INNER_ALLOCATOR.allocator;

    if (DEBUG) {
      childAllocators = new IdentityHashMap<>();
      reservations = new IdentityHashMap<>();
      childLedgers = new IdentityHashMap<>();
      historicalLog = new HistoricalLog(4, "allocator[%d]", id);
      hist("created by \"%s\", owned = %d", name.toString(), this.getAllocatedMemory());
    } else {
      childAllocators = null;
      reservations = null;
      historicalLog = null;
      childLedgers = null;
    }

  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DrillBuf getEmpty() {
    return empty;
  }

  /**
   * For debug/verification purposes only. Allows an AllocatorManager to tell the allocator that we have a new ledger
   * associated with this allocator.
   */
  void associateLedger(BufferLedger ledger) {
    if (DEBUG) {
      synchronized (DEBUG_LOCK) {
        childLedgers.put(ledger, null);
      }
    }
  }

  /**
   * For debug/verification purposes only. Allows an AllocatorManager to tell the allocator that we are removing a
   * ledger associated with this allocator
   */
  void dissociateLedger(BufferLedger ledger) {
    if (DEBUG) {
      synchronized (DEBUG_LOCK) {
        if (!childLedgers.containsKey(ledger)) {
          throw new IllegalStateException("Trying to remove a child ledger that doesn't exist.");
        }
        childLedgers.remove(ledger);
      }
    }
  }

  /**
   * Track when a ChildAllocator of this BaseAllocator is closed. Used for debugging purposes.
   *
   * @param childAllocator
   *          The child allocator that has been closed.
   */
  private void childClosed(final BaseAllocator childAllocator) {
    if (DEBUG) {
      Preconditions.checkArgument(childAllocator != null, "child allocator can't be null");

      synchronized (DEBUG_LOCK) {
        final Object object = childAllocators.remove(childAllocator);
        if (object == null) {
          childAllocator.historicalLog.logHistory(logger);
          throw new IllegalStateException("Child allocator[" + childAllocator.id
              + "] not found in parent allocator[" + id + "]'s childAllocators");
        }
      }
    }
  }

  private static String createErrorMsg(final BufferAllocator allocator, final int rounded, final int requested) {
    if (rounded != requested) {
      return String.format(
          "Unable to allocate buffer of size %d (rounded from %d) due to memory limit. Current allocation: %d",
          rounded, requested, allocator.getAllocatedMemory());
    } else {
      return String.format("Unable to allocate buffer of size %d due to memory limit. Current allocation: %d",
          rounded, allocator.getAllocatedMemory());
    }
  }

  @Override
  public DrillBuf buffer(final int initialRequestSize) {
    return buffer(initialRequestSize, null);
  }

  private DrillBuf createEmpty(){
    return new DrillBuf(new AtomicInteger(), null, AllocatorManager.INNER_ALLOCATOR.empty, null, null, 0, 0, true);
  }

  @Override
  public DrillBuf buffer(final int initialRequestSize, BufferManager manager) {

    Preconditions.checkArgument(initialRequestSize >= 0, "the minimimum requested size must be non-negative");
    Preconditions.checkArgument(initialRequestSize >= 0, "the maximum requested size must be non-negative");

    if (initialRequestSize == 0) {
      return empty;
    }

    // round to next largest power of two if we're within a chunk since that is how our allocator operates
    final int actualRequestSize = initialRequestSize < CHUNK_SIZE ?
        nextPowerOfTwo(initialRequestSize)
        : initialRequestSize;
    AllocationOutcome outcome = this.allocateBytes(actualRequestSize);
    if (!outcome.isOk()) {
      throw new OutOfMemoryException(createErrorMsg(this, actualRequestSize, initialRequestSize));
    }

    boolean success = true;
    try {
      DrillBuf buffer = bufferWithoutReservation(actualRequestSize, manager);
      success = true;
      return buffer;
    } finally {
      if (!success) {
        releaseBytes(actualRequestSize);
      }
    }

  }

  /**
   * Used by usual allocation as well as for allocating a pre-reserved buffer. Skips the typical accounting associated
   * with creating a new buffer.
   */
  private DrillBuf bufferWithoutReservation(final int size, BufferManager bufferManager) throws OutOfMemoryException {
    AllocatorManager manager = new AllocatorManager(this, size);
    BufferLedger ledger = manager.associate(this);
    DrillBuf buffer = ledger.newDrillBuf(0, size, bufferManager);

    // make sure that our allocation is equal to what we expected.
    Preconditions.checkArgument(buffer.capacity() == size,
        "Allocated capacity %d was not equal to requested capacity %d.", buffer.capacity(), size);

    return buffer;
  }

  @Override
  public ByteBufAllocator getAsByteBufAllocator() {
    return thisAsByteBufAllocator;
  }

  /**
   * Return a unique Id for an allocator. Id's may be recycled after a long period of time.
   *
   * <p>
   * Primary use for this is for debugging output.
   * </p>
   *
   * @return the allocator's id
   */
  long getId() {
    return id;
  }

  @Override
  public BufferAllocator newChildAllocator(
      final String name,
      final long initReservation,
      final long maxAllocation) {
    final ChildAllocator childAllocator = new ChildAllocator(this, name, initReservation, maxAllocation);

    if (DEBUG) {
      childAllocators.put(childAllocator, childAllocator);
      historicalLog.recordEvent("allocator[%d] created new child allocator[%d]",
          id, childAllocator.id);
    }

    return childAllocator;
  }

  private class Reservation extends AllocationReservation {
    private final HistoricalLog historicalLog;

    public Reservation() {
      if (DEBUG) {
        historicalLog = new HistoricalLog("Reservation[allocator[%d], %d]", id, System.identityHashCode(this));
        historicalLog.recordEvent("created");
        synchronized (DEBUG_LOCK) {
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
          synchronized (DEBUG_LOCK) {
            object = reservations.remove(this);
          }
          if (object == null) {
            final StringBuilder sb = new StringBuilder();
            print(sb, 0, Verbosity.LOG_WITH_STACKTRACE);
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
      final AllocationOutcome outcome = BaseAllocator.this.allocateBytes(nBytes);

      if (DEBUG) {
        historicalLog.recordEvent("reserve(%d) => %s", nBytes, Boolean.toString(outcome.isOk()));
      }

      return outcome.isOk();
    }

    @Override
    protected DrillBuf allocate(int nBytes) {
      boolean success = false;

      /*
       * The reservation already added the requested bytes to the allocators owned and allocated bytes via reserve().
       * This ensures that they can't go away. But when we ask for the buffer here, that will add to the allocated bytes
       * as well, so we need to return the same number back to avoid double-counting them.
       */
      try {
        final DrillBuf drillBuf = BaseAllocator.this.bufferWithoutReservation(nBytes, null);

        if (DEBUG) {
          historicalLog.recordEvent("allocate() => %s",
              drillBuf == null ? "null" : String.format("DrillBuf[%d]", drillBuf.getId()));
        }
        success = true;
        return drillBuf;
      } finally {
        if (!success) {
          releaseBytes(nBytes);
        }
      }
    }

    @Override
    protected void releaseReservation(int nBytes) {
      releaseBytes(nBytes);

      if (DEBUG) {
        historicalLog.recordEvent("releaseReservation(%d)", nBytes);
      }
    }

  }

  @Override
  public AllocationReservation newReservation() {
    return new Reservation();
  }


  @Override
  public synchronized void close() {
    /*
     * Some owners may close more than once because of complex cleanup and shutdown
     * procedures.
     */
    if (isClosed) {
      return;
    }

    if (DEBUG) {
      synchronized(DEBUG_LOCK) {
        verifyAllocator();

        // are there outstanding child allocators?
        if (!childAllocators.isEmpty()) {
          for (final BaseAllocator childAllocator : childAllocators.keySet()) {
            if (childAllocator.isClosed) {
              logger.warn(String.format(
                  "Closed child allocator[%s] on parent allocator[%s]'s child list.\n%s",
                  childAllocator.name, name, toString()));
            }
          }

          throw new IllegalStateException(
              String.format("Allocator[%s] closed with outstanding child allocators.\n%s", name, toString()));
        }

        // are there outstanding buffers?
        final int allocatedCount = childLedgers.size();
        if (allocatedCount > 0) {
          throw new IllegalStateException(
              String.format("Allocator[%s] closed with outstanding buffers allocated (%d).\n%s",
                  name, allocatedCount, toString()));
        }

        if (reservations.size() != 0) {
          throw new IllegalStateException(
              String.format("Allocator[%s] closed with outstanding reservations (%d).\n%s", name, reservations.size(),
                  toString()));
        }

      }
    }

      // Is there unaccounted-for outstanding allocation?
      final long allocated = getAllocatedMemory();
      if (allocated > 0) {
        throw new IllegalStateException(
          String.format("Unaccounted for outstanding allocation (%d)\n%s", allocated, toString()));
      }

    // we need to release our memory to our parent before we tell it we've closed.
    super.close();

    // Inform our parent allocator that we've closed
    if (parentAllocator != null) {
      parentAllocator.childClosed(this);
    }

    if (DEBUG) {
      historicalLog.recordEvent("closed");
      logger.debug(String.format(
          "closed allocator[%s].",
          name));
    }

    isClosed = true;


  }

  public String toString() {
    final Verbosity verbosity = logger.isTraceEnabled() || true ? Verbosity.LOG_WITH_STACKTRACE : Verbosity.BASIC;
    final StringBuilder sb = new StringBuilder();
    print(sb, 0, verbosity);
    return sb.toString();
  }

  /**
   * Provide a verbose string of the current allocator state. Includes the state of all child allocators, along with
   * historical logs of each object and including stacktraces.
   *
   * @return A Verbose string of current allocator state.
   */
  public String toVerboseString() {
    final StringBuilder sb = new StringBuilder();
    print(sb, 0, Verbosity.LOG_WITH_STACKTRACE);
    return sb.toString();
  }

  private void hist(String noteFormat, Object... args) {
    historicalLog.recordEvent(noteFormat, args);
  }

  /**
   * Rounds up the provided value to the nearest power of two.
   *
   * @param val
   *          An integer value.
   * @return The closest power of two of that value.
   */
  static int nextPowerOfTwo(int val) {
    int highestBit = Integer.highestOneBit(val);
    if (highestBit == val) {
      return val;
    } else {
      return highestBit << 1;
    }
  }


  /**
   * Verifies the accounting state of the allocator. Only works for DEBUG.
   *
   * @throws IllegalStateException
   *           when any problems are found
   */
  void verifyAllocator() {
    final IdentityHashMap<UnsafeDirectLittleEndian, BaseAllocator> buffersSeen = new IdentityHashMap<>();
    verifyAllocator(buffersSeen);
  }

  /**
   * Verifies the accounting state of the allocator. Only works for DEBUG.
   *
   * <p>
   * This overload is used for recursive calls, allowing for checking that DrillBufs are unique across all allocators
   * that are checked.
   * </p>
   *
   * @param buffersSeen
   *          a map of buffers that have already been seen when walking a tree of allocators
   * @throws IllegalStateException
   *           when any problems are found
   */
  private void verifyAllocator(final IdentityHashMap<UnsafeDirectLittleEndian, BaseAllocator> buffersSeen) {
    synchronized (DEBUG_LOCK) {

      // The remaining tests can only be performed if we're in debug mode.
      if (!DEBUG) {
        return;
      }

      final long allocated = getAllocatedMemory();

      // verify my direct descendants
      final Set<BaseAllocator> childSet = childAllocators.keySet();
      for (final BaseAllocator childAllocator : childSet) {
        childAllocator.verifyAllocator(buffersSeen);
      }

      /*
       * Verify my relationships with my descendants.
       *
       * The sum of direct child allocators' owned memory must be <= my allocated memory; my allocated memory also
       * includes DrillBuf's directly allocated by me.
       */
      long childTotal = 0;
      for (final BaseAllocator childAllocator : childSet) {
        childTotal += Math.max(childAllocator.getAllocatedMemory(), childAllocator.reservation);
      }
      if (childTotal > getAllocatedMemory()) {
        historicalLog.logHistory(logger);
        logger.debug("allocator[" + id + "] child event logs BEGIN");
        for (final BaseAllocator childAllocator : childSet) {
          childAllocator.historicalLog.logHistory(logger);
        }
        logger.debug("allocator[" + id + "] child event logs END");
        throw new IllegalStateException(
            "Child allocators own more memory (" + childTotal + ") than their parent (id = "
                + id + " ) has allocated (" + getAllocatedMemory() + ')');
      }

      // Furthermore, the amount I've allocated should be that plus buffers I've allocated.
      long bufferTotal = 0;

      final Set<BufferLedger> ledgerSet = childLedgers.keySet();
      for (final BufferLedger ledger : ledgerSet) {
        if (!ledger.isOwningLedger()) {
          continue;
        }

        final UnsafeDirectLittleEndian udle = ledger.getUnderlying();
        /*
         * Even when shared, DrillBufs are rewrapped, so we should never see the same instance twice.
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
      for (final Reservation reservation : reservationSet) {
        if (!reservation.isUsed()) {
          reservedTotal += reservation.getSize();
        }
      }

      if (bufferTotal + reservedTotal + childTotal != getAllocatedMemory()) {
        final StringBuilder sb = new StringBuilder();
        sb.append("allocator[");
        sb.append(Long.toString(id));
        sb.append("]\nallocated: ");
        sb.append(Long.toString(allocated));
        sb.append(" allocated - (bufferTotal + reservedTotal + childTotal): ");
        sb.append(Long.toString(allocated - (bufferTotal + reservedTotal + childTotal)));
        sb.append('\n');

        if (bufferTotal != 0) {
          sb.append("buffer total: ");
          sb.append(Long.toString(bufferTotal));
          sb.append('\n');
          dumpBuffers(sb, ledgerSet);
        }

        if (childTotal != 0) {
          sb.append("child total: ");
          sb.append(Long.toString(childTotal));
          sb.append('\n');

          for (final BaseAllocator childAllocator : childSet) {
            sb.append("child allocator[");
            sb.append(Long.toString(childAllocator.id));
            sb.append("] owned ");
            sb.append(Long.toString(childAllocator.getAllocatedMemory()));
            sb.append('\n');
          }
        }

        if (reservedTotal != 0) {
          sb.append(String.format("reserved total : ", reservedTotal));
          for (final Reservation reservation : reservationSet) {
            reservation.historicalLog.buildHistory(sb, 0, true);
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

  void print(StringBuilder sb, int level, Verbosity verbosity) {

    indent(sb, level)
        .append("Allocator(")
        .append(name)
        .append(") ")
        .append(reservation)
        .append('/')
        .append(getAllocatedMemory())
        .append('/')
        .append(getPeakMemoryAllocation())
        .append('/')
        .append(getLimit())
        .append(" (res/actual/peak/limit)")
        .append('\n');

    indent(sb, level + 1).append(String.format("child allocators: %d\n", childAllocators.size()));
    for (BaseAllocator child : childAllocators.keySet()) {
      child.print(sb, level + 2, verbosity);
    }

    if (DEBUG) {
      indent(sb, level + 1).append(String.format("ledgers: %d\n", childLedgers.size()));
      for (BufferLedger ledger : childLedgers.keySet()) {
        ledger.print(sb, level + 2, verbosity);
      }

      final Set<Reservation> reservations = this.reservations.keySet();
      indent(sb, level + 1).append(String.format("reservations: %d\n", reservations.size()));
      for (final Reservation reservation : reservations) {
        if (verbosity.includeHistoricalLog) {
          reservation.historicalLog.buildHistory(sb, level + 3, true);
        }
      }

    }

  }

  private void dumpBuffers(final StringBuilder sb, final Set<BufferLedger> ledgerSet) {
    for (final BufferLedger ledger : ledgerSet) {
      if (!ledger.isOwningLedger()) {
        continue;
      }
      final UnsafeDirectLittleEndian udle = ledger.getUnderlying();
      sb.append("UnsafeDirectLittleEndian[dentityHashCode == ");
      sb.append(Integer.toString(System.identityHashCode(udle)));
      sb.append("] size ");
      sb.append(Integer.toString(udle.maxCapacity()));
      sb.append('\n');
    }
  }


  static StringBuilder indent(StringBuilder sb, int indent) {
    final char[] indentation = new char[indent * 2];
    Arrays.fill(indentation, ' ');
    sb.append(indentation);
    return sb;
  }

  public static enum Verbosity {
    BASIC(false, false), // only include basic information
    LOG(true, false), // include basic
    LOG_WITH_STACKTRACE(true, true) //
    ;

    public final boolean includeHistoricalLog;
    public final boolean includeStackTraces;

    Verbosity(boolean includeHistoricalLog, boolean includeStackTraces) {
      this.includeHistoricalLog = includeHistoricalLog;
      this.includeStackTraces = includeStackTraces;
    }
  }

  public static boolean isDebug() {
    return DEBUG;
  }
}
