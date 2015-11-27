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

import static org.apache.drill.exec.memory.BaseAllocator.indent;
import io.netty.buffer.DrillBuf;
import io.netty.buffer.PooledByteBufAllocatorL;
import io.netty.buffer.UnsafeDirectLittleEndian;

import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.drill.common.HistoricalLog;
import org.apache.drill.exec.memory.BaseAllocator.Verbosity;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.ops.BufferManager;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.google.common.base.Preconditions;

/**
 * Manages the relationship between one or more allocators and a particular UDLE. Ensures that one allocator owns the
 * memory that multiple allocators may be referencing. Manages a BufferLedger between each of its associated allocators.
 * This class is also responsible for managing when memory is allocated and returned to the Netty-based
 * PooledByteBufAllocatorL.
 *
 * The only reason that this isn't package private is we're forced to put DrillBuf in Netty's package which need access
 * to these objects or methods.
 *
 * Threading: AllocatorManager manages thread-safety internally. Operations within the context of a single BufferLedger
 * are lockless in nature and can be leveraged by multiple threads. Operations that cross the context of two ledgers
 * will acquire a lock on the AllocatorManager instance. Important note, there is one AllocatorManager per
 * UnsafeDirectLittleEndian buffer allocation. As such, there will be thousands of these in a typical query. The
 * contention of acquiring a lock on AllocatorManager should be very low.
 *
 */
public class AllocatorManager {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AllocatorManager.class);

  private static final AtomicLong LEDGER_ID_GENERATOR = new AtomicLong(0);
  static final PooledByteBufAllocatorL INNER_ALLOCATOR = new PooledByteBufAllocatorL(DrillMetrics.getInstance());

  private final RootAllocator root;
  private volatile BufferLedger owningLedger;
  private final int size;
  private final UnsafeDirectLittleEndian underlying;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final LongObjectOpenHashMap<BufferLedger> map = new LongObjectOpenHashMap<>();
  private final AutoCloseableLock readLock = new AutoCloseableLock(lock.readLock());
  private final AutoCloseableLock writeLock = new AutoCloseableLock(lock.writeLock());
  private final IdentityHashMap<DrillBuf, Object> buffers =
      BaseAllocator.DEBUG ? new IdentityHashMap<DrillBuf, Object>() : null;

  AllocatorManager(BaseAllocator accountingAllocator, int size) {
    Preconditions.checkNotNull(accountingAllocator);
    this.root = accountingAllocator.root;
    this.underlying = INNER_ALLOCATOR.allocate(size);
    this.owningLedger = associate(accountingAllocator);
    this.size = underlying.capacity();
  }

  /**
   * Associate the existing underlying buffer with a new allocator.
   *
   * @param allocator
   *          The target allocator to associate this buffer with.
   * @return The Ledger (new or existing) that associates the underlying buffer to this new ledger.
   */
  public BufferLedger associate(BaseAllocator allocator) {
    if (root != allocator.root) {
      throw new IllegalStateException(
          "A buffer can only be associated between two allocators that share the same root.");
    }

    final long allocatorId = allocator.getId();
    try (AutoCloseableLock read = readLock.open()) {

      final BufferLedger ledger = map.get(allocatorId);
      if (ledger != null) {
        return ledger;
      }

    }
    try (AutoCloseableLock write = writeLock.open()) {
      final BufferLedger ledger = new BufferLedger(allocator, new ReleaseListener(allocatorId));
      map.put(allocatorId, ledger);
      allocator.associateLedger(ledger);
      return ledger;
    }
  }


  /**
   * The way that a particular BufferLedger communicates back to the AllocatorManager that it now longer needs to hold a
   * reference to particular piece of memory.
   */
  private class ReleaseListener {

    private final long allocatorId;

    public ReleaseListener(long allocatorId) {
      this.allocatorId = allocatorId;
    }

    public void release() {
      try (AutoCloseableLock write = writeLock.open()) {
        final BufferLedger oldLedger = map.remove(allocatorId);
        oldLedger.allocator.dissociateLedger(oldLedger);

        if (oldLedger == owningLedger) {
          if (map.isEmpty()) {
            // no one else owns, lets release.
            oldLedger.allocator.releaseBytes(size);
            underlying.release();
          } else {
            // we need to change the owning allocator. we've been removed so we'll get whatever is top of list
            BufferLedger newLedger = map.iterator().next().value;

            // we'll forcefully transfer the ownership and not worry about whether we exceeded the limit
            // since this consumer can do anything with this.
            oldLedger.transferBalance(newLedger);
            owningLedger = newLedger;
          }
        }


      }
    }
  }

  /**
   * Simple wrapper class that allows Locks to be released via an try-with-resources block.
   */
  private class AutoCloseableLock implements AutoCloseable {

    private final Lock lock;

    public AutoCloseableLock(Lock lock) {
      this.lock = lock;
    }

    public AutoCloseableLock open() {
      lock.lock();
      return this;
    }

    @Override
    public void close() {
      lock.unlock();
    }

  }

  /**
   * The reference manager that binds an allocator manager to a particular BaseAllocator. Also responsible for creating
   * a set of DrillBufs that share a common fate and set of reference counts.
   *
   * As with AllocatorManager, the only reason this is public is due to DrillBuf being in io.netty.buffer package.
   */
  public class BufferLedger {
    private final long id = LEDGER_ID_GENERATOR.incrementAndGet(); // unique ID assigned to each ledger
    private final AtomicInteger bufRefCnt = new AtomicInteger(0); // start at zero so we can manage request for retain
                                                                  // correctly
    private final BaseAllocator allocator;
    private final ReleaseListener listener;
    private final HistoricalLog historicalLog = BaseAllocator.DEBUG ? new HistoricalLog(BaseAllocator.DEBUG_LOG_LENGTH,
        "BufferLedger[%d]", 1)
        : null;

    private BufferLedger(BaseAllocator allocator, ReleaseListener listener) {
      this.allocator = allocator;
      this.listener = listener;
    }

    /**
     * Transfer any balance the current ledger has to the target ledger. In the case that the current ledger holds no
     * memory, no transfer is made to the new ledger.
     *
     * @param target
     *          The ledger to transfer ownership account to.
     * @return Whether transfer fit within target ledgers limits.
     */
    public boolean transferBalance(BufferLedger target) {
      Preconditions.checkNotNull(target);
      Preconditions.checkArgument(allocator.root == target.allocator.root,
          "You can only transfer between two allocators that share the same root.");

      // since two balance transfers out from the allocator manager could cause incorrect accounting, we need to ensure
      // that this won't happen by synchronizing on the allocator manager instance.
      synchronized (AllocatorManager.this) {
        if (this != owningLedger || target == this) {
          return true;
        }

        if (BaseAllocator.DEBUG) {
          this.historicalLog.recordEvent("transferBalance(%s)", target.allocator.name);
          target.historicalLog.recordEvent("incoming(from %s)", owningLedger.allocator.name);
        }

        boolean overlimit = target.allocator.forceAllocate(size);
        allocator.releaseBytes(size);
        owningLedger = target;
        return overlimit;
      }

    }

    /**
     * Print the current ledger state to a the provided StringBuilder.
     *
     * @param sb
     *          The StringBuilder to populate.
     * @param indent
     *          The level of indentation to position the data.
     * @param verbosity
     *          The level of verbosity to print.
     */
    public void print(StringBuilder sb, int indent, Verbosity verbosity) {
      indent(sb, indent)
          .append("ledger (allocator: ")
          .append(allocator.name)
          .append("), isOwning: ")
          .append(owningLedger == this)
          .append(", size: ")
          .append(size)
          .append(", references: ")
          .append(bufRefCnt.get())
          .append('\n');

      if (BaseAllocator.DEBUG) {
        // This doesn't seem as useful as the individual buffer logs below. Removing from default presentation.
        // if (verbosity.includeHistoricalLog) {
        // historicalLog.buildHistory(sb, indent + 2, verbosity.includeStackTraces);
        // }
        synchronized (buffers) {
          indent(sb, indent + 1).append("BufferLedger[" + id + "] holds ").append(buffers.size())
              .append(" buffers. \n");
          for (DrillBuf buf : buffers.keySet()) {
            buf.print(sb, indent + 2, verbosity);
          }
        }
      }

    }

    /**
     * Release this ledger. This means that all reference counts associated with this ledger are no longer used. This
     * will inform the AllocatorManager to make a decision about how to manage any memory owned by this particular
     * BufferLedger
     */
    public void release() {
      listener.release();
    }

    /**
     * Returns the ledger associated with a particular BufferAllocator. If the BufferAllocator doesn't currently have a
     * ledger associated with this AllocatorManager, a new one is created. This is placed on BufferLedger rather than
     * AllocatorManager direclty because DrillBufs don't have access to AllocatorManager and they are the ones
     * responsible for exposing the ability to associate mutliple allocators with a particular piece of underlying
     * memory.
     *
     * @param allocator
     * @return
     */
    public BufferLedger getLedgerForAllocator(BufferAllocator allocator) {
      return associate((BaseAllocator) allocator);
    }

    /**
     * Create a new DrillBuf associated with this AllocatorManager and memory. Does not impact reference count.
     * Typically used for slicing.
     * @param offset
     *          The offset in bytes to start this new DrillBuf.
     * @param length
     *          The length in bytes that this DrillBuf will provide access to.
     * @return A new DrillBuf that shares references with all DrillBufs associated with this BufferLedger
     */
    public DrillBuf newDrillBuf(int offset, int length) {
      return newDrillBuf(offset, length, null, false);
    }

    /**
     * Create a new DrillBuf associated with this AllocatorManager and memory.
     * @param offset
     *          The offset in bytes to start this new DrillBuf.
     * @param length
     *          The length in bytes that this DrillBuf will provide access to.
     * @param manager
     *          An optional BufferManager argument that can be used to manage expansion of this DrillBuf
     * @param retain
     *          Whether or not the newly created buffer should get an additional reference count added to it.
     * @return A new DrillBuf that shares references with all DrillBufs associated with this BufferLedger
     * @return
     */
    public DrillBuf newDrillBuf(int offset, int length, BufferManager manager, boolean retain) {
      final DrillBuf buf = new DrillBuf(
          bufRefCnt,
          this,
          underlying,
          manager,
          allocator.getAsByteBufAllocator(),
          offset,
          length,
          false);

      if (retain) {
        buf.retain();
      }

      if (BaseAllocator.DEBUG) {
        historicalLog.recordEvent(
            "DrillBuf(BufferLedger, BufferAllocator[%d], UnsafeDirectLittleEndian[identityHashCode == "
                + "%d](%s)) => ledger hc == %d",
            allocator.getId(), System.identityHashCode(buf), buf.toString(),
            System.identityHashCode(this));

        synchronized (buffers) {
          buffers.put(buf, null);
        }
      }

      return buf;

    }

    /**
     * What is the total size (in bytes) of memory underlying this ledger.
     *
     * @return Size in bytes
     */
    public int getSize() {
      return size;
    }

    /**
     * How much memory is accounted for by this ledger. This is either getSize() if this is the owning ledger for the
     * memory or zero in the case that this is not the owning ledger associated with this memory.
     *
     * @return Amount of accounted(owned) memory associated with this ledger.
     */
    public int getAccountedSize() {
      try (AutoCloseableLock read = readLock.open()) {
        if (owningLedger == this) {
          return size;
        } else {
          return 0;
        }
      }
    }

    /**
     * Package visible for debugging/verification only.
     */
    UnsafeDirectLittleEndian getUnderlying() {
      return underlying;
    }

    /**
     * Package visible for debugging/verification only.
     */
    boolean isOwningLedger() {
      return this == owningLedger;
    }

  }

}