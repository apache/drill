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

import com.google.common.base.Preconditions;

/**
 * BaseAllocator wrapper that locks methods for DEBUG.
 *
 * <p>Most methods just acquire the ALLOCATOR_LOCK and call super; a few
 * do more extensive checking.</p>
 */
class BaseAllocatorDebug extends BaseAllocator {
  @Override
  public DrillBuf buffer(final int minSize, final int maxSize) {
    Preconditions.checkArgument(minSize >= 0,
        "the minimimum requested size must be non-negative");
    Preconditions.checkArgument(maxSize >= 0,
        "the maximum requested size must be non-negative");
    Preconditions.checkArgument(minSize <= maxSize,
        "the minimum requested size must be <= the maximum requested size");

    synchronized(DEBUG_LOCK) {
      verifyParentOrThis(); // TODO(cwestin) remove
      final DrillBuf drillBuf = super.buffer(minSize, maxSize);
      verifyParentOrThis(); // TODO(cwestin) remove
      return drillBuf;
    }
  }

  @Override
  public BufferAllocator newChildAllocator(final AllocatorOwner allocatorOwner,
      final long initReservation, final long maxAllocation, final int flags) {
    synchronized(DEBUG_LOCK) {
      verifyParentOrThis(); // TODO(cwestin) remove
      return super.newChildAllocator(allocatorOwner, initReservation, maxAllocation, flags);
    }
  }

  @Override
  public boolean takeOwnership(final DrillBuf drillBuf) {
    synchronized(DEBUG_LOCK) {
      verifyParentOrThis(); // TODO(cwestin) remove
      final boolean withinLimit = super.takeOwnership(drillBuf);
      verifyParentOrThis(); // TODO(cwestin) remove
      return withinLimit;
    }
  }

  @Override
  public boolean shareOwnership(final DrillBuf drillBuf, final Pointer<DrillBuf> bufOut) {
    synchronized(DEBUG_LOCK) {
      verifyParentOrThis(); // TODO(cwestin) remove
      final boolean withinLimit = super.shareOwnership(drillBuf, bufOut);
      verifyParentOrThis(); // TODO(cwestin) remove
      return withinLimit;
    }
  }

  @Override
  public void close() throws Exception {
    synchronized(DEBUG_LOCK) {
      verifyParentOrThis(); // TODO(cwestin) remove
      super.close();
      verifyParentOrThis(); // TODO(cwestin) remove
    }
  }

  @Override
  public AllocationReservation newReservation() {
    synchronized(DEBUG_LOCK) {
      verifyParentOrThis(); // TODO(cwestin) remove
      final AllocationReservation reservation = new ReservationDebug();
      verifyParentOrThis(); // TODO(cwestin) remove
      return reservation;
    }
  }

  protected BaseAllocatorDebug(BaseAllocator parentAllocator,
      AllocatorOwner allocatorOwner, AllocationPolicy allocationPolicy,
      long initReservation, long maxAllocation, int flags)
      throws OutOfMemoryRuntimeException {
      super(parentAllocator, allocatorOwner, allocationPolicy, initReservation,
          maxAllocation, flags);

    assert DEBUG : "Not in allocator DEBUG mode";

    synchronized(DEBUG_LOCK) {
      if (parentAllocator != null) {
        parentAllocator.childAllocators.put(this, this);
        parentAllocator.verifyAllocator();
      }
    }
  }

  @Override
  protected boolean transferTo(final BaseAllocator newAllocator,
      final BufferLedger newLedger, final DrillBuf drillBuf) {
    synchronized(DEBUG_LOCK) {
      verifyParentOrThis();
      final boolean withinLimit = super.transferTo(newAllocator, newLedger, drillBuf);
      verifyParentOrThis(); // TODO(cwestin) remove
      return withinLimit;
    }
  }

  @Override
  protected void onChildClosed(final BaseAllocator childAllocator) {
    Preconditions.checkArgument(childAllocator != null, "child allocator can't be null");

    synchronized(DEBUG_LOCK) {
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
