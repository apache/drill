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

/**
 *
 *
 * TODO: Fix this so that preallocation can never be released back to general pool until allocator is closed.
 */
public class AtomicRemainder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AtomicRemainder.class);

  private static final boolean DEBUG = true;

  private final AtomicRemainder parent;
  private final AtomicLong availableShared;
  private final AtomicLong availablePrivate;
  private final long initTotal;
  private final long initShared;
  private final long initPrivate;
  private long limit;       // An Allocator can set a variable limit less than or equal to the initTotal
  private boolean hasLimit; // True for Atomic Remainders associated with a Fragment. May be true for Operator Level allocators some day.
  private boolean closed = false;
  private final boolean errorOnLeak;
  private final boolean applyFragmentLimit;

  public AtomicRemainder(boolean errorOnLeak, AtomicRemainder parent, long max, long pre, boolean applyFragLimit) {
    this.errorOnLeak = errorOnLeak;
    this.parent = parent;
    this.availableShared = new AtomicLong(max - pre);
    this.availablePrivate = new AtomicLong(pre);
    this.initTotal = max;
    this.initShared = max - pre;
    this.initPrivate = pre;
    this.limit = max;
    this.hasLimit=false;
    this.applyFragmentLimit=applyFragLimit; // If this is an operator that is exempt from the fragment limit, set this to false.
//    logger.info("new AtomicRemainder. a.s. {} a.p. {} hashcode {}", availableShared, availablePrivate, hashCode(), new Exception());
  }

  public long getRemainder() {
    return availableShared.get() + availablePrivate.get();
  }

  public long getUsed() {
    return initTotal - getRemainder();
  }

  /**
   * Allow an allocator to constrain the remainder to a particular limit that is lower than the initTotal.
   * If limit is larger than initTotal, then the function will do nothing and the hasLimit flag will not be set.
   * @param limit
   */
  public void setLimit(long limit) {
    if(limit<initTotal){
      this.hasLimit=true;
      this.limit=limit;
    }

  }
  /**
   * Automatically allocate memory. This is used when an actual allocation happened to be larger than requested. This
   * memory has already been used up so it must be accurately accounted for in future allocations.
   *
   * @param size
   */
  public boolean forceGet(long size) {
    if (get(size, this.applyFragmentLimit)) {
      return true;
    } else {
      availableShared.addAndGet(size);
      if (parent != null) {
        parent.forceGet(size);
      }
      return false;
    }
  }

  public boolean get(long size, boolean applyFragmentLimitForChild) {
    if (availablePrivate.get() < 1) {
      // if there is no preallocated memory, we can operate normally.

      // if there is a parent allocator, check it before allocating.
      if (parent != null && !parent.get(size, this.applyFragmentLimit)) {
        return false;
      }

      // If we need to allocate memory beyond the allowed Fragment Limit
      if(applyFragmentLimitForChild && this.applyFragmentLimit && this.hasLimit && (getUsed()+size > this.limit)){
        if (parent != null) {
          parent.returnAllocation(size);
        }
        StackTraceElement[] ste = (new Throwable()).getStackTrace();
        StringBuffer sb = new StringBuffer();
        for (StackTraceElement s : ste) {
          sb.append(s.toString());
          sb.append("\n");
        }
        logger.warn("No more memory. Fragment limit ("+this.limit +
          " bytes) reached. Trying to allocate "+size+ " bytes. "+getUsed()+" bytes already allocated.\n"+sb.toString());
        return false;
      }

      // attempt to get shared memory, if fails, return false.
      long outcome = availableShared.addAndGet(-size);
//      assert outcome <= initShared;
      if (outcome < 0) {
        long newAvailableShared = availableShared.addAndGet(size);
        // assert newAvailableShared <= initShared;
        if (parent != null) {
          parent.returnAllocation(size);
        }
        return false;
      } else {
//        if (DEBUG)
//          logger.info("Get {}. a.s. {} a.p. {} hashcode {}", size, availableShared, availablePrivate, hashCode(), new Exception());
        return true;
      }

    } else {
      // if there is preallocated memory, use that first.
      long unaccount = availablePrivate.addAndGet(-size);
      if (unaccount >= 0) {
//        if (DEBUG)
//          logger.info("Get {}. a.s. {} a.p. {} hashcode {}", size, availableShared, availablePrivate, hashCode(), new Exception());
        return true;
      } else {

        long additionalSpaceNeeded = -unaccount;
        // if there is a parent allocator, check it before allocating.
        if (parent != null && !parent.get(additionalSpaceNeeded, this.applyFragmentLimit)) {
          // parent allocation failed, return space to private pool.
          availablePrivate.getAndAdd(size);
          return false;
        }

        // we got space from parent pool. lets make sure we have space locally available.
        long account = availableShared.addAndGet(-additionalSpaceNeeded);
        if (account >= 0) {
          // we were succesful, move private back to zero (since we allocated using shared).
          availablePrivate.addAndGet(additionalSpaceNeeded);
//          if (DEBUG)
//            logger.info("Get {}. a.s. {} a.p. {} hashcode {}", size, availableShared, availablePrivate, hashCode(), new Exception());
          return true;
        } else {
          // we failed to get space from available shared. Return allocations to initial state.
          availablePrivate.addAndGet(size);
          availableShared.addAndGet(additionalSpaceNeeded);
          parent.returnAllocation(additionalSpaceNeeded);
          return false;
        }
      }

    }

  }

  /**
   * Return the memory accounting to the allocation pool. Make sure to first maintain hold of the preallocated memory.
   *
   * @param size
   */
  public void returnAllocation(long size) {
    long privateSize = availablePrivate.get();
    long privateChange = Math.min(size, initPrivate - privateSize);
    long sharedChange = size - privateChange;
    availablePrivate.addAndGet(privateChange);
    availableShared.addAndGet(sharedChange);
//    if (DEBUG)
//      logger.info("Return allocation {}, a.s. {} a.p. {} hashcode {}", size, availableShared, availablePrivate, hashCode(), new Exception());
    if (parent != null) {
      parent.returnAllocation(sharedChange);
    }
    assert getUsed() <= initTotal;
  }

  public void close() {
    if (closed) {
      logger.warn("Tried to close remainder, but it has already been closed", new Exception());
      return;
    }
    if (availablePrivate.get() != initPrivate || availableShared.get() != initShared) {
      IllegalStateException e = new IllegalStateException(
          String
              .format(ERROR, initPrivate, availablePrivate.get(), initPrivate - availablePrivate.get(), initShared, availableShared.get(), initShared - availableShared.get()));
      if (errorOnLeak) {
        throw e;
      } else {
        logger.warn("Memory leaked during query.", e);
      }
    }
    if (parent != null) {
      parent.returnAllocation(initPrivate);
    }
    closed = true;
  }

  static final String ERROR = "Failure while closing accountor.  Expected private and shared pools to be set to initial values.  However, one or more were not.  Stats are\n\tzone\tinit\tallocated\tdelta \n\tprivate\t%d\t%d\t%d \n\tshared\t%d\t%d\t%d.";
}
