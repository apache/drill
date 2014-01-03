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

  private final AtomicRemainder parent;
  private final AtomicLong total;
  private final AtomicLong unaccountable;
  private final long max;
  private final long pre;

  public AtomicRemainder(AtomicRemainder parent, long max, long pre) {
    this.parent = parent;
    this.total = new AtomicLong(max - pre);
    this.unaccountable = new AtomicLong(pre);
    this.max = max;
    this.pre = pre;
  }

  public long getRemainder() {
    return total.get() + unaccountable.get();
  }

  public long getUsed() {
    return max - getRemainder();
  }

  /**
   * Automatically allocate memory. This is used when an actual allocation happened to be larger than requested. This
   * memory has already been used up so it must be accurately accounted for in future allocations.
   * 
   * @param size
   */
  public void forceGet(long size) {
    total.addAndGet(size);
    if (parent != null)
      parent.forceGet(size);
  }

  public boolean get(long size) {
    if (unaccountable.get() < 1) {
      // if there is no preallocated memory, we can operate normally.
      long outcome = total.addAndGet(-size);
      if (outcome < 0) {
        total.addAndGet(size);
        return false;
      } else {
        return true;
      }
    } else {
      // if there is preallocated memory, use that first.
      long unaccount = unaccountable.getAndAdd(-size);
      if (unaccount > -1) {
        return true;
      } else {

        // if there is a parent allocator, check it before allocating.
        if (parent != null && !parent.get(-unaccount)) {
          unaccountable.getAndAdd(size);
          return false;
        }

        long account = total.addAndGet(unaccount);
        if (account >= 0) {
          unaccountable.getAndAdd(unaccount);
          return true;
        } else {
          unaccountable.getAndAdd(size);
          total.addAndGet(-unaccount);
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
    long preSize = unaccountable.get();
    long preChange = Math.min(size, pre - preSize);
    long totalChange = size - preChange;
    unaccountable.addAndGet(preChange);
    total.addAndGet(totalChange);
    if (parent != null){
      parent.returnAllocation(totalChange);
    }
  }

}
