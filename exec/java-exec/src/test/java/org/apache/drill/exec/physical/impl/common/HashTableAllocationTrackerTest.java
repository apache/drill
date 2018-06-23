/*
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
package org.apache.drill.exec.physical.impl.common;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class HashTableAllocationTrackerTest {
  @Test
  public void testDoubleGetNextCall() {
    final HashTableConfig config = new HashTableConfig(100, true, .5f, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList());
    final HashTableAllocationTracker tracker = new HashTableAllocationTracker(config, 30);

    for (int counter = 0; counter < 100; counter++) {
      Assert.assertEquals(30, tracker.getNextBatchHolderSize());
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testPrematureCommit() {
    final HashTableConfig config = new HashTableConfig(100, .5f, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList());
    final HashTableAllocationTracker tracker = new HashTableAllocationTracker(config, 30);

    tracker.commit();
  }

  @Test(expected = IllegalStateException.class)
  public void testDoubleCommit() {
    final HashTableConfig config = new HashTableConfig(100, .5f, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList());
    final HashTableAllocationTracker tracker = new HashTableAllocationTracker(config, 30);

    tracker.commit();
    tracker.commit();
  }

  @Test
  public void testOverAsking() {
    final HashTableConfig config = new HashTableConfig(100, .5f, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList());
    final HashTableAllocationTracker tracker = new HashTableAllocationTracker(config, 30);

    tracker.getNextBatchHolderSize();
  }

  /**
   * Test for when we do not know the final size of the hash table.
   */
  @Test
  public void testLifecycle1() {
    final HashTableConfig config = new HashTableConfig(100, .5f, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList());
    final HashTableAllocationTracker tracker = new HashTableAllocationTracker(config, 30);

    for (int counter = 0; counter < 100; counter++) {
      Assert.assertEquals(30, tracker.getNextBatchHolderSize());
      tracker.commit();
    }
  }

  /**
   * Test for when we know the final size of the hash table
   */
  @Test
  public void testLifecycle() {
    final HashTableConfig config = new HashTableConfig(100, true, .5f, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList());
    final HashTableAllocationTracker tracker = new HashTableAllocationTracker(config, 30);

    Assert.assertEquals(30, tracker.getNextBatchHolderSize());
    tracker.commit();
    Assert.assertEquals(30, tracker.getNextBatchHolderSize());
    tracker.commit();
    Assert.assertEquals(30, tracker.getNextBatchHolderSize());
    tracker.commit();
    Assert.assertEquals(10, tracker.getNextBatchHolderSize());
    tracker.commit();

    boolean caughtException = false;

    try {
      tracker.getNextBatchHolderSize();
    } catch (IllegalStateException ex) {
      caughtException = true;
    }

    Assert.assertTrue(caughtException);
  }
}
