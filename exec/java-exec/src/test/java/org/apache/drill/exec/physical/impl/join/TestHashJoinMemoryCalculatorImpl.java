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
package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.exec.vector.IntVector;
import org.junit.Assert;
import org.junit.Test;

public class TestHashJoinMemoryCalculatorImpl {
  @Test
  public void testComputeMaxBatchSizeNoHash() {
    final long expected = 1200;
    final long actual = HashJoinMemoryCalculatorImpl.computeMaxBatchSize(
      100,
      25,
      100,
      2.0,
      1.5,
      false);
    final long actualNoHash = HashJoinMemoryCalculatorImpl.computeMaxBatchSizeNoHash(
      100,
      25,
      100,
      2.0,
      1.5);

    Assert.assertEquals(expected, actual);
    Assert.assertEquals(expected, actualNoHash);
  }

  @Test
  public void testComputeMaxBatchSizeHash()
  {
    long expected = HashJoinMemoryCalculatorImpl.computeMaxBatchSizeNoHash(
      100,
      25,
      100,
      2.0,
      4.0) +
      100 * IntVector.VALUE_WIDTH * 2;

    final long actual = HashJoinMemoryCalculatorImpl.computeMaxBatchSize(
      100,
      25,
      100,
      2.0,
      4.0,
      true);

    Assert.assertEquals(expected, actual);
  }

  @Test // Make sure no exception is thrown
  public void testMakeDebugString()
  {
    final PartitionStatImpl partitionStat1 = new PartitionStatImpl();
    final PartitionStatImpl partitionStat2 = new PartitionStatImpl();
    final PartitionStatImpl partitionStat3 = new PartitionStatImpl();
    final PartitionStatImpl partitionStat4 = new PartitionStatImpl();

    final HashJoinMemoryCalculator.PartitionStatSet partitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(partitionStat1, partitionStat2, partitionStat3, partitionStat4);
    partitionStat1.add(new HashJoinMemoryCalculator.BatchStat(10, 7));
    partitionStat2.add(new HashJoinMemoryCalculator.BatchStat(11, 20));
    partitionStat3.spill();
  }
}
