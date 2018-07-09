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

import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.record.RecordBatch;
import org.junit.Assert;
import org.junit.Test;

public class TestBuildSidePartitioningImpl {
  @Test
  public void testSimpleReserveMemoryCalculationNoHash() {
    final int maxBatchNumRecords = 20;
    final HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl(
        new HashTableSizeCalculatorConservativeImpl(RecordBatch.MAX_BATCH_SIZE, HashTableSizeCalculatorConservativeImpl.HASHTABLE_DOUBLING_FACTOR),
        HashJoinHelperSizeCalculatorImpl.INSTANCE,
        2.0,
        1.5);

    final CaseInsensitiveMap<Long> buildValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> probeValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> keySizes = CaseInsensitiveMap.newHashMap();

    calc.initialize(true,
      false,
      keySizes,
      200,
      2,
      20,
      10,
      20,
      10,
      10,
      5,
      maxBatchNumRecords,
      maxBatchNumRecords,
      16000,
      .75);

    final HashJoinMemoryCalculator.PartitionStatSet partitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(new PartitionStatImpl(), new PartitionStatImpl());
    calc.setPartitionStatSet(partitionStatSet);

    long expectedReservedMemory = 60 // Max incoming batch size
      + 2 * 30 // build side batch for each spilled partition
      + 60; // Max incoming probe batch size
    long actualReservedMemory = calc.getBuildReservedMemory();

    Assert.assertEquals(expectedReservedMemory, actualReservedMemory);
    Assert.assertEquals(2, calc.getNumPartitions());
  }

  @Test
  public void testSimpleReserveMemoryCalculationHash() {
    final int maxBatchNumRecords = 20;
    final HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl(
        new HashTableSizeCalculatorConservativeImpl(RecordBatch.MAX_BATCH_SIZE, HashTableSizeCalculatorConservativeImpl.HASHTABLE_DOUBLING_FACTOR),
        HashJoinHelperSizeCalculatorImpl.INSTANCE,
        2.0,
        1.5);

    final CaseInsensitiveMap<Long> buildValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> probeValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> keySizes = CaseInsensitiveMap.newHashMap();

    calc.initialize(false,
      true,
      keySizes,
      350,
      2,
      20,
      10,
      20,
      10,
      10,
      5,
      maxBatchNumRecords,
      maxBatchNumRecords,
      16000,
      .75);

    final HashJoinMemoryCalculator.PartitionStatSet partitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(new PartitionStatImpl(), new PartitionStatImpl());
    calc.setPartitionStatSet(partitionStatSet);

    long expectedReservedMemory = 60 // Max incoming batch size
      + 2 * (/* data size for batch */ 30 + /* Space reserved for hash value vector */ 10 * 4 * 2) // build side batch for each spilled partition
      + 60; // Max incoming probe batch size
    long actualReservedMemory = calc.getBuildReservedMemory();

    Assert.assertEquals(expectedReservedMemory, actualReservedMemory);
    Assert.assertEquals(2, calc.getNumPartitions());
  }

  @Test
  public void testAdjustInitialPartitions() {
    final int maxBatchNumRecords = 20;
    final HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl(
        new HashTableSizeCalculatorConservativeImpl(RecordBatch.MAX_BATCH_SIZE, HashTableSizeCalculatorConservativeImpl.HASHTABLE_DOUBLING_FACTOR),
        HashJoinHelperSizeCalculatorImpl.INSTANCE,
        2.0,
        1.5);

    final CaseInsensitiveMap<Long> buildValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> probeValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> keySizes = CaseInsensitiveMap.newHashMap();

    calc.initialize(
      true,
      false,
      keySizes,
      200,
      4,
      20,
      10,
      20,
      10,
      10,
      5,
      maxBatchNumRecords,
      maxBatchNumRecords,
      16000,
      .75);

    final HashJoinMemoryCalculator.PartitionStatSet partitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(new PartitionStatImpl(), new PartitionStatImpl(),
        new PartitionStatImpl(), new PartitionStatImpl());
    calc.setPartitionStatSet(partitionStatSet);

    long expectedReservedMemory = 60 // Max incoming batch size
      + 2 * 30 // build side batch for each spilled partition
      + 60; // Max incoming probe batch size
    long actualReservedMemory = calc.getBuildReservedMemory();

    Assert.assertEquals(expectedReservedMemory, actualReservedMemory);
    Assert.assertEquals(2, calc.getNumPartitions());
  }

  @Test
  public void testNoRoomInMemoryForBatch1() {
    final int maxBatchNumRecords = 20;

    final HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl(
        new HashTableSizeCalculatorConservativeImpl(RecordBatch.MAX_BATCH_SIZE, HashTableSizeCalculatorConservativeImpl.HASHTABLE_DOUBLING_FACTOR),
        HashJoinHelperSizeCalculatorImpl.INSTANCE,
        2.0,
        1.5);

    final CaseInsensitiveMap<Long> buildValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> probeValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> keySizes = CaseInsensitiveMap.newHashMap();

    calc.initialize(
      true,
      false,
      keySizes,
      180,
      2,
      20,
      10,
      20,
      10,
      10,
      5,
      maxBatchNumRecords,
      maxBatchNumRecords,
      16000,
      .75);

    final PartitionStatImpl partition1 = new PartitionStatImpl();
    final PartitionStatImpl partition2 = new PartitionStatImpl();
    final HashJoinMemoryCalculator.PartitionStatSet partitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(partition1, partition2);
    calc.setPartitionStatSet(partitionStatSet);

    long expectedReservedMemory = 60 // Max incoming batch size
      + 2 * 30 // build side batch for each spilled partition
      + 60; // Max incoming probe batch size
    long actualReservedMemory = calc.getBuildReservedMemory();

    Assert.assertEquals(expectedReservedMemory, actualReservedMemory);
    Assert.assertEquals(2, calc.getNumPartitions());

    partition1.add(new HashJoinMemoryCalculator.BatchStat(10, 8));
    Assert.assertTrue(calc.shouldSpill());
  }

  @Test
  public void testCompleteLifeCycle() {
    final int maxBatchNumRecords = 20;
    final HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl calc =
      new HashJoinMemoryCalculatorImpl.BuildSidePartitioningImpl(
        new HashTableSizeCalculatorConservativeImpl(RecordBatch.MAX_BATCH_SIZE, HashTableSizeCalculatorConservativeImpl.HASHTABLE_DOUBLING_FACTOR),
        HashJoinHelperSizeCalculatorImpl.INSTANCE,
        2.0,
        1.5);

    final CaseInsensitiveMap<Long> buildValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> probeValueSizes = CaseInsensitiveMap.newHashMap();
    final CaseInsensitiveMap<Long> keySizes = CaseInsensitiveMap.newHashMap();

    calc.initialize(
      true,
      false,
      keySizes,
      210,
      2,
      20,
      10,
      20,
      10,
      10,
      5,
      maxBatchNumRecords,
      maxBatchNumRecords,
      16000,
      .75);

    final PartitionStatImpl partition1 = new PartitionStatImpl();
    final PartitionStatImpl partition2 = new PartitionStatImpl();
    final HashJoinMemoryCalculator.PartitionStatSet partitionStatSet =
      new HashJoinMemoryCalculator.PartitionStatSet(partition1, partition2);
    calc.setPartitionStatSet(partitionStatSet);


    // Add to partition 1, no spill needed
    {
      partition1.add(new HashJoinMemoryCalculator.BatchStat(10, 7));
      Assert.assertFalse(calc.shouldSpill());
    }

    // Add to partition 2, no spill needed
    {
      partition2.add(new HashJoinMemoryCalculator.BatchStat(10, 8));
      Assert.assertFalse(calc.shouldSpill());
    }

    // Add to partition 1, and partition 1 spilled
    {
      partition1.add(new HashJoinMemoryCalculator.BatchStat(10, 8));
      Assert.assertTrue(calc.shouldSpill());
      partition1.spill();
    }

    // Add to partition 2, no spill needed
    {
      partition2.add(new HashJoinMemoryCalculator.BatchStat(10, 7));
      Assert.assertFalse(calc.shouldSpill());
    }

    // Add to partition 2, and partition 2 spilled
    {
      partition2.add(new HashJoinMemoryCalculator.BatchStat(10, 8));
      Assert.assertTrue(calc.shouldSpill());
      partition2.spill();
    }

    Assert.assertNotNull(calc.next());
  }
}
