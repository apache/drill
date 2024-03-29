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

import com.google.common.collect.Maps;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * This is a test for the more conservative hash table memory calculator {@link HashTableSizeCalculatorConservativeImpl}.
 */
public class TestHashTableSizeCalculatorConservativeImpl extends BaseTest {
  @Test
  public void testCalculateHashTableSize() {
    final int maxNumRecords = 40;
    double loadFactor = .75;

    final Map<String, Long> keySizes = Maps.newHashMap();
    keySizes.put("a", 3L);
    keySizes.put("b", 8L);

    // 60 * 4/3 = 80 rounded to nearest power of 2 is 128 buckets
    long expected = RecordBatchSizer.multiplyByFactor(
      UInt4Vector.VALUE_WIDTH * 128, HashTableSizeCalculatorConservativeImpl.HASHTABLE_DOUBLING_FACTOR);
    // First bucket key value vector sizes
    expected += BatchSizePredictorImpl.computeValueVectorSize(maxNumRecords, 3L);
    expected += BatchSizePredictorImpl.computeValueVectorSize(maxNumRecords, 8L);

    // Second bucket key value vector sizes
    expected += RecordBatchSizer.multiplyByFactor(
      BatchSizePredictorImpl.computeValueVectorSize(20, 3L), HashTableSizeCalculatorConservativeImpl.HASHTABLE_DOUBLING_FACTOR);
    expected += RecordBatchSizer.multiplyByFactor(
      BatchSizePredictorImpl.computeValueVectorSize(20, 8L), HashTableSizeCalculatorConservativeImpl.HASHTABLE_DOUBLING_FACTOR);

    // Overhead vectors for links and hash values for each batchHolder
    expected += 2 * UInt4Vector.VALUE_WIDTH // links and hash values */
       * 2 * maxNumRecords; // num batch holders

    PartitionStatImpl partitionStat = new PartitionStatImpl();
    partitionStat.add(
      new HashJoinMemoryCalculator.BatchStat(maxNumRecords + 20, 1));

    final HashTableSizeCalculatorConservativeImpl calc = new HashTableSizeCalculatorConservativeImpl(maxNumRecords, HashTableSizeCalculatorConservativeImpl.HASHTABLE_DOUBLING_FACTOR);
    long actual = calc.calculateSize(partitionStat, keySizes, loadFactor, 1.0, 1.0);

    Assert.assertEquals(expected, actual);
  }
}
