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
package org.apache.drill.exec.vector;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.vector.NullableVarCharVector.Accessor;
import org.junit.Assert;
import org.junit.Test;

public class TestSplitAndTransfer {

  @Test
  public void test() throws Exception {
    BufferAllocator allocator = new TopLevelAllocator();
    MaterializedField field = MaterializedField.create("field", Types.optional(MinorType.VARCHAR));
    NullableVarCharVector varCharVector = new NullableVarCharVector(field, allocator);
    varCharVector.allocateNew(10000, 1000);

    String[] compareArray = new String[500];

    for (int i = 0; i < 500; i += 3) {
      String s = String.format("%010d", i);
      varCharVector.getMutator().set(i, s.getBytes());
      compareArray[i] = s;
    }
    varCharVector.getMutator().setValueCount(500);

    TransferPair tp = varCharVector.getTransferPair();
    NullableVarCharVector newVarCharVector = (NullableVarCharVector) tp.getTo();
    Accessor accessor = newVarCharVector.getAccessor();
    int[][] startLengths = {{0, 201}, {201, 200}, {401, 99}};

    for (int[] startLength : startLengths) {
      int start = startLength[0];
      int length = startLength[1];
      tp.splitAndTransfer(start, length);
      newVarCharVector.getMutator().setValueCount(length);
      for (int i = 0; i < length; i++) {
        boolean expectedSet = ((start + i) % 3) == 0;
        if (expectedSet) {
          byte[] expectedValue = compareArray[start + i].getBytes();
          Assert.assertFalse(accessor.isNull(i));
//          System.out.println(new String(accessor.get(i)));
          Assert.assertArrayEquals(expectedValue, accessor.get(i));
        } else {
          Assert.assertTrue(accessor.isNull(i));
        }
      }
      newVarCharVector.clear();
    }

    varCharVector.clear();
    allocator.close();
  }
}
