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

import java.util.Random;

public class TestAdaptiveAllocation {

  @Test
  public void test() throws Exception {
    BufferAllocator allocator = new TopLevelAllocator();
    MaterializedField field = MaterializedField.create("field", Types.required(MinorType.VARCHAR));
    VarBinaryVector varBinaryVector = new VarBinaryVector(field, allocator);

    Random rand = new Random();
//    int valuesToWrite = rand.nextInt(4000) + 1000;
//    int bytesToWrite = rand.nextInt(100);
    int valuesToWrite = 100;
    int bytesToWrite = 1;
//    System.out.println("value: " + valuesToWrite);
//    System.out.println("bytes: " + bytesToWrite);

    byte[] value = new byte[bytesToWrite];

    for (int i = 0; i < 10000; i++) {
      varBinaryVector.allocateNew();
//      System.out.println("Value Capacity: " + varBinaryVector.getValueCapacity());
//      System.out.println("Byte Capacity: " + varBinaryVector.getByteCapacity());
      int offset = 0;
      int j = 0;
      for (j = 0; j < valuesToWrite; j++) {
        if (!varBinaryVector.getMutator().setSafe(j - offset, value)) {
          varBinaryVector.getMutator().setValueCount(j - offset);
          offset = j;
          varBinaryVector.allocateNew();
//          System.out.println("Value Capacity: " + varBinaryVector.getValueCapacity());
//          System.out.println("Byte Capacity: " + varBinaryVector.getByteCapacity());
        }
      }
      varBinaryVector.getMutator().setValueCount(j - offset);
    }
    varBinaryVector.allocateNew();
    System.out.println(varBinaryVector.getValueCapacity());
    System.out.println(varBinaryVector.getByteCapacity());
  }
}
