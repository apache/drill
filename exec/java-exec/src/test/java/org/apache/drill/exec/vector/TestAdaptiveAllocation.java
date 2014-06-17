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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAdaptiveAllocation {

  @Test
  public void test() throws Exception {
    BufferAllocator allocator = new TopLevelAllocator();
    MaterializedField field = MaterializedField.create("field", Types.required(MinorType.VARCHAR));
    NullableVarBinaryVector vector1 = new NullableVarBinaryVector(field, allocator);
    NullableVarCharVector vector2 = new NullableVarCharVector(field, allocator);
    NullableBigIntVector vector3 = new NullableBigIntVector(field, allocator);

    Random rand = new Random();
//    int valuesToWrite = rand.nextInt(4000) + 1000;
//    int bytesToWrite = rand.nextInt(100);
    int valuesToWrite = 8000;
    int bytesToWrite1 = 2;
    int bytesToWrite2 = 200;
//    System.out.println("value: " + valuesToWrite);
//    System.out.println("bytes: " + bytesToWrite);

    byte[] value1 = new byte[bytesToWrite1];
    byte[] value2 = new byte[bytesToWrite2];

    NullableVarBinaryVector copyVector1 = new NullableVarBinaryVector(field, allocator);
    NullableVarCharVector copyVector2 = new NullableVarCharVector(field, allocator);
    NullableBigIntVector copyVector3 = new NullableBigIntVector(field, allocator);

    copyVector1.allocateNew();
    copyVector2.allocateNew();
    copyVector3.allocateNew();

    copyVector1.getMutator().set(0, value1);
    copyVector2.getMutator().set(0, value2);
    copyVector3.getMutator().set(0, 100);

    for (int i = 0; i < 10000; i++) {
      vector1.allocateNew();
      vector2.allocateNew();
      vector3.allocateNew();
//      System.out.println("Value Capacity: " + varBinaryVector.getValueCapacity());
//      System.out.println("Byte Capacity: " + varBinaryVector.getByteCapacity());
      int offset = 0;
      int j = 0;
      int toWrite = (int) valuesToWrite * (int) (2 + rand.nextGaussian()) / 2;
      for (j = 0; j < toWrite; j += 1) {
//        if (!(vector1.getMutator().setSafe(j - offset, value1, 0, value1.length) &&
//        vector2.getMutator().setSafe(j - offset, value2, 0 , value2.length) &&
//        vector3.getMutator().setSafe(j - offset, 100))) {
        if (!(vector1.copyFromSafe(0, j - offset, copyVector1) &&
          vector2.copyFromSafe(0, j - offset, copyVector2) &&
          vector3.copyFromSafe(0, j - offset, copyVector3))) {
          vector1.getMutator().setValueCount(j - offset);
          vector2.getMutator().setValueCount(j - offset);
          vector3.getMutator().setValueCount(j - offset);
          offset = j;
          vector1.clear();
          vector2.clear();
          vector3.clear();
          vector1.allocateNew();
          vector2.allocateNew();
          vector3.allocateNew();
//          System.out.println("Value Capacity: " + varBinaryVector.getValueCapacity());
//          System.out.println("Byte Capacity: " + varBinaryVector.getByteCapacity());
        }
      }
      vector1.getMutator().setValueCount(j - offset);
      vector2.getMutator().setValueCount(j - offset);
      vector3.getMutator().setValueCount(j - offset);
    }
    vector1.allocateNew();
    vector2.allocateNew();
    vector3.allocateNew();
    assertTrue(vector1.getValueCapacity() > 8000);
    assertTrue(vector2.getValueCapacity() > 8000);
    assertTrue(vector3.getValueCapacity() > 8000);
    assertTrue(vector1.getByteCapacity() > 8000 * 2);
    assertTrue(vector2.getByteCapacity() > 8000 * 200);
  }
}
