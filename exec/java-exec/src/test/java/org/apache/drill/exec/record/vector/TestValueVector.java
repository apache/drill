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
package org.apache.drill.exec.record.vector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableUInt4Vector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.junit.Test;

public class TestValueVector extends ExecTest {

  TopLevelAllocator allocator = new TopLevelAllocator();

  @Test
  public void testFixedType() {
    // Build a required uint field definition
    MajorType.Builder typeBuilder = MajorType.newBuilder();
    typeBuilder
        .setMinorType(MinorType.UINT4)
        .setMode(DataMode.REQUIRED)
        .setWidth(4);
        MaterializedField field = MaterializedField.create(SchemaPath.getSimplePath(""), typeBuilder.build());

    // Create a new value vector for 1024 integers
    UInt4Vector v = new UInt4Vector(field, allocator);
    UInt4Vector.Mutator m = v.getMutator();
    v.allocateNew(1024);

    // Put and set a few values
    m.setSafe(0, 100);
    m.setSafe(1, 101);
    m.setSafe(100, 102);
    m.setSafe(1022, 103);
    m.setSafe(1023, 104);
    assertEquals(100, v.getAccessor().get(0));
    assertEquals(101, v.getAccessor().get(1));
    assertEquals(102, v.getAccessor().get(100));
    assertEquals(103, v.getAccessor().get(1022));
    assertEquals(104, v.getAccessor().get(1023));

  }

  @Test
  public void testNullableVarLen2() {
    // Build an optional varchar field definition
    MajorType.Builder typeBuilder = MajorType.newBuilder();
    SerializedField.Builder defBuilder = SerializedField.newBuilder();
    typeBuilder
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .setWidth(2);
    defBuilder
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    NullableVarCharVector v = new NullableVarCharVector(field, allocator);
    NullableVarCharVector.Mutator m = v.getMutator();
    v.allocateNew(1024*10, 1024);

    // Create and set 3 sample strings
    String str1 = new String("AAAAA1");
    String str2 = new String("BBBBBBBBB2");
    String str3 = new String("CCCC3");
    m.set(0, str1.getBytes(Charset.forName("UTF-8")));
    m.set(1, str2.getBytes(Charset.forName("UTF-8")));
    m.set(2, str3.getBytes(Charset.forName("UTF-8")));

    // Check the sample strings
    assertEquals(str1, new String(v.getAccessor().get(0), Charset.forName("UTF-8")));
    assertEquals(str2, new String(v.getAccessor().get(1), Charset.forName("UTF-8")));
    assertEquals(str3, new String(v.getAccessor().get(2), Charset.forName("UTF-8")));

    // Ensure null value throws
    boolean b = false;
    try {
      v.getAccessor().get(3);
    } catch(AssertionError e) {
      b = true;
    }finally{
      if(!b){
        assert false;
      }
    }

  }


  @Test
  public void testNullableFixedType() {
    // Build an optional uint field definition
    MajorType.Builder typeBuilder = MajorType.newBuilder();
    SerializedField.Builder defBuilder = SerializedField.newBuilder();
    typeBuilder
        .setMinorType(MinorType.UINT4)
        .setMode(DataMode.OPTIONAL)
        .setWidth(4);
    defBuilder
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    NullableUInt4Vector v = new NullableUInt4Vector(field, allocator);
    NullableUInt4Vector.Mutator m = v.getMutator();
    v.allocateNew(1024);

    // Put and set a few values
    m.set(0, 100);
    m.set(1, 101);
    m.set(100, 102);
    m.set(1022, 103);
    m.set(1023, 104);
    assertEquals(100, v.getAccessor().get(0));
    assertEquals(101, v.getAccessor().get(1));
    assertEquals(102, v.getAccessor().get(100));
    assertEquals(103, v.getAccessor().get(1022));
    assertEquals(104, v.getAccessor().get(1023));

    // Ensure null values throw
    {
      boolean b = false;
      try {
        v.getAccessor().get(3);
      } catch(AssertionError e) {
        b = true;
      }finally{
        if(!b){
          assert false;
        }
      }
    }


    v.allocateNew(2048);
    {
      boolean b = false;
      try {
        v.getAccessor().get(0);
      } catch(AssertionError e) {
        b = true;
      }finally{
        if(!b){
          assert false;
        }
      }
    }

    m.set(0, 100);
    m.set(1, 101);
    m.set(100, 102);
    m.set(1022, 103);
    m.set(1023, 104);
    assertEquals(100, v.getAccessor().get(0));
    assertEquals(101, v.getAccessor().get(1));
    assertEquals(102, v.getAccessor().get(100));
    assertEquals(103, v.getAccessor().get(1022));
    assertEquals(104, v.getAccessor().get(1023));

    // Ensure null values throw

    {
      boolean b = false;
      try {
        v.getAccessor().get(3);
      } catch(AssertionError e) {
        b = true;
      }finally{
        if(!b){
          assert false;
        }
      }
    }

  }

  @Test
  public void testNullableFloat() {
    // Build an optional float field definition
    MajorType.Builder typeBuilder = MajorType.newBuilder();
    SerializedField.Builder defBuilder = SerializedField.newBuilder();
    typeBuilder
        .setMinorType(MinorType.FLOAT4)
        .setMode(DataMode.OPTIONAL)
        .setWidth(4);
    defBuilder
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    NullableFloat4Vector v = (NullableFloat4Vector) TypeHelper.getNewVector(field, allocator);
    NullableFloat4Vector.Mutator m = v.getMutator();
    v.allocateNew(1024);

    // Put and set a few values
    m.set(0, 100.1f);
    m.set(1, 101.2f);
    m.set(100, 102.3f);
    m.set(1022, 103.4f);
    m.set(1023, 104.5f);
    assertEquals(100.1f, v.getAccessor().get(0), 0);
    assertEquals(101.2f, v.getAccessor().get(1), 0);
    assertEquals(102.3f, v.getAccessor().get(100), 0);
    assertEquals(103.4f, v.getAccessor().get(1022), 0);
    assertEquals(104.5f, v.getAccessor().get(1023), 0);

    // Ensure null values throw
    {
      boolean b = false;
      try {
        v.getAccessor().get(3);
      } catch(AssertionError e) {
        b = true;
      }finally{
        if(!b){
          assert false;
        }
      }
    }

    v.allocateNew(2048);
    {
      boolean b = false;
      try {
        v.getAccessor().get(0);
      } catch(AssertionError e) {
        b = true;
      }finally{
        if(!b){
          assert false;
        }
      }
    }
  }

  @Test
  public void testBitVector() {
    // Build a required boolean field definition
    MajorType.Builder typeBuilder = MajorType.newBuilder();
    SerializedField.Builder defBuilder = SerializedField.newBuilder();
    typeBuilder
        .setMinorType(MinorType.BIT)
        .setMode(DataMode.REQUIRED)
        .setWidth(4);
    defBuilder
        .setMajorType(typeBuilder.build());
    MaterializedField field = MaterializedField.create(defBuilder.build());

    // Create a new value vector for 1024 integers
    BitVector v = new BitVector(field, allocator);
    BitVector.Mutator m = v.getMutator();
    v.allocateNew(1024);

    // Put and set a few values
    m.set(0, 1);
    m.set(1, 0);
    m.set(100, 0);
    m.set(1022, 1);
    assertEquals(1, v.getAccessor().get(0));
    assertEquals(0, v.getAccessor().get(1));
    assertEquals(0, v.getAccessor().get(100));
    assertEquals(1, v.getAccessor().get(1022));

    // test setting the same value twice
    m.set(0, 1);
    m.set(0, 1);
    m.set(1, 0);
    m.set(1, 0);
    assertEquals(1, v.getAccessor().get(0));
    assertEquals(0, v.getAccessor().get(1));

    // test toggling the values
    m.set(0, 0);
    m.set(1, 1);
    assertEquals(0, v.getAccessor().get(0));
    assertEquals(1, v.getAccessor().get(1));

    // Ensure unallocated space returns 0
    assertEquals(0, v.getAccessor().get(3));
  }


  @Test
  public void testReAllocNullableFixedWidthVector() throws Exception {
    // Build an optional float field definition
    MajorType floatType = MajorType.newBuilder()
        .setMinorType(MinorType.FLOAT4)
        .setMode(DataMode.OPTIONAL)
        .setWidth(4).build();

    MaterializedField field = MaterializedField.create(
        SerializedField.newBuilder()
            .setMajorType(floatType)
            .build());

    // Create a new value vector for 1024 integers
    NullableFloat4Vector v = (NullableFloat4Vector) TypeHelper.getNewVector(field, allocator);
    NullableFloat4Vector.Mutator m = v.getMutator();
    v.allocateNew(1024);

    assertEquals(1024, v.getValueCapacity());

    // Put values in indexes that fall within the initial allocation
    m.setSafe(0, 100.1f);
    m.setSafe(100, 102.3f);
    m.setSafe(1023, 104.5f);

    // Now try to put values in space that falls beyond the initial allocation
    m.setSafe(2000, 105.5f);

    // Check valueCapacity is more than initial allocation
    assertEquals(1024*2, v.getValueCapacity());

    assertEquals(100.1f, v.getAccessor().get(0), 0);
    assertEquals(102.3f, v.getAccessor().get(100), 0);
    assertEquals(104.5f, v.getAccessor().get(1023), 0);
    assertEquals(105.5f, v.getAccessor().get(2000), 0);


    // Set the valueCount to be more than valueCapacity of current allocation. This is possible for NullableValueVectors
    // as we don't call setSafe for null values, but we do call setValueCount when all values are inserted into the
    // vector
    m.setValueCount(v.getValueCapacity() + 200);
  }

  @Test
  public void testReAllocNullableVariableWidthVector() throws Exception {
    // Build an optional float field definition
    MajorType floatType = MajorType.newBuilder()
        .setMinorType(MinorType.VARCHAR)
        .setMode(DataMode.OPTIONAL)
        .setWidth(4).build();

    MaterializedField field = MaterializedField.create(
        SerializedField.newBuilder()
            .setMajorType(floatType)
            .build());

    // Create a new value vector for 1024 integers
    NullableVarCharVector v = (NullableVarCharVector) TypeHelper.getNewVector(field, allocator);
    NullableVarCharVector.Mutator m = v.getMutator();
    v.allocateNew();

    int initialCapacity = v.getValueCapacity();

    // Put values in indexes that fall within the initial allocation
    byte[] str1 = new String("AAAAA1").getBytes(Charset.forName("UTF-8"));
    byte[] str2 = new String("BBBBBBBBB2").getBytes(Charset.forName("UTF-8"));
    byte[] str3 = new String("CCCC3").getBytes(Charset.forName("UTF-8"));

    m.setSafe(0, str1, 0, str1.length);
    m.setSafe(initialCapacity - 1, str2, 0, str2.length);

    // Now try to put values in space that falls beyond the initial allocation
    m.setSafe(initialCapacity + 200, str3, 0, str3.length);

    // Check valueCapacity is more than initial allocation
    assertEquals((initialCapacity+1)*2-1, v.getValueCapacity());

    assertArrayEquals(str1, v.getAccessor().get(0));
    assertArrayEquals(str2, v.getAccessor().get(initialCapacity-1));
    assertArrayEquals(str3, v.getAccessor().get(initialCapacity + 200));

    // Set the valueCount to be more than valueCapacity of current allocation. This is possible for NullableValueVectors
    // as we don't call setSafe for null values, but we do call setValueCount when the current batch is processed.
    m.setValueCount(v.getValueCapacity() + 200);
  }
}
