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
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableFloat4Holder;
import org.apache.drill.exec.expr.holders.NullableUInt4Holder;
import org.apache.drill.exec.expr.holders.NullableVar16CharHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.RepeatedFloat4Holder;
import org.apache.drill.exec.expr.holders.RepeatedVarBinaryHolder;
import org.apache.drill.exec.expr.holders.UInt4Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableUInt4Vector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.junit.Test;

public class TestValueVector extends ExecTest {
  private final static SchemaPath EMPTY_SCHEMA_PATH = SchemaPath.getSimplePath("");

  private final static byte[] STR1 = new String("AAAAA1").getBytes(Charset.forName("UTF-8"));
  private final static byte[] STR2 = new String("BBBBBBBBB2").getBytes(Charset.forName("UTF-8"));
  private final static byte[] STR3 = new String("CCCC3").getBytes(Charset.forName("UTF-8"));

  TopLevelAllocator allocator = new TopLevelAllocator();

  @Test
  public void testFixedType() {
    MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, UInt4Holder.TYPE);

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
    MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, NullableVarCharHolder.TYPE);

    // Create a new value vector for 1024 integers
    NullableVarCharVector v = new NullableVarCharVector(field, allocator);
    NullableVarCharVector.Mutator m = v.getMutator();
    v.allocateNew(1024*10, 1024);

    m.set(0, STR1);
    m.set(1, STR2);
    m.set(2, STR3);

    // Check the sample strings
    assertArrayEquals(STR1, v.getAccessor().get(0));
    assertArrayEquals(STR2, v.getAccessor().get(1));
    assertArrayEquals(STR3, v.getAccessor().get(2));

    // Ensure null value throws
    boolean b = false;
    try {
      v.getAccessor().get(3);
    } catch(IllegalStateException e) {
      b = true;
    }finally{
      assertTrue(b);
    }

  }


  @Test
  public void testNullableFixedType() {
    MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, NullableUInt4Holder.TYPE);

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
      } catch(IllegalStateException e) {
        b = true;
      }finally{
        assertTrue(b);
      }
    }


    v.allocateNew(2048);
    {
      boolean b = false;
      try {
        v.getAccessor().get(0);
      } catch(IllegalStateException e) {
        b = true;
      }finally{
        assertTrue(b);
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
      } catch(IllegalStateException e) {
        b = true;
      }finally{
        assertTrue(b);
      }
    }

  }

  @Test
  public void testNullableFloat() {
    MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, NullableFloat4Holder.TYPE);

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
      } catch(IllegalStateException e) {
        b = true;
      }finally{
        assertTrue(b);
      }
    }

    v.allocateNew(2048);
    {
      boolean b = false;
      try {
        v.getAccessor().get(0);
      } catch(IllegalStateException e) {
        b = true;
      }finally{
        assertTrue(b);
      }
    }
  }

  @Test
  public void testBitVector() {
    MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, BitHolder.TYPE);

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
    MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, NullableFloat4Holder.TYPE);

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
    MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, NullableVarCharHolder.TYPE);

    // Create a new value vector for 1024 integers
    NullableVarCharVector v = (NullableVarCharVector) TypeHelper.getNewVector(field, allocator);
    NullableVarCharVector.Mutator m = v.getMutator();
    v.allocateNew();

    int initialCapacity = v.getValueCapacity();

    // Put values in indexes that fall within the initial allocation
    m.setSafe(0, STR1, 0, STR1.length);
    m.setSafe(initialCapacity - 1, STR2, 0, STR2.length);

    // Now try to put values in space that falls beyond the initial allocation
    m.setSafe(initialCapacity + 200, STR3, 0, STR3.length);

    // Check valueCapacity is more than initial allocation
    assertEquals((initialCapacity+1)*2-1, v.getValueCapacity());

    assertArrayEquals(STR1, v.getAccessor().get(0));
    assertArrayEquals(STR2, v.getAccessor().get(initialCapacity-1));
    assertArrayEquals(STR3, v.getAccessor().get(initialCapacity + 200));

    // Set the valueCount to be more than valueCapacity of current allocation. This is possible for NullableValueVectors
    // as we don't call setSafe for null values, but we do call setValueCount when the current batch is processed.
    m.setValueCount(v.getValueCapacity() + 200);
  }

  @Test
  public void testVVInitialCapacity() {
    final MaterializedField[] fields = new MaterializedField[9];
    final ValueVector[] valueVectors = new ValueVector[9];

    fields[0] = MaterializedField.create(EMPTY_SCHEMA_PATH, BitHolder.TYPE);
    fields[1] = MaterializedField.create(EMPTY_SCHEMA_PATH, IntHolder.TYPE);
    fields[2] = MaterializedField.create(EMPTY_SCHEMA_PATH, VarCharHolder.TYPE);
    fields[3] = MaterializedField.create(EMPTY_SCHEMA_PATH, NullableVar16CharHolder.TYPE);
    fields[4] = MaterializedField.create(EMPTY_SCHEMA_PATH, RepeatedFloat4Holder.TYPE);
    fields[5] = MaterializedField.create(EMPTY_SCHEMA_PATH, RepeatedVarBinaryHolder.TYPE);

    fields[6] = MaterializedField.create(EMPTY_SCHEMA_PATH, MapVector.TYPE);
    fields[6].addChild(fields[0] /*bit*/);
    fields[6].addChild(fields[2] /*varchar*/);

    fields[7] = MaterializedField.create(EMPTY_SCHEMA_PATH, RepeatedMapVector.TYPE);
    fields[7].addChild(fields[1] /*int*/);
    fields[7].addChild(fields[3] /*optional var16char*/);

    fields[8] = MaterializedField.create(EMPTY_SCHEMA_PATH, RepeatedListVector.TYPE);
    fields[8].addChild(fields[1] /*int*/);

    final int initialCapacity = 1024;

    for(int i=0; i<valueVectors.length; i++) {
      valueVectors[i] = TypeHelper.getNewVector(fields[i], allocator);
      valueVectors[i].setInitialCapacity(initialCapacity);
      valueVectors[i].allocateNew();
    }

    for(int i=0; i<valueVectors.length; i++) {
      final ValueVector vv = valueVectors[i];
      final int vvCapacity = vv.getValueCapacity();
      assertEquals(String.format("Incorrect value capacity for %s [%d]", vv.getField(), vvCapacity),
          initialCapacity, vvCapacity);
    }
  }
}
