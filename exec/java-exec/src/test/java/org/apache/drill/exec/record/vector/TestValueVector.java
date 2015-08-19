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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.DrillBuf;

import java.nio.charset.Charset;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.exception.OversizedAllocationException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableFloat4Holder;
import org.apache.drill.exec.expr.holders.NullableUInt4Holder;
import org.apache.drill.exec.expr.holders.NullableVar16CharHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.RepeatedFloat4Holder;
import org.apache.drill.exec.expr.holders.RepeatedIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedVarBinaryHolder;
import org.apache.drill.exec.expr.holders.UInt4Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableUInt4Vector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.RepeatedIntVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestValueVector extends ExecTest {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestValueVector.class);

  private final static SchemaPath EMPTY_SCHEMA_PATH = SchemaPath.getSimplePath("");

  private final static Charset utf8Charset = Charset.forName("UTF-8");
  private final static byte[] STR1 = new String("AAAAA1").getBytes(utf8Charset);
  private final static byte[] STR2 = new String("BBBBBBBBB2").getBytes(utf8Charset);
  private final static byte[] STR3 = new String("CCCC3").getBytes(utf8Charset);

  private final DrillConfig drillConfig = DrillConfig.create();
  private final BufferAllocator allocator = RootAllocatorFactory.newRoot(drillConfig);

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test(expected = OversizedAllocationException.class)
  public void testFixedVectorReallocation() {
    final MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, UInt4Holder.TYPE);
    final UInt4Vector vector = new UInt4Vector(field, allocator);
    // edge case 1: buffer size = max value capacity
    final int expectedValueCapacity = BaseValueVector.MAX_ALLOCATION_SIZE / 4;
    try {
      vector.allocateNew(expectedValueCapacity);
      assertEquals(expectedValueCapacity, vector.getValueCapacity());
      vector.reAlloc();
      assertEquals(expectedValueCapacity * 2, vector.getValueCapacity());
    } finally {
      vector.close();
    }

    // common case: value count < max value capacity
    try {
      vector.allocateNew(BaseValueVector.MAX_ALLOCATION_SIZE / 8);
      vector.reAlloc(); // value allocation reaches to MAX_VALUE_ALLOCATION
      vector.reAlloc(); // this should throw an IOOB
    } finally {
      vector.close();
    }
  }

  @Test(expected = OversizedAllocationException.class)
  public void testBitVectorReallocation() {
    final MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, UInt4Holder.TYPE);
    final BitVector vector = new BitVector(field, allocator);
    // edge case 1: buffer size ~ max value capacity
    final int expectedValueCapacity = 1 << 29;
    try {
      vector.allocateNew(expectedValueCapacity);
      assertEquals(expectedValueCapacity, vector.getValueCapacity());
      vector.reAlloc();
      assertEquals(expectedValueCapacity * 2, vector.getValueCapacity());
    } finally {
      vector.close();
    }

    // common: value count < MAX_VALUE_ALLOCATION
    try {
      vector.allocateNew(expectedValueCapacity);
      for (int i=0; i<3;i++) {
        vector.reAlloc(); // expand buffer size
      }
      assertEquals(Integer.MAX_VALUE, vector.getValueCapacity());
      vector.reAlloc(); // buffer size ~ max allocation
      assertEquals(Integer.MAX_VALUE, vector.getValueCapacity());
      vector.reAlloc(); // overflow
    } finally {
      vector.close();
    }
  }


  @Test(expected = OversizedAllocationException.class)
  public void testVariableVectorReallocation() {
    final MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, UInt4Holder.TYPE);
    final VarCharVector vector = new VarCharVector(field, allocator);
    // edge case 1: value count = MAX_VALUE_ALLOCATION
    final int expectedAllocationInBytes = BaseValueVector.MAX_ALLOCATION_SIZE;
    final int expectedOffsetSize = 10;
    try {
      vector.allocateNew(expectedAllocationInBytes, 10);
      assertEquals(expectedOffsetSize, vector.getValueCapacity());
      assertEquals(expectedAllocationInBytes, vector.getBuffer().capacity());
      vector.reAlloc();
      assertEquals(expectedOffsetSize * 2, vector.getValueCapacity());
      assertEquals(expectedAllocationInBytes * 2, vector.getBuffer().capacity());
    } finally {
      vector.close();
    }

    // common: value count < MAX_VALUE_ALLOCATION
    try {
      vector.allocateNew(BaseValueVector.MAX_ALLOCATION_SIZE / 2, 0);
      vector.reAlloc(); // value allocation reaches to MAX_VALUE_ALLOCATION
      vector.reAlloc(); // this tests if it overflows
    } finally {
      vector.close();
    }
  }

  @Test
  public void testFixedType() {
    MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, UInt4Holder.TYPE);

    // Create a new value vector for 1024 integers.
    try (final UInt4Vector vector = new UInt4Vector(field, allocator)) {
      final UInt4Vector.Mutator m = vector.getMutator();
      vector.allocateNew(1024);

      // Put and set a few values
      m.setSafe(0, 100);
      m.setSafe(1, 101);
      m.setSafe(100, 102);
      m.setSafe(1022, 103);
      m.setSafe(1023, 104);

      final UInt4Vector.Accessor accessor = vector.getAccessor();
      assertEquals(100, accessor.get(0));
      assertEquals(101, accessor.get(1));
      assertEquals(102, accessor.get(100));
      assertEquals(103, accessor.get(1022));
      assertEquals(104, accessor.get(1023));
    }
  }

  @Test
  public void testNullableVarLen2() {
    final MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, NullableVarCharHolder.TYPE);

    // Create a new value vector for 1024 integers.
    try (final NullableVarCharVector vector = new NullableVarCharVector(field, allocator)) {
      final NullableVarCharVector.Mutator m = vector.getMutator();
      vector.allocateNew(1024 * 10, 1024);

      m.set(0, STR1);
      m.set(1, STR2);
      m.set(2, STR3);

      // Check the sample strings.
      final NullableVarCharVector.Accessor accessor = vector.getAccessor();
      assertArrayEquals(STR1, accessor.get(0));
      assertArrayEquals(STR2, accessor.get(1));
      assertArrayEquals(STR3, accessor.get(2));

      // Ensure null value throws.
      boolean b = false;
      try {
        vector.getAccessor().get(3);
      } catch (IllegalStateException e) {
        b = true;
      } finally {
        assertTrue(b);
      }
    }
  }

  private static DrillBuf combineBuffers(final BufferAllocator allocator, final DrillBuf[] buffers) {
    // find the total size we'll need
    int size = 0;
    for(final DrillBuf buffer : buffers) {
      size += buffer.readableBytes();
    }

    // create the new buffer
    final DrillBuf newBuf = allocator.buffer(size);
    final DrillBuf writeBuf = newBuf;
    for(final DrillBuf buffer : buffers) {
      final DrillBuf readBuf = (DrillBuf) buffer.slice();
      final int nBytes = readBuf.readableBytes();
      for(int i = 0; i < nBytes; ++i) {
        writeBuf.writeByte(readBuf.readByte());
      }
    }

    return newBuf;
  }

  @Test
  public void testRepeatedIntVector() {
    final MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, RepeatedIntHolder.TYPE);

    // Create a new value vector.
    final RepeatedIntVector vector1 = new RepeatedIntVector(field, allocator);

    // Populate the vector.
    final int[] values = {2, 3, 5, 7, 11, 13, 17, 19, 23, 27}; // some tricksy primes
    final int nRecords = 7;
    final int nElements = values.length;
    vector1.allocateNew(nRecords, nRecords * nElements);
    final RepeatedIntVector.Mutator mutator = vector1.getMutator();
    for(int recordIndex = 0; recordIndex < nRecords; ++recordIndex) {
      mutator.startNewValue(recordIndex);
      for(int elementIndex = 0; elementIndex < nElements; ++elementIndex) {
        mutator.add(recordIndex, recordIndex * values[elementIndex]);
      }
    }
    mutator.setValueCount(nRecords);

    // Verify the contents.
    final RepeatedIntVector.Accessor accessor1 = vector1.getAccessor();
    assertEquals(nRecords, accessor1.getValueCount());
    for(int recordIndex = 0; recordIndex < nRecords; ++recordIndex) {
      for(int elementIndex = 0; elementIndex < nElements; ++elementIndex) {
        final int value = accessor1.get(recordIndex, elementIndex);
        assertEquals(recordIndex * values[elementIndex], value);
      }
    }

/* TODO(cwestin)
the interface to load has changed
    // Serialize, reify, and verify.
    final DrillBuf[] buffers1 = vector1.getBuffers(false);
    final DrillBuf buffer1 = combineBuffers(allocator, buffers1);
    final RepeatedIntVector vector2 = new RepeatedIntVector(field, allocator);
    vector2.load(nRecords, nRecords * nElements, buffer1);

    final RepeatedIntVector.Accessor accessor2 = vector2.getAccessor();
    for(int recordIndex = 0; recordIndex < nRecords; ++recordIndex) {
      for(int elementIndex = 0; elementIndex < nElements; ++elementIndex) {
        final int value = accessor2.get(recordIndex, elementIndex);
        assertEquals(accessor1.get(recordIndex,  elementIndex), value);
      }
    }
*/

    vector1.close();
/* TODO(cwestin)
    vector2.close();
    buffer1.release();
*/
  }

  @Test
  public void testVarCharVectorLoad() {
    final MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, VarCharHolder.TYPE);

    // Create a new value vector for 1024 variable length strings.
    final VarCharVector vector1 = new VarCharVector(field, allocator);
    final VarCharVector.Mutator mutator = vector1.getMutator();
    vector1.allocateNew(1024 * 10, 1024);

    // Populate the vector.
    final StringBuilder stringBuilder = new StringBuilder();
    final int valueCount = 10;
    for(int i = 0; i < valueCount; ++i) {
      stringBuilder.append('x');
      mutator.setSafe(i, stringBuilder.toString().getBytes(utf8Charset));
    }
    mutator.setValueCount(valueCount);
    assertEquals(valueCount, vector1.getAccessor().getValueCount());

    // Combine the backing buffers so we can load them into a new vector.
    final DrillBuf[] buffers1 = vector1.getBuffers(false);
    final DrillBuf buffer1 = combineBuffers(allocator, buffers1);
    final VarCharVector vector2 = new VarCharVector(field, allocator);
    vector2.load(buffer1.readableBytes(), valueCount, buffer1);

    // Check the contents of the new vector.
    final VarCharVector.Accessor accessor = vector2.getAccessor();
    stringBuilder.setLength(0);
    for(int i = 0; i < valueCount; ++i) {
      stringBuilder.append('x');
      final Object object = accessor.getObject(i);
      System.out.println(object);
      assertEquals(stringBuilder.toString(), object.toString());
    }

    vector1.close();
    vector2.close();
    buffer1.release();
  }

  @Test
  public void testNullableVarCharVectorLoad() {
    final MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, NullableVarCharHolder.TYPE);

    // Create a new value vector for 1024 nullable variable length strings.
    final NullableVarCharVector vector1 = new NullableVarCharVector(field, allocator);
    final NullableVarCharVector.Mutator mutator = vector1.getMutator();
    vector1.allocateNew(1024 * 10, 1024);

    // Populate the vector.
    final StringBuilder stringBuilder = new StringBuilder();
    final int valueCount = 10;
    for(int i = 0; i < valueCount; ++i) {
      stringBuilder.append('x');
      mutator.set(i, stringBuilder.toString().getBytes(utf8Charset));
    }

    // Check the contents.
    final NullableVarCharVector.Accessor accessor1 = vector1.getAccessor();
    stringBuilder.setLength(0);
    for(int i = 0; i < valueCount; ++i) {
      stringBuilder.append('x');
      final Object object = accessor1.getObject(i);
      assertEquals(stringBuilder.toString(), object.toString());
    }

    mutator.setValueCount(valueCount);
    assertEquals(valueCount, vector1.getAccessor().getValueCount());

    // Still ok after setting value count?
    stringBuilder.setLength(0);
    for(int i = 0; i < valueCount; ++i) {
      stringBuilder.append('x');
      final Object object = accessor1.getObject(i);
      System.out.println(object);
      assertEquals(stringBuilder.toString(), object.toString());
    }

    // Combine into a single buffer so we can load it into a new vector.
    final DrillBuf[] buffers1 = vector1.getBuffers(false);
    final DrillBuf buffer1 = combineBuffers(allocator, buffers1);
    final NullableVarCharVector vector2 = new NullableVarCharVector(field, allocator);
    vector2.load(buffer1.readableBytes(), valueCount, buffer1);

    // Check the vector's contents.
    final NullableVarCharVector.Accessor accessor2 = vector2.getAccessor();
    stringBuilder.setLength(0);
    for(int i = 0; i < valueCount; ++i) {
      stringBuilder.append('x');
      final Object object = accessor2.getObject(i);
      assertEquals(stringBuilder.toString(), object.toString());
    }

    vector1.close();
    vector2.close();
    buffer1.release();
  }

  @Test
  public void testNullableFixedType() {
    final MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, NullableUInt4Holder.TYPE);

    // Create a new value vector for 1024 integers.
    try (final NullableUInt4Vector vector = new NullableUInt4Vector(field, allocator)) {
      final NullableUInt4Vector.Mutator m = vector.getMutator();
      vector.allocateNew(1024);

      // Put and set a few values
      m.set(0, 100);
      m.set(1, 101);
      m.set(100, 102);
      m.set(1022, 103);
      m.set(1023, 104);

      final NullableUInt4Vector.Accessor accessor = vector.getAccessor();
      assertEquals(100, accessor.get(0));
      assertEquals(101, accessor.get(1));
      assertEquals(102, accessor.get(100));
      assertEquals(103, accessor.get(1022));
      assertEquals(104, accessor.get(1023));

      // Ensure null values throw
      {
        boolean b = false;
        try {
          accessor.get(3);
        } catch (IllegalStateException e) {
          b = true;
        } finally {
          assertTrue(b);
        }
      }

      vector.allocateNew(2048);
      {
        boolean b = false;
        try {
          accessor.get(0);
        } catch (IllegalStateException e) {
          b = true;
        } finally {
          assertTrue(b);
        }
      }

      m.set(0, 100);
      m.set(1, 101);
      m.set(100, 102);
      m.set(1022, 103);
      m.set(1023, 104);
      assertEquals(100, accessor.get(0));
      assertEquals(101, accessor.get(1));
      assertEquals(102, accessor.get(100));
      assertEquals(103, accessor.get(1022));
      assertEquals(104, accessor.get(1023));

      // Ensure null values throw.
      {
        boolean b = false;
        try {
          vector.getAccessor().get(3);
        } catch (IllegalStateException e) {
          b = true;
        } finally {
          assertTrue(b);
        }
      }
    }
  }

  @Test
  public void testNullableFloat() {
    final MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, NullableFloat4Holder.TYPE);

    // Create a new value vector for 1024 integers
    try (final NullableFloat4Vector vector = (NullableFloat4Vector) TypeHelper.getNewVector(field, allocator)) {
      final NullableFloat4Vector.Mutator m = vector.getMutator();
      vector.allocateNew(1024);

      // Put and set a few values.
      m.set(0, 100.1f);
      m.set(1, 101.2f);
      m.set(100, 102.3f);
      m.set(1022, 103.4f);
      m.set(1023, 104.5f);

      final NullableFloat4Vector.Accessor accessor = vector.getAccessor();
      assertEquals(100.1f, accessor.get(0), 0);
      assertEquals(101.2f, accessor.get(1), 0);
      assertEquals(102.3f, accessor.get(100), 0);
      assertEquals(103.4f, accessor.get(1022), 0);
      assertEquals(104.5f, accessor.get(1023), 0);

      // Ensure null values throw.
      {
        boolean b = false;
        try {
          vector.getAccessor().get(3);
        } catch (IllegalStateException e) {
          b = true;
        } finally {
          assertTrue(b);
        }
      }

      vector.allocateNew(2048);
      {
        boolean b = false;
        try {
          accessor.get(0);
        } catch (IllegalStateException e) {
          b = true;
        } finally {
          assertTrue(b);
        }
      }
    }
  }

  @Test
  public void testBitVector() {
    final MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, BitHolder.TYPE);

    // Create a new value vector for 1024 integers
    try (final BitVector vector = new BitVector(field, allocator)) {
      final BitVector.Mutator m = vector.getMutator();
      vector.allocateNew(1024);

      // Put and set a few values
      m.set(0, 1);
      m.set(1, 0);
      m.set(100, 0);
      m.set(1022, 1);

      final BitVector.Accessor accessor = vector.getAccessor();
      assertEquals(1, accessor.get(0));
      assertEquals(0, accessor.get(1));
      assertEquals(0, accessor.get(100));
      assertEquals(1, accessor.get(1022));

      // test setting the same value twice
      m.set(0, 1);
      m.set(0, 1);
      m.set(1, 0);
      m.set(1, 0);
      assertEquals(1, accessor.get(0));
      assertEquals(0, accessor.get(1));

      // test toggling the values
      m.set(0, 0);
      m.set(1, 1);
      assertEquals(0, accessor.get(0));
      assertEquals(1, accessor.get(1));

      // Ensure unallocated space returns 0
      assertEquals(0, accessor.get(3));
    }
  }

  @Test
  public void testReAllocNullableFixedWidthVector() {
    final MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, NullableFloat4Holder.TYPE);

    // Create a new value vector for 1024 integers
    try (final NullableFloat4Vector vector = (NullableFloat4Vector) TypeHelper.getNewVector(field, allocator)) {
      final NullableFloat4Vector.Mutator m = vector.getMutator();
      vector.allocateNew(1024);

      assertEquals(1024, vector.getValueCapacity());

      // Put values in indexes that fall within the initial allocation
      m.setSafe(0, 100.1f);
      m.setSafe(100, 102.3f);
      m.setSafe(1023, 104.5f);

      // Now try to put values in space that falls beyond the initial allocation
      m.setSafe(2000, 105.5f);

      // Check valueCapacity is more than initial allocation
      assertEquals(1024 * 2, vector.getValueCapacity());

      final NullableFloat4Vector.Accessor accessor = vector.getAccessor();
      assertEquals(100.1f, accessor.get(0), 0);
      assertEquals(102.3f, accessor.get(100), 0);
      assertEquals(104.5f, accessor.get(1023), 0);
      assertEquals(105.5f, accessor.get(2000), 0);

      // Set the valueCount to be more than valueCapacity of current allocation. This is possible for NullableValueVectors
      // as we don't call setSafe for null values, but we do call setValueCount when all values are inserted into the
      // vector
      m.setValueCount(vector.getValueCapacity() + 200);
    }
  }

  @Test
  public void testReAllocNullableVariableWidthVector() {
    final MaterializedField field = MaterializedField.create(EMPTY_SCHEMA_PATH, NullableVarCharHolder.TYPE);

    // Create a new value vector for 1024 integers
    try (final NullableVarCharVector vector = (NullableVarCharVector) TypeHelper.getNewVector(field, allocator)) {
      final NullableVarCharVector.Mutator m = vector.getMutator();
      vector.allocateNew();

      int initialCapacity = vector.getValueCapacity();

      // Put values in indexes that fall within the initial allocation
      m.setSafe(0, STR1, 0, STR1.length);
      m.setSafe(initialCapacity - 1, STR2, 0, STR2.length);

      // Now try to put values in space that falls beyond the initial allocation
      m.setSafe(initialCapacity + 200, STR3, 0, STR3.length);

      // Check valueCapacity is more than initial allocation
      assertEquals((initialCapacity + 1) * 2 - 1, vector.getValueCapacity());

      final NullableVarCharVector.Accessor accessor = vector.getAccessor();
      assertArrayEquals(STR1, accessor.get(0));
      assertArrayEquals(STR2, accessor.get(initialCapacity - 1));
      assertArrayEquals(STR3, accessor.get(initialCapacity + 200));

      // Set the valueCount to be more than valueCapacity of current allocation. This is possible for NullableValueVectors
      // as we don't call setSafe for null values, but we do call setValueCount when the current batch is processed.
      m.setValueCount(vector.getValueCapacity() + 200);
    }
  }

  @Test
  public void testVVInitialCapacity() throws Exception {
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

    try {
      for (int i = 0; i < valueVectors.length; i++) {
        valueVectors[i] = TypeHelper.getNewVector(fields[i], allocator);
        valueVectors[i].setInitialCapacity(initialCapacity);
        valueVectors[i].allocateNew();
      }

      for (int i = 0; i < valueVectors.length; i++) {
        final ValueVector vv = valueVectors[i];
        final int vvCapacity = vv.getValueCapacity();
        assertEquals(String.format("Incorrect value capacity for %s [%d]", vv.getField(), vvCapacity),
            initialCapacity, vvCapacity);
      }
    } finally {
      AutoCloseables.close(valueVectors);
    }
  }
}
