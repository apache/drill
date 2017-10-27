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
package org.apache.drill.vector;

import static org.junit.Assert.*;

import org.apache.drill.categories.VectorTest;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.RepeatedIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.bouncycastle.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.DrillBuf;
import org.junit.experimental.categories.Category;

/**
 * Test the setScalar() methods in the various generated vector
 * classes. Rather than test all 100+ vectors, we sample a few and
 * rely on the fact that code is generated from a common template.
 */

@Category(VectorTest.class)
public class TestVectorLimits extends DrillTest {

  public static OperatorFixture fixture;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    fixture = OperatorFixture.builder().build();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fixture.close();
  }

  /**
   * Test a vector directly using the vector mutator to ensure
   * that the <tt>setScalar</tt> method works for the maximum
   * row count.
   * <p>
   * This test is a proxy for all the other fixed types, since all
   * share the same code template.
   */

  @Test
  public void testFixedVector() {

    // Create a non-nullable int vector: a typical fixed-size vector

    @SuppressWarnings("resource")
    IntVector vector = new IntVector(makeField(MinorType.INT, DataMode.REQUIRED), fixture.allocator() );

    // Sanity test of generated constants.

    assertTrue( IntVector.MAX_SCALAR_COUNT <= ValueVector.MAX_ROW_COUNT );
    assertEquals( 4, IntVector.VALUE_WIDTH );
    assertTrue( IntVector.NET_MAX_SCALAR_SIZE <= ValueVector.MAX_BUFFER_SIZE );

    // Allocate a default size, small vector. Forces test of
    // the auto-grow (setSafe()) aspect of setScalar().

    vector.allocateNew( );

    // Write to the vector until it complains. At that point,
    // we should have written up to the static fixed value count
    // (which is computed to stay below the capacity limit.)

    IntVector.Mutator mutator = vector.getMutator();
    for (int i = 0; i < 2 * ValueVector.MAX_ROW_COUNT; i++) {
      try {
        mutator.setScalar(i, i);
      } catch (VectorOverflowException e) {
        assertEquals(IntVector.MAX_SCALAR_COUNT, i);
        break;
      }
    }

    // The vector should be below the allocation limit. Since this
    // is an int vector, in practice the size will be far below
    // the overall limit (if the limit stays at 16 MB.) But, it should
    // be at the type-specific limit since we filled up the vector.

    assertEquals(IntVector.NET_MAX_SCALAR_SIZE, vector.getBuffer().getActualMemoryConsumed());
    vector.close();
  }

  @Test
  public void testNullableFixedVector() {

    @SuppressWarnings("resource")
    NullableIntVector vector = new NullableIntVector(makeField(MinorType.INT, DataMode.OPTIONAL), fixture.allocator() );
    vector.allocateNew( );

    NullableIntVector.Mutator mutator = vector.getMutator();
    for (int i = 0; i < 2 * ValueVector.MAX_ROW_COUNT; i++) {
      try {
        mutator.setScalar(i, i);
      } catch (VectorOverflowException e) {
        assertEquals(IntVector.MAX_SCALAR_COUNT, i);
        break;
      }
    }

    vector.close();
  }

  /**
   * Repeated fixed vector. Using an int vector, each column array can hold
   * 256 / 4 = 64 values. We write only 10. The vector becomes full when we
   * exceed 64K items.
   */

  @Test
  public void testRepeatedFixedVectorCountLimit() {

    @SuppressWarnings("resource")
    RepeatedIntVector vector = new RepeatedIntVector(makeField(MinorType.INT, DataMode.REPEATED), fixture.allocator() );
    vector.allocateNew( );

    RepeatedIntVector.Mutator mutator = vector.getMutator();
    top:
    for (int i = 0; i < 2 * ValueVector.MAX_ROW_COUNT; i++) {
      if (! mutator.startNewValueBounded(i)) {
        assertEquals(ValueVector.MAX_ROW_COUNT, i);
        // Continue, let's check the addBounded method also
      }
      for (int j = 0; j < 10; j++) {
        try {
          mutator.addEntry(i, i * 100 + j);
        } catch (VectorOverflowException e) {
          assertEquals(ValueVector.MAX_ROW_COUNT, i);
          mutator.setValueCount(i);
          break top;
        }
      }
    }

    vector.close();
  }

  /**
   * Repeated fixed vector. Using an int vector, each column array can hold
   * 256 / 4 = 64 values. We write 100. The vector becomes full when we
   * exceed the 16 MB size limit.
   */

  @Test
  public void testRepeatedFixedVectorBufferLimit() {

    @SuppressWarnings("resource")
    RepeatedIntVector vector = new RepeatedIntVector(makeField(MinorType.INT, DataMode.REPEATED), fixture.allocator() );
    vector.allocateNew( );

    RepeatedIntVector.Mutator mutator = vector.getMutator();
    top:
    for (int i = 0; i < 2 * ValueVector.MAX_ROW_COUNT; i++) {
      // We'll never hit the value count limit
      assertTrue(mutator.startNewValueBounded(i));
      for (int j = 0; j < 100; j++) {
        try {
          mutator.addEntry(i, i * 100 + j);
        } catch (VectorOverflowException e) {
          // We should have hit the buffer limit before the value limit.
          assertTrue(i < ValueVector.MAX_ROW_COUNT);
          mutator.setValueCount(i);
          break top;
        }
      }
    }

    vector.close();
  }

  // To be replaced by a test method in a separate commit.

  public static MaterializedField makeField(MinorType dataType, DataMode mode) {
    MajorType type = MajorType.newBuilder()
        .setMinorType(dataType)
        .setMode(mode)
        .build();

    return MaterializedField.create("foo", type);
  }

  /**
   * Baseline test for a variable-width vector using <tt>setSafe</tt> and
   * loading the vector up to the maximum size. Doing so will cause the vector
   * to have a buffer that exceeds the maximum size, demonstrating the
   * need for <tt>setScalar()</tt>.
   */

  @Test
  public void variableVectorBaseline() {

    // Create a non-nullable VarChar vector: a typical variable-size vector

    @SuppressWarnings("resource")
    VarCharVector vector = new VarCharVector(makeField(MinorType.VARCHAR, DataMode.REQUIRED), fixture.allocator() );
    vector.allocateNew( );

    // A 16 MB value can hold 64K values of up to 256 bytes each.
    // To force a size overflow, write values much larger.
    // Write the maximum number of values which will silently
    // allow the vector to grow beyond the critical size of 16 MB.
    // Doing this in production would lead to memory fragmentation.
    // So, this is what the setScalar() method assures we don't do.

    byte dummyValue[] = new byte[512];
    Arrays.fill(dummyValue, (byte) 'X');
    VarCharVector.Mutator mutator = vector.getMutator();
    for (int i = 0; i < 2 * ValueVector.MAX_ROW_COUNT; i++) {
      mutator.setSafe(i, dummyValue, 0, dummyValue.length);
    }

    // The vector should be above the allocation limit.
    // This is why code must migrate to the setScalar() call
    // away from the setSafe() call.

    assertTrue(ValueVector.MAX_BUFFER_SIZE < vector.getBuffer().getActualMemoryConsumed());
    vector.close();
  }

  /**
   * Test a vector directly using the vector mutator to ensure
   * that the <tt>setScalar</tt> method works for the maximum
   * vector size.
   */

  @Test
  public void testWideVariableVector() {

    @SuppressWarnings("resource")
    VarCharVector vector = new VarCharVector(makeField(MinorType.VARCHAR, DataMode.REQUIRED), fixture.allocator() );
    vector.allocateNew( );

    // A 16 MB value can hold 64K values of up to 256 bytes each.
    // To force a size overflow, write values much larger.
    // Write to the vector until it complains. At that point,
    // we should have written up to the maximum buffer size.

    byte dummyValue[] = makeVarCharValue(512);
    VarCharVector.Mutator mutator = vector.getMutator();
    int count = 0;
    for ( ; count < 2 * ValueVector.MAX_ROW_COUNT; count++) {
      try {
        mutator.setScalar(count, dummyValue, 0, dummyValue.length);
      } catch (VectorOverflowException e) {
        break;
      }
    }

    // The vector should be at the allocation limit. If it wasn't, we
    // should have grown it to hold more data. The value count will
    // be below the maximum.

    mutator.setValueCount(count);
    assertEquals(ValueVector.MAX_BUFFER_SIZE, vector.getBuffer().getActualMemoryConsumed());
    assertTrue(count < ValueVector.MAX_ROW_COUNT);
    vector.close();
  }

  private byte[] makeVarCharValue(int n) {
    byte dummyValue[] = new byte[n];
    Arrays.fill(dummyValue, (byte) 'X');
    return dummyValue;
  }

  @Test
  public void testNullableWideVariableVector() {

    @SuppressWarnings("resource")
    NullableVarCharVector vector = new NullableVarCharVector(makeField(MinorType.VARCHAR, DataMode.OPTIONAL), fixture.allocator() );
    vector.allocateNew( );

    byte dummyValue[] = makeVarCharValue(512);
    NullableVarCharVector.Mutator mutator = vector.getMutator();
    int count = 0;
    for ( ; count < 2 * ValueVector.MAX_ROW_COUNT; count++) {
      try {
        mutator.setScalar(count, dummyValue, 0, dummyValue.length);
      } catch (VectorOverflowException e) {
        break;
      }
    }

    mutator.setValueCount(count);
    assertEquals(ValueVector.MAX_BUFFER_SIZE, vector.getValuesVector().getBuffer().getActualMemoryConsumed());
    assertTrue(count < ValueVector.MAX_ROW_COUNT);
    vector.close();
  }

  /**
   * Test a vector directly using the vector mutator to ensure
   * that the <tt>setScalar</tt> method works for the maximum
   * value count.
   */

  @Test
  public void testNarrowVariableVector() {

    @SuppressWarnings("resource")
    VarCharVector vector = new VarCharVector(makeField(MinorType.VARCHAR, DataMode.REQUIRED), fixture.allocator() );
    vector.allocateNew( );

    // Write small values that fit into 16 MB. We should stop writing
    // when we reach the value count limit.

    byte dummyValue[] = makeVarCharValue(254);
    VarCharVector.Mutator mutator = vector.getMutator();
    int count = 0;
    for (; count < 2 * ValueVector.MAX_ROW_COUNT; count++) {
      try {
        mutator.setScalar(count, dummyValue, 0, dummyValue.length);
      } catch (VectorOverflowException e) {
        break;
      }
    }

    // Buffer size should be at or below the maximum, with count
    // at the maximum.

    mutator.setValueCount(count);
    assertTrue(vector.getBuffer().getActualMemoryConsumed() <= ValueVector.MAX_BUFFER_SIZE);
    assertEquals(ValueVector.MAX_ROW_COUNT, count);
    vector.close();
  }

  /**
   * Test a vector directly using the vector mutator to ensure
   * that the <tt>setScalar</tt> method works for the maximum
   * value count. Uses a DrillBuf as input.
   */

  @Test
  public void testDirectVariableVector() {

    @SuppressWarnings("resource")
    VarCharVector vector = new VarCharVector(makeField(MinorType.VARCHAR, DataMode.REQUIRED), fixture.allocator() );
    vector.allocateNew( );

    // Repeat the big-value test, but with data coming from a DrillBuf
    // (direct memory) rather than a heap array.

    @SuppressWarnings("resource")
    DrillBuf drillBuf = makeVarCharValueDirect(260);
    VarCharVector.Mutator mutator = vector.getMutator();
    int count = 0;
    for (; count < 2 * ValueVector.MAX_ROW_COUNT; count++) {
      try {
        mutator.setScalar(count, drillBuf, 0, 260);
      } catch (VectorOverflowException e) {
        break;
      }
    }
    drillBuf.close();

    // Again, vector should be at the size limit, count below the
    // value limit.

    mutator.setValueCount(count);
    assertEquals(ValueVector.MAX_BUFFER_SIZE, vector.getBuffer().getActualMemoryConsumed());
    assertTrue(count < ValueVector.MAX_ROW_COUNT);
    vector.close();
  }

  private DrillBuf makeVarCharValueDirect(int n) {
    byte dummyValue[] = makeVarCharValue(n);
    DrillBuf drillBuf = fixture.allocator().buffer(dummyValue.length);
    drillBuf.setBytes(0, dummyValue);
    return drillBuf;
  }

  @Test
  public void testDirectNullableVariableVector() {

    @SuppressWarnings("resource")
    NullableVarCharVector vector = new NullableVarCharVector(makeField(MinorType.VARCHAR, DataMode.OPTIONAL), fixture.allocator() );
    vector.allocateNew( );

    @SuppressWarnings("resource")
    DrillBuf drillBuf = makeVarCharValueDirect(260);
    NullableVarCharVector.Mutator mutator = vector.getMutator();
    int count = 0;
    for (; count < 2 * ValueVector.MAX_ROW_COUNT; count++) {
      try {
        mutator.setScalar(count, drillBuf, 0, 260);
      } catch (VectorOverflowException e) {
        break;
      }
    }
    drillBuf.close();

    mutator.setValueCount(count);
    assertEquals(ValueVector.MAX_BUFFER_SIZE, vector.getValuesVector().getBuffer().getActualMemoryConsumed());
    assertTrue(count < ValueVector.MAX_ROW_COUNT);
    vector.close();
  }

  public static void main(String args[]) {
    try {
      setUpBeforeClass();
      new TestVectorLimits().performanceTest();
      tearDownAfterClass();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void performanceTest() {
    @SuppressWarnings("resource")
    VarCharVector vector = new VarCharVector(makeField(MinorType.VARCHAR, DataMode.OPTIONAL), fixture.allocator() );
    byte value[] = makeVarCharValue(1);
    int warmCount = 100;
    timeSetSafe(vector, value, warmCount);
    runSetBounded(vector, value, warmCount);
    int runCount = 1000;
    timeSetSafe(vector, value, runCount);
    runSetBounded(vector, value, runCount);
    timeSetSafe(vector, value, runCount);
    vector.close();
  }

  private void timeSetSafe(VarCharVector vector, byte[] value, int iterCount) {
    long start = System.currentTimeMillis();
    for (int i = 0; i < iterCount; i++) {
      vector.clear();
      vector.allocateNew( );

      VarCharVector.Mutator mutator = vector.getMutator();
      for (int j = 0; j < ValueVector.MAX_ROW_COUNT; j++) {
        mutator.setSafe(j, value, 0, value.length);
      }
    }
    long elapsed = System.currentTimeMillis() - start;
    System.out.println( iterCount + " runs of setSafe: " + elapsed + " ms." );
  }

  private void runSetBounded(VarCharVector vector, byte[] value, int iterCount) {
    long start = System.currentTimeMillis();
    for (int i = 0; i < iterCount; i++) {
      vector.clear();
      vector.allocateNew( );

      VarCharVector.Mutator mutator = vector.getMutator();
      int posn = 0;
      for (;;) {
        try {
          mutator.setScalar(posn++, value, 0, value.length);
        } catch (VectorOverflowException e) {
          break;
        }
      }
    }
    long elapsed = System.currentTimeMillis() - start;
    System.out.println( iterCount + " runs of setScalar: " + elapsed + " ms." );
  }
}
