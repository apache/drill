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
 ******************************************************************************/

package org.apache.drill.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.drill.categories.VectorTest;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.DrillBuf;
import org.junit.experimental.categories.Category;

@Category(VectorTest.class)
public class TestFillEmpties extends DrillTest {

  public static OperatorFixture fixture;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    fixture = OperatorFixture.builder().build();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fixture.close();
  }

  // To be replaced by a test method in a separate commit.

  public static MaterializedField makeField(String name, MinorType dataType, DataMode mode) {
    MajorType type = MajorType.newBuilder()
        .setMinorType(dataType)
        .setMode(mode)
        .build();

    return MaterializedField.create(name, type);
  }

  @Test
  public void testNullableVarChar() {
    @SuppressWarnings("resource")
    NullableVarCharVector vector = new NullableVarCharVector(makeField("a", MinorType.VARCHAR, DataMode.OPTIONAL), fixture.allocator());
    vector.allocateNew( );

    // Create "foo", null, "bar", but omit the null.

    NullableVarCharVector.Mutator mutator = vector.getMutator();
    byte[] value = makeValue( "foo" );
    mutator.setSafe(0, value, 0, value.length);

    value = makeValue("bar");
    mutator.setSafe(2, value, 0, value.length);

    visualize(vector, 3);
    verifyOffsets(vector.getValuesVector().getOffsetVector(), new int[] {0, 3, 3, 6});
    vector.close();
  }

  @Test
  public void testVarChar() {
    @SuppressWarnings("resource")
    VarCharVector vector = new VarCharVector(makeField("a", MinorType.VARCHAR, DataMode.REQUIRED), fixture.allocator());
    vector.allocateNew( );

    // Create "foo", null, "bar", but omit the null.

    VarCharVector.Mutator mutator = vector.getMutator();
    byte[] value = makeValue( "foo" );
    mutator.setSafe(0, value, 0, value.length);

    // Work around: test fails without this. But, only the new column writers
    // call this method.

    try {
      mutator.fillEmptiesBounded(0, 2);
    } catch (VectorOverflowException e) {
      fail();
    }
    value = makeValue("bar");
    mutator.setSafe(2, value, 0, value.length);

    visualize(vector, 3);
    verifyOffsets(vector.getOffsetVector(), new int[] {0, 3, 3, 6});
    vector.close();
  }

  @Test
  public void testInt() {
    @SuppressWarnings("resource")
    IntVector vector = new IntVector(makeField("a", MinorType.INT, DataMode.REQUIRED), fixture.allocator());
    vector.allocateNew( );

    // Create 1, 0, 2, but omit the 0.

    IntVector.Mutator mutator = vector.getMutator();
    mutator.setSafe(0, 1);

    mutator.setSafe(2, 3);

    visualize(vector, 3);
    vector.close();
  }

  @Test
  public void testRepeatedVarChar() {
    @SuppressWarnings("resource")
    RepeatedVarCharVector vector = new RepeatedVarCharVector(makeField("a", MinorType.VARCHAR, DataMode.REPEATED), fixture.allocator());
    vector.allocateNew( );

    // Create "foo", null, "bar", but omit the null.

    RepeatedVarCharVector.Mutator mutator = vector.getMutator();
    mutator.startNewValue(0);
    byte[] value = makeValue( "a" );
    mutator.addSafe(0, value, 0, value.length);
    value = makeValue( "b" );
    mutator.addSafe(0, value, 0, value.length);

    // Work around: test fails without this. But, only the new column writers
    // call this method.

    try {
      mutator.fillEmptiesBounded(0, 2);
    } catch (VectorOverflowException e) {
      fail();
    }
    mutator.startNewValue(2);
    value = makeValue( "c" );
    mutator.addSafe(2, value, 0, value.length);
    value = makeValue( "d" );
    mutator.addSafe(2, value, 0, value.length);

    visualize(vector, 3);
    verifyOffsets(vector.getOffsetVector(), new int[] {0, 2, 2, 4});
    verifyOffsets(vector.getDataVector().getOffsetVector(), new int[] {0, 1, 2, 3, 4});
    vector.close();
  }

  private void visualize(RepeatedVarCharVector vector, int valueCount) {
    visualize("Array Offsets", vector.getOffsetVector(), valueCount + 1);
    visualize(vector.getDataVector(), vector.getOffsetVector().getAccessor().get(valueCount));
  }

  private void visualize(IntVector vector, int valueCount) {
    System.out.print("Values: [");
    IntVector.Accessor accessor = vector.getAccessor();
    for (int i = 0; i < valueCount; i++) {
      if (i > 0) { System.out.print(" "); }
      System.out.print(accessor.get(i));
    }
    System.out.println("]");
  }

  private void visualize(NullableVarCharVector vector, int valueCount) {
    visualize("Is-set", vector.getAccessor(), valueCount);
    visualize(vector.getValuesVector(), valueCount);
  }

  private void visualize(VarCharVector vector, int valueCount) {
    visualize("Offsets", vector.getOffsetVector(), valueCount + 1);
    visualize("Data", vector.getBuffer(), vector.getOffsetVector().getAccessor().get(valueCount));
  }

  private void visualize(String label, UInt4Vector offsetVector,
      int valueCount) {
    System.out.print(label + ": [");
    UInt4Vector.Accessor accessor = offsetVector.getAccessor();
    for (int i = 0; i < valueCount; i++) {
      if (i > 0) { System.out.print(" "); }
      System.out.print(accessor.get(i));
    }
    System.out.println("]");
  }

  private void visualize(String label, DrillBuf buffer, int valueCount) {
    System.out.print(label + ": [");
    for (int i = 0; i < valueCount; i++) {
      if (i > 0) { System.out.print(" "); }
      System.out.print((char) buffer.getByte(i));
    }
    System.out.println("]");
  }

  private void visualize(String label, BaseDataValueVector.BaseAccessor accessor, int valueCount) {
    System.out.print(label + ": [");
    for (int i = 0; i < valueCount; i++) {
      if (i > 0) { System.out.print(" "); }
      System.out.print(accessor.isNull(i) ? 0 : 1);
    }
    System.out.println("]");
  }

  private void verifyOffsets(UInt4Vector offsetVector, int[] expected) {
    UInt4Vector.Accessor accessor = offsetVector.getAccessor();
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], accessor.get(i));
    }
  }

  /**
   * Create a test value. Works only for the ASCII subset of characters, obviously.
   * @param string
   * @return
   */
  private byte[] makeValue(String string) {
    byte value[] = new byte[string.length()];
    for (int i = 0; i < value.length; i++) {
      value[i] = (byte) string.charAt(i);
    }
    return value;
  }

}
