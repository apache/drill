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
package org.apache.drill.exec.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.RecordBatchSizer.ColumnSize;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.Test;

public class TestRecordBatchSizer extends SubOperatorTest {
  private final int testRowCount = 1000;
  private final int testRowCountPowerTwo = 2048;


  private void verifyColumnValues(ColumnSize column, int stdDataSizePerEntry, int stdNetSizePerEntry,
                                  int dataSizePerEntry, int netSizePerEntry, int totalDataSize,
                                  int totalNetSize, int valueCount, int elementCount,
                                  int estElementCountPerArray, boolean isVariableWidth) {
    assertNotNull(column);

    assertEquals(stdDataSizePerEntry, column.getStdDataSizePerEntry());
    assertEquals(stdNetSizePerEntry, column.getStdNetSizePerEntry());

    assertEquals(dataSizePerEntry, column.getDataSizePerEntry());
    assertEquals(netSizePerEntry, column.getNetSizePerEntry());

    assertEquals(totalDataSize, column.getTotalDataSize());
    assertEquals(totalNetSize, column.getTotalNetSize());

    assertEquals(valueCount, column.getValueCount());
    assertEquals(elementCount, column.getElementCount());

    assertEquals(estElementCountPerArray, column.getCardinality(), 0.01);
    assertEquals(isVariableWidth, column.isVariableWidth());
  }

  @Test
  public void testSizerFixedWidth() {
    BatchSchema schema = new SchemaBuilder().add("a", MinorType.BIGINT).add("b", MinorType.FLOAT8).build();
    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    for (long i = 0; i < 10; i++) {
      builder.addRow(i, i * 0.1);
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(2, sizer.columns().size());

    ColumnSize aColumn = sizer.columns().get("a");

    /**
     * stdDataSize:8, stdNetSize:8, dataSizePerEntry:8, netSizePerEntry:8,
     * totalDataSize:8*10, totalNetSize:8*10, valueCount:10,
     * elementCount:10, estElementCountPerArray:1, isVariableWidth:false
     */
    verifyColumnValues(aColumn, 8, 8, 8, 8, 80, 80, 10, 10, 1, false);

    ColumnSize bColumn = sizer.columns().get("b");
    verifyColumnValues(bColumn, 8, 8, 8, 8, 80, 80, 10, 10, 1,false);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();
      RecordBatchSizer.ColumnSize colSize = sizer.getColumn(v.getField().getName());

      // Allocates to nearest power of two
      colSize.allocateVector(v, testRowCount);
      assertEquals((Integer.highestOneBit(testRowCount) << 1), v.getValueCapacity());
      v.clear();

      // Allocates the same as value passed since it is already power of two.
      colSize.allocateVector(v, testRowCountPowerTwo);
      assertEquals(testRowCountPowerTwo, v.getValueCapacity());
      v.clear();

      // Allocate for max rows.
      colSize.allocateVector(v, ValueVector.MAX_ROW_COUNT);
      assertEquals(ValueVector.MAX_ROW_COUNT, v.getValueCapacity());
      v.clear();

      // Allocate for 0 rows. should atleast do allocation for 1 row.
      colSize.allocateVector(v,  0);
      assertEquals(ValueVector.MIN_ROW_COUNT, v.getValueCapacity());
      v.clear();
    }

    rows.clear();
    empty.clear();
  }


  @Test
  public void testSizerRepeatedFixedWidth() {
    BatchSchema schema = new SchemaBuilder().addArray("a", MinorType.BIGINT).addArray("b", MinorType.FLOAT8).build();
    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    for (long i = 0; i < 10; i++) {
      builder.addRow(new long[] {1, 2, 3, 4, 5}, new double[] {i*0.1, i*0.1, i*0.1, i*0.2, i*0.3});
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(2, sizer.columns().size());

    /**
     * stdDataSize:8*10, stdNetSize:8*10+4, dataSizePerEntry:5*8, netSizePerEntry:5*8+4,
     * totalDataSize:5*8*10, totalNetSize:5*8*10+5*8, valueCount:10,
     * elementCount:50, estElementCountPerArray:5, isVariableWidth:false
     */
    verifyColumnValues(sizer.columns().get("a"),
      80, 84, 40, 44, 400, 440, 10, 50, 5, false);

    verifyColumnValues(sizer.columns().get("b"),
      80, 84, 40, 44, 400, 440, 10, 50, 5, false);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    UInt4Vector offsetVector;
    ValueVector dataVector;

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();
      RecordBatchSizer.ColumnSize colSize = sizer.getColumn(v.getField().getName());

      // Allocates to nearest power of two
      colSize.allocateVector(v, testRowCount);
      offsetVector = ((RepeatedValueVector) v).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());
      dataVector = ((RepeatedValueVector) v).getDataVector();
      assertEquals(Integer.highestOneBit((testRowCount * 5)  << 1), dataVector.getValueCapacity());
      v.clear();

      // Allocates the same as value passed since it is already power of two.
      // -1 is done for adjustment needed for offset vector.
      colSize.allocateVector(v, testRowCountPowerTwo - 1);
      offsetVector = ((RepeatedValueVector) v).getOffsetVector();
      assertEquals(testRowCountPowerTwo, offsetVector.getValueCapacity());
      dataVector = ((RepeatedValueVector) v).getDataVector();
      assertEquals(Integer.highestOneBit((testRowCountPowerTwo -1) * 5) << 1, dataVector.getValueCapacity());
      v.clear();

      // Allocate for max rows.
      colSize.allocateVector(v, ValueVector.MAX_ROW_COUNT - 1);
      offsetVector = ((RepeatedValueVector) v).getOffsetVector();
      assertEquals(ValueVector.MAX_ROW_COUNT, offsetVector.getValueCapacity());
      dataVector = ((RepeatedValueVector) v).getDataVector();
      assertEquals(Integer.highestOneBit(((ValueVector.MAX_ROW_COUNT - 1)* 5) << 1), dataVector.getValueCapacity());
      v.clear();

      // Allocate for 0 rows. should atleast do allocation for 1 row.
      colSize.allocateVector(v, 0);
      offsetVector = ((RepeatedValueVector) v).getOffsetVector();
      assertEquals(ValueVector.MIN_ROW_COUNT + 1, offsetVector.getValueCapacity());
      dataVector = ((RepeatedValueVector) v).getDataVector();
      assertEquals(ValueVector.MIN_ROW_COUNT, dataVector.getValueCapacity());
      v.clear();
    }

    empty.clear();
    rows.clear();
  }

  @Test
  public void testSizerNullableFixedWidth() {
    BatchSchema schema = new SchemaBuilder().addNullable("a", MinorType.BIGINT).addNullable("b", MinorType.FLOAT8).build();
    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    for (long i = 0; i < 10; i++) {
      builder.addRow(i, i*0.1);
    }

    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(2, sizer.columns().size());

    ColumnSize aColumn = sizer.columns().get("a");
    ColumnSize bColumn = sizer.columns().get("b");

    /**
     * stdDataSize:8, stdNetSize:8+1, dataSizePerEntry:8, netSizePerEntry:8+1,
     * totalDataSize:8*10, totalNetSize:(8+1)*10, valueCount:10,
     * elementCount:10, estElementCountPerArray:1, isVariableWidth:false
     */
    verifyColumnValues(aColumn,
      8, 9, 8, 9, 80, 90, 10, 10, 1, false);

    verifyColumnValues(bColumn,
      8, 9, 8, 9, 80, 90, 10, 10, 1, false);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    ValueVector bitVector, valueVector;

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();
      RecordBatchSizer.ColumnSize colSize = sizer.getColumn(v.getField().getName());

      // Allocates to nearest power of two
      colSize.allocateVector(v, testRowCount);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), bitVector.getValueCapacity());
      valueVector = ((NullableVector) v).getValuesVector();
      assertEquals(Integer.highestOneBit(testRowCount << 1), valueVector.getValueCapacity());
      v.clear();

      // Allocates the same as value passed since it is already power of two.
      colSize.allocateVector(v, testRowCountPowerTwo);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals(testRowCountPowerTwo, bitVector.getValueCapacity());
      valueVector = ((NullableVector) v).getValuesVector();
      assertEquals(testRowCountPowerTwo, valueVector.getValueCapacity());
      v.clear();

      // Allocate for max rows.
      colSize.allocateVector(v, ValueVector.MAX_ROW_COUNT);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals(ValueVector.MAX_ROW_COUNT, bitVector.getValueCapacity());
      valueVector = ((NullableVector) v).getValuesVector();
      assertEquals(ValueVector.MAX_ROW_COUNT, valueVector.getValueCapacity());
      v.clear();

      // Allocate for 0 rows. should atleast do allocation for 1 row.
      colSize.allocateVector(v, 0);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals(ValueVector.MIN_ROW_COUNT, bitVector.getValueCapacity());
      valueVector = ((NullableVector) v).getValuesVector();
      assertEquals(ValueVector.MIN_ROW_COUNT, valueVector.getValueCapacity());
      v.clear();
    }

    empty.clear();
    rows.clear();
  }

  @Test
  public void testSizerVariableWidth() {
    BatchSchema schema = new SchemaBuilder().add("a", MinorType.VARCHAR).build();
    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    StringBuilder stringBuilder = new StringBuilder();

    // a, aa, aaa, ... aaaaaaaaaa. totalSize = (10*11)/2 = 55
    for (long i = 0; i < 10; i++) {
      stringBuilder.append("a");
      builder.addRow(stringBuilder.toString());
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(1, sizer.columns().size());

    ColumnSize aColumn = sizer.columns().get("a");

    /**
     * stdDataSize:50, stdNetSize:50+4, dataSizePerEntry:8, netSizePerEntry:8,
     * totalDataSize:(10*11)/2, totalNetSize:(10*11)/2 + 4*10, valueCount:10,
     * elementCount:10, estElementCountPerArray:1, isVariableWidth:true
     */
    verifyColumnValues(aColumn,
      50, 54, 6, 10, 55, 95, 10, 10, 1, true);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    UInt4Vector offsetVector;

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();
      RecordBatchSizer.ColumnSize colSize = sizer.getColumn(v.getField().getName());

      // Allocates to nearest power of two
      colSize.allocateVector(v, testRowCount);
      offsetVector = ((VariableWidthVector)v).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCount  << 1)-1, v.getValueCapacity());
      v.clear();

      // Allocates the same as value passed since it is already power of two.
      // -1 is done for adjustment needed for offset vector.
      colSize.allocateVector(v, testRowCountPowerTwo - 1);
      offsetVector = ((VariableWidthVector)v).getOffsetVector();
      assertEquals(testRowCountPowerTwo, offsetVector.getValueCapacity());
      assertEquals(testRowCountPowerTwo - 1, v.getValueCapacity());
      v.clear();

      // Allocate for max rows.
      colSize.allocateVector(v, ValueVector.MAX_ROW_COUNT - 1);
      offsetVector = ((VariableWidthVector)v).getOffsetVector();
      assertEquals(ValueVector.MAX_ROW_COUNT, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MAX_ROW_COUNT - 1, v.getValueCapacity());
      v.clear();

      // Allocate for 0 rows. should atleast do allocation for 1 row.
      colSize.allocateVector(v, 0);
      offsetVector = ((VariableWidthVector)v).getOffsetVector();
      assertEquals(ValueVector.MIN_ROW_COUNT + 1, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MIN_ROW_COUNT, v.getValueCapacity());
      v.clear();
    }

    empty.clear();
    rows.clear();
  }


  @Test
  public void testSizerRepeatedVariableWidth() {
    BatchSchema schema = new SchemaBuilder().addArray("b", MinorType.VARCHAR).build();
    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    // size = (5*6)/2 = 15
    String[] newString = new String [] {"a", "aa", "aaa", "aaaa", "aaaaa"};

    for (long i = 0; i < 10; i++) {
      builder.addRow((Object) (newString));
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(1, sizer.columns().size());

    ColumnSize bColumn = sizer.columns().get("b");

    /**
     * stdDataSize:50*10, stdNetSize:50*10+4*10+4, dataSizePerEntry:(5*6)/2, netSizePerEntry:(5*6)/2+5*4+4,
     * totalDataSize:(5*6)/2 * 10, totalNetSize: ((5*6)/2+5*4+4)*10, valueCount:10,
     * elementCount:50, estElementCountPerArray:5, isVariableWidth:true
     */
    verifyColumnValues(bColumn, 500, 544, 15, 39, 150, 390, 10, 50, 5,true);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();
      RecordBatchSizer.ColumnSize colSize = sizer.getColumn(v.getField().getName());

      // Allocates to nearest power of two
      colSize.allocateVector(v, testRowCount - 1);
      UInt4Vector offsetVector = ((RepeatedValueVector)v).getOffsetVector();
      assertEquals(Integer.highestOneBit(testRowCount) << 1, offsetVector.getValueCapacity());
      VariableWidthVector vwVector = ((VariableWidthVector) ((RepeatedValueVector) v).getDataVector());
      offsetVector = vwVector.getOffsetVector();
      assertEquals((Integer.highestOneBit((testRowCount-1) * 5) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit((testRowCount-1) * 5  << 1)-1, vwVector.getValueCapacity());
      v.clear();

      // Allocates the same as value passed since it is already power of two.
      colSize.allocateVector(v, testRowCountPowerTwo);
      offsetVector = ((RepeatedValueVector)v).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCountPowerTwo) << 1), offsetVector.getValueCapacity());
      vwVector = ((VariableWidthVector) ((RepeatedValueVector) v).getDataVector());
      offsetVector = vwVector.getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCountPowerTwo * 5) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCountPowerTwo * 5  << 1)-1, vwVector.getValueCapacity());
      v.clear();

      // Allocate for max rows.
      colSize.allocateVector(v, ValueVector.MAX_ROW_COUNT);
      offsetVector = ((RepeatedValueVector)v).getOffsetVector();
      assertEquals(ValueVector.MAX_ROW_COUNT << 1, offsetVector.getValueCapacity());
      vwVector = ((VariableWidthVector) ((RepeatedValueVector) v).getDataVector());
      offsetVector = vwVector.getOffsetVector();
      assertEquals((Integer.highestOneBit(ValueVector.MAX_ROW_COUNT * 5) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(ValueVector.MAX_ROW_COUNT * 5  << 1)-1, vwVector.getValueCapacity());
      v.clear();

      // Allocate for 0 rows. should atleast do allocation for 1 row.
      colSize.allocateVector(v,  0);
      offsetVector = ((RepeatedValueVector)v).getOffsetVector();
      assertEquals(ValueVector.MIN_ROW_COUNT + 1, offsetVector.getValueCapacity());
      vwVector = ((VariableWidthVector) ((RepeatedValueVector) v).getDataVector());
      offsetVector = vwVector.getOffsetVector();
      assertEquals(ValueVector.MIN_ROW_COUNT + 1, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MIN_ROW_COUNT, vwVector.getValueCapacity());
      v.clear();
    }

    empty.clear();
    rows.clear();
  }


  @Test
  public void testSizerNullableVariableWidth() {
    BatchSchema schema = new SchemaBuilder().addNullable("b", MinorType.VARCHAR).build();
    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    StringBuilder stringBuilder = new StringBuilder();

    for (long i = 0; i < 10; i++) {
      stringBuilder.append("a");
      builder.addRow( (Object) stringBuilder.toString());
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(1, sizer.columns().size());

    /**
     * stdDataSize:50, stdNetSize:50+4+1, dataSizePerEntry:ceil((10*11)/2)*10),
     * netSizePerEntry: dataSizePerEntry+4+1,
     * totalDataSize:(10*11)/2, totalNetSize: (10*11)/2 + (4*10) + (1*10),
     * valueCount:10,
     * elementCount:10, estElementCountPerArray:1, isVariableWidth:true
     */
    verifyColumnValues(sizer.columns().get("b"),
      50, 55, 6, 11, 55, 105, 10, 10, 1,true);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    ValueVector bitVector, valueVector;
    VariableWidthVector vwVector;
    UInt4Vector offsetVector;

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();
      RecordBatchSizer.ColumnSize colSize = sizer.getColumn(v.getField().getName());

      // Allocates to nearest power of two
      colSize.allocateVector(v, testRowCount);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), bitVector.getValueCapacity());
      vwVector = (VariableWidthVector) ((NullableVector) v).getValuesVector();
      offsetVector = vwVector.getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCount  << 1)-1, vwVector.getValueCapacity());

      // Allocates the same as value passed since it is already power of two.
      colSize.allocateVector(v,  testRowCountPowerTwo-1);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals(Integer.highestOneBit(testRowCountPowerTwo), bitVector.getValueCapacity());
      vwVector = (VariableWidthVector) ((NullableVector) v).getValuesVector();
      offsetVector = vwVector.getOffsetVector();
      assertEquals(testRowCountPowerTwo, offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCountPowerTwo)-1, vwVector.getValueCapacity());

      // Allocate for max rows.
      colSize.allocateVector(v, ValueVector.MAX_ROW_COUNT-1);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals((Integer.highestOneBit(ValueVector.MAX_ROW_COUNT)), bitVector.getValueCapacity());
      vwVector = (VariableWidthVector) ((NullableVector) v).getValuesVector();
      offsetVector = vwVector.getOffsetVector();
      assertEquals(ValueVector.MAX_ROW_COUNT, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MAX_ROW_COUNT-1, vwVector.getValueCapacity());

      // Allocate for 0 rows. should atleast do allocation for 1 row.
      colSize.allocateVector(v,  0);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals((Integer.highestOneBit(ValueVector.MIN_ROW_COUNT)), bitVector.getValueCapacity());
      vwVector = (VariableWidthVector) ((NullableVector) v).getValuesVector();
      offsetVector = vwVector.getOffsetVector();
      assertEquals(ValueVector.MIN_ROW_COUNT+1, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MIN_ROW_COUNT, vwVector.getValueCapacity());
    }

    empty.clear();
    rows.clear();
  }


  @Test
  public void testSizerMap() {
    BatchSchema schema = new SchemaBuilder()
      .addMap("map")
        .add("key", MinorType.INT)
        .add("value", MinorType.VARCHAR)
      .resumeSchema()
      .build();

    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    for (int i = 0; i < 10; i++) {
      builder.addRow((Object) (new Object[] {10, "a"}));
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(1, sizer.columns().size());

    /**
     * stdDataSize:50+4, stdNetSize:50+4+4, dataSizePerEntry:4+1,
     * netSizePerEntry: 4+1+4,
     * totalDataSize:5*10, totalNetSize:4*10+4*10+1*10,
     * valueCount:10,
     * elementCount:10, estElementCountPerArray:1, isVariableWidth:true
     */
    verifyColumnValues(sizer.columns().get("map"), 54, 58, 5, 9, 50, 90, 10, 10, 1, false);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();
      RecordBatchSizer.ColumnSize colSize = sizer.getColumn(v.getField().getName());

      // Allocates to nearest power of two
      colSize.allocateVector(v, testRowCount);
      MapVector mapVector = (MapVector)v;
      ValueVector keyVector = mapVector.getChild("key");
      ValueVector valueVector1 = mapVector.getChild("value");
      assertEquals((Integer.highestOneBit(testRowCount) << 1), keyVector.getValueCapacity());
      UInt4Vector offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCount  << 1)-1, valueVector1.getValueCapacity());

      // Allocates the same as value passed since it is already power of two.
      colSize.allocateVector(v, testRowCountPowerTwo-1);
      mapVector = (MapVector)v;
      keyVector = mapVector.getChild("key");
      valueVector1 = mapVector.getChild("value");
      assertEquals((Integer.highestOneBit(testRowCountPowerTwo -1) << 1), keyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals(testRowCountPowerTwo, offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCountPowerTwo)-1, valueVector1.getValueCapacity());

      // Allocate for max rows.
      colSize.allocateVector(v, ValueVector.MAX_ROW_COUNT -1);
      mapVector = (MapVector)v;
      keyVector = mapVector.getChild("key");
      valueVector1 = mapVector.getChild("value");
      assertEquals(ValueVector.MAX_ROW_COUNT, keyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals(ValueVector.MAX_ROW_COUNT, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MAX_ROW_COUNT-1, valueVector1.getValueCapacity());

      // Allocate for 0 rows. should atleast do allocation for 1 row.
      colSize.allocateVector(v, 0);
      mapVector = (MapVector)v;
      keyVector = mapVector.getChild("key");
      valueVector1 = mapVector.getChild("value");
      assertEquals(ValueVector.MIN_ROW_COUNT, keyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals(ValueVector.MIN_ROW_COUNT+1, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MIN_ROW_COUNT, valueVector1.getValueCapacity());

      v.clear();
    }

    empty.clear();
    rows.clear();

  }

  @Test
  public void testSizerRepeatedMap() {
    BatchSchema schema = new SchemaBuilder().addMapArray("map").
      add("key", MinorType.INT).
      add("value", MinorType.VARCHAR).
      resumeSchema().build();

    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    for (int i = 0; i < 10; i++) {
      builder.addRow((Object) new Object[] {
        new Object[] {110, "a"},
        new Object[] {120, "b"}});
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(1, sizer.columns().size());

    /**
     * stdDataSize:50+4, stdNetSize:50+4+4+4, dataSizePerEntry:(4+1)*2,
     * netSizePerEntry: 4*2+1*2+4*2+4,
     * totalDataSize:5*2*10, totalNetSize:netSizePerEntry*2,
     * valueCount:10,
     * elementCount:20, estElementCountPerArray:2, isVariableWidth:true
     */
    verifyColumnValues(sizer.columns().get("map"), 54,62, 10, 22, 100, 220, 10, 20, 2, false);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    UInt4Vector offsetVector;

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();
      RecordBatchSizer.ColumnSize colSize = sizer.getColumn(v.getField().getName());

      // Allocates to nearest power of two
      colSize.allocateVector(v, testRowCount);
      RepeatedMapVector mapVector = (RepeatedMapVector)v;

      offsetVector = ((RepeatedValueVector)mapVector).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());

      ValueVector keyVector = mapVector.getChild("key");
      ValueVector valueVector1 = mapVector.getChild("value");
      assertEquals(((Integer.highestOneBit(testRowCount) << 1) * 2), keyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1)*2, offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCount  << 1)*2 - 1, valueVector1.getValueCapacity());

      // Allocates the same as value passed since it is already power of two.
      colSize.allocateVector(v, testRowCountPowerTwo-1);
      mapVector = (RepeatedMapVector)v;

      offsetVector = ((RepeatedValueVector)mapVector).getOffsetVector();
      assertEquals(testRowCountPowerTwo, offsetVector.getValueCapacity());

      keyVector = mapVector.getChild("key");
      valueVector1 = mapVector.getChild("value");
      assertEquals(testRowCountPowerTwo*2, keyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals(testRowCountPowerTwo*2, offsetVector.getValueCapacity());
      assertEquals(testRowCountPowerTwo*2 - 1, valueVector1.getValueCapacity());

      // Allocate for max rows.
      colSize.allocateVector(v,  ValueVector.MAX_ROW_COUNT -1);
      mapVector = (RepeatedMapVector)v;

      offsetVector = ((RepeatedValueVector)mapVector).getOffsetVector();
      assertEquals(ValueVector.MAX_ROW_COUNT, offsetVector.getValueCapacity());

      keyVector = mapVector.getChild("key");
      valueVector1 = mapVector.getChild("value");
      assertEquals(ValueVector.MAX_ROW_COUNT * 2, keyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals(ValueVector.MAX_ROW_COUNT * 2, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MAX_ROW_COUNT * 2 - 1, valueVector1.getValueCapacity());

      // Allocate for 0 rows. should atleast do allocation for 1 row.
      colSize.allocateVector(v, 0);
      mapVector = (RepeatedMapVector)v;

      offsetVector = ((RepeatedValueVector)mapVector).getOffsetVector();
      assertEquals(ValueVector.MIN_ROW_COUNT, offsetVector.getValueCapacity());

      keyVector = mapVector.getChild("key");
      valueVector1 = mapVector.getChild("value");
      assertEquals(ValueVector.MIN_ROW_COUNT, keyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals(ValueVector.MIN_ROW_COUNT+1, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MIN_ROW_COUNT, valueVector1.getValueCapacity());

      v.clear();
    }

    empty.clear();
    rows.clear();
  }

  @Test
  public void testSizerNestedMap() {
    BatchSchema schema = new SchemaBuilder()
      .addMap("map")
        .add("key", MinorType.INT)
        .add("value", MinorType.VARCHAR)
        .addMap("childMap")
          .add("childKey", MinorType.INT)
          .add("childValue", MinorType.VARCHAR)
          .resumeMap()
       .resumeSchema()
      .build();

    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    for (int i = 0; i < 10; i++) {
      builder.addRow((Object) (new Object[] {10, "a", new Object[] {5, "b"}}));
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(1, sizer.columns().size());

    /**
     * stdDataSize:(50+4)*2, stdNetSize:(50+4)*2+4+4, dataSizePerEntry:(4+1)*2,
     * netSizePerEntry: 4*2+1*2+4*2,
     * totalDataSize:5*2*10, totalNetSize:netSizePerEntry*2,
     * valueCount:10,
     * elementCount:10, estElementCountPerArray:1, isVariableWidth:true
     */
    verifyColumnValues(sizer.columns().get("map"), 108, 116, 10, 18, 100, 180, 10, 10, 1, false);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    UInt4Vector offsetVector;

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();
      RecordBatchSizer.ColumnSize colSize = sizer.getColumn(v.getField().getName());

      // Allocates to nearest power of two
      colSize.allocateVector(v, testRowCount);
      MapVector mapVector = (MapVector)v;
      ValueVector keyVector = mapVector.getChild("key");
      ValueVector valueVector1 = mapVector.getChild("value");
      assertEquals((Integer.highestOneBit(testRowCount) << 1), keyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCount  << 1)-1, valueVector1.getValueCapacity());
      MapVector childMapVector = (MapVector) mapVector.getChild("childMap");
      ValueVector childKeyVector = childMapVector.getChild("childKey");
      ValueVector childValueVector1 = childMapVector.getChild("childValue");
      assertEquals((Integer.highestOneBit(testRowCount) << 1), childKeyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCount  << 1)-1, childValueVector1.getValueCapacity());

      // Allocates the same as value passed since it is already power of two.
      colSize.allocateVector(v, testRowCountPowerTwo-1);
      mapVector = (MapVector)v;
      keyVector = mapVector.getChild("key");
      valueVector1 = mapVector.getChild("value");
      assertEquals(testRowCountPowerTwo, keyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals(testRowCountPowerTwo, offsetVector.getValueCapacity());
      assertEquals(testRowCountPowerTwo-1, valueVector1.getValueCapacity());
      childMapVector = (MapVector) mapVector.getChild("childMap");
      childKeyVector = childMapVector.getChild("childKey");
      childValueVector1 = childMapVector.getChild("childValue");
      assertEquals(testRowCountPowerTwo, childKeyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals(testRowCountPowerTwo, offsetVector.getValueCapacity());
      assertEquals(testRowCountPowerTwo-1, childValueVector1.getValueCapacity());

      // Allocate for max rows.
      colSize.allocateVector(v, ValueVector.MAX_ROW_COUNT-1);
      mapVector = (MapVector)v;
      keyVector = mapVector.getChild("key");
      valueVector1 = mapVector.getChild("value");
      assertEquals(ValueVector.MAX_ROW_COUNT, keyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals(ValueVector.MAX_ROW_COUNT, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MAX_ROW_COUNT-1, valueVector1.getValueCapacity());
      childMapVector = (MapVector) mapVector.getChild("childMap");
      childKeyVector = childMapVector.getChild("childKey");
      childValueVector1 = childMapVector.getChild("childValue");
      assertEquals(ValueVector.MAX_ROW_COUNT, childKeyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals(ValueVector.MAX_ROW_COUNT, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MAX_ROW_COUNT-1, childValueVector1.getValueCapacity());

      // Allocate for 0 rows. should atleast do allocation for 1 row.
      colSize.allocateVector(v,  0);
      mapVector = (MapVector)v;
      keyVector = mapVector.getChild("key");
      valueVector1 = mapVector.getChild("value");
      assertEquals(ValueVector.MIN_ROW_COUNT, keyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals(ValueVector.MIN_ROW_COUNT+1, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MIN_ROW_COUNT, valueVector1.getValueCapacity());
      childMapVector = (MapVector) mapVector.getChild("childMap");
      childKeyVector = childMapVector.getChild("childKey");
      childValueVector1 = childMapVector.getChild("childValue");
      assertEquals(ValueVector.MIN_ROW_COUNT, childKeyVector.getValueCapacity());
      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals(ValueVector.MIN_ROW_COUNT+1, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MIN_ROW_COUNT, childValueVector1.getValueCapacity());

      v.clear();
    }

    empty.clear();
    rows.clear();

  }

}