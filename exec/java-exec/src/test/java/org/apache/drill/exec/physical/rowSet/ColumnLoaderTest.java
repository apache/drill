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
package org.apache.drill.exec.physical.rowSet;

import static org.junit.Assert.*;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.bouncycastle.util.Arrays;
import org.junit.Test;

import com.google.common.base.Charsets;

public class ColumnLoaderTest extends SubOperatorTest {

  private static class ColumnLoaderFixture {

    ResultSetLoader rsMutator;
    TupleLoader rootWriter;
    private ColumnLoader colWriter;
    RowSetBuilder rsBuilder;

    public ColumnLoaderFixture(MinorType type, DataMode mode) {
      rsMutator = new ResultSetLoaderImpl(fixture.allocator());
      rootWriter = rsMutator.writer();
      TupleSchema schema = rootWriter.schema();
      MaterializedField field = SchemaBuilder.columnSchema("col", type, mode);
      schema.addColumn(field);
      rsMutator.startBatch();
      colWriter = rootWriter.column(0);
      rsBuilder = fixture.rowSetBuilder(schema.schema());
    }

    public void verify() {
      SingleRowSet expected = rsBuilder.build();
      RowSet actual = fixture.wrap(rsMutator.harvest());
      new RowSetComparison(expected)
        .verifyAndClearAll(actual);
      rsMutator.close();
    }
  }

  public final int ROW_COUNT = 100;
  public final int MAX_MEMBERS = 10;

  public void testScalarInt(MinorType type, boolean nullable, int maxValue) {
    ColumnLoaderFixture loader = new ColumnLoaderFixture(type, nullable ? DataMode.OPTIONAL : DataMode.REQUIRED);

    for (int i = 0; i < ROW_COUNT; i++) {
      loader.rsMutator.startRow();
      if (nullable && i % 5 == 0) {
        loader.colWriter.setNull();
        loader.rsBuilder.addSingleCol(null);
      } else {
        int value = (i * 7) & maxValue;
        loader.colWriter.setInt(value);
        loader.rsBuilder.addSingleCol(value);
      }
      loader.rsMutator.saveRow();
    }

    loader.verify();
  }

  public void testArrayInt(MinorType type, int maxValue) {
    ColumnLoaderFixture loader = new ColumnLoaderFixture(type, DataMode.REPEATED);

    ArrayLoader memberWriter = loader.colWriter.array();
    for (int i = 0; i < ROW_COUNT; i++) {
      loader.rsMutator.startRow();
      int memberCount = (i * 7) % MAX_MEMBERS;
      int expected[] = new int[memberCount];
      for (int j = 0; j < memberCount; j++) {
        int value = (i * MAX_MEMBERS + j) % maxValue;
        memberWriter.setInt(value);
        expected[j] = value;
      }
      loader.rsBuilder.addSingleCol(expected);
      loader.rsMutator.saveRow();
    }

    loader.verify();
  }

  private void testIntModes(MinorType type, int maxValue) {
    testScalarInt(type, false, maxValue);
    testScalarInt(type, true, maxValue);
    testArrayInt(type, maxValue);
  }

  @Test
  public void testIntTypes() {
    testIntModes(MinorType.TINYINT, 127);
    testIntModes(MinorType.BIT, 255);
    testIntModes(MinorType.UINT1, 255);
    testIntModes(MinorType.SMALLINT, 32767);
    testIntModes(MinorType.UINT2, Character.MAX_VALUE);
    testIntModes(MinorType.INT, Integer.MAX_VALUE);
    testIntModes(MinorType.UINT4, Integer.MAX_VALUE);
  }

  public void testLong(MinorType type, boolean nullable, long maxValue) {
    ColumnLoaderFixture loader = new ColumnLoaderFixture(type, nullable ? DataMode.OPTIONAL : DataMode.REQUIRED);

    for (int i = 0; i < ROW_COUNT; i++) {
      loader.rsMutator.startRow();
      if (nullable && i % 5 == 0) {
        loader.colWriter.setNull();
        loader.rsBuilder.addSingleCol(null);
      } else {
        long value = (i * 7) & maxValue;
        loader.colWriter.setLong(value);
        loader.rsBuilder.addSingleCol(value);
      }
      loader.rsMutator.saveRow();
    }

    loader.verify();
  }

  public void testArrayLong(MinorType type, long maxValue) {
    ColumnLoaderFixture loader = new ColumnLoaderFixture(type, DataMode.REPEATED);

    ArrayLoader memberWriter = loader.colWriter.array();
    for (int i = 0; i < ROW_COUNT; i++) {
      loader.rsMutator.startRow();
      int memberCount = (i * 7) % MAX_MEMBERS;
      long expected[] = new long[memberCount];
      for (int j = 0; j < memberCount; j++) {
        long value = (i * MAX_MEMBERS + j) % maxValue;
        memberWriter.setLong(value);
        expected[j] = value;
      }
      loader.rsBuilder.addSingleCol(expected);
      loader.rsMutator.saveRow();
    }

    loader.verify();
  }

  private void testLongModes(MinorType type, long maxValue) {
    testLong(type, false, maxValue);
    testLong(type, true, maxValue);
    testArrayLong(type, maxValue);
  }

  @Test
  public void testLongTypes() {
    testLongModes(MinorType.BIGINT, Long.MAX_VALUE);
    testLongModes(MinorType.UINT8, Long.MAX_VALUE);
  }

  public void testVarchar(boolean nullable) {
    ColumnLoaderFixture loader = new ColumnLoaderFixture(MinorType.VARCHAR, nullable ? DataMode.OPTIONAL : DataMode.REQUIRED);

    for (int i = 0; i < ROW_COUNT; i++) {
      loader.rsMutator.startRow();
      if (nullable && i % 5 == 0) {
        loader.colWriter.setNull();
        loader.rsBuilder.addSingleCol(null);
      } else {
        byte value[] = new byte[17];
        Arrays.fill(value, (byte) (i % 64 + ' '));
        String strValue = new String(value, Charsets.UTF_8);
        loader.rsBuilder.addSingleCol(value);
        if (i % 2 == 0) {
          loader.colWriter.setString(strValue);
        } else {
          loader.colWriter.setBytes(value);
        }
      }
      loader.rsMutator.saveRow();
    }

    loader.verify();
  }

  public void testArrayVarchar() {
    ColumnLoaderFixture loader = new ColumnLoaderFixture(MinorType.VARCHAR, DataMode.REPEATED);

    ArrayLoader memberWriter = loader.colWriter.array();
    for (int i = 0; i < ROW_COUNT; i++) {
      loader.rsMutator.startRow();
      int memberCount = (i * 7) % MAX_MEMBERS;
      String expected[] = new String[memberCount];
      for (int j = 0; j < memberCount; j++) {
        byte value[] = new byte[17];
        Arrays.fill(value, (byte) ((i+j) % 64 + ' '));
        String strValue = new String(value, Charsets.UTF_8);
        if (i % 2 == 0) {
          memberWriter.setString(strValue);
        } else {
          memberWriter.setBytes(value);
        }
        expected[j] = strValue;
      }
      loader.rsBuilder.addSingleCol(expected);
      loader.rsMutator.saveRow();
    }

    loader.verify();
  }

  @Test
  public void testVarchar() {
    testVarchar(false);
    testVarchar(true);
    testArrayVarchar();
  }

  // TODO: Var16Char
  // TODO: VarBinary
  // TODO: Date types
  // TODO: Decimal types

}
