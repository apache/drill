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

package org.apache.drill.exec.store.googlesheets;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.googlesheets.columns.GoogleSheetsColumnRange;
import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestGoogleSheetUtils {

  @Test
  public void testSchemaInference() {
    List<List<Object>> data = new ArrayList<>();
    List<Object> row = new ArrayList<>(Arrays.asList("Col1", "Col2", "Col3"));
    SchemaPath sp = new SchemaPath(SchemaPath.parseFromString("Col1"));
    List<SchemaPath> projectedColumns = new ArrayList<>(
      Arrays.asList(
        new SchemaPath(SchemaPath.parseFromString("Col1")),
        new SchemaPath(SchemaPath.parseFromString("Col2")),
        new SchemaPath(SchemaPath.parseFromString("Col3"))
      )
    );
    data.add(row);

    row = new ArrayList<>(Arrays.asList("Rosaline Thales", 1));
    data.add(row);

    row = new ArrayList<>(Arrays.asList("Abdolhossein Detlev", "2.0001", "2020-04-30"));
    data.add(row);

    row = new ArrayList<>(Arrays.asList("Yosuke  Simon", "", "2020-05-22"));
    data.add(row);

    row = new ArrayList<>(Arrays.asList("", "4", "2020-06-30"));
    data.add(row);

    Map<String, GoogleSheetsColumn> columnMap = GoogleSheetsUtils.getColumnMap(data, projectedColumns, false);
    assertEquals(3, columnMap.size());
    assertEquals(MinorType.VARCHAR, columnMap.get("Col1").getDrillDataType());
    assertEquals(MinorType.FLOAT8, columnMap.get("Col2").getDrillDataType());
    assertEquals(MinorType.DATE, columnMap.get("Col3").getDrillDataType());
  }

  @Test
  public void testBuildSchema() {
    List<List<Object>> data = new ArrayList<>();
    List<SchemaPath> projectedColumns = new ArrayList<>(
      Arrays.asList(
        new SchemaPath(SchemaPath.parseFromString("Col1")),
        new SchemaPath(SchemaPath.parseFromString("Col2")),
        new SchemaPath(SchemaPath.parseFromString("Col3"))
      )
    );
    List<Object> row = new ArrayList<>(Arrays.asList("Col1", "Col2", "Col3"));
    data.add(row);

    row = new ArrayList<>(Arrays.asList("Rosaline Thales", 1));
    data.add(row);

    row = new ArrayList<>(Arrays.asList("Abdolhossein Detlev", "2.0001", "2020-04-30"));
    data.add(row);

    row = new ArrayList<>(Arrays.asList("Yosuke  Simon", "", "2020-05-22"));
    data.add(row);

    row = new ArrayList<>(Arrays.asList("", "4", "2020-06-30"));
    data.add(row);

    Map<String, GoogleSheetsColumn> columnMap = GoogleSheetsUtils.getColumnMap(data, projectedColumns, false);
    TupleMetadata actualSchema = GoogleSheetsUtils.buildSchema(columnMap);

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("Col1", MinorType.VARCHAR)
      .addNullable("Col2", MinorType.FLOAT8)
      .addNullable("Col3", MinorType.DATE)
      .build();

    assertEquals(actualSchema, expectedSchema);
  }

  @Test
  public void testColumnProjector() {
    Map<String, GoogleSheetsColumn> columnMap = new LinkedHashMap<>();
    columnMap.put("f1", new GoogleSheetsColumn("f1", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 0, 0));
    columnMap.put("f2", new GoogleSheetsColumn("f2", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 1, 1));
    columnMap.put("f3", new GoogleSheetsColumn("f3", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 2, 2));
    columnMap.put("f4", new GoogleSheetsColumn("f4", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 3, 3));
    columnMap.put("f6", new GoogleSheetsColumn("f6", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 5, 4));
    columnMap.put("f9", new GoogleSheetsColumn("f9", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 8, 5));
    columnMap.put("f10", new GoogleSheetsColumn("f10", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 9, 6));

    List<GoogleSheetsColumnRange> results = GoogleSheetsUtils.getProjectedRanges("Sheet1", columnMap);
    assertEquals(3, results.size());
  }

  @Test
  public void testColumnProjectorWithSingleColumns() {
    Map<String, GoogleSheetsColumn> columnMap = new LinkedHashMap<>();
    columnMap.put("f1", new GoogleSheetsColumn("f1", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 0,0));
    columnMap.put("f2", new GoogleSheetsColumn("f2", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 2,1));
    columnMap.put("f3", new GoogleSheetsColumn("f3", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 4,2));
    columnMap.put("f4", new GoogleSheetsColumn("f4", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 6, 3));
    columnMap.put("f6", new GoogleSheetsColumn("f6", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 7, 4));
    columnMap.put("f9", new GoogleSheetsColumn("f9", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 8, 5));
    columnMap.put("f10", new GoogleSheetsColumn("f10", GoogleSheetsUtils.DATA_TYPES.UNKNOWN, 9, 6));

    List<GoogleSheetsColumnRange> results = GoogleSheetsUtils.getProjectedRanges("Sheet1", columnMap);
    assertEquals(4, results.size());
  }

  @Test
  public void testColumnConversion() {
    assertEquals("A", GoogleSheetsUtils.columnToLetter(1));
    assertEquals("B", GoogleSheetsUtils.columnToLetter(2));
    assertEquals("AA", GoogleSheetsUtils.columnToLetter(27));
    assertEquals("CV", GoogleSheetsUtils.columnToLetter(100));
    // Close to largest possible column index
    assertEquals("ZWZ", GoogleSheetsUtils.columnToLetter(18200));
  }

  @Test
  public void testA1toIntResolution() {
    assertEquals(1, GoogleSheetsUtils.letterToColumnIndex("A"));
    assertEquals(2, GoogleSheetsUtils.letterToColumnIndex("B"));
    assertEquals(27, GoogleSheetsUtils.letterToColumnIndex("AA"));
    assertEquals(100, GoogleSheetsUtils.letterToColumnIndex("CV"));
    // Close to largest possible column index
    assertEquals(18200, GoogleSheetsUtils.letterToColumnIndex("ZWZ"));
  }
}
