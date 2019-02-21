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
package org.apache.drill.exec.store.avro;

import static org.apache.drill.exec.store.avro.AvroTestUtil.generateDoubleNestedSchema_NoNullValues;
import static org.apache.drill.exec.store.avro.AvroTestUtil.generateLinkedList;
import static org.apache.drill.exec.store.avro.AvroTestUtil.generateMapSchemaComplex_withNullValues;
import static org.apache.drill.exec.store.avro.AvroTestUtil.generateMapSchema_withNullValues;
import static org.apache.drill.exec.store.avro.AvroTestUtil.generateNestedArraySchema;
import static org.apache.drill.exec.store.avro.AvroTestUtil.generateSimpleArraySchema_NoNullValues;
import static org.apache.drill.exec.store.avro.AvroTestUtil.generateSimpleEnumSchema_NoNullValues;
import static org.apache.drill.exec.store.avro.AvroTestUtil.generateSimpleNestedSchema_NoNullValues;
import static org.apache.drill.exec.store.avro.AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues;
import static org.apache.drill.exec.store.avro.AvroTestUtil.generateStringAndUtf8Data;
import static org.apache.drill.exec.store.avro.AvroTestUtil.generateUnionNestedArraySchema_withNullValues;
import static org.apache.drill.exec.store.avro.AvroTestUtil.generateUnionNestedSchema_withNullValues;
import static org.apache.drill.exec.store.avro.AvroTestUtil.generateUnionSchema_WithNonNullValues;
import static org.apache.drill.exec.store.avro.AvroTestUtil.generateUnionSchema_WithNullValues;
import static org.apache.drill.exec.store.avro.AvroTestUtil.write;
import static org.apache.drill.test.TestBuilder.listOf;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.TestRecordWithLogicalTypes;
import org.apache.commons.io.FileUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.work.ExecErrorConstants;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.test.TestBuilder;
import org.junit.Assert;
import org.junit.Test;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

/**
 * Unit tests for Avro record reader.
 */
public class AvroFormatTest extends BaseTestQuery {

  // XXX
  //      1. Need to test nested field names with same name as top-level names for conflict.
  //      2. Avro supports recursive types? Can we test this?

  @Test
  public void testBatchCutoff() throws Exception {
    final AvroTestUtil.AvroTestRecordWriter testSetup = generateSimplePrimitiveSchema_NoNullValues(5000);
    final String file = testSetup.getFileName();
    final String sql = "select a_string, b_int, c_long, d_float, e_double, f_bytes, h_boolean, g_null from dfs.`%s`";
    test(sql, file);
    testBuilder()
        .sqlQuery(sql, file)
        .unOrdered()
        .expectsNumBatches(2)
        .baselineRecords(testSetup.getExpectedRecords())
        .go();
  }

  /**
   * Previously a bug in the Avro table metadata would cause wrong results
   * for some queries on varchar types, as a length was not provided during metadata
   * population. In some cases casts were being added with the default length
   * of 1 and truncating values.
   *
   * @throws Exception
   */
  @Test
  public void testFiltersOnVarchar() throws Exception {
    final String file = generateSimplePrimitiveSchema_NoNullValues(5000).getFileName();
    final String sql = "select a_string from dfs.`%s` where a_string = 'a_1'";
    testBuilder()
        .sqlQuery(sql, file)
        .unOrdered()
        .baselineColumns("a_string")
        .baselineValues("a_1")
        .go();

    final String sql2 = "select a_string from dfs.`%s` where a_string IN ('a_1')";
    testBuilder()
        .sqlQuery(sql2, file)
        .unOrdered()
        .baselineColumns("a_string")
        .baselineValues("a_1")
        .go();
  }

  @Test
  public void testFiltersOnVarBinary() throws Exception {
    final String file = generateSimplePrimitiveSchema_NoNullValues(5000).getFileName();
    final String sql = "select f_bytes from dfs.`%s` where f_bytes = BINARY_STRING('\\x61\\x31')";
    TestBuilder testBuilder = testBuilder()
        .sqlQuery(sql, file)
        .unOrdered()
        .baselineColumns("f_bytes");

    for (int i = 0; i < 500; i++) {
      testBuilder.baselineValues(new byte[] {'a', '1'});
    }
    testBuilder.go();

    final String sql2 = "select f_bytes from dfs.`%s` where f_bytes IN (BINARY_STRING('\\x61\\x31'))";
    testBuilder = testBuilder()
        .sqlQuery(sql2, file)
        .unOrdered()
        .baselineColumns("f_bytes");

    for (int i = 0; i < 500; i++) {
      testBuilder.baselineValues(new byte[] {'a', '1'});
    }
    testBuilder.go();
  }

  @Test
  public void testSimplePrimitiveSchema_NoNullValues() throws Exception {
    final AvroTestUtil.AvroTestRecordWriter testSetup = generateSimplePrimitiveSchema_NoNullValues();
    final String file = testSetup.getFileName();
    final String sql = "select a_string, b_int, c_long, d_float, e_double, f_bytes, h_boolean, g_null from dfs.`%s`";
    test(sql, file);
    testBuilder()
        .sqlQuery(sql, file)
        .unOrdered()
        .baselineRecords(testSetup.getExpectedRecords())
        .go();
  }

  @Test
  public void testSimplePrimitiveSchema_StarQuery() throws Exception {
    simpleAvroTestHelper(generateSimplePrimitiveSchema_NoNullValues(), "select * from dfs.`%s`");
  }

  private List<Map<String, Object>> project(
      List<Map<String,Object>> incomingRecords,
      List<String> projectCols) {
    List<Map<String,Object>> output = Lists.newArrayList();
    for (Map<String, Object> incomingRecord : incomingRecords) {
      final JsonStringHashMap<String, Object> newRecord = new JsonStringHashMap<>();
      for (String s : incomingRecord.keySet()) {
        if (projectCols.contains(s)) {
          newRecord.put(s, incomingRecord.get(s));
        }
      }
      output.add(newRecord);
    }
    return output;
  }

  @Test
  public void testSimplePrimitiveSchema_SelectColumnSubset() throws Exception {
    final AvroTestUtil.AvroTestRecordWriter testSetup = generateSimplePrimitiveSchema_NoNullValues();
    final String file = testSetup.getFileName();
    List<String> projectList = Lists.newArrayList("`h_boolean`", "`e_double`");
    testBuilder()
        .sqlQuery("select h_boolean, e_double from dfs.`%s`", file)
        .unOrdered()
        .baselineRecords(project(testSetup.getExpectedRecords(), projectList))
        .go();
  }

  @Test
  public void testSimplePrimitiveSchema_NoColumnsExistInTheSchema() throws Exception {
    final String file = generateSimplePrimitiveSchema_NoNullValues().getFileName();
    try {
      test("select h_dummy1, e_dummy2 from dfs.`%s`", file);
      Assert.fail("Test should fail as h_dummy1 and e_dummy2 does not exist.");
    } catch(UserException ue) {
      Assert.assertTrue("Test should fail as h_dummy1 and e_dummy2 does not exist.",
          ue.getMessage().contains("Column 'h_dummy1' not found in any table"));
    }
  }

  @Test
  public void testSimplePrimitiveSchema_OneExistAndOneDoesNotExistInTheSchema() throws Exception {
    final String file = generateSimplePrimitiveSchema_NoNullValues().getFileName();
    try {
      test("select h_boolean, e_dummy2 from dfs.`%s`", file);
      Assert.fail("Test should fail as e_dummy2 does not exist.");
    } catch(UserException ue) {
      Assert.assertTrue("Test should fail as e_dummy2 does not exist.", true);
    }
  }

  @Test
  public void testImplicitColumnsWithStar() throws Exception {
    AvroTestUtil.AvroTestRecordWriter testWriter = generateSimplePrimitiveSchema_NoNullValues(1);
    final String file = testWriter.getFileName();
    // removes "." and ".." from the path
    String tablePath = new File(testWriter.getFilePath()).getCanonicalPath();

    List<Map<String, Object>> expectedRecords = testWriter.getExpectedRecords();
    expectedRecords.get(0).put("`filename`", file);
    expectedRecords.get(0).put("`suffix`", "avro");
    expectedRecords.get(0).put("`fqn`", tablePath);
    expectedRecords.get(0).put("`filepath`", new File(tablePath).getParent());
    try {
      testBuilder()
          .sqlQuery("select filename, *, suffix, fqn, filepath from dfs.`%s`", file)
          .unOrdered()
          .baselineRecords(expectedRecords)
          .go();
    } finally {
      FileUtils.deleteQuietly(new File(tablePath));
    }
  }

  @Test
  public void testImplicitColumnAlone() throws Exception {
    AvroTestUtil.AvroTestRecordWriter testWriter = generateSimplePrimitiveSchema_NoNullValues(1);
    final String file = testWriter.getFileName();
    // removes "." and ".." from the path
    String tablePath = new File(testWriter.getFilePath()).getCanonicalPath();
    try {
      testBuilder()
          .sqlQuery("select filename from dfs.`%s`", file)
          .unOrdered()
          .baselineColumns("filename")
          .baselineValues(file)
          .go();
    } finally {
      FileUtils.deleteQuietly(new File(tablePath));
    }
  }

  @Test
  public void testImplicitColumnInWhereClause() throws Exception {
    AvroTestUtil.AvroTestRecordWriter testWriter = generateSimplePrimitiveSchema_NoNullValues(1);
    final String file = testWriter.getFileName();
    // removes "." and ".." from the path
    String tablePath = new File(testWriter.getFilePath()).getCanonicalPath();

    List<Map<String, Object>> expectedRecords = testWriter.getExpectedRecords();
    try {
      testBuilder()
          .sqlQuery("select * from dfs.`%1$s` where filename = '%1$s'", file)
          .unOrdered()
          .baselineRecords(expectedRecords)
          .go();
    } finally {
      FileUtils.deleteQuietly(new File(tablePath));
    }
  }

  @Test
  public void testPartitionColumn() throws Exception {
    setSessionOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL, "directory");
    String file = "avroTable";
    String partitionColumn = "2018";
    AvroTestUtil.AvroTestRecordWriter testWriter =
        generateSimplePrimitiveSchema_NoNullValues(1, FileUtils.getFile(file, partitionColumn).getPath());
    try {
      testBuilder()
          .sqlQuery("select directory0 from dfs.`%s`", file)
          .unOrdered()
          .baselineColumns("directory0")
          .baselineValues(partitionColumn)
          .go();
    } finally {
      FileUtils.deleteQuietly(new File(testWriter.getFilePath()));
      resetSessionOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL);
    }
  }

  @Test
  public void testSelectAllWithPartitionColumn() throws Exception {
    String file = "avroTable";
    String partitionColumn = "2018";
    AvroTestUtil.AvroTestRecordWriter testWriter =
      generateSimplePrimitiveSchema_NoNullValues(1, FileUtils.getFile(file, partitionColumn).getPath());
    List<Map<String, Object>> expectedRecords = testWriter.getExpectedRecords();
    expectedRecords.get(0).put("`dir0`", partitionColumn);
    try {
      testBuilder()
          .sqlQuery("select * from dfs.`%s`", file)
          .unOrdered()
          .baselineRecords(expectedRecords)
          .go();
    } finally {
      FileUtils.deleteQuietly(new File(testWriter.getFilePath()));
    }
  }

  @Test
  public void testAvroTableWithLogicalTypesDecimal() throws Exception {
    ExecTest.mockUtcDateTimeZone();
    LocalDate date = DateUtility.parseLocalDate("2018-02-03");
    LocalTime time = DateUtility.parseLocalTime("19:25:03.0");
    LocalDateTime timestamp = DateUtility.parseLocalDateTime("2018-02-03 19:25:03.0");

    // Avro uses joda package
    org.joda.time.DateTime jodaDateTime = org.joda.time.DateTime.parse("2018-02-03T19:25:03");
    BigDecimal bigDecimal = new BigDecimal("123.45");

    TestRecordWithLogicalTypes record = new TestRecordWithLogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        "abc",
        jodaDateTime.toLocalDate(),
        jodaDateTime.toLocalTime(),
        jodaDateTime,
        bigDecimal
    );

    File data = write(TestRecordWithLogicalTypes.getClassSchema(), record);

    final String query = "select * from dfs.`%s`";

    testBuilder()
        .sqlQuery(query, data.getName())
        .unOrdered()
        .baselineColumns("b", "i32", "i64", "f32", "f64", "s", "d", "t", "ts", "dec")
        .baselineValues(true, 34, 35L, 3.14F, 3019.34, "abc", date, time, timestamp, bigDecimal)
        .go();
  }

  @Test
  public void testAvroWithDisabledDecimalType() throws Exception {
    TestRecordWithLogicalTypes record = new TestRecordWithLogicalTypes(
        true,
        34,
        35L,
        3.14F,
        3019.34,
        "abc",
        org.joda.time.LocalDate.now(),
        org.joda.time.LocalTime.now(),
        org.joda.time.DateTime.now(),
        new BigDecimal("123.45")
    );

    File data = write(TestRecordWithLogicalTypes.getClassSchema(), record);
    final String query = String.format("select * from dfs.`%s`", data.getName());

    try {
      alterSession(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, false);
      errorMsgTestHelper(query, ExecErrorConstants.DECIMAL_DISABLE_ERR_MSG);
    } finally {
      resetSessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY);
    }
  }

  @Test
  public void testSimpleArraySchema_NoNullValues() throws Exception {
    final String file = generateSimpleArraySchema_NoNullValues().getFileName();
    final String sql = "select a_string, c_string_array[0], e_float_array[2] from dfs.`%s`";
    test(sql, file);
  }

  @Test
  public void testSimpleArraySchema_StarQuery() throws Exception {
    simpleAvroTestHelper(generateSimpleArraySchema_NoNullValues(), "select * from dfs.`%s`");
  }

  @Test
  public void testDoubleNestedSchema_NoNullValues_NotAllColumnsProjected() throws Exception {
    final String file = generateDoubleNestedSchema_NoNullValues().getFileName();
    final String sql = "select t.c_record.nested_1_int, t.c_record.nested_1_record.double_nested_1_int from dfs.`%s` t";
    test(sql, file);
  }

  @Test
  public void testSimpleNestedSchema_NoNullValues() throws Exception {
    final String file = generateSimpleNestedSchema_NoNullValues().getFileName();
    final String sql = "select a_string, b_int, t.c_record.nested_1_string, t.c_record.nested_1_int from dfs.`%s` t";
    test(sql, file);
  }

  @Test
  public void testSimpleNestedSchema_StarQuery() throws Exception {
    final AvroTestUtil.AvroTestRecordWriter testSetup = generateSimpleNestedSchema_NoNullValues();
    final String file = testSetup.getFileName();
    testBuilder()
        .sqlQuery("select * from dfs.`%s`", file)
        .unOrdered()
        .baselineRecords(testSetup.getExpectedRecords())
        .go();
  }
  @Test
  public void testDoubleNestedSchema_NoNullValues() throws Exception {
    final String file = generateDoubleNestedSchema_NoNullValues().getFileName();
    final String sql = "select a_string, b_int, t.c_record.nested_1_string, t.c_record.nested_1_int, " +
            "t.c_record.nested_1_record.double_nested_1_string, " +
            "t.c_record.nested_1_record.double_nested_1_int " +
            "from dfs.`%s` t";
    test(sql, file);

    final String sql2 = "select t.c_record.nested_1_string from dfs.`%s` t limit 1";
    TestBuilder testBuilder = testBuilder()
        .sqlQuery(sql2, file)
        .unOrdered()
        .baselineColumns("EXPR$0");
    for (int i = 0; i < 1; i++) {
      testBuilder.baselineValues("nested_1_string_" + i);
    }
    testBuilder.go();
  }

  @Test
  public void testDoubleNestedSchema_StarQuery() throws Exception {
    simpleAvroTestHelper(generateDoubleNestedSchema_NoNullValues(), "select * from dfs.`%s`");
  }

  private static void simpleAvroTestHelper(AvroTestUtil.AvroTestRecordWriter testSetup, final String sql) throws Exception {
    testBuilder()
        .sqlQuery(sql, testSetup.getFileName())
        .unOrdered()
        .baselineRecords(testSetup.getExpectedRecords())
        .go();
  }

  @Test
  public void testSimpleEnumSchema_NoNullValues() throws Exception {
    final AvroTestUtil.AvroTestRecordWriter testSetup = generateSimpleEnumSchema_NoNullValues();
    final String file = testSetup.getFileName();
    final String sql = "select a_string, b_enum from dfs.`%s`";
    List<String> projectList = Lists.newArrayList("`a_string`", "`b_enum`");
    testBuilder()
        .sqlQuery(sql, file)
        .unOrdered()
        .baselineRecords(project(testSetup.getExpectedRecords(), projectList))
        .go();
  }

  @Test
  public void testSimpleEnumSchema_StarQuery() throws Exception {
    simpleAvroTestHelper(generateSimpleEnumSchema_NoNullValues(), "select * from dfs.`%s`");
  }

  @Test
  public void testSimpleUnionSchema_StarQuery() throws Exception {
    simpleAvroTestHelper(generateUnionSchema_WithNullValues(), "select * from dfs.`%s`");
  }

  @Test
  public void testShouldFailSimpleUnionNonNullSchema_StarQuery() throws Exception {
    final String file = generateUnionSchema_WithNonNullValues().getFileName();
    try {
      test("select * from dfs.`%s`", file);
      Assert.fail("Test should fail as union is only supported for optional fields");
    } catch(UserRemoteException e) {
      String message = e.getMessage();
      Assert.assertTrue(message.contains("Avro union type must be of the format : [\"null\", \"some-type\"]"));
    }
  }

  @Test
  public void testNestedUnionSchema_withNullValues() throws Exception {
    final String file = generateUnionNestedSchema_withNullValues().getFileName();
    final String sql = "select t.c_record.nested_1_string,t.c_record.nested_1_int from dfs.`%s` t";
    test(sql, file);
  }

  // DRILL-4574"></a>
  @Test
  public void testFlattenPrimitiveArray() throws Exception {
    final String file = generateSimpleArraySchema_NoNullValues().getFileName();
    final String sql = "select a_string, flatten(c_string_array) as array_item from dfs.`%s` t";

    TestBuilder testBuilder = testBuilder()
      .sqlQuery(sql, file)
      .unOrdered()
      .baselineColumns("a_string", "array_item");

    for (int i = 0; i < AvroTestUtil.RECORD_COUNT; i++) {

      for (int j = 0; j < AvroTestUtil.ARRAY_SIZE; j++) {
        testBuilder.baselineValues("a_" + i, "c_string_array_" + i + "_" + j);
      }
    }

    testBuilder.go();
  }

  private TestBuilder nestedArrayQueryTestBuilder(String file) {
    final String sql = "select rec_nr, array_item['nested_1_int'] as array_item_nested_int from "
        + "(select a_int as rec_nr, flatten(t.b_array) as array_item from dfs.`%s` t) a";

    TestBuilder testBuilder = testBuilder()
      .sqlQuery(sql, file)
      .unOrdered()
      .baselineColumns("rec_nr", "array_item_nested_int");

    return testBuilder;
  }

  //DRILL-4574
  @Test
  public void testFlattenComplexArray() throws Exception {
    final String file = generateNestedArraySchema().getFileName();

    TestBuilder testBuilder = nestedArrayQueryTestBuilder(file);
    for (int i = 0; i < AvroTestUtil.RECORD_COUNT; i++) {
      for (int j = 0; j < AvroTestUtil.ARRAY_SIZE; j++) {
        testBuilder.baselineValues(i, j);
      }
    }
    testBuilder.go();

  }

  //DRILL-4574
  @Test
  public void testFlattenEmptyComplexArrayMustYieldNoResults() throws Exception {
    final String file = generateNestedArraySchema(AvroTestUtil.RECORD_COUNT, 0).getFilePath();
    TestBuilder testBuilder = nestedArrayQueryTestBuilder(file);
    testBuilder.expectsEmptyResultSet();
  }

  @Test
  public void testNestedUnionArraySchema_withNullValues() throws Exception {
    final String file = generateUnionNestedArraySchema_withNullValues().getFileName();
    final String sql = "select t.c_array[0].nested_1_string,t.c_array[0].nested_1_int from dfs.`%s` t";
    test(sql, file);
  }

  @Test
  public void testMapSchema_withNullValues() throws Exception {
    final String file = generateMapSchema_withNullValues().getFileName();
    final String sql = "select c_map['key1'],c_map['key2'] from dfs.`%s`";
    test(sql, file);
  }

  @Test
  public void testMapSchemaComplex_withNullValues() throws Exception {
    final String file = generateMapSchemaComplex_withNullValues().getFileName();
    final String sql = "select d_map['key1'] nested_key1, d_map['key2'] nested_key2 from dfs.`%s`";

    TestBuilder testBuilder = testBuilder()
        .sqlQuery(sql, file)
        .unOrdered()
        .baselineColumns("nested_key1", "nested_key2");

    final List<Object> expectedList = Lists.newArrayList();
    for (int i = 0; i < AvroTestUtil.ARRAY_SIZE; i++) {
      expectedList.add((double)i);
    }
    final List<Object> emptyList = listOf();
    for (int i = 0; i < AvroTestUtil.RECORD_COUNT; i += 2) {
      testBuilder.baselineValues(expectedList, expectedList);
      testBuilder.baselineValues(emptyList, emptyList);
    }
    testBuilder.go();
  }

  @Test
  public void testStringAndUtf8Data() throws Exception {
    simpleAvroTestHelper(generateStringAndUtf8Data(), "select * from dfs.`%s`");
  }

  @Test
  public void testLinkedList() throws Exception {
    final String file = generateLinkedList();
    final String sql = "select * from dfs.`%s`";
    test(sql, file);
  }

  @Test
  public void testCountStar() throws Exception {
    final String file = generateStringAndUtf8Data().getFileName();
    final String sql = "select count(*) as row_count from dfs.`%s`";
    testBuilder()
        .sqlQuery(sql, file)
        .ordered()
        .baselineColumns("row_count")
        .baselineValues((long)AvroTestUtil.RECORD_COUNT)
        .go();
  }
}
