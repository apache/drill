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
package org.apache.drill.exec.store.avro;

import com.google.common.collect.Lists;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.TestBuilder;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.apache.drill.TestBuilder.listOf;

/**
 * Unit tests for Avro record reader.
 */
public class AvroFormatTest extends BaseTestQuery {

  // XXX
  //      1. Need to test nested field names with same name as top-level names for conflict.
  //      2. Avro supports recursive types? Can we test this?

  @Test
  public void testBatchCutoff() throws Exception {

    final AvroTestUtil.AvroTestRecordWriter testSetup = AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues(5000);
    final String file = testSetup.getFilePath();
    final String sql =
        "select a_string, b_int, c_long, d_float, e_double, f_bytes, h_boolean, g_null " +
            "from dfs_test.`" + file + "`";
    test(sql);
    testBuilder()
        .sqlQuery(sql)
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

    final AvroTestUtil.AvroTestRecordWriter testSetup = AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues(5000);
    final String file = testSetup.getFilePath();
    final String sql =
        "select a_string " +
            "from dfs_test.`" + file + "` where a_string = 'a_1'";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("a_string")
        .baselineValues("a_1")
        .go();

    final String sql2 =
        "select a_string " +
            "from dfs_test.`" + file + "` where a_string IN ('a_1')";
    testBuilder()
        .sqlQuery(sql2)
        .unOrdered()
        .baselineColumns("a_string")
        .baselineValues("a_1")
        .go();
  }

  @Test
  public void testFiltersOnVarBinary() throws Exception {
    final AvroTestUtil.AvroTestRecordWriter testSetup = AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues(5000);
    final String file = testSetup.getFilePath();
    final String sql =
        "select f_bytes " +
            "from dfs_test.`" + file + "` where f_bytes = BINARY_STRING('\\x61\\x31')";
    TestBuilder testBuilder = testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("f_bytes");

    for (int i = 0; i < 500; i++) {
      testBuilder.baselineValues(new byte[] {'a', '1'});
    }
    testBuilder.go();

    final String sql2 =
        "select f_bytes " +
            "from dfs_test.`" + file + "` where f_bytes IN (BINARY_STRING('\\x61\\x31'))";
    testBuilder = testBuilder()
        .sqlQuery(sql2)
        .unOrdered()
        .baselineColumns("f_bytes");

    for (int i = 0; i < 500; i++) {
      testBuilder.baselineValues(new byte[] {'a', '1'});
    }
    testBuilder.go();
  }

  @Test
  public void testSimplePrimitiveSchema_NoNullValues() throws Exception {

    final AvroTestUtil.AvroTestRecordWriter testSetup = AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues();
    final String file = testSetup.getFilePath();
    final String sql =
            "select a_string, b_int, c_long, d_float, e_double, f_bytes, h_boolean, g_null " +
             "from dfs_test.`" + file + "`";
    test(sql);
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineRecords(testSetup.getExpectedRecords())
        .go();
  }

  @Test
  public void testSimplePrimitiveSchema_StarQuery() throws Exception {
    simpleAvroTestHelper(AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues(), "select * from dfs_test.`%s`");
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

    final AvroTestUtil.AvroTestRecordWriter testSetup = AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues();
    final String file = testSetup.getFilePath();
    final String sql = "select h_boolean, e_double from dfs_test.`" + file + "`";
    List<String> projectList = Lists.newArrayList("`h_boolean`", "`e_double`");
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineRecords(project(testSetup.getExpectedRecords(), projectList))
        .go();
  }

  @Test
  public void testSimplePrimitiveSchema_NoColumnsExistInTheSchema() throws Exception {

    final String file = AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues().getFilePath();
    final String sql = "select h_dummy1, e_dummy2 from dfs_test.`" + file + "`";
    try {
      test(sql);
      Assert.fail("Test should fail as h_dummy1 and e_dummy2 does not exist.");
    } catch(UserException ue) {
      Assert.assertTrue("Test should fail as h_dummy1 and e_dummy2 does not exist.",
          ue.getMessage().contains("Column 'h_dummy1' not found in any table"));
    }
  }

  @Test
  public void testSimplePrimitiveSchema_OneExistAndOneDoesNotExistInTheSchema() throws Exception {

    final String file = AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues().getFilePath();
    final String sql = "select h_boolean, e_dummy2 from dfs_test.`" + file + "`";
    try {
      test(sql);
      Assert.fail("Test should fail as e_dummy2 does not exist.");
    } catch(UserException ue) {
      Assert.assertTrue("Test should fail as e_dummy2 does not exist.", true);
    }
  }

  @Test
  public void testSimpleArraySchema_NoNullValues() throws Exception {
    final String file = AvroTestUtil.generateSimpleArraySchema_NoNullValues().getFilePath();
    final String sql = "select a_string, c_string_array[0], e_float_array[2] " +
            "from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testSimpleArraySchema_StarQuery() throws Exception {
    simpleAvroTestHelper(AvroTestUtil.generateSimpleArraySchema_NoNullValues(), "select * from dfs_test.`%s`");
  }

  @Test
  public void testDoubleNestedSchema_NoNullValues_NotAllColumnsProjected() throws Exception {
    final String file = AvroTestUtil.generateDoubleNestedSchema_NoNullValues().getFilePath();
    final String sql = "select t.c_record.nested_1_int, " +
            "t.c_record.nested_1_record.double_nested_1_int " +
            "from dfs_test.`" + file + "` t";
    test(sql);
  }

  @Test
  public void testSimpleNestedSchema_NoNullValues() throws Exception {

    final AvroTestUtil.AvroTestRecordWriter testSetup = AvroTestUtil.generateSimpleNestedSchema_NoNullValues();
    final String file = testSetup.getFilePath();
    final String sql = "select a_string, b_int, t.c_record.nested_1_string, t.c_record.nested_1_int " +
        "from dfs_test.`" + file + "` t";
    test(sql);
  }

  @Test
  public void testSimpleNestedSchema_StarQuery() throws Exception {

    final AvroTestUtil.AvroTestRecordWriter testSetup = AvroTestUtil.generateSimpleNestedSchema_NoNullValues();
    final String file = testSetup.getFilePath();
    final String sql = "select * from dfs_test.`" + file + "`";
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineRecords(testSetup.getExpectedRecords())
        .go();
  }
  @Test
  public void testDoubleNestedSchema_NoNullValues() throws Exception {
    final String file = AvroTestUtil.generateDoubleNestedSchema_NoNullValues().getFilePath();
    final String sql = "select a_string, b_int, t.c_record.nested_1_string, t.c_record.nested_1_int, " +
            "t.c_record.nested_1_record.double_nested_1_string, " +
            "t.c_record.nested_1_record.double_nested_1_int " +
            "from dfs_test.`" + file + "` t";
    test(sql);

    final String sql2 = "select t.c_record.nested_1_string " +
        "from dfs_test.`" + file + "` t limit 1";
    TestBuilder testBuilder = testBuilder()
        .sqlQuery(sql2)
        .unOrdered()
        .baselineColumns("EXPR$0");
    for (int i = 0; i < 1; i++) {
      testBuilder
          .baselineValues("nested_1_string_" + i);
    }
    testBuilder.go();
  }

  @Test
  public void testDoubleNestedSchema_StarQuery() throws Exception {
    simpleAvroTestHelper(AvroTestUtil.generateDoubleNestedSchema_NoNullValues(), "select * from dfs_test.`%s`");
  }

  private static void simpleAvroTestHelper(AvroTestUtil.AvroTestRecordWriter testSetup, final String sql) throws Exception {
    final String file = testSetup.getFilePath();
    final String sqlWithTable = String.format(sql, file);
    testBuilder()
        .sqlQuery(sqlWithTable)
        .unOrdered()
        .baselineRecords(testSetup.getExpectedRecords())
        .go();
  }

  @Test
  public void testSimpleEnumSchema_NoNullValues() throws Exception {
    final AvroTestUtil.AvroTestRecordWriter testSetup = AvroTestUtil.generateSimpleEnumSchema_NoNullValues();
    final String file = testSetup.getFilePath();
    final String sql = "select a_string, b_enum from dfs_test.`" + file + "`";
    List<String> projectList = Lists.newArrayList("`a_string`", "`b_enum`");
    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineRecords(project(testSetup.getExpectedRecords(), projectList))
        .go();
  }

  @Test
  public void testSimpleEnumSchema_StarQuery() throws Exception {
    simpleAvroTestHelper(AvroTestUtil.generateSimpleEnumSchema_NoNullValues(), "select * from dfs_test.`%s`");
  }

  @Test
  public void testSimpleUnionSchema_StarQuery() throws Exception {
    simpleAvroTestHelper(AvroTestUtil.generateUnionSchema_WithNullValues(), "select * from dfs_test.`%s`");
  }

  @Test
  public void testShouldFailSimpleUnionNonNullSchema_StarQuery() throws Exception {

    final String file = AvroTestUtil.generateUnionSchema_WithNonNullValues().getFilePath();
    final String sql = "select * from dfs_test.`" + file + "`";
    try {
      test(sql);
      Assert.fail("Test should fail as union is only supported for optional fields");
    } catch(UserRemoteException e) {
      String message = e.getMessage();
      Assert.assertTrue(message.contains("Avro union type must be of the format : [\"null\", \"some-type\"]"));
    }
  }

  @Test
  public void testNestedUnionSchema_withNullValues() throws Exception {

    final String file = AvroTestUtil.generateUnionNestedSchema_withNullValues().getFilePath();
    final String sql = "select t.c_record.nested_1_string,t.c_record.nested_1_int from dfs_test.`" + file + "` t";
    test(sql);
  }

  /**
   *  See <a href="https://issues.apache.org/jira/browse/DRILL-4574"></a>
   *
   */
  @Test
  public void testFlattenPrimitiveArray() throws Exception {
    final String file = AvroTestUtil.generateSimpleArraySchema_NoNullValues().getFilePath();

    final String sql = "select a_string, flatten(c_string_array) as array_item "
        + "from dfs_test.`" + file + "` t";

    TestBuilder testBuilder = testBuilder().sqlQuery(sql).unOrdered()
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
        + "(select a_int as rec_nr, flatten(t.b_array) as array_item " + "from dfs_test.`" + file + "` t) a";

    TestBuilder testBuilder = testBuilder().sqlQuery(sql).unOrdered().baselineColumns("rec_nr",
        "array_item_nested_int");

    return testBuilder;

  }


  /**
   * See <a href="https://issues.apache.org/jira/browse/DRILL-4574"></a>
   */
  @Test
  public void testFlattenComplexArray() throws Exception {
    final String file = AvroTestUtil.generateNestedArraySchema().getFilePath();

    TestBuilder testBuilder = nestedArrayQueryTestBuilder(file);
    for (int i = 0; i < AvroTestUtil.RECORD_COUNT; i++) {
      for (int j = 0; j < AvroTestUtil.ARRAY_SIZE; j++) {
        testBuilder.baselineValues(i, j);
      }
    }
    testBuilder.go();

  }
  /**
   * See <a href="https://issues.apache.org/jira/browse/DRILL-4574"></a>
   */
  @Test
  public void testFlattenEmptyComplexArrayMustYieldNoResults() throws Exception {
    final String file = AvroTestUtil.generateNestedArraySchema(AvroTestUtil.RECORD_COUNT, 0).getFilePath();
    TestBuilder testBuilder = nestedArrayQueryTestBuilder(file);
    testBuilder.expectsEmptyResultSet();
  }

  @Test
  public void testNestedUnionArraySchema_withNullValues() throws Exception {

    final String file = AvroTestUtil.generateUnionNestedArraySchema_withNullValues().getFilePath();
    final String sql = "select t.c_array[0].nested_1_string,t.c_array[0].nested_1_int from dfs_test.`" + file + "` t";
    test(sql);
  }

  @Test
  public void testMapSchema_withNullValues() throws Exception {

    final String file = AvroTestUtil.generateMapSchema_withNullValues().getFilePath();
    final String sql = "select c_map['key1'],c_map['key2'] from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testMapSchemaComplex_withNullValues() throws Exception {

    final String file = AvroTestUtil.generateMapSchemaComplex_withNullValues().getFilePath();
    final String sql = "select d_map['key1'] nested_key1, d_map['key2'] nested_key2 from dfs_test.`" + file + "`";

    TestBuilder testBuilder = testBuilder()
        .sqlQuery(sql)
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
    simpleAvroTestHelper(AvroTestUtil.generateStringAndUtf8Data(), "select * from dfs_test.`%s`");
  }

  @Test
  public void testLinkedList() throws Exception {
    final String file = AvroTestUtil.generateLinkedList();
    final String sql = "select * from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testCountStar() throws Exception {
    final String file = AvroTestUtil.generateStringAndUtf8Data().getFilePath();
    final String sql = "select count(*) as row_count from dfs_test.`" + file + "`";
    testBuilder()
        .sqlQuery(sql)
        .ordered()
        .baselineColumns("row_count")
        .baselineValues((long)AvroTestUtil.RECORD_COUNT)
        .go();
  }

}
