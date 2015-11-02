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

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for Avro record reader.
 */
public class AvroFormatTest extends BaseTestQuery {

  // XXX
  //      1. Need to test nested field names with same name as top-level names for conflict.
  //      2. Avro supports recursive types? Can we test this?

  @Test
  public void testSimplePrimitiveSchema_NoNullValues() throws Exception {

    final String file = AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues();
    final String sql =
            "select a_string, b_int, c_long, d_float, e_double, f_bytes, h_boolean, g_null " +
             "from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testSimplePrimitiveSchema_StarQuery() throws Exception {

    final String file = AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues();
    final String sql = "select * from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testSimplePrimitiveSchema_SelectColumnSubset() throws Exception {

    final String file = AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues();
    final String sql = "select h_boolean, e_double from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testSimplePrimitiveSchema_NoColumnsExistInTheSchema() throws Exception {

    final String file = AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues();
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

    final String file = AvroTestUtil.generateSimplePrimitiveSchema_NoNullValues();
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

    final String file = AvroTestUtil.generateSimpleArraySchema_NoNullValues();
    final String sql = "select a_string, c_string_array[0], e_float_array[2] " +
            "from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testSimpleArraySchema_StarQuery() throws Exception {

    final String file = AvroTestUtil.generateSimpleArraySchema_NoNullValues();
    final String sql = "select * from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testDoubleNestedSchema_NoNullValues_NotAllColumnsProjected() throws Exception {

    final String file = AvroTestUtil.generateDoubleNestedSchema_NoNullValues();
    final String sql = "select t.c_record.nested_1_int, " +
            "t.c_record.nested_1_record.double_nested_1_int " +
            "from dfs_test.`" + file + "` t";
    test(sql);
  }

  @Test
  public void testSimpleNestedSchema_NoNullValues() throws Exception {

    final String file = AvroTestUtil.generateSimpleNestedSchema_NoNullValues();
    final String sql = "select a_string, b_int, t.c_record.nested_1_string, t.c_record.nested_1_int " +
            "from dfs_test.`" + file + "` t";
    test(sql);
  }

  @Test
  public void testSimpleNestedSchema_StarQuery() throws Exception {

    final String file = AvroTestUtil.generateSimpleNestedSchema_NoNullValues();
    final String sql = "select * from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testDoubleNestedSchema_NoNullValues() throws Exception {

    final String file = AvroTestUtil.generateDoubleNestedSchema_NoNullValues();
    final String sql = "select a_string, b_int, t.c_record.nested_1_string, t.c_record.nested_1_int, " +
            "t.c_record.nested_1_record.double_nested_1_string, " +
            "t.c_record.nested_1_record.double_nested_1_int " +
            "from dfs_test.`" + file + "` t";
    test(sql);
  }

  @Test
  public void testDoubleNestedSchema_StarQuery() throws Exception {

    final String file = AvroTestUtil.generateDoubleNestedSchema_NoNullValues();
    final String sql = "select * from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testSimpleEnumSchema_NoNullValues() throws Exception {

    final String file = AvroTestUtil.generateSimpleEnumSchema_NoNullValues();
    final String sql = "select a_string, b_enum from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testSimpleEnumSchema_StarQuery() throws Exception {

    final String file = AvroTestUtil.generateSimpleEnumSchema_NoNullValues();
    final String sql = "select * from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testSimpleUnionSchema_StarQuery() throws Exception {

    final String file = AvroTestUtil.generateUnionSchema_WithNullValues();
    final String sql = "select * from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testShouldFailSimpleUnionNonNullSchema_StarQuery() throws Exception {

    final String file = AvroTestUtil.generateUnionSchema_WithNonNullValues();
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

    final String file = AvroTestUtil.generateUnionNestedSchema_withNullValues();
    final String sql = "select t.c_record.nested_1_string,t.c_record.nested_1_int from dfs_test.`" + file + "` t";
    test(sql);
  }

  @Test
  public void testNestedUnionArraySchema_withNullValues() throws Exception {

    final String file = AvroTestUtil.generateUnionNestedArraySchema_withNullValues();
    final String sql = "select t.c_array[0].nested_1_string,t.c_array[0].nested_1_int from dfs_test.`" + file + "` t";
    test(sql);
  }

  @Test
  public void testMapSchema_withNullValues() throws Exception {

    final String file = AvroTestUtil.generateMapSchema_withNullValues();
    final String sql = "select c_map['key1'],c_map['key2'] from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testMapSchemaComplex_withNullValues() throws Exception {

    final String file = AvroTestUtil.generateMapSchemaComplex_withNullValues();
    final String sql = "select d_map['key1'],d_map['key2'] from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testStringAndUtf8Data() throws Exception {

    final String file = AvroTestUtil.generateStringAndUtf8Data();
    final String sql = "select * from dfs_test.`" + file + "`";
    test(sql);
  }

  @Test
  public void testLinkedList() throws Exception {

    final String file = AvroTestUtil.generateLinkedList();
    final String sql = "select * from dfs_test.`" + file + "`";
    test(sql);
  }
}
