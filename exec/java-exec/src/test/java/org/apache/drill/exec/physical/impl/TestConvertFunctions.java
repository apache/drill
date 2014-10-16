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
package org.apache.drill.exec.physical.impl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.buffer.DrillBuf;

import java.util.ArrayList;
import java.util.List;

import mockit.Injectable;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.util.ByteBufUtil.HadoopWritables;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class TestConvertFunctions extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestConvertFunctions.class);

  private static final String CONVERSION_TEST_LOGICAL_PLAN = "functions/conv/conversionTestWithLogicalPlan.json";
  private static final String CONVERSION_TEST_PHYSICAL_PLAN = "functions/conv/conversionTestWithPhysicalPlan.json";

  private static final float DELTA = (float) 0.0001;

  // "1980-01-01 01:23:45.678"
  private static final String DATE_TIME_BE = "\\x00\\x00\\x00\\x49\\x77\\x85\\x1f\\x8e";
  private static final String DATE_TIME_LE = "\\x8e\\x1f\\x85\\x77\\x49\\x00\\x00\\x00";

  private static DateTime time = DateTime.parse("01:23:45.678", DateUtility.getTimeFormatter());
  private static DateTime date = DateTime.parse("1980-01-01", DateUtility.getDateTimeFormatter());

  String textFileContent;

  @Test
  public void test_JSON_convertTo_empty_list_drill_1416() throws Exception {
    test("select cast(convert_to(rl[1], 'JSON') as varchar(100)) from cp.`/store/json/input2.json`");
    test("select convert_from(convert_to(rl[1], 'JSON'), 'JSON') from cp.`/store/json/input2.json`");
    test("select convert_from(convert_to(rl[1], 'JSON'), 'JSON') from cp.`/store/json/json_project_null_object_from_list.json`");
  }

  @Test
  public void testDateTime1() throws Throwable {
    verifyPhysicalPlan("(convert_from(binary_string('" + DATE_TIME_BE + "'), 'TIME_EPOCH_BE'))", time);
  }

  @Test
  public void testDateTime2() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('" + DATE_TIME_LE + "'), 'TIME_EPOCH')", time);
  }

  @Test
  public void testDateTime3() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('" + DATE_TIME_BE + "'), 'DATE_EPOCH_BE')", date );
  }

  @Test
  public void testDateTime4() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('" + DATE_TIME_LE + "'), 'DATE_EPOCH')", date);
  }

  @Test
  public void testFixedInts1() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('\\xAD'), 'TINYINT')", (byte) 0xAD);
  }

  @Test
  public void testFixedInts2() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('\\xFE\\xCA'), 'SMALLINT')", (short) 0xCAFE);
  }

  @Test
  public void testFixedInts3() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('\\xCA\\xFE'), 'SMALLINT_BE')", (short) 0xCAFE);
  }

  @Test
  public void testFixedInts4() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('\\xBE\\xBA\\xFE\\xCA'), 'INT')", 0xCAFEBABE);
  }

  @Test
  public void testFixedInts4SQL_from() throws Throwable {
    verifySQL("select"
           + "   convert_from(binary_string('\\xBE\\xBA\\xFE\\xCA'), 'INT')"
           + " from"
           + "   cp.`employee.json` LIMIT 1",
            0xCAFEBABE);
  }

  @Test
  public void testFixedInts4SQL_to() throws Throwable {
    verifySQL("select"
           + "   convert_to(-889275714, 'INT')"
           + " from"
           + "   cp.`employee.json` LIMIT 1",
           new byte[] {(byte) 0xBE, (byte) 0xBA, (byte) 0xFE, (byte) 0xCA});
  }

  @Test
  public void testFixedInts5() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('\\xCA\\xFE\\xBA\\xBE'), 'INT_BE')", 0xCAFEBABE);
  }

  @Test
  public void testFixedInts6() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('\\xEF\\xBE\\xAD\\xDE\\xBE\\xBA\\xFE\\xCA'), 'BIGINT')", 0xCAFEBABEDEADBEEFL);
  }

  @Test
  public void testFixedInts7() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('\\xCA\\xFE\\xBA\\xBE\\xDE\\xAD\\xBE\\xEF'), 'BIGINT_BE')", 0xCAFEBABEDEADBEEFL);
  }

  @Test
  public void testFixedInts8() throws Throwable {
    verifyPhysicalPlan("convert_from(convert_to(cast(77 as varchar(2)), 'INT_BE'), 'INT_BE')", 77);
  }

  @Test
  public void testFixedInts9() throws Throwable {
    verifyPhysicalPlan("convert_to(cast(77 as varchar(2)), 'INT_BE')", new byte[] {0, 0, 0, 77});
  }

  @Test
  public void testFixedInts10() throws Throwable {
    verifyPhysicalPlan("convert_to(cast(77 as varchar(2)), 'INT')", new byte[] {77, 0, 0, 0});
  }

  @Test
  public void testFixedInts11() throws Throwable {
    verifyPhysicalPlan("convert_to(77, 'BIGINT_BE')", new byte[] {0, 0, 0, 0, 0, 0, 0, 77});
  }

  @Test
  public void testFixedInts12() throws Throwable {
    verifyPhysicalPlan("convert_to(9223372036854775807, 'BIGINT')", new byte[] {-1, -1, -1, -1, -1, -1, -1, 0x7f});
  }

  @Test
  public void testFixedInts13() throws Throwable {
    verifyPhysicalPlan("convert_to(-9223372036854775808, 'BIGINT')", new byte[] {0, 0, 0, 0, 0, 0, 0, (byte)0x80});
  }

  @Test
  public void testVInts1() throws Throwable {
    verifyPhysicalPlan("convert_to(cast(0 as int), 'INT_HADOOPV')", new byte[] {0});
  }

  @Test
  public void testVInts2() throws Throwable {
    verifyPhysicalPlan("convert_to(cast(128 as int), 'INT_HADOOPV')", new byte[] {-113, -128});
  }

  @Test
  public void testVInts3() throws Throwable {
    verifyPhysicalPlan("convert_to(cast(256 as int), 'INT_HADOOPV')", new byte[] {-114, 1, 0});
  }

  @Test
  public void testVInts4() throws Throwable {
    verifyPhysicalPlan("convert_to(cast(65536 as int), 'INT_HADOOPV')", new byte[] {-115, 1, 0, 0});
  }

  @Test
  public void testVInts5() throws Throwable {
    verifyPhysicalPlan("convert_to(cast(16777216 as int), 'INT_HADOOPV')", new byte[] {-116, 1, 0, 0, 0});
  }

  @Test
  public void testVInts6() throws Throwable {
    verifyPhysicalPlan("convert_to(4294967296, 'BIGINT_HADOOPV')", new byte[] {-117, 1, 0, 0, 0, 0});
  }

  @Test
  public void testVInts7() throws Throwable {
    verifyPhysicalPlan("convert_to(1099511627776, 'BIGINT_HADOOPV')", new byte[] {-118, 1, 0, 0, 0, 0, 0});
  }

  @Test
  public void testVInts8() throws Throwable {
    verifyPhysicalPlan("convert_to(281474976710656, 'BIGINT_HADOOPV')", new byte[] {-119, 1, 0, 0, 0, 0, 0, 0});
  }

  @Test
  public void testVInts9() throws Throwable {
    verifyPhysicalPlan("convert_to(72057594037927936, 'BIGINT_HADOOPV')", new byte[] {-120, 1, 0, 0, 0, 0, 0, 0, 0});
  }

  @Test
  public void testVInts10() throws Throwable {
    verifyPhysicalPlan("convert_to(9223372036854775807, 'BIGINT_HADOOPV')", new byte[] {-120, 127, -1, -1, -1, -1, -1, -1, -1});
  }

  @Test
  public void testVInts11() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('\\x88\\x7f\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF'), 'BIGINT_HADOOPV')", 9223372036854775807L);
  }

  @Test
  public void testVInts12() throws Throwable {
    verifyPhysicalPlan("convert_to(-9223372036854775808, 'BIGINT_HADOOPV')", new byte[] {-128, 127, -1, -1, -1, -1, -1, -1, -1});
  }

  @Test
  public void testVInts13() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('\\x80\\x7f\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF'), 'BIGINT_HADOOPV')", -9223372036854775808L);
  }

  @Test
  public void testBool1() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('\\x01'), 'BOOLEAN_BYTE')", true);
  }

  @Test
  public void testBool2() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('\\x00'), 'BOOLEAN_BYTE')", false);
  }

  @Test
  public void testBool3() throws Throwable {
    verifyPhysicalPlan("convert_to(true, 'BOOLEAN_BYTE')", new byte[] {1});
  }

  @Test
  public void testBool4() throws Throwable {
    verifyPhysicalPlan("convert_to(false, 'BOOLEAN_BYTE')", new byte[] {0});
  }

  @Test
  public void testFloats1() throws Throwable {
  }

  @Test
  public void testFloats2() throws Throwable {
    verifyPhysicalPlan("convert_from(convert_to(cast(77 as float4), 'FLOAT'), 'FLOAT')", new Float(77.0));
  }

  @Test
  public void testFloats2be() throws Throwable {
    verifyPhysicalPlan("convert_from(convert_to(cast(77 as float4), 'FLOAT_BE'), 'FLOAT_BE')", new Float(77.0));
  }

  @Test
  public void testFloats3() throws Throwable {
    verifyPhysicalPlan("convert_to(cast(1.4e-45 as float4), 'FLOAT')", new byte[] {1, 0, 0, 0});
  }

  @Test
  public void testFloats4() throws Throwable {
    verifyPhysicalPlan("convert_to(cast(3.4028235e+38 as float4), 'FLOAT')", new byte[] {-1, -1, 127, 127});
  }

  @Test
  public void testFloats5(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    verifyPhysicalPlan("convert_from(convert_to(cast(77 as float8), 'DOUBLE'), 'DOUBLE')", 77.0);
  }

  @Test
  public void testFloats5be(@Injectable final DrillbitContext bitContext,
                          @Injectable UserServer.UserClientConnection connection) throws Throwable {
    verifyPhysicalPlan("convert_from(convert_to(cast(77 as float8), 'DOUBLE_BE'), 'DOUBLE_BE')", 77.0);
  }

  @Test
  public void testFloats6(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    verifyPhysicalPlan("convert_to(cast(77 as float8), 'DOUBLE')", new byte[] {0, 0, 0, 0, 0, 64, 83, 64});
  }

  @Test
  public void testFloats7(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    verifyPhysicalPlan("convert_to(4.9e-324, 'DOUBLE')", new byte[] {1, 0, 0, 0, 0, 0, 0, 0});
  }

  @Test
  public void testFloats8(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    verifyPhysicalPlan("convert_to(1.7976931348623157e+308, 'DOUBLE')", new byte[] {-1, -1, -1, -1, -1, -1, -17, 127});
  }

  @Test
  public void testUTF8() throws Throwable {
    verifyPhysicalPlan("convert_from(binary_string('apache_drill'), 'UTF8')", "apache_drill");
    verifyPhysicalPlan("convert_to('apache_drill', 'UTF8')", new byte[] {'a', 'p', 'a', 'c', 'h', 'e', '_', 'd', 'r', 'i', 'l', 'l'});
  }

  @Test
  public void testBigIntVarCharReturnTripConvertLogical() throws Exception {
    String logicalPlan = Resources.toString(Resources.getResource(CONVERSION_TEST_LOGICAL_PLAN), Charsets.UTF_8);
    List<QueryResultBatch> results =  testLogicalWithResults(logicalPlan);
    int count = 0;
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for (QueryResultBatch result : results){
      count += result.getHeader().getRowCount();
      loader.load(result.getHeader().getDef(), result.getData());
      if (loader.getRecordCount() > 0) {
        VectorUtil.showVectorAccessibleContent(loader);
      }
      loader.clear();
      result.release();
    }
    assertTrue(count == 10);
  }

  @Test
  public void testHadooopVInt() throws Exception {
    final int _0 = 0;
    final int _9 = 9;
    final DrillBuf buffer = getAllocator().buffer(_9);

    long longVal = 0;
    buffer.clear();
    HadoopWritables.writeVLong(buffer, _0, _9, 0);
    longVal = HadoopWritables.readVLong(buffer, _0, _9);
    assertEquals(longVal, 0);

    buffer.clear();
    HadoopWritables.writeVLong(buffer, _0, _9, Long.MAX_VALUE);
    longVal = HadoopWritables.readVLong(buffer, _0, _9);
    assertEquals(longVal, Long.MAX_VALUE);

    buffer.clear();
    HadoopWritables.writeVLong(buffer, _0, _9, Long.MIN_VALUE);
    longVal = HadoopWritables.readVLong(buffer, _0, _9);
    assertEquals(longVal, Long.MIN_VALUE);

    int intVal = 0;
    buffer.clear();
    HadoopWritables.writeVInt(buffer, _0, _9, 0);
    intVal = HadoopWritables.readVInt(buffer, _0, _9);
    assertEquals(intVal, 0);

    buffer.clear();
    HadoopWritables.writeVInt(buffer, _0, _9, Integer.MAX_VALUE);
    intVal = HadoopWritables.readVInt(buffer, _0, _9);
    assertEquals(intVal, Integer.MAX_VALUE);

    buffer.clear();
    HadoopWritables.writeVInt(buffer, _0, _9, Integer.MIN_VALUE);
    intVal = HadoopWritables.readVInt(buffer, _0, _9);
    assertEquals(intVal, Integer.MIN_VALUE);
    buffer.release();
  }

  protected <T> void verifySQL(String sql, T expectedResults) throws Throwable {
    verifyResults(sql, expectedResults, getRunResult(QueryType.SQL, sql));
  }

  protected <T> void verifyPhysicalPlan(String expression, T expectedResults) throws Throwable {
    expression = expression.replace("\\", "\\\\\\\\"); // "\\\\\\\\" => Java => "\\\\" => JsonParser => "\\" => AntlrParser "\"

    if (textFileContent == null) {
      textFileContent = Resources.toString(Resources.getResource(CONVERSION_TEST_PHYSICAL_PLAN), Charsets.UTF_8);
    }
    String planString = textFileContent.replace("__CONVERT_EXPRESSION__", expression);

    verifyResults(expression, expectedResults, getRunResult(QueryType.PHYSICAL, planString));
  }

  protected Object[] getRunResult(QueryType queryType, String planString) throws Exception {
    List<QueryResultBatch> resultList = testRunAndReturn(queryType, planString);

    List<Object> res = new ArrayList<Object>();
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for(QueryResultBatch result : resultList) {
      if (result.getData() != null) {
        loader.load(result.getHeader().getDef(), result.getData());
        ValueVector v = loader.iterator().next().getValueVector();
        for (int j = 0; j < v.getAccessor().getValueCount(); j++) {
          if  (v instanceof VarCharVector) {
            res.add(new String(((VarCharVector) v).getAccessor().get(j)));
          } else {
            res.add(v.getAccessor().getObject(j));
          }
        }
        loader.clear();
        result.release();
      }
    }

    return res.toArray();
  }

  protected <T> void verifyResults(String expression, T expectedResults, Object[] actualResults) throws Throwable {
    String testName = String.format("Expression: %s.", expression);
    assertEquals(testName, 1, actualResults.length);
    assertNotNull(testName, actualResults[0]);
    if (expectedResults.getClass().isArray()) {
      assertArraysEquals(testName, expectedResults, actualResults[0]);
    } else {
      assertEquals(testName, expectedResults, actualResults[0]);
    }
  }

  protected void assertArraysEquals(Object expected, Object actual) {
    assertArraysEquals(null, expected, actual);
  }

  protected void assertArraysEquals(String message, Object expected, Object actual) {
    if (expected instanceof byte[] && actual instanceof byte[]) {
      assertArrayEquals(message, (byte[]) expected, (byte[]) actual);
    } else if (expected instanceof Object[] && actual instanceof Object[]) {
      assertArrayEquals(message, (Object[]) expected, (Object[]) actual);
    } else if (expected instanceof char[] && actual instanceof char[]) {
      assertArrayEquals(message, (char[]) expected, (char[]) actual);
    } else if (expected instanceof short[] && actual instanceof short[]) {
      assertArrayEquals(message, (short[]) expected, (short[]) actual);
    } else if (expected instanceof int[] && actual instanceof int[]) {
      assertArrayEquals(message, (int[]) expected, (int[]) actual);
    } else if (expected instanceof long[] && actual instanceof long[]) {
      assertArrayEquals(message, (long[]) expected, (long[]) actual);
    } else if (expected instanceof float[] && actual instanceof float[]) {
      assertArrayEquals(message, (float[]) expected, (float[]) actual, DELTA);
    } else if (expected instanceof double[] && actual instanceof double[]) {
      assertArrayEquals(message, (double[]) expected, (double[]) actual, DELTA);
    } else {
      fail(String.format("%s: Error comparing arrays of type '%s' and '%s'",
          expected.getClass().getName(), (actual == null ? "null" : actual.getClass().getName())));
    }
  }

}
