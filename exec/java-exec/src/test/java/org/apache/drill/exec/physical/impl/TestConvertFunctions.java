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
import io.netty.buffer.ByteBuf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.UserProtos.QueryType;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.util.ConvertUtil;
import org.apache.drill.exec.util.ConvertUtil.HadoopWritables;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class TestConvertFunctions extends PopUnitTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestConvertFunctions.class);

  private static final String CONVERSION_TEST_LOGICAL_PLAN = "functions/conv/conversionTestWithLogicalPlan.json";
  private static final String CONVERSION_TEST_PHYSICAL_PLAN = "functions/conv/conversionTestWithPhysicalPlan.json";

  private static final float DELTA = (float) 0.0001;

  private static final DateFormat DATE_FORMAT;
  private static final DateFormat DATE_TIME_FORMAT;

  // "1980-01-01 01:23:45.678"
  private static final String DATE_TIME_BE = "\\x00\\x00\\x00\\x49\\x77\\x85\\x1f\\x8e";
  private static final String DATE_TIME_LE = "\\x8e\\x1f\\x85\\x77\\x49\\x00\\x00\\x00";

  private static Date time = null;
  private static Date date = null;

  static {
    DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    DATE_TIME_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");
    try {
      time = DATE_TIME_FORMAT.parse("01:23:45.678"); // 5025678
      date = DATE_FORMAT.parse("1980-01-01"); // 0x4977387000
    } catch (ParseException e) { }
  }

  DrillConfig c = DrillConfig.create();
  PhysicalPlanReader reader;
  FunctionImplementationRegistry registry;
  FragmentContext context;
  String textFileContent;

  @Test
  public void testDateTime1(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('" + DATE_TIME_BE + "'), 'TIME_EPOCH_BE')", time);
  }

  @Test
  public void testDateTime2(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('" + DATE_TIME_LE + "'), 'TIME_EPOCH')", time);
  }

  @Test
  public void testDateTime3(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('" + DATE_TIME_BE + "'), 'DATE_EPOCH_BE')", date );
  }

  @Test
  public void testDateTime4(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('" + DATE_TIME_LE + "'), 'DATE_EPOCH')", date);
  }

  @Test
  public void testFixedInts1(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('\\xAD'), 'TINYINT')", (byte) 0xAD);
  }

  @Test
  public void testFixedInts2(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('\\xFE\\xCA'), 'SMALLINT')", (short) 0xCAFE);
  }

  @Test
  public void testFixedInts3(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('\\xCA\\xFE'), 'SMALLINT_BE')", (short) 0xCAFE);
  }

  @Test
  public void testFixedInts4(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('\\xBE\\xBA\\xFE\\xCA'), 'INT')", 0xCAFEBABE);
  }

  @Test
  public void testFixedInts5(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('\\xCA\\xFE\\xBA\\xBE'), 'INT_BE')", 0xCAFEBABE);
  }

  @Test
  public void testFixedInts6(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('\\xEF\\xBE\\xAD\\xDE\\xBE\\xBA\\xFE\\xCA'), 'BIGINT')", 0xCAFEBABEDEADBEEFL);
  }

  @Test
  public void testFixedInts7(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('\\xCA\\xFE\\xBA\\xBE\\xDE\\xAD\\xBE\\xEF'), 'BIGINT_BE')", 0xCAFEBABEDEADBEEFL);
  }

  @Test
  public void testFixedInts8(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(convert_to(cast(77 as varchar(2)), 'INT_BE'), 'INT_BE')", 77);
  }

  @Test
  public void testFixedInts9(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(cast(77 as varchar(2)), 'INT_BE')", new byte[] {0, 0, 0, 77});
  }

  @Test
  public void testFixedInts10(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(cast(77 as varchar(2)), 'INT')", new byte[] {77, 0, 0, 0});
  }

  @Test
  public void testFixedInts11(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(77, 'BIGINT_BE')", new byte[] {0, 0, 0, 0, 0, 0, 0, 77});
  }

  @Test
  public void testFixedInts12(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(9223372036854775807, 'BIGINT')", new byte[] {-1, -1, -1, -1, -1, -1, -1, 0x7f});
  }

  @Test
  public void testFixedInts13(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(-9223372036854775808, 'BIGINT')", new byte[] {0, 0, 0, 0, 0, 0, 0, (byte)0x80});
  }

  @Test
  public void testVInts1(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(cast(0 as int), 'INT_HADOOPV')", new byte[] {0});
  }

  @Test
  public void testVInts2(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(cast(128 as int), 'INT_HADOOPV')", new byte[] {-113, -128});
  }

  @Test
  public void testVInts3(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(cast(256 as int), 'INT_HADOOPV')", new byte[] {-114, 1, 0});
  }

  @Test
  public void testVInts4(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(cast(65536 as int), 'INT_HADOOPV')", new byte[] {-115, 1, 0, 0});
  }

  @Test
  public void testVInts5(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(cast(16777216 as int), 'INT_HADOOPV')", new byte[] {-116, 1, 0, 0, 0});
  }

  @Test
  public void testVInts6(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(4294967296, 'BIGINT_HADOOPV')", new byte[] {-117, 1, 0, 0, 0, 0});
  }

  @Test
  public void testVInts7(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(1099511627776, 'BIGINT_HADOOPV')", new byte[] {-118, 1, 0, 0, 0, 0, 0});
  }

  @Test
  public void testVInts8(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(281474976710656, 'BIGINT_HADOOPV')", new byte[] {-119, 1, 0, 0, 0, 0, 0, 0});
  }

  @Test
  public void testVInts9(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(72057594037927936, 'BIGINT_HADOOPV')", new byte[] {-120, 1, 0, 0, 0, 0, 0, 0, 0});
  }

  @Test
  public void testVInts10(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(9223372036854775807, 'BIGINT_HADOOPV')", new byte[] {-120, 127, -1, -1, -1, -1, -1, -1, -1});
  }

  @Test
  public void testVInts11(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('\\x88\\x7f\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF'), 'BIGINT_HADOOPV')", 9223372036854775807L);
  }

  @Test
  public void testVInts12(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(-9223372036854775808, 'BIGINT_HADOOPV')", new byte[] {-128, 127, -1, -1, -1, -1, -1, -1, -1});
  }

  @Test
  public void testVInts13(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('\\x80\\x7f\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF'), 'BIGINT_HADOOPV')", -9223372036854775808L);
  }

  @Test
  public void testBool1(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('\\x01'), 'BOOLEAN_BYTE')", true);
  }

  @Test
  public void testBool2(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('\\x00'), 'BOOLEAN_BYTE')", false);
  }

  @Test
  public void testBool3(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(true, 'BOOLEAN_BYTE')", new byte[] {1});
  }

  @Test
  public void testBool4(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(false, 'BOOLEAN_BYTE')", new byte[] {0});
  }

  @Test
  public void testFloats1(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
  }

  @Test
  public void testFloats2(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(convert_to(cast(77 as float4), 'FLOAT'), 'FLOAT')", new Float(77.0));
  }

  @Test
  public void testFloats3(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(cast(1.4e-45 as float4), 'FLOAT')", new byte[] {1, 0, 0, 0});
  }

  @Test
  public void testFloats4(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(cast(3.4028235e+38 as float4), 'FLOAT')", new byte[] {-1, -1, 127, 127});
  }

  @Test
  public void testFloats5(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(convert_to(cast(77 as float8), 'DOUBLE'), 'DOUBLE')", 77.0);
  }

  @Test
  public void testFloats6(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(cast(77 as float8), 'DOUBLE')", new byte[] {0, 0, 0, 0, 0, 64, 83, 64});
  }

  @Test
  public void testFloats7(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(4.9e-324, 'DOUBLE')", new byte[] {1, 0, 0, 0, 0, 0, 0, 0});
  }

  @Test
  public void testFloats8(@Injectable final DrillbitContext bitContext,
                           @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_to(1.7976931348623157e+308, 'DOUBLE')", new byte[] {-1, -1, -1, -1, -1, -1, -17, 127});
  }

  @Test
  public void testUTF8(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection) throws Throwable {
    runTest(bitContext, connection, "convert_from(binary_string('apache_drill'), 'UTF8')", "apache_drill");
    runTest(bitContext, connection, "convert_to('apache_drill', 'UTF8')", new byte[] {'a', 'p', 'a', 'c', 'h', 'e', '_', 'd', 'r', 'i', 'l', 'l'});
  }

  @Test
  public void testBigIntVarCharReturnTripConvertLogical() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
        DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {
      bit1.run();
      client.connect();
      String logicalPlan = Resources.toString(Resources.getResource(CONVERSION_TEST_LOGICAL_PLAN), Charsets.UTF_8);
      List<QueryResultBatch> results = client.runQuery(QueryType.LOGICAL, logicalPlan);
      int count = 0;
      RecordBatchLoader loader = new RecordBatchLoader(bit1.getContext().getAllocator());
      for(QueryResultBatch b : results){
        count += b.getHeader().getRowCount();
        loader.load(b.getHeader().getDef(), b.getData());
        if (loader.getRecordCount() <= 0) {
          break;
        }
        VectorUtil.showVectorAccessibleContent(loader);
      }
      assertTrue(count == 10);
    }
  }

  @Test
  public void testHadooopVInt() throws Exception {
    final int _0 = 0;
    final int _9 = 9;
    final ByteBuf buffer = ConvertUtil.createBuffer(_9);

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
  }

  @SuppressWarnings("deprecation")
  protected <T> void runTest(@Injectable final DrillbitContext bitContext,
      @Injectable UserServer.UserClientConnection connection, String expression, T expectedResults) throws Throwable {

    final BufferAllocator allocator = new TopLevelAllocator();
    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = allocator;
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
    }};

    String testName = String.format("Expression: %s.", expression);
    expression = expression.replace("\\", "\\\\\\\\"); // "\\\\\\\\" => Java => "\\\\" => JsonParser => "\\" => AntlrParser "\"

    if (textFileContent == null) textFileContent = Resources.toString(Resources.getResource(CONVERSION_TEST_PHYSICAL_PLAN), Charsets.UTF_8);
    if(reader == null) reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    if(registry == null) registry = new FunctionImplementationRegistry(c);
    if(context == null) context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), connection, registry);

    String planString = textFileContent.replace("__CONVERT_EXPRESSION__", expression);
    PhysicalPlan plan = reader.readPhysicalPlan(planString);
    SimpleRootExec exec = new SimpleRootExec(
        ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    exec.next();
    Object[] results = getRunResult(exec);
    assertEquals(testName, 1, results.length);
    assertNotNull(testName, results[0]);
    if (expectedResults.getClass().isArray()) {
      assertArraysEquals(testName, expectedResults, results[0]);
    } else {
      assertEquals(testName, expectedResults, results[0]);
    }

    allocator.close();

    if(context.getFailureCause() != null){
      throw context.getFailureCause();
    }

    assertTrue(!context.isFailed());
  }

  protected Object[] getRunResult(SimpleRootExec exec) {
    Object[] res = new Object [exec.getRecordCount()];
    int i = 0;
    for (ValueVector v : exec) {
      for (int j = 0; j < v.getAccessor().getValueCount(); j++) {
        if  (v instanceof VarCharVector) {
          res[i++] = new String(((VarCharVector) v).getAccessor().get(j));
        } else {
          res[i++] = v.getAccessor().getObject(j);
        }
      }
      break;
    }
    return res;
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
