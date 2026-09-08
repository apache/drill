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
package org.apache.drill.exec.store.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;

import org.apache.drill.common.FunctionNames;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.test.BaseTest;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Unit tests for AccumuloCompareFunctionsProcessor and AccumuloFilterBuilder.
 */
public class AccumuloFilterBuilderTest extends BaseTest {

  @Test
  public void testIsCompareFunction() {
    assertTrue(AccumuloCompareFunctionsProcessor.isCompareFunction(FunctionNames.EQ));
    assertTrue(AccumuloCompareFunctionsProcessor.isCompareFunction(FunctionNames.NE));
    assertTrue(AccumuloCompareFunctionsProcessor.isCompareFunction(FunctionNames.LT));
    assertTrue(AccumuloCompareFunctionsProcessor.isCompareFunction(FunctionNames.LE));
    assertTrue(AccumuloCompareFunctionsProcessor.isCompareFunction(FunctionNames.GT));
    assertTrue(AccumuloCompareFunctionsProcessor.isCompareFunction(FunctionNames.GE));
    assertTrue(AccumuloCompareFunctionsProcessor.isCompareFunction(FunctionNames.IS_NULL));
    assertTrue(AccumuloCompareFunctionsProcessor.isCompareFunction(FunctionNames.IS_NOT_NULL));

    assertFalse(AccumuloCompareFunctionsProcessor.isCompareFunction("unknown"));
    assertFalse(AccumuloCompareFunctionsProcessor.isCompareFunction(FunctionNames.AND));
    assertFalse(AccumuloCompareFunctionsProcessor.isCompareFunction(FunctionNames.OR));
  }

  @Test
  public void testProcessEqualFunction() {
    // row_key = 'test'
    SchemaPath path = SchemaPath.getSimplePath("row_key");
    ValueExpressions.QuotedString value = new ValueExpressions.QuotedString("test", 0, null);

    FunctionCall call = new FunctionCall(
        FunctionNames.EQ,
        ImmutableList.of(path, value),
        null);

    AccumuloCompareFunctionsProcessor processor =
        AccumuloCompareFunctionsProcessor.createFunctionsProcessorInstance(call);

    assertTrue(processor.isSuccess());
    assertEquals("row_key", processor.getPath().getRootSegmentPath());
    assertEquals("test", new String(processor.getValue(), StandardCharsets.UTF_8));
    assertEquals(FunctionNames.EQ, processor.getFunctionName());
  }

  @Test
  public void testProcessGreaterThanFunction() {
    // row_key > 'start'
    SchemaPath path = SchemaPath.getSimplePath("row_key");
    ValueExpressions.QuotedString value = new ValueExpressions.QuotedString("start", 0, null);

    FunctionCall call = new FunctionCall(
        FunctionNames.GT,
        ImmutableList.of(path, value),
        null);

    AccumuloCompareFunctionsProcessor processor =
        AccumuloCompareFunctionsProcessor.createFunctionsProcessorInstance(call);

    assertTrue(processor.isSuccess());
    assertEquals("row_key", processor.getPath().getRootSegmentPath());
    assertEquals("start", new String(processor.getValue(), StandardCharsets.UTF_8));
    assertEquals(FunctionNames.GT, processor.getFunctionName());
  }

  @Test
  public void testProcessLessThanFunction() {
    // row_key < 'end'
    SchemaPath path = SchemaPath.getSimplePath("row_key");
    ValueExpressions.QuotedString value = new ValueExpressions.QuotedString("end", 0, null);

    FunctionCall call = new FunctionCall(
        FunctionNames.LT,
        ImmutableList.of(path, value),
        null);

    AccumuloCompareFunctionsProcessor processor =
        AccumuloCompareFunctionsProcessor.createFunctionsProcessorInstance(call);

    assertTrue(processor.isSuccess());
    assertEquals(FunctionNames.LT, processor.getFunctionName());
  }

  @Test
  public void testProcessSwappedOperands() {
    // 'test' = row_key  (value on left)
    SchemaPath path = SchemaPath.getSimplePath("row_key");
    ValueExpressions.QuotedString value = new ValueExpressions.QuotedString("test", 0, null);

    FunctionCall call = new FunctionCall(
        FunctionNames.EQ,
        ImmutableList.of(value, path),
        null);

    AccumuloCompareFunctionsProcessor processor =
        AccumuloCompareFunctionsProcessor.createFunctionsProcessorInstance(call);

    assertTrue(processor.isSuccess());
    assertEquals("row_key", processor.getPath().getRootSegmentPath());
    assertEquals("test", new String(processor.getValue(), StandardCharsets.UTF_8));
    // Function should remain EQ since it's symmetric
    assertEquals(FunctionNames.EQ, processor.getFunctionName());
  }

  @Test
  public void testProcessSwappedGreaterThan() {
    // 'value' > row_key  should become row_key < 'value'
    SchemaPath path = SchemaPath.getSimplePath("row_key");
    ValueExpressions.QuotedString value = new ValueExpressions.QuotedString("value", 0, null);

    FunctionCall call = new FunctionCall(
        FunctionNames.GT,
        ImmutableList.of(value, path),
        null);

    AccumuloCompareFunctionsProcessor processor =
        AccumuloCompareFunctionsProcessor.createFunctionsProcessorInstance(call);

    assertTrue(processor.isSuccess());
    assertEquals("row_key", processor.getPath().getRootSegmentPath());
    // GT transposes to LT when operands are swapped
    assertEquals(FunctionNames.LT, processor.getFunctionName());
  }

  @Test
  public void testProcessIntegerValue() {
    // row_key = 123
    SchemaPath path = SchemaPath.getSimplePath("row_key");
    ValueExpressions.IntExpression value = new ValueExpressions.IntExpression(123, null);

    FunctionCall call = new FunctionCall(
        FunctionNames.EQ,
        ImmutableList.of(path, value),
        null);

    AccumuloCompareFunctionsProcessor processor =
        AccumuloCompareFunctionsProcessor.createFunctionsProcessorInstance(call);

    assertTrue(processor.isSuccess());
    assertEquals("123", new String(processor.getValue(), StandardCharsets.UTF_8));
  }

  @Test
  public void testProcessLongValue() {
    // row_key = 9999999999
    SchemaPath path = SchemaPath.getSimplePath("row_key");
    ValueExpressions.LongExpression value = new ValueExpressions.LongExpression(9999999999L, null);

    FunctionCall call = new FunctionCall(
        FunctionNames.EQ,
        ImmutableList.of(path, value),
        null);

    AccumuloCompareFunctionsProcessor processor =
        AccumuloCompareFunctionsProcessor.createFunctionsProcessorInstance(call);

    assertTrue(processor.isSuccess());
    assertEquals("9999999999", new String(processor.getValue(), StandardCharsets.UTF_8));
  }

  @Test
  public void testCompareTransposeMap() {
    // Verify the transpose map entries
    assertEquals(FunctionNames.LE,
        AccumuloCompareFunctionsProcessor.COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(FunctionNames.GE));
    assertEquals(FunctionNames.LT,
        AccumuloCompareFunctionsProcessor.COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(FunctionNames.GT));
    assertEquals(FunctionNames.GE,
        AccumuloCompareFunctionsProcessor.COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(FunctionNames.LE));
    assertEquals(FunctionNames.GT,
        AccumuloCompareFunctionsProcessor.COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(FunctionNames.LT));
    assertEquals(FunctionNames.EQ,
        AccumuloCompareFunctionsProcessor.COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(FunctionNames.EQ));
    assertEquals(FunctionNames.NE,
        AccumuloCompareFunctionsProcessor.COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(FunctionNames.NE));
  }
}
