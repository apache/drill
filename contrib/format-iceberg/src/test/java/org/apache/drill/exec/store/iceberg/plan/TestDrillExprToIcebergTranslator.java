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
package org.apache.drill.exec.store.iceberg.plan;

import java.util.Arrays;

import org.apache.drill.common.FunctionNames;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.test.BaseTest;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestDrillExprToIcebergTranslator extends BaseTest {
  @Test
  public void testGreaterThanOrEqualIncludesBoundary() {
    LogicalExpression filter = comparison(FunctionNames.GE, ValueExpressions.getInt(2));
    assertFalse(matches(filter, Types.IntegerType.get(), 1));
    assertTrue(matches(filter, Types.IntegerType.get(), 2));
    assertTrue(matches(filter, Types.IntegerType.get(), 3));
  }

  @Test
  public void testAndOrFunctionCallsAndNaryBooleanOperators() {
    LogicalExpression lower = comparison(FunctionNames.GE, ValueExpressions.getInt(2));
    LogicalExpression upper = comparison(FunctionNames.LT, ValueExpressions.getInt(4));
    LogicalExpression and = call(FunctionNames.AND, lower, upper);
    assertFalse(matches(and, Types.IntegerType.get(), 1));
    assertTrue(matches(and, Types.IntegerType.get(), 2));
    assertFalse(matches(and, Types.IntegerType.get(), 4));
    LogicalExpression or = call(FunctionNames.OR,
        comparison(FunctionNames.EQ, ValueExpressions.getInt(1)),
        comparison(FunctionNames.EQ, ValueExpressions.getInt(3)));
    assertTrue(matches(or, Types.IntegerType.get(), 1));
    assertFalse(matches(or, Types.IntegerType.get(), 2));
    assertTrue(matches(or, Types.IntegerType.get(), 3));
    LogicalExpression nary = new BooleanOperator(FunctionNames.OR, Arrays.asList(or,
        comparison(FunctionNames.EQ, ValueExpressions.getInt(5)),
        comparison(FunctionNames.EQ, ValueExpressions.getInt(7))), ExpressionPosition.UNKNOWN);
    assertTrue(matches(nary, Types.IntegerType.get(), 7));
    assertFalse(matches(nary, Types.IntegerType.get(), 6));
    LogicalExpression naryAnd = new BooleanOperator(FunctionNames.AND, Arrays.asList(lower, upper,
        comparison(FunctionNames.NE, ValueExpressions.getInt(3))), ExpressionPosition.UNKNOWN);
    assertTrue(matches(naryAnd, Types.IntegerType.get(), 2));
    assertFalse(matches(naryAnd, Types.IntegerType.get(), 3));
  }

  @Test
  public void testUnsupportedBooleanBranchRejectsWholePredicate() {
    LogicalExpression supported = comparison(FunctionNames.GE, ValueExpressions.getInt(2));
    LogicalExpression unsupported = call("unsupported_predicate", SchemaPath.getSimplePath("v"));
    assertNull(translate(call(FunctionNames.AND, supported, unsupported)));
    assertNull(translate(call(FunctionNames.OR, supported, unsupported)));
    assertNull(translate(new BooleanOperator(FunctionNames.OR,
        Arrays.asList(supported, unsupported), ExpressionPosition.UNKNOWN)));
  }

  @Test
  public void testDateUsesEpochDaysIncludingBeforeEpoch() {
    assertTrue(matches(comparison(FunctionNames.EQ, ValueExpressions.getDate(86_400_000L)),
        Types.DateType.get(), 1));
    assertFalse(matches(comparison(FunctionNames.EQ, ValueExpressions.getDate(86_400_000L)),
        Types.DateType.get(), 86_400_000));
    assertTrue(matches(comparison(FunctionNames.EQ, ValueExpressions.getDate(-1L)),
        Types.DateType.get(), -1));
    assertTrue(matches(comparison(FunctionNames.EQ, cast(-1L, MinorType.DATE)),
        Types.DateType.get(), -1));
  }

  @Test
  public void testTimeAndTimestampUseMicroseconds() {
    assertTrue(matches(comparison(FunctionNames.EQ, ValueExpressions.getTime(1234)),
        Types.TimeType.get(), 1_234_000L));
    assertFalse(matches(comparison(FunctionNames.EQ, ValueExpressions.getTime(1234)),
        Types.TimeType.get(), 1234L));
    assertTrue(matches(comparison(FunctionNames.EQ, cast(1234L, MinorType.TIME)),
        Types.TimeType.get(), 1_234_000L));
    assertTrue(matches(comparison(FunctionNames.EQ, ValueExpressions.getTimeStamp(-1234L)),
        Types.TimestampType.withoutZone(), -1_234_000L));
    assertTrue(matches(comparison(FunctionNames.EQ, cast(-1234L, MinorType.TIMESTAMP)),
        Types.TimestampType.withoutZone(), -1_234_000L));
  }

  @Test
  public void testComparisonsRejectNullAndNotStaysInDrill() {
    for (String operation : Arrays.asList(FunctionNames.LT, FunctionNames.LE, FunctionNames.NE)) {
      LogicalExpression filter = comparison(operation, ValueExpressions.getInt(2));
      Schema schema = new Schema(Types.NestedField.optional(1, "v", Types.IntegerType.get()));
      GenericRecord record = GenericRecord.create(schema);
      assertFalse(new Evaluator(schema.asStruct(), translate(filter)).eval(record));
    }
    assertNull(translate(call(FunctionNames.NOT, comparison(FunctionNames.GT, ValueExpressions.getInt(2)))));
  }

  private static LogicalExpression cast(long millis, MinorType type) {
    return new CastExpression(ValueExpressions.getBigInt(millis),
        org.apache.drill.common.types.Types.required(type), ExpressionPosition.UNKNOWN);
  }

  private static LogicalExpression comparison(String operation, LogicalExpression value) {
    return call(operation, SchemaPath.getSimplePath("v"), value);
  }

  private static LogicalExpression call(String name, LogicalExpression... arguments) {
    return new FunctionCall(name, Arrays.asList(arguments), ExpressionPosition.UNKNOWN);
  }

  private static Expression translate(LogicalExpression expression) {
    return expression.accept(DrillExprToIcebergTranslator.INSTANCE, null);
  }

  private static boolean matches(LogicalExpression expression, org.apache.iceberg.types.Type type, Object value) {
    Schema schema = new Schema(Types.NestedField.required(1, "v", type));
    GenericRecord record = GenericRecord.create(schema);
    record.setField("v", value);
    return new Evaluator(schema.asStruct(), translate(expression)).eval(record);
  }
}
