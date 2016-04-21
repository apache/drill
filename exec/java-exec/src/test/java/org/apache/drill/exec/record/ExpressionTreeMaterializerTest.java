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
package org.apache.drill.exec.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.arrow.vector.types.MaterializedField;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.TypedNullConstant;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.DataMode;
import org.apache.arrow.vector.types.Types.MajorType;
import org.apache.arrow.vector.types.Types.MinorType;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

public class ExpressionTreeMaterializerTest extends ExecTest {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionTreeMaterializerTest.class);

  final MajorType boolConstant = new MajorType(MinorType.BIT,DataMode.REQUIRED);
  final MajorType bigIntType = new MajorType(MinorType.BIGINT,DataMode.REQUIRED);
  final MajorType intType = new MajorType(MinorType.INT,DataMode.REQUIRED);

  DrillConfig c = DrillConfig.create();
  FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);

  private MaterializedField getField(int fieldId, String name, MajorType type) {
    return MaterializedField.create(name, type);
  }

  @Test
  public void testMaterializingConstantTree(@Injectable RecordBatch batch) throws SchemaChangeException {

    ErrorCollector ec = new ErrorCollectorImpl();
    LogicalExpression expr = ExpressionTreeMaterializer.materialize(new ValueExpressions.LongExpression(1L,
        ExpressionPosition.UNKNOWN), batch, ec, registry);
    assertTrue(expr instanceof ValueExpressions.LongExpression);
    assertEquals(1L, ValueExpressions.LongExpression.class.cast(expr).getLong());
    assertFalse(ec.hasErrors());
  }

  @Test
  public void testMaterializingLateboundField(final @Injectable RecordBatch batch) throws SchemaChangeException {
    final SchemaBuilder builder = BatchSchema.newBuilder();
    builder.addField(getField(2, "test", bigIntType));
    final BatchSchema schema = builder.build();

    new NonStrictExpectations() {
      {
        batch.getValueVectorId(new SchemaPath("test", ExpressionPosition.UNKNOWN));
        result = new TypedFieldId(Types.required(MinorType.BIGINT), -5);
      }
    };

    ErrorCollector ec = new ErrorCollectorImpl();
    LogicalExpression expr = ExpressionTreeMaterializer.materialize(new FieldReference("test",
        ExpressionPosition.UNKNOWN), batch, ec, registry);
    assertEquals(bigIntType, expr.getMajorType());
    assertFalse(ec.hasErrors());
  }

  @Test
  public void testMaterializingLateboundTree(final @Injectable RecordBatch batch) throws SchemaChangeException {
    new NonStrictExpectations() {
      {
        batch.getValueVectorId(SchemaPath.getSimplePath("test"));
        result = new TypedFieldId(Types.required(MinorType.BIT), -4);
        batch.getValueVectorId(SchemaPath.getSimplePath("test1"));
        result = new TypedFieldId(Types.required(MinorType.BIGINT), -5);
      }
    };

    ErrorCollector ec = new ErrorCollectorImpl();


    LogicalExpression elseExpression = new IfExpression.Builder().setElse(new ValueExpressions.LongExpression(1L, ExpressionPosition.UNKNOWN))
        .setIfCondition(new IfExpression.IfCondition(new ValueExpressions.BooleanExpression("true", ExpressionPosition.UNKNOWN),
            new FieldReference("test1", ExpressionPosition.UNKNOWN)))
        .build();

    LogicalExpression expr = new IfExpression.Builder()
        .setIfCondition(new IfExpression.IfCondition(new FieldReference("test", ExpressionPosition.UNKNOWN), new ValueExpressions.LongExpression(2L, ExpressionPosition.UNKNOWN)))
        .setElse(elseExpression).build();

    LogicalExpression newExpr = ExpressionTreeMaterializer.materialize(expr, batch, ec, registry);
    assertTrue(newExpr instanceof IfExpression);
    IfExpression newIfExpr = (IfExpression) newExpr;
    //assertEquals(1, newIfExpr.conditions.size());
    IfExpression.IfCondition ifCondition = newIfExpr.ifCondition;
    assertTrue(newIfExpr.elseExpression instanceof IfExpression);
    //assertEquals(1, newIfExpr.conditions.size());
    //ifCondition = newIfExpr.conditions.get(0);
    assertEquals(bigIntType, ifCondition.expression.getMajorType());
    assertEquals(true, ((ValueExpressions.BooleanExpression) ((IfExpression)(newIfExpr.elseExpression)).ifCondition.condition).value);
    if (ec.hasErrors()) {
      System.out.println(ec.toErrorString());
    }
    assertFalse(ec.hasErrors());
  }

  @Test
  public void testMaterializingLateboundTreeValidated(final @Injectable RecordBatch batch) throws SchemaChangeException {
    ErrorCollector ec = new ErrorCollector() {
      int errorCount = 0;

      @Override
      public void addGeneralError(ExpressionPosition expr, String s) {
        errorCount++;
      }

      @Override
      public void addUnexpectedArgumentType(ExpressionPosition expr, String name, org.apache.drill.common.types.TypeProtos.MajorType actual,
          org.apache.drill.common.types.TypeProtos.MajorType[] expected, int argumentIndex) {
        errorCount++;
      }

      @Override
      public void addUnexpectedArgumentCount(ExpressionPosition expr, int actual, Range<Integer> expected) {
        errorCount++;
      }

      @Override
      public void addUnexpectedArgumentCount(ExpressionPosition expr, int actual, int expected) {
        errorCount++;
      }

      @Override
      public void addNonNumericType(ExpressionPosition expr, org.apache.drill.common.types.TypeProtos.MajorType actual) {
        errorCount++;
      }

      @Override
      public void addUnexpectedType(ExpressionPosition expr, int index, org.apache.drill.common.types.TypeProtos.MajorType actual) {
        errorCount++;
      }

      @Override
      public void addExpectedConstantValue(ExpressionPosition expr, int actual, String s) {
        errorCount++;
      }

      @Override
      public boolean hasErrors() {
        return errorCount > 0;
      }

      @Override
      public String toErrorString() {
        return String.format("Found %s errors.", errorCount);
      }

      @Override
      public int getErrorCount() {
        return errorCount;
      }
    };

    new NonStrictExpectations() {
      {
        batch.getValueVectorId(new SchemaPath("test", ExpressionPosition.UNKNOWN));
        result = new TypedFieldId(Types.required(MinorType.BIGINT), -5);
      }
    };


    LogicalExpression functionCallExpr = new FunctionCall("testFunc",
      ImmutableList.of((LogicalExpression) new FieldReference("test", ExpressionPosition.UNKNOWN) ),
      ExpressionPosition.UNKNOWN);
    LogicalExpression newExpr = ExpressionTreeMaterializer.materialize(functionCallExpr, batch, ec, registry);
    assertTrue(newExpr instanceof TypedNullConstant);
    assertEquals(1, ec.getErrorCount());
    System.out.println(ec.toErrorString());
  }

}
