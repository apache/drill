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

import java.nio.charset.StandardCharsets;

import org.apache.drill.common.FunctionNames;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Processor for comparison functions in filter expressions.
 *
 * <p>Extracts the field path and comparison value from expressions like:
 * <ul>
 *   <li>row_key = 'value'</li>
 *   <li>row_key > 'start'</li>
 *   <li>row_key < 'end'</li>
 * </ul>
 */
public class AccumuloCompareFunctionsProcessor
    extends AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {

  private byte[] value;
  private boolean success;
  private SchemaPath path;
  private String functionName;

  public static boolean isCompareFunction(String functionName) {
    return COMPARE_FUNCTIONS_TRANSPOSE_MAP.containsKey(functionName);
  }

  public static AccumuloCompareFunctionsProcessor createFunctionsProcessorInstance(
      FunctionCall call) {
    String functionName = call.getName();
    AccumuloCompareFunctionsProcessor evaluator =
        new AccumuloCompareFunctionsProcessor(functionName);

    LogicalExpression nameArg = call.arg(0);
    LogicalExpression valueArg = call.argCount() >= 2 ? call.arg(1) : null;

    if (valueArg != null) {
      // Binary function (e.g., row_key = 'value')
      if (VALUE_EXPRESSION_CLASSES.contains(nameArg.getClass())) {
        // Value on left side, field on right - swap and transpose function
        LogicalExpression swapArg = valueArg;
        valueArg = nameArg;
        nameArg = swapArg;
        evaluator.functionName = COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(evaluator.functionName);
      }
      evaluator.success = nameArg.accept(evaluator, valueArg);
    } else if (call.arg(0) instanceof SchemaPath) {
      // Unary function (IS NULL, IS NOT NULL)
      evaluator.success = true;
      evaluator.path = (SchemaPath) nameArg;
    }

    return evaluator;
  }

  public AccumuloCompareFunctionsProcessor(String functionName) {
    this.success = false;
    this.functionName = functionName;
  }

  public byte[] getValue() {
    return value;
  }

  public boolean isSuccess() {
    return success;
  }

  public SchemaPath getPath() {
    return path;
  }

  public String getFunctionName() {
    return functionName;
  }

  @Override
  public Boolean visitCastExpression(CastExpression e, LogicalExpression valueArg)
      throws RuntimeException {
    if (e.getInput() instanceof CastExpression || e.getInput() instanceof SchemaPath) {
      return e.getInput().accept(this, valueArg);
    }
    return false;
  }

  @Override
  public Boolean visitUnknown(LogicalExpression e, LogicalExpression valueArg)
      throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg)
      throws RuntimeException {
    if (valueArg instanceof QuotedString) {
      this.value = ((QuotedString) valueArg).value.getBytes(StandardCharsets.UTF_8);
      this.path = path;
      return true;
    }
    if (valueArg instanceof IntExpression) {
      this.value = String.valueOf(((IntExpression) valueArg).getInt())
          .getBytes(StandardCharsets.UTF_8);
      this.path = path;
      return true;
    }
    if (valueArg instanceof LongExpression) {
      this.value = String.valueOf(((LongExpression) valueArg).getLong())
          .getBytes(StandardCharsets.UTF_8);
      this.path = path;
      return true;
    }
    return false;
  }

  private static final ImmutableSet<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES;
  static {
    ImmutableSet.Builder<Class<? extends LogicalExpression>> builder = ImmutableSet.builder();
    VALUE_EXPRESSION_CLASSES = builder
        .add(QuotedString.class)
        .add(IntExpression.class)
        .add(LongExpression.class)
        .build();
  }

  static final ImmutableMap<String, String> COMPARE_FUNCTIONS_TRANSPOSE_MAP;
  static {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    COMPARE_FUNCTIONS_TRANSPOSE_MAP = builder
        // Unary functions
        .put(FunctionNames.IS_NOT_NULL, FunctionNames.IS_NOT_NULL)
        .put(FunctionNames.IS_NULL, FunctionNames.IS_NULL)
        // Binary functions - transpose for when value is on left
        .put(FunctionNames.EQ, FunctionNames.EQ)
        .put(FunctionNames.NE, FunctionNames.NE)
        .put(FunctionNames.GE, FunctionNames.LE)
        .put(FunctionNames.GT, FunctionNames.LT)
        .put(FunctionNames.LE, FunctionNames.GE)
        .put(FunctionNames.LT, FunctionNames.GT)
        .build();
  }
}
