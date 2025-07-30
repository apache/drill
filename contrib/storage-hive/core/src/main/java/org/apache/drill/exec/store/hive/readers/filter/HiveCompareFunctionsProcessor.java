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
package org.apache.drill.exec.store.hive.readers.filter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.FunctionNames;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.ValueExpressions.TimeStampExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.sql.Timestamp;

public class HiveCompareFunctionsProcessor extends AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {

  private static final ImmutableSet<String> IS_FUNCTIONS_SET;

  private Object value;
  private PredicateLeaf.Type valueType;
  private boolean success;
  private SchemaPath path;
  private String functionName;

  public HiveCompareFunctionsProcessor(String functionName) {
    this.success = false;
    this.functionName = functionName;
  }

  static {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    IS_FUNCTIONS_SET = builder
        .add(FunctionNames.IS_NOT_NULL)
        .add("isNotNull")
        .add("is not null")
        .add(FunctionNames.IS_NULL)
        .add("isNull")
        .add("is null")
        .add(FunctionNames.IS_TRUE)
        .add(FunctionNames.IS_NOT_TRUE)
        .add(FunctionNames.IS_FALSE)
        .add(FunctionNames.IS_NOT_FALSE)
        .build();

  }

  private static final ImmutableMap<String, String> COMPARE_FUNCTIONS_TRANSPOSE_MAP;
  static {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    COMPARE_FUNCTIONS_TRANSPOSE_MAP = builder
        // binary functions
        .put(FunctionNames.LIKE, FunctionNames.LIKE)
        .put(FunctionNames.EQ, FunctionNames.EQ)
        .put(FunctionNames.NE, FunctionNames.NE)
        .put(FunctionNames.GE, FunctionNames.LE)
        .put(FunctionNames.GT, FunctionNames.LT)
        .put(FunctionNames.LE, FunctionNames.GE)
        .put(FunctionNames.LT, FunctionNames.GT)
        .build();
  }

  private static final ImmutableSet<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES;
  static {
    ImmutableSet.Builder<Class<? extends LogicalExpression>> builder = ImmutableSet.builder();
    VALUE_EXPRESSION_CLASSES = builder
        .add(BooleanExpression.class)
        .add(DateExpression.class)
        .add(DoubleExpression.class)
        .add(FloatExpression.class)
        .add(IntExpression.class)
        .add(LongExpression.class)
        .add(QuotedString.class)
        .add(TimeExpression.class)
        .build();
  }

  public static boolean isCompareFunction(final String functionName) {
    return COMPARE_FUNCTIONS_TRANSPOSE_MAP.keySet().contains(functionName);
  }

  public static boolean isIsFunction(final String funcName) {
    return IS_FUNCTIONS_SET.contains(funcName);
  }

  // shows whether function is simplified IS FALSE
  public static boolean isNot(final FunctionCall call, final String funcName) {
    return !call.args().isEmpty()
        && FunctionNames.NOT.equals(funcName);
  }

  public static HiveCompareFunctionsProcessor createFunctionsProcessorInstance(final FunctionCall call) {
    String functionName = call.getName();
    HiveCompareFunctionsProcessor evaluator = new HiveCompareFunctionsProcessor(functionName);

    return createFunctionsProcessorInstanceInternal(call, evaluator);
  }

  protected static <T extends HiveCompareFunctionsProcessor> T createFunctionsProcessorInstanceInternal(FunctionCall call, T evaluator) {
    LogicalExpression nameArg = call.arg(0);
    LogicalExpression valueArg = call.argCount() >= 2 ? call.arg(1) : null;
    if (valueArg != null) { // binary function
      if (VALUE_EXPRESSION_CLASSES.contains(nameArg.getClass())) {
        LogicalExpression swapArg = valueArg;
        valueArg = nameArg;
        nameArg = swapArg;
        evaluator.setFunctionName(COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(evaluator.getFunctionName()));
      }
      evaluator.setSuccess(nameArg.accept(evaluator, valueArg));
    } else if (call.arg(0) instanceof SchemaPath) {
      evaluator.setPath((SchemaPath) nameArg);
    }
    evaluator.setSuccess(true);
    return evaluator;
  }

  public boolean isSuccess() {
    return success;
  }

  protected void setSuccess(boolean success) {
    this.success = success;
  }

  public SchemaPath getPath() {
    return path;
  }

  protected void setPath(SchemaPath path) {
    this.path = path;
  }

  public String getFunctionName() {
    return functionName;
  }

  protected void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public PredicateLeaf.Type getValueType() {
    return valueType;
  }

  public void setValueType(PredicateLeaf.Type valueType) {
    this.valueType = valueType;
  }

  @Override
  public Boolean visitCastExpression(CastExpression e, LogicalExpression valueArg) throws RuntimeException {
    if (e.getInput() instanceof CastExpression || e.getInput() instanceof SchemaPath) {
      return e.getInput().accept(this, valueArg);
    }
    return false;
  }

  @Override
  public Boolean visitUnknown(LogicalExpression e, LogicalExpression valueArg) throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg) throws RuntimeException {
    if (valueArg instanceof QuotedString) {
      this.value = ((QuotedString) valueArg).getString();
      this.path = path;
      this.valueType = PredicateLeaf.Type.STRING;
      return true;
    }

    if (valueArg instanceof IntExpression) {
      int expValue = ((IntExpression) valueArg).getInt();
      this.value = ((Integer) expValue).longValue();
      this.path = path;
      this.valueType = PredicateLeaf.Type.LONG;
      return true;
    }

    if (valueArg instanceof LongExpression) {
      this.value = ((LongExpression) valueArg).getLong();
      this.path = path;
      this.valueType = PredicateLeaf.Type.LONG;
      return true;
    }

    if (valueArg instanceof FloatExpression) {
      this.value = ((FloatExpression) valueArg).getFloat();
      this.path = path;
      this.valueType = PredicateLeaf.Type.FLOAT;
      return true;
    }

    if (valueArg instanceof DoubleExpression) {
      this.value = ((DoubleExpression) valueArg).getDouble();
      this.path = path;
      this.valueType = PredicateLeaf.Type.FLOAT;
      return true;
    }

    if (valueArg instanceof BooleanExpression) {
      this.value = ((BooleanExpression) valueArg).getBoolean();
      this.path = path;
      this.valueType = PredicateLeaf.Type.BOOLEAN;
      return true;
    }

    if (valueArg instanceof DateExpression) {
      this.value = ((DateExpression) valueArg).getDate();
      this.path = path;
      this.valueType = PredicateLeaf.Type.LONG;
      return true;
    }

    if (valueArg instanceof TimeStampExpression) {
      long timeStamp = ((TimeStampExpression) valueArg).getTimeStamp();
      this.value = new Timestamp(timeStamp);
      this.path = path;
      this.valueType = PredicateLeaf.Type.TIMESTAMP;
      return true;
    }

    if (valueArg instanceof ValueExpressions.VarDecimalExpression) {
      double v = ((ValueExpressions.VarDecimalExpression) valueArg).getBigDecimal().doubleValue();
      this.value = new HiveDecimalWritable(String.valueOf(v));
      this.path = path;
      this.valueType = PredicateLeaf.Type.DECIMAL;
      return true;
    }
    return false;
  }
}
