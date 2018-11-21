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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.TypedFieldExpr;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.fn.FunctionReplacementUtils;
import org.apache.drill.common.expression.fn.FuncHolder;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.fn.DrillSimpleFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.expr.holders.VarDecimalHolder;
import org.apache.drill.exec.expr.stat.ParquetBooleanPredicate;
import org.apache.drill.exec.expr.stat.ParquetComparisonPredicate;
import org.apache.drill.exec.expr.stat.ParquetFilterPredicate;
import org.apache.drill.exec.expr.stat.ParquetIsPredicate;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.util.DecimalUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A visitor which visits a materialized logical expression, and build ParquetFilterPredicate
 * If a visitXXX method returns null, that means the corresponding filter branch is not qualified for push down.
 */
public class ParquetFilterBuilder extends AbstractExprVisitor<LogicalExpression, Set<LogicalExpression>, RuntimeException> {
  static final Logger logger = LoggerFactory.getLogger(ParquetFilterBuilder.class);

  private final UdfUtilities udfUtilities;
  // Flag to check whether predicate cannot be fully converted
  // to parquet filter predicate without omitting its parts.
  // It should be set to false for the case when we want to
  // verify that predicate is fully convertible to parquet filter predicate,
  // otherwise null is returned instead of the converted expression.
  private final boolean omitUnsupportedExprs;

  /**
   * @param expr materialized filter expression
   * @param constantBoundaries set of constant expressions
   * @param udfUtilities udf utilities
   *
   * @return parquet filter predicate
   */
  public static ParquetFilterPredicate buildParquetFilterPredicate(LogicalExpression expr,
      Set<LogicalExpression> constantBoundaries, UdfUtilities udfUtilities, boolean omitUnsupportedExprs) {
    LogicalExpression logicalExpression =
        expr.accept(new ParquetFilterBuilder(udfUtilities, omitUnsupportedExprs), constantBoundaries);
    if (logicalExpression instanceof ParquetFilterPredicate) {
      return (ParquetFilterPredicate) logicalExpression;
    } else if (logicalExpression instanceof TypedFieldExpr) {
      // Calcite simplifies `= true` expression to field name, wrap it with is true predicate
      return (ParquetFilterPredicate) ParquetIsPredicate.createIsPredicate(FunctionGenerationHelper.IS_TRUE, logicalExpression);
    }
    logger.debug("Logical expression {} was not qualified for filter push down", logicalExpression);
    return null;
  }


  private ParquetFilterBuilder(UdfUtilities udfUtilities, boolean omitUnsupportedExprs) {
    this.udfUtilities = udfUtilities;
    this.omitUnsupportedExprs = omitUnsupportedExprs;
  }

  @Override
  public LogicalExpression visitUnknown(LogicalExpression e, Set<LogicalExpression> value) {
    // for the unknown expression, do nothing
    return null;
  }

  @Override
  public LogicalExpression visitTypedFieldExpr(TypedFieldExpr typedFieldExpr, Set<LogicalExpression> value) throws RuntimeException {
    return typedFieldExpr;
  }

  @Override
  public LogicalExpression visitIntConstant(ValueExpressions.IntExpression intExpr, Set<LogicalExpression> value)
      throws RuntimeException {
    return intExpr;
  }

  @Override
  public LogicalExpression visitDoubleConstant(ValueExpressions.DoubleExpression dExpr, Set<LogicalExpression> value)
      throws RuntimeException {
    return dExpr;
  }

  @Override
  public LogicalExpression visitFloatConstant(ValueExpressions.FloatExpression fExpr, Set<LogicalExpression> value)
      throws RuntimeException {
    return fExpr;
  }

  @Override
  public LogicalExpression visitLongConstant(ValueExpressions.LongExpression intExpr, Set<LogicalExpression> value)
      throws RuntimeException {
    return intExpr;
  }

  @Override
  public LogicalExpression visitVarDecimalConstant(ValueExpressions.VarDecimalExpression decExpr, Set<LogicalExpression> value)
      throws RuntimeException {
    return decExpr;
  }

  @Override
  public LogicalExpression visitDateConstant(ValueExpressions.DateExpression dateExpr, Set<LogicalExpression> value) throws RuntimeException {
    return dateExpr;
  }

  @Override
  public LogicalExpression visitTimeStampConstant(ValueExpressions.TimeStampExpression tsExpr, Set<LogicalExpression> value) throws RuntimeException {
    return tsExpr;
  }

  @Override
  public LogicalExpression visitTimeConstant(ValueExpressions.TimeExpression timeExpr, Set<LogicalExpression> value) throws RuntimeException {
    return timeExpr;
  }

  @Override
  public LogicalExpression visitBooleanConstant(ValueExpressions.BooleanExpression booleanExpression, Set<LogicalExpression> value) throws RuntimeException {
    return booleanExpression;
  }

  @Override
  public LogicalExpression visitQuotedStringConstant(ValueExpressions.QuotedString quotedString, Set<LogicalExpression> value) throws RuntimeException {
    return quotedString;
  }

  @Override
  public LogicalExpression visitBooleanOperator(BooleanOperator op, Set<LogicalExpression> value) {
    List<LogicalExpression> childPredicates = new ArrayList<>();
    String functionName = op.getName();

    for (LogicalExpression arg : op.args) {
      LogicalExpression childPredicate = arg.accept(this, value);
      if (childPredicate == null) {
        if (functionName.equals("booleanOr") || !omitUnsupportedExprs) {
          // we can't include any leg of the OR if any of the predicates cannot be converted
          // or prohibited omitting of unconverted operands
          return null;
        }
      } else {
        if (childPredicate instanceof TypedFieldExpr) {
          // Calcite simplifies `= true` expression to field name, wrap it with is true predicate
          childPredicate = ParquetIsPredicate.createIsPredicate(FunctionGenerationHelper.IS_TRUE, childPredicate);
        }
        childPredicates.add(childPredicate);
      }
    }

    if (childPredicates.size() == 0) {
      return null; // none leg is qualified, return null.
    } else if (childPredicates.size() == 1) {
      return childPredicates.get(0); // only one leg is qualified, remove boolean op.
    } else {
      return ParquetBooleanPredicate.createBooleanPredicate(functionName, op.getName(), childPredicates, op.getPosition());
    }
  }

  private LogicalExpression getValueExpressionFromConst(ValueHolder holder, TypeProtos.MinorType type) {
    switch (type) {
      case INT:
        return ValueExpressions.getInt(((IntHolder) holder).value);
      case BIGINT:
        return ValueExpressions.getBigInt(((BigIntHolder) holder).value);
      case FLOAT4:
        return ValueExpressions.getFloat4(((Float4Holder) holder).value);
      case FLOAT8:
        return ValueExpressions.getFloat8(((Float8Holder) holder).value);
      case VARDECIMAL:
        VarDecimalHolder decimalHolder = (VarDecimalHolder) holder;
        return ValueExpressions.getVarDecimal(
            DecimalUtility.getBigDecimalFromDrillBuf(decimalHolder.buffer,
                decimalHolder.start, decimalHolder.end - decimalHolder.start, decimalHolder.scale),
            decimalHolder.precision,
            decimalHolder.scale);
      case DATE:
        return ValueExpressions.getDate(((DateHolder) holder).value);
      case TIMESTAMP:
        return ValueExpressions.getTimeStamp(((TimeStampHolder) holder).value);
      case TIME:
        return ValueExpressions.getTime(((TimeHolder) holder).value);
      case BIT:
        return ValueExpressions.getBit(((BitHolder) holder).value == 1);
      case VARCHAR:
        VarCharHolder varCharHolder = (VarCharHolder) holder;
        String value = StringFunctionHelpers.toStringFromUTF8(varCharHolder.start, varCharHolder.end, varCharHolder.buffer);
        return ValueExpressions.getChar(value, value.length());
      default:
        return null;
    }
  }

  @Override
  public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression funcHolderExpr, Set<LogicalExpression> value)
      throws RuntimeException {
    FuncHolder holder = funcHolderExpr.getHolder();

    if (! (holder instanceof DrillSimpleFuncHolder)) {
      return null;
    }

    if (value.contains(funcHolderExpr)) {
      ValueHolder result;
      try {
        result = InterpreterEvaluator.evaluateConstantExpr(udfUtilities, funcHolderExpr);
      } catch (Exception e) {
        logger.warn("Error in evaluating function of {}", funcHolderExpr.getName());
        return null;
      }

      logger.debug("Reduce a constant function expression into a value expression");
      return getValueExpressionFromConst(result, funcHolderExpr.getMajorType().getMinorType());
    }

    final String funcName = ((DrillSimpleFuncHolder) holder).getRegisteredNames()[0];

    if (isCompareFunction(funcName)) {
      return handleCompareFunction(funcHolderExpr, value);
    }

    if (isIsFunction(funcName)) {
      return handleIsFunction(funcHolderExpr, value);
    }

    if (FunctionReplacementUtils.isCastFunction(funcName)) {
      List<LogicalExpression> newArgs = generateNewExpressions(funcHolderExpr.args, value);
      if (newArgs == null) {
        return null;
      }

      return funcHolderExpr.copy(newArgs);
    } else {
      return null;
    }
  }

  private List<LogicalExpression> generateNewExpressions(List<LogicalExpression> expressions, Set<LogicalExpression> value) {
    List<LogicalExpression> newExpressions = new ArrayList<>();
    for (LogicalExpression arg : expressions) {
      final LogicalExpression newArg = arg.accept(this, value);
      if (newArg == null) {
        return null;
      }
      newExpressions.add(newArg);
    }
    return newExpressions;
  }

  private LogicalExpression handleCompareFunction(FunctionHolderExpression functionHolderExpression, Set<LogicalExpression> value) {
    List<LogicalExpression> newArgs = generateNewExpressions(functionHolderExpression.args, value);
    if (newArgs == null) {
      return null;
    }

    String funcName = ((DrillSimpleFuncHolder) functionHolderExpression.getHolder()).getRegisteredNames()[0];

    return ParquetComparisonPredicate.createComparisonPredicate(funcName, newArgs.get(0), newArgs.get(1));
  }

  private LogicalExpression handleIsFunction(FunctionHolderExpression functionHolderExpression, Set<LogicalExpression> value) {
    String funcName;

    if (functionHolderExpression.getHolder() instanceof DrillSimpleFuncHolder) {
      funcName = ((DrillSimpleFuncHolder) functionHolderExpression.getHolder()).getRegisteredNames()[0];
    } else {
      logger.warn("Can not cast {} to DrillSimpleFuncHolder. Parquet filter pushdown can not handle function.",
          functionHolderExpression.getHolder());
      return null;
    }
    LogicalExpression arg = functionHolderExpression.args.get(0);

    return ParquetIsPredicate.createIsPredicate(funcName, arg.accept(this, value));
  }

  private static boolean isCompareFunction(String funcName) {
    return COMPARE_FUNCTIONS_SET.contains(funcName);
  }

  private static boolean isIsFunction(String funcName) {
    return IS_FUNCTIONS_SET.contains(funcName);
  }

  private static final ImmutableSet<String> COMPARE_FUNCTIONS_SET;

  static {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    COMPARE_FUNCTIONS_SET = builder
        .add(FunctionGenerationHelper.EQ)
        .add(FunctionGenerationHelper.GT)
        .add(FunctionGenerationHelper.GE)
        .add(FunctionGenerationHelper.LT)
        .add(FunctionGenerationHelper.LE)
        .add(FunctionGenerationHelper.NE)
        .build();
  }

  private static final ImmutableSet<String> IS_FUNCTIONS_SET;

  static {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    IS_FUNCTIONS_SET = builder
        .add(FunctionGenerationHelper.IS_NULL)
        .add(FunctionGenerationHelper.IS_NOT_NULL)
        .add(FunctionGenerationHelper.IS_TRUE)
        .add(FunctionGenerationHelper.IS_NOT_TRUE)
        .add(FunctionGenerationHelper.IS_FALSE)
        .add(FunctionGenerationHelper.IS_NOT_FALSE)
        .build();
  }

}
