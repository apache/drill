/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.fn.CastFunctions;
import org.apache.drill.common.expression.fn.FuncHolder;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.fn.DrillSimpleFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.expr.stat.ParquetPredicates;
import org.apache.drill.exec.expr.stat.TypedFieldExpr;
import org.apache.drill.exec.ops.UdfUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A visitor which visits a materialized logical expression, and build ParquetFilterPredicate
 * If a visitXXX method returns null, that means the corresponding filter branch is not qualified for pushdown.
 */
public class ParquetFilterBuilder extends AbstractExprVisitor<LogicalExpression, Set<LogicalExpression>, RuntimeException> {
  static final Logger logger = LoggerFactory.getLogger(ParquetFilterBuilder.class);

  private final UdfUtilities udfUtilities;

  /**
   * @param expr materialized filter expression
   * @param constantBoundaries set of constant expressions
   * @param udfUtilities
   */
  public static LogicalExpression buildParquetFilterPredicate(LogicalExpression expr, final Set<LogicalExpression> constantBoundaries, UdfUtilities udfUtilities) {
    final LogicalExpression predicate = expr.accept(new ParquetFilterBuilder(udfUtilities), constantBoundaries);
    return predicate;
  }

  private ParquetFilterBuilder(UdfUtilities udfUtilities) {
    this.udfUtilities = udfUtilities;
  }

  @Override
  public LogicalExpression visitUnknown(LogicalExpression e, Set<LogicalExpression> value) {
    if (e instanceof TypedFieldExpr &&
        ! containsArraySeg(((TypedFieldExpr) e).getPath()) &&
        e.getMajorType().getMode() != TypeProtos.DataMode.REPEATED) {
      // A filter is not qualified for push down, if
      // 1. it contains an array segment : a.b[1], a.b[1].c.d
      // 2. it's repeated type.
      return e;
    }

    return null;
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
  public LogicalExpression visitBooleanOperator(BooleanOperator op, Set<LogicalExpression> value) {
    List<LogicalExpression> childPredicates = new ArrayList<>();
    String functionName = op.getName();

    for (LogicalExpression arg : op.args) {
      LogicalExpression childPredicate = arg.accept(this, value);
      if (childPredicate == null) {
        if (functionName.equals("booleanOr")) {
          // we can't include any leg of the OR if any of the predicates cannot be converted
          return null;
        }
      } else {
        childPredicates.add(childPredicate);
      }
    }

    if (childPredicates.size() == 0) {
      return null; // none leg is qualified, return null.
    } else if (childPredicates.size() == 1) {
      return childPredicates.get(0); // only one leg is qualified, remove boolean op.
    } else {
      if (functionName.equals("booleanOr")) {
        return new ParquetPredicates.OrPredicate(op.getName(), childPredicates, op.getPosition());
      } else {
        return new ParquetPredicates.AndPredicate(op.getName(), childPredicates, op.getPosition());
      }
    }
  }

  private boolean containsArraySeg(final SchemaPath schemaPath) {
    PathSegment seg = schemaPath.getRootSegment();

    while (seg != null) {
      if (seg.isArray()) {
        return true;
      }
      seg = seg.getChild();
    }
    return false;
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
    case DATE:
      return ValueExpressions.getDate(((DateHolder) holder).value);
    case TIMESTAMP:
      return ValueExpressions.getTimeStamp(((TimeStampHolder) holder).value);
    case TIME:
      return ValueExpressions.getTime(((TimeHolder) holder).value);
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
      ValueHolder result ;
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

    if (CastFunctions.isCastFunction(funcName)) {
      List<LogicalExpression> newArgs = new ArrayList();
      for (LogicalExpression arg : funcHolderExpr.args) {
        final LogicalExpression newArg = arg.accept(this, value);
        if (newArg == null) {
          return null;
        }
        newArgs.add(newArg);
      }

      return funcHolderExpr.copy(newArgs);
    } else {
      return null;
    }
  }

  private LogicalExpression handleCompareFunction(FunctionHolderExpression functionHolderExpression, Set<LogicalExpression> value) {
    List<LogicalExpression> newArgs = new ArrayList();

    for (LogicalExpression arg : functionHolderExpression.args) {
      LogicalExpression newArg = arg.accept(this, value);
      if (newArg == null) {
        return null;
      }
      newArgs.add(newArg);
    }

    String funcName = ((DrillSimpleFuncHolder) functionHolderExpression.getHolder()).getRegisteredNames()[0];

    switch (funcName) {
    case FunctionGenerationHelper.EQ :
      return new ParquetPredicates.EqualPredicate(newArgs.get(0), newArgs.get(1));
    case FunctionGenerationHelper.GT :
      return new ParquetPredicates.GTPredicate(newArgs.get(0), newArgs.get(1));
    case FunctionGenerationHelper.GE :
      return new ParquetPredicates.GEPredicate(newArgs.get(0), newArgs.get(1));
    case FunctionGenerationHelper.LT :
      return new ParquetPredicates.LTPredicate(newArgs.get(0), newArgs.get(1));
    case FunctionGenerationHelper.LE :
      return new ParquetPredicates.LEPredicate(newArgs.get(0), newArgs.get(1));
    case FunctionGenerationHelper.NE :
      return new ParquetPredicates.NEPredicate(newArgs.get(0), newArgs.get(1));
    default:
      return null;
    }
  }

  private LogicalExpression handleCastFunction(FunctionHolderExpression functionHolderExpression, Set<LogicalExpression> value) {
    for (LogicalExpression arg : functionHolderExpression.args) {
      LogicalExpression newArg = arg.accept(this, value);
      if (newArg == null) {
        return null;
      }
    }

    String funcName = ((DrillSimpleFuncHolder) functionHolderExpression.getHolder()).getRegisteredNames()[0];

    return null;
  }

  private static boolean isCompareFunction(String funcName) {
    return COMPARE_FUNCTIONS_SET.contains(funcName);
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

}
