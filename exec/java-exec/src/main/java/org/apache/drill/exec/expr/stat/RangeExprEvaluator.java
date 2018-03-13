/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expr.stat;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.TypedFieldExpr;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.fn.CastFunctions;
import org.apache.drill.common.expression.fn.FuncHolder;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.fn.DrillSimpleFuncHolder;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.store.parquet.stat.ColumnStatistics;
import org.apache.drill.exec.vector.ValueHolderHelper;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RangeExprEvaluator extends AbstractExprVisitor<Statistics, Void, RuntimeException> {
  static final Logger logger = LoggerFactory.getLogger(RangeExprEvaluator.class);

  private final Map<SchemaPath, ColumnStatistics> columnStatMap;
  private final long rowCount;

  public RangeExprEvaluator(final Map<SchemaPath, ColumnStatistics> columnStatMap, long rowCount) {
    this.columnStatMap = columnStatMap;
    this.rowCount = rowCount;
  }

  public long getRowCount() {
    return this.rowCount;
  }

  @Override
  public Statistics visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    // do nothing for the unknown expression
    return null;
  }

  @Override
  public Statistics visitTypedFieldExpr(TypedFieldExpr typedFieldExpr, Void value) throws RuntimeException {
    final ColumnStatistics columnStatistics = columnStatMap.get(typedFieldExpr.getPath());
    if (columnStatistics != null) {
      return columnStatistics.getStatistics();
    } else if (typedFieldExpr.getMajorType().equals(Types.OPTIONAL_INT)) {
      // field does not exist.
      IntStatistics intStatistics = new IntStatistics();
      intStatistics.setNumNulls(rowCount); // all values are nulls
      return intStatistics;
    }
    return null;
  }

  @Override
  public Statistics visitIntConstant(ValueExpressions.IntExpression expr, Void value) throws RuntimeException {
    return getStatistics(expr.getInt());
  }

  @Override
  public Statistics visitBooleanConstant(ValueExpressions.BooleanExpression expr, Void value) throws RuntimeException {
    return getStatistics(expr.getBoolean());
  }

  @Override
  public Statistics visitLongConstant(ValueExpressions.LongExpression expr, Void value) throws RuntimeException {
    return getStatistics(expr.getLong());
  }

  @Override
  public Statistics visitFloatConstant(ValueExpressions.FloatExpression expr, Void value) throws RuntimeException {
    return getStatistics(expr.getFloat());
  }

  @Override
  public Statistics visitDoubleConstant(ValueExpressions.DoubleExpression expr, Void value) throws RuntimeException {
    return getStatistics(expr.getDouble());
  }

  @Override
  public Statistics visitDateConstant(ValueExpressions.DateExpression expr, Void value) throws RuntimeException {
    long dateInMillis = expr.getDate();
    return getStatistics(dateInMillis);
  }

  @Override
  public Statistics visitTimeStampConstant(ValueExpressions.TimeStampExpression tsExpr, Void value) throws RuntimeException {
    long tsInMillis = tsExpr.getTimeStamp();
    return getStatistics(tsInMillis);
  }

  @Override
  public Statistics visitTimeConstant(ValueExpressions.TimeExpression timeExpr, Void value) throws RuntimeException {
    int milliSeconds = timeExpr.getTime();
    return getStatistics(milliSeconds);
  }

  @Override
  public Statistics visitFunctionHolderExpression(FunctionHolderExpression holderExpr, Void value) throws RuntimeException {
    FuncHolder funcHolder = holderExpr.getHolder();

    if (! (funcHolder instanceof DrillSimpleFuncHolder)) {
      // Only Drill function is allowed.
      return null;
    }

    final String funcName = ((DrillSimpleFuncHolder) funcHolder).getRegisteredNames()[0];

    if (CastFunctions.isCastFunction(funcName)) {
      Statistics stat = holderExpr.args.get(0).accept(this, null);
      if (stat != null && ! stat.isEmpty()) {
        return evalCastFunc(holderExpr, stat);
      }
    }
    return null;
  }

  private IntStatistics getStatistics(int value) {
    return getStatistics(value, value);
  }

  private IntStatistics getStatistics(int min, int max) {
    final IntStatistics intStatistics = new IntStatistics();
    intStatistics.setMinMax(min, max);
    return intStatistics;
  }

  private BooleanStatistics getStatistics(boolean value) {
    return getStatistics(value, value);
  }

  private BooleanStatistics getStatistics(boolean min, boolean max) {
    final BooleanStatistics booleanStatistics = new BooleanStatistics();
    booleanStatistics.setMinMax(min, max);
    return booleanStatistics;
  }

  private LongStatistics getStatistics(long value) {
    return getStatistics(value, value);
  }

  private LongStatistics getStatistics(long min, long max) {
    final LongStatistics longStatistics = new LongStatistics();
    longStatistics.setMinMax(min, max);
    return longStatistics;
  }

  private DoubleStatistics getStatistics(double value) {
    return getStatistics(value, value);
  }

  private DoubleStatistics getStatistics(double min, double max) {
    final DoubleStatistics doubleStatistics = new DoubleStatistics();
    doubleStatistics.setMinMax(min, max);
    return doubleStatistics;
  }

  private FloatStatistics getStatistics(float value) {
    return getStatistics(value, value);
  }

  private FloatStatistics getStatistics(float min, float max) {
    final FloatStatistics floatStatistics = new FloatStatistics();
    floatStatistics.setMinMax(min, max);
    return floatStatistics;
  }

  private Statistics evalCastFunc(FunctionHolderExpression holderExpr, Statistics input) {
    try {
      DrillSimpleFuncHolder funcHolder = (DrillSimpleFuncHolder) holderExpr.getHolder();

      DrillSimpleFunc interpreter = funcHolder.createInterpreter();

      final ValueHolder minHolder, maxHolder;

      TypeProtos.MinorType srcType = holderExpr.args.get(0).getMajorType().getMinorType();
      TypeProtos.MinorType destType = holderExpr.getMajorType().getMinorType();

      if (srcType.equals(destType)) {
        // same type cast ==> NoOp.
        return input;
      } else if (!CAST_FUNC.containsKey(srcType) || !CAST_FUNC.get(srcType).contains(destType)) {
        return null; // cast func between srcType and destType is NOT allowed.
      }

      switch (srcType) {
      case INT :
        minHolder = ValueHolderHelper.getIntHolder(((IntStatistics)input).getMin());
        maxHolder = ValueHolderHelper.getIntHolder(((IntStatistics)input).getMax());
        break;
      case BIGINT:
        minHolder = ValueHolderHelper.getBigIntHolder(((LongStatistics)input).getMin());
        maxHolder = ValueHolderHelper.getBigIntHolder(((LongStatistics)input).getMax());
        break;
      case FLOAT4:
        minHolder = ValueHolderHelper.getFloat4Holder(((FloatStatistics)input).getMin());
        maxHolder = ValueHolderHelper.getFloat4Holder(((FloatStatistics)input).getMax());
        break;
      case FLOAT8:
        minHolder = ValueHolderHelper.getFloat8Holder(((DoubleStatistics)input).getMin());
        maxHolder = ValueHolderHelper.getFloat8Holder(((DoubleStatistics)input).getMax());
        break;
      case DATE:
        minHolder = ValueHolderHelper.getDateHolder(((LongStatistics)input).getMin());
        maxHolder = ValueHolderHelper.getDateHolder(((LongStatistics)input).getMax());
        break;
      default:
        return null;
      }

      final ValueHolder[] args1 = {minHolder};
      final ValueHolder[] args2 = {maxHolder};

      final ValueHolder minFuncHolder = InterpreterEvaluator.evaluateFunction(interpreter, args1, holderExpr.getName());
      final ValueHolder maxFuncHolder = InterpreterEvaluator.evaluateFunction(interpreter, args2, holderExpr.getName());

      switch (destType) {
      //TODO : need handle # of nulls.
      case INT:
        return getStatistics( ((IntHolder)minFuncHolder).value, ((IntHolder)maxFuncHolder).value);
      case BIGINT:
        return getStatistics( ((BigIntHolder)minFuncHolder).value, ((BigIntHolder)maxFuncHolder).value);
      case FLOAT4:
        return getStatistics( ((Float4Holder)minFuncHolder).value, ((Float4Holder)maxFuncHolder).value);
      case FLOAT8:
        return getStatistics( ((Float8Holder)minFuncHolder).value, ((Float8Holder)maxFuncHolder).value);
      case TIMESTAMP:
        return getStatistics(((TimeStampHolder) minFuncHolder).value, ((TimeStampHolder) maxFuncHolder).value);
      default:
        return null;
      }
    } catch (Exception e) {
      throw new DrillRuntimeException("Error in evaluating function of " + holderExpr.getName() );
    }
  }

  private static final Map<TypeProtos.MinorType, Set<TypeProtos.MinorType>> CAST_FUNC = new HashMap<>();
  static {
    // float -> double , int, bigint
    CAST_FUNC.put(TypeProtos.MinorType.FLOAT4, new HashSet<TypeProtos.MinorType>());
    CAST_FUNC.get(TypeProtos.MinorType.FLOAT4).add(TypeProtos.MinorType.FLOAT8);
    CAST_FUNC.get(TypeProtos.MinorType.FLOAT4).add(TypeProtos.MinorType.INT);
    CAST_FUNC.get(TypeProtos.MinorType.FLOAT4).add(TypeProtos.MinorType.BIGINT);

    // double -> float, int, bigint
    CAST_FUNC.put(TypeProtos.MinorType.FLOAT8, new HashSet<TypeProtos.MinorType>());
    CAST_FUNC.get(TypeProtos.MinorType.FLOAT8).add(TypeProtos.MinorType.FLOAT4);
    CAST_FUNC.get(TypeProtos.MinorType.FLOAT8).add(TypeProtos.MinorType.INT);
    CAST_FUNC.get(TypeProtos.MinorType.FLOAT8).add(TypeProtos.MinorType.BIGINT);

    // int -> float, double, bigint
    CAST_FUNC.put(TypeProtos.MinorType.INT, new HashSet<TypeProtos.MinorType>());
    CAST_FUNC.get(TypeProtos.MinorType.INT).add(TypeProtos.MinorType.FLOAT4);
    CAST_FUNC.get(TypeProtos.MinorType.INT).add(TypeProtos.MinorType.FLOAT8);
    CAST_FUNC.get(TypeProtos.MinorType.INT).add(TypeProtos.MinorType.BIGINT);

    // bigint -> int, float, double
    CAST_FUNC.put(TypeProtos.MinorType.BIGINT, new HashSet<TypeProtos.MinorType>());
    CAST_FUNC.get(TypeProtos.MinorType.BIGINT).add(TypeProtos.MinorType.INT);
    CAST_FUNC.get(TypeProtos.MinorType.BIGINT).add(TypeProtos.MinorType.FLOAT4);
    CAST_FUNC.get(TypeProtos.MinorType.BIGINT).add(TypeProtos.MinorType.FLOAT8);

    // date -> timestamp
    CAST_FUNC.put(TypeProtos.MinorType.DATE, new HashSet<TypeProtos.MinorType>());
    CAST_FUNC.get(TypeProtos.MinorType.DATE).add(TypeProtos.MinorType.TIMESTAMP);
  }

}
