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
package org.apache.drill.exec.expr;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.TypedFieldExpr;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.fn.FuncHolder;
import org.apache.drill.common.expression.fn.FunctionReplacementUtils;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.fn.DrillSimpleFuncHolder;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.vector.ValueHolderHelper;
import org.apache.drill.metastore.ColumnStatistics;
import org.apache.drill.metastore.ColumnStatisticsImpl;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.StatisticsKind;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public class StatisticsProvider<T extends Comparable<T>> extends AbstractExprVisitor<ColumnStatistics, Void, RuntimeException> {

  private final Map<SchemaPath, ColumnStatistics> columnStatMap;
  private final long rowCount;

  public StatisticsProvider(Map<SchemaPath, ColumnStatistics> columnStatMap, long rowCount) {
    this.columnStatMap = columnStatMap;
    this.rowCount = rowCount;
  }

  public long getRowCount() {
    return this.rowCount;
  }

  @Override
  public ColumnStatisticsImpl visitUnknown(LogicalExpression e, Void value) {
    // do nothing for the unknown expression
    return null;
  }

  @Override
  public ColumnStatistics visitTypedFieldExpr(TypedFieldExpr typedFieldExpr, Void value) {
    ColumnStatistics columnStatistics = columnStatMap.get(typedFieldExpr.getPath().getUnIndexed());
    if (columnStatistics != null) {
      return columnStatistics;
    } else if (typedFieldExpr.getMajorType().equals(Types.OPTIONAL_INT)) {
      // field does not exist.
      MinMaxStatistics<Integer> statistics = new MinMaxStatistics<>(null, null, Integer::compareTo);
      statistics.setNullsCount(rowCount); // all values are nulls
      return statistics;
    }
    return null;
  }

  @Override
  public ColumnStatistics<Integer> visitIntConstant(ValueExpressions.IntExpression expr, Void value) {
    int exprValue = expr.getInt();
    return new MinMaxStatistics<>(exprValue, exprValue, Integer::compareTo);
  }

  @Override
  public ColumnStatistics<Boolean> visitBooleanConstant(ValueExpressions.BooleanExpression expr, Void value) {
    boolean exprValue = expr.getBoolean();
    return new MinMaxStatistics<>(exprValue, exprValue, Boolean::compareTo);
  }

  @Override
  public ColumnStatistics<Long> visitLongConstant(ValueExpressions.LongExpression expr, Void value) {
    long exprValue = expr.getLong();
    return new MinMaxStatistics<>(exprValue, exprValue, Long::compareTo);
  }

  @Override
  public ColumnStatistics<Float> visitFloatConstant(ValueExpressions.FloatExpression expr, Void value) {
    float exprValue = expr.getFloat();
    return new MinMaxStatistics<>(exprValue, exprValue, Float::compareTo);
  }

  @Override
  public ColumnStatistics<Double> visitDoubleConstant(ValueExpressions.DoubleExpression expr, Void value) {
    double exprValue = expr.getDouble();
    return new MinMaxStatistics<>(exprValue, exprValue, Double::compareTo);
  }

  @Override
  public ColumnStatistics<Long> visitDateConstant(ValueExpressions.DateExpression expr, Void value) {
    long exprValue = expr.getDate();
    return new MinMaxStatistics<>(exprValue, exprValue, Long::compareTo);
  }

  @Override
  public ColumnStatistics<Long> visitTimeStampConstant(ValueExpressions.TimeStampExpression tsExpr, Void value) {
    long exprValue = tsExpr.getTimeStamp();
    return new MinMaxStatistics<>(exprValue, exprValue, Long::compareTo);
  }

  @Override
  public ColumnStatistics<Integer> visitTimeConstant(ValueExpressions.TimeExpression timeExpr, Void value) {
    int exprValue = timeExpr.getTime();
    return new MinMaxStatistics<>(exprValue, exprValue, Integer::compareTo);
  }

  @Override
  public ColumnStatistics<String> visitQuotedStringConstant(ValueExpressions.QuotedString quotedString, Void value) {
    String binary = quotedString.getString();
    return new MinMaxStatistics<>(binary, binary, Comparator.nullsFirst(Comparator.naturalOrder()));
  }

  @Override
  public ColumnStatistics<BigInteger> visitVarDecimalConstant(ValueExpressions.VarDecimalExpression decExpr, Void value) {
    BigInteger unscaled = decExpr.getBigDecimal().unscaledValue();
    return new MinMaxStatistics<>(
        unscaled,
        unscaled,
        Comparator.nullsFirst(Comparator.naturalOrder()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public ColumnStatistics visitFunctionHolderExpression(FunctionHolderExpression holderExpr, Void value) {
    FuncHolder funcHolder = holderExpr.getHolder();

    if (!(funcHolder instanceof DrillSimpleFuncHolder)) {
      // Only Drill function is allowed.
      return null;
    }

    String funcName = ((DrillSimpleFuncHolder) funcHolder).getRegisteredNames()[0];

    if (FunctionReplacementUtils.isCastFunction(funcName)) {
      ColumnStatistics<T> stat = holderExpr.args.get(0).accept(this, null);
      if (!IsPredicate.isNullOrEmpty(stat)) {
        return evalCastFunc(holderExpr, stat);
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private ColumnStatistics<T> evalCastFunc(FunctionHolderExpression holderExpr, ColumnStatistics<T> input) {
    try {
      DrillSimpleFuncHolder funcHolder = (DrillSimpleFuncHolder) holderExpr.getHolder();

      DrillSimpleFunc interpreter = funcHolder.createInterpreter();

      ValueHolder minHolder;
      ValueHolder maxHolder;

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
          minHolder = ValueHolderHelper.getIntHolder((Integer) ComparisonPredicate.getMinValue(input));
          maxHolder = ValueHolderHelper.getIntHolder((Integer) ComparisonPredicate.getMaxValue(input));
          break;
        case BIGINT:
          minHolder = ValueHolderHelper.getBigIntHolder((Long) ComparisonPredicate.getMinValue(input));
          maxHolder = ValueHolderHelper.getBigIntHolder((Long) ComparisonPredicate.getMaxValue(input));
          break;
        case FLOAT4:
          minHolder = ValueHolderHelper.getFloat4Holder((Float) ComparisonPredicate.getMinValue(input));
          maxHolder = ValueHolderHelper.getFloat4Holder((Float) ComparisonPredicate.getMaxValue(input));
          break;
        case FLOAT8:
          minHolder = ValueHolderHelper.getFloat8Holder((Double) ComparisonPredicate.getMinValue(input));
          maxHolder = ValueHolderHelper.getFloat8Holder((Double) ComparisonPredicate.getMaxValue(input));
          break;
        case DATE:
          minHolder = ValueHolderHelper.getDateHolder((Long) ComparisonPredicate.getMinValue(input));
          maxHolder = ValueHolderHelper.getDateHolder((Long) ComparisonPredicate.getMaxValue(input));
          break;
        default:
          return null;
      }

      ValueHolder[] args1 = {minHolder};
      ValueHolder[] args2 = {maxHolder};

      ValueHolder minFuncHolder = InterpreterEvaluator.evaluateFunction(interpreter, args1, holderExpr.getName());
      ValueHolder maxFuncHolder = InterpreterEvaluator.evaluateFunction(interpreter, args2, holderExpr.getName());

      MinMaxStatistics statistics;
      switch (destType) {
        case INT:
          statistics = new MinMaxStatistics<>(((IntHolder) minFuncHolder).value, ((IntHolder) maxFuncHolder).value, Integer::compareTo);
          break;
        case BIGINT:
          statistics = new MinMaxStatistics<>(((BigIntHolder) minFuncHolder).value, ((BigIntHolder) maxFuncHolder).value, Long::compareTo);
          break;
        case FLOAT4:
          statistics = new MinMaxStatistics<>(((Float4Holder) minFuncHolder).value, ((Float4Holder) maxFuncHolder).value, Float::compareTo);
          break;
        case FLOAT8:
          statistics = new MinMaxStatistics<>(((Float8Holder) minFuncHolder).value, ((Float8Holder) maxFuncHolder).value, Double::compareTo);
          break;
        case TIMESTAMP:
          statistics = new MinMaxStatistics<>(((TimeStampHolder) minFuncHolder).value, ((TimeStampHolder) maxFuncHolder).value, Long::compareTo);
          break;
        default:
          return null;
      }
      statistics.setNullsCount((long) input.getStatistic(ColumnStatisticsKind.NULLS_COUNT));
      return statistics;
    } catch (Exception e) {
      throw new DrillRuntimeException("Error in evaluating function of " + holderExpr.getName() );
    }
  }

  public static class MinMaxStatistics<V> implements ColumnStatistics<V> {
    private V minVal;
    private V maxVal;
    private long nullsCount;
    private Comparator<V> valueComparator;

    public MinMaxStatistics(V minVal, V maxVal, Comparator<V> valueComparator) {
      this.minVal = minVal;
      this.maxVal = maxVal;
      this.valueComparator = valueComparator;
    }

    @Override
    public Object getStatistic(StatisticsKind statisticsKind) {
      switch (statisticsKind.getName()) {
        case ExactStatisticsConstants.MIN_VALUE:
          return minVal;
        case ExactStatisticsConstants.MAX_VALUE:
          return maxVal;
        case ExactStatisticsConstants.NULLS_COUNT:
          return nullsCount;
        default:
          return null;
      }
    }

    @Override
    public boolean containsStatistic(StatisticsKind statisticsKind) {
      switch (statisticsKind.getName()) {
        case ExactStatisticsConstants.MIN_VALUE:
        case ExactStatisticsConstants.MAX_VALUE:
        case ExactStatisticsConstants.NULLS_COUNT:
          return true;
        default:
          return false;
      }
    }

    @Override
    public Comparator<V> getValueComparator() {
      return valueComparator;
    }

    @Override
    public ColumnStatistics<V> cloneWithStats(ColumnStatistics statistics) {
      throw new UnsupportedOperationException("MinMaxStatistics does not support cloneWithStats");
    }

    void setNullsCount(long nullsCount) {
      this.nullsCount = nullsCount;
    }
  }

  private static final Map<TypeProtos.MinorType, Set<TypeProtos.MinorType>> CAST_FUNC = new EnumMap<>(TypeProtos.MinorType.class);
  static {
    // float -> double , int, bigint
    Set<TypeProtos.MinorType> float4Types = EnumSet.noneOf(TypeProtos.MinorType.class);
    CAST_FUNC.put(TypeProtos.MinorType.FLOAT4, float4Types);
    float4Types.add(TypeProtos.MinorType.FLOAT8);
    float4Types.add(TypeProtos.MinorType.INT);
    float4Types.add(TypeProtos.MinorType.BIGINT);

    // double -> float, int, bigint
    Set<TypeProtos.MinorType> float8Types = EnumSet.noneOf(TypeProtos.MinorType.class);
    CAST_FUNC.put(TypeProtos.MinorType.FLOAT8, float8Types);
    float8Types.add(TypeProtos.MinorType.FLOAT4);
    float8Types.add(TypeProtos.MinorType.INT);
    float8Types.add(TypeProtos.MinorType.BIGINT);

    // int -> float, double, bigint
    Set<TypeProtos.MinorType> intTypes = EnumSet.noneOf(TypeProtos.MinorType.class);
    CAST_FUNC.put(TypeProtos.MinorType.INT, intTypes);
    intTypes.add(TypeProtos.MinorType.FLOAT4);
    intTypes.add(TypeProtos.MinorType.FLOAT8);
    intTypes.add(TypeProtos.MinorType.BIGINT);

    // bigint -> int, float, double
    Set<TypeProtos.MinorType> bigIntTypes = EnumSet.noneOf(TypeProtos.MinorType.class);
    CAST_FUNC.put(TypeProtos.MinorType.BIGINT, bigIntTypes);
    bigIntTypes.add(TypeProtos.MinorType.INT);
    bigIntTypes.add(TypeProtos.MinorType.FLOAT4);
    bigIntTypes.add(TypeProtos.MinorType.FLOAT8);

    // date -> timestamp
    Set<TypeProtos.MinorType> dateTypes = EnumSet.noneOf(TypeProtos.MinorType.class);
    CAST_FUNC.put(TypeProtos.MinorType.DATE, dateTypes);
    dateTypes.add(TypeProtos.MinorType.TIMESTAMP);
  }
}
