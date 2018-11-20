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
package org.apache.drill.exec.planner.logical;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.drill.shaded.guava.com.google.common.base.Function;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import io.netty.buffer.DrillBuf;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionStringBuilder;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.IntervalDayHolder;
import org.apache.drill.exec.expr.holders.IntervalYearHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.NullableDateHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal18Holder;
import org.apache.drill.exec.expr.holders.NullableDecimal28SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal38SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal9Holder;
import org.apache.drill.exec.expr.holders.NullableFloat4Holder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableIntervalDayHolder;
import org.apache.drill.exec.expr.holders.NullableIntervalYearHolder;
import org.apache.drill.exec.expr.holders.NullableTimeHolder;
import org.apache.drill.exec.expr.holders.NullableTimeStampHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.NullableVarDecimalHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.expr.holders.VarDecimalHolder;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.TypeInferenceUtils;
import org.apache.drill.exec.vector.DateUtilities;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.List;

public class DrillConstExecutor implements RexExecutor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillConstExecutor.class);

  private final PlannerSettings plannerSettings;

  // This is a list of all types that cannot be folded at planning time for various reasons, most of the types are
  // currently not supported at all. The reasons for the others can be found in the evaluation code in the reduce method
  public static final List<Object> NON_REDUCIBLE_TYPES = ImmutableList.builder().add(
      // cannot represent this as a literal according to calcite
      TypeProtos.MinorType.INTERVAL,

      // TODO - map and list are used in Drill but currently not expressible as literals, these can however be
      // outputs of functions that take literals as inputs (such as a convert_fromJSON with a literal string
      // as input), so we need to identify functions with these return types as non-foldable until we have a
      // literal representation for them
      TypeProtos.MinorType.MAP, TypeProtos.MinorType.LIST,

      // TODO - DRILL-2551 - Varbinary is used in execution, but it is missing a literal definition
      // in the logical expression representation and subsequently is not supported in
      // RexToDrill and the logical expression visitors
      TypeProtos.MinorType.VARBINARY,

      TypeProtos.MinorType.TIMESTAMPTZ, TypeProtos.MinorType.TIMETZ, TypeProtos.MinorType.LATE,
      TypeProtos.MinorType.TINYINT, TypeProtos.MinorType.SMALLINT, TypeProtos.MinorType.GENERIC_OBJECT, TypeProtos.MinorType.NULL,
      TypeProtos.MinorType.DECIMAL28DENSE, TypeProtos.MinorType.DECIMAL38DENSE, TypeProtos.MinorType.MONEY,
      TypeProtos.MinorType.FIXEDBINARY, TypeProtos.MinorType.FIXEDCHAR, TypeProtos.MinorType.FIXED16CHAR,
      TypeProtos.MinorType.VAR16CHAR, TypeProtos.MinorType.UINT1, TypeProtos.MinorType.UINT2, TypeProtos.MinorType.UINT4,
      TypeProtos.MinorType.UINT8)
      .build();

  FunctionImplementationRegistry funcImplReg;
  UdfUtilities udfUtilities;

  public DrillConstExecutor(FunctionImplementationRegistry funcImplReg, UdfUtilities udfUtilities, PlannerSettings plannerSettings) {
    this.funcImplReg = funcImplReg;
    this.udfUtilities = udfUtilities;
    this.plannerSettings = plannerSettings;
  }

  @Override
  public void reduce(final RexBuilder rexBuilder, List<RexNode> constExps, final List<RexNode> reducedValues) {
    for (final RexNode newCall : constExps) {
      LogicalExpression logEx = DrillOptiq.toDrill(new DrillParseContext(plannerSettings), (RelNode) null /* input rel */, newCall);

      ErrorCollectorImpl errors = new ErrorCollectorImpl();
      final LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(logEx, null, errors, funcImplReg);
      if (errors.getErrorCount() != 0) {
        String message = String.format(
            "Failure while materializing expression in constant expression evaluator [%s].  Errors: %s",
            newCall.toString(), errors.toString());
        throw UserException.planError()
          .message(message)
          .build(logger);
      }

      if (NON_REDUCIBLE_TYPES.contains(materializedExpr.getMajorType().getMinorType())) {
        logger.debug("Constant expression not folded due to return type {}, complete expression: {}",
            materializedExpr.getMajorType(),
            ExpressionStringBuilder.toString(materializedExpr));
        reducedValues.add(newCall);
        continue;
      }

      ValueHolder output = InterpreterEvaluator.evaluateConstantExpr(udfUtilities, materializedExpr);
      final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

      if (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL && TypeHelper.isNull(output)) {
        SqlTypeName sqlTypeName = TypeInferenceUtils.getCalciteTypeFromDrillType(materializedExpr.getMajorType().getMinorType());
        if (sqlTypeName == null) {
          String message = String.format("Error reducing constant expression, unsupported type: %s.",
              materializedExpr.getMajorType().getMinorType());
          throw UserException.unsupportedError()
            .message(message)
            .build(logger);
        }

        RelDataType type = TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, sqlTypeName, true);
        reducedValues.add(rexBuilder.makeNullLiteral(type));
        continue;
      }

      Function<ValueHolder, RexNode> literator = new Function<ValueHolder, RexNode>() {
        @Override
        public RexNode apply(ValueHolder output) {
          switch(materializedExpr.getMajorType().getMinorType()) {
            case INT: {
              int value = (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) ?
                ((NullableIntHolder) output).value : ((IntHolder) output).value;
              return rexBuilder.makeLiteral(new BigDecimal(value),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.INTEGER, newCall.getType().isNullable()), false);
            }
            case BIGINT: {
              long value = (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) ?
                ((NullableBigIntHolder) output).value : ((BigIntHolder) output).value;
              return rexBuilder.makeLiteral(new BigDecimal(value),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.BIGINT, newCall.getType().isNullable()), false);
            }
            case FLOAT4: {
              float value = (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) ?
                ((NullableFloat4Holder) output).value : ((Float4Holder) output).value;
              return rexBuilder.makeLiteral(new BigDecimal(value),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.FLOAT, newCall.getType().isNullable()), false);
            }
            case FLOAT8: {
              double value = (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) ?
                ((NullableFloat8Holder) output).value : ((Float8Holder) output).value;
              return rexBuilder.makeLiteral(new BigDecimal(value),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.DOUBLE, newCall.getType().isNullable()), false);
            }
            case VARCHAR: {
              String value = (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) ?
                StringFunctionHelpers.getStringFromVarCharHolder((NullableVarCharHolder)output) :
                StringFunctionHelpers.getStringFromVarCharHolder((VarCharHolder)output);
              return rexBuilder.makeLiteral(value,
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.VARCHAR, newCall.getType().isNullable()), false);
            }
            case BIT: {
              boolean value = (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) ?
                ((NullableBitHolder) output).value == 1 : ((BitHolder) output).value == 1;
              return rexBuilder.makeLiteral(value,
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.BOOLEAN, newCall.getType().isNullable()), false);
            }
            case DATE: {
              Calendar value = (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) ?
                new DateTime(((NullableDateHolder) output).value, DateTimeZone.UTC).toCalendar(null) :
                new DateTime(((DateHolder) output).value, DateTimeZone.UTC).toCalendar(null);
              return rexBuilder.makeLiteral(DateString.fromCalendarFields(value),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.DATE, newCall.getType().isNullable()), false);
            }
            case DECIMAL9: {
              long value;
              int scale;
              if (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) {
                NullableDecimal9Holder decimal9Out = (NullableDecimal9Holder)output;
                value = decimal9Out.value;
                scale = decimal9Out.scale;
              } else {
                Decimal9Holder decimal9Out = (Decimal9Holder)output;
                value = decimal9Out.value;
                scale = decimal9Out.scale;
              }
              return rexBuilder.makeLiteral(
                new BigDecimal(BigInteger.valueOf(value), scale),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.DECIMAL, newCall.getType().isNullable()),
                false);
            }
            case DECIMAL18: {
              long value;
              int scale;
              if (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) {
                NullableDecimal18Holder decimal18Out = (NullableDecimal18Holder)output;
                value = decimal18Out.value;
                scale = decimal18Out.scale;
              } else {
                Decimal18Holder decimal18Out = (Decimal18Holder)output;
                value = decimal18Out.value;
                scale = decimal18Out.scale;
              }
              return rexBuilder.makeLiteral(
                new BigDecimal(BigInteger.valueOf(value), scale),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.DECIMAL, newCall.getType().isNullable()),
                false);
            }
            case VARDECIMAL: {
              DrillBuf buffer;
              int start;
              int end;
              int scale;
              int precision;
              if (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) {
                NullableVarDecimalHolder varDecimalHolder = (NullableVarDecimalHolder) output;
                buffer = varDecimalHolder.buffer;
                start = varDecimalHolder.start;
                end = varDecimalHolder.end;
                scale = varDecimalHolder.scale;
                precision = varDecimalHolder.precision;
              } else {
                VarDecimalHolder varDecimalHolder = (VarDecimalHolder) output;
                buffer = varDecimalHolder.buffer;
                start = varDecimalHolder.start;
                end = varDecimalHolder.end;
                scale = varDecimalHolder.scale;
                precision = varDecimalHolder.precision;
              }
              return rexBuilder.makeLiteral(
                  org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromDrillBuf(buffer, start, end - start, scale),
                  typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale),
                  false);
            }
            case DECIMAL28SPARSE: {
              DrillBuf buffer;
              int start;
              int scale;
              if (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) {
                NullableDecimal28SparseHolder decimal28Out = (NullableDecimal28SparseHolder)output;
                buffer = decimal28Out.buffer;
                start = decimal28Out.start;
                scale = decimal28Out.scale;
              } else {
                Decimal28SparseHolder decimal28Out = (Decimal28SparseHolder)output;
                buffer = decimal28Out.buffer;
                start = decimal28Out.start;
                scale = decimal28Out.scale;
              }
              return rexBuilder.makeLiteral(
                org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(buffer, start * 20, 5, scale),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.DECIMAL, newCall.getType().isNullable()), false);
            }
            case DECIMAL38SPARSE: {
              DrillBuf buffer;
              int start;
              int scale;
              if (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) {
                NullableDecimal38SparseHolder decimal38Out = (NullableDecimal38SparseHolder)output;
                buffer = decimal38Out.buffer;
                start = decimal38Out.start;
                scale = decimal38Out.scale;
              } else {
                Decimal38SparseHolder decimal38Out = (Decimal38SparseHolder)output;
                buffer = decimal38Out.buffer;
                start = decimal38Out.start;
                scale = decimal38Out.scale;
              }
              return rexBuilder.makeLiteral(org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(buffer, start * 24, 6, scale),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.DECIMAL, newCall.getType().isNullable()),
                false);
            }
            case TIME: {
              Calendar value = (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) ?
                new DateTime(((NullableTimeHolder) output).value, DateTimeZone.UTC).toCalendar(null) :
                new DateTime(((TimeHolder) output).value, DateTimeZone.UTC).toCalendar(null);
              return rexBuilder.makeLiteral(TimeString.fromCalendarFields(value),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.TIME, newCall.getType().isNullable()), false);
            }
            case TIMESTAMP: {
              Calendar value = (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) ?
                new DateTime(((NullableTimeStampHolder) output).value, DateTimeZone.UTC).toCalendar(null) :
                new DateTime(((TimeStampHolder) output).value, DateTimeZone.UTC).toCalendar(null);
              return rexBuilder.makeLiteral(TimestampString.fromCalendarFields(value),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.TIMESTAMP, newCall.getType().isNullable()), false);
            }
            case INTERVALYEAR: {
              BigDecimal value = (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) ?
                new BigDecimal(((NullableIntervalYearHolder) output).value) :
                new BigDecimal(((IntervalYearHolder) output).value);
              return rexBuilder.makeLiteral(value,
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.INTERVAL_YEAR_MONTH, newCall.getType().isNullable()), false);
            }
            case INTERVALDAY: {
              int days;
              int milliseconds;
              if (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) {
                NullableIntervalDayHolder intervalDayOut = (NullableIntervalDayHolder) output;
                days = intervalDayOut.days;
                milliseconds = intervalDayOut.milliseconds;
              } else {
                IntervalDayHolder intervalDayOut = (IntervalDayHolder) output;
                days = intervalDayOut.days;
                milliseconds = intervalDayOut.milliseconds;
              }
              return rexBuilder.makeLiteral(
                  new BigDecimal(days * (long) DateUtilities.daysToStandardMillis + milliseconds),
                  TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.INTERVAL_DAY,
                      newCall.getType().isNullable()), false);
            }
            // The list of known unsupported types is used to trigger this behavior of re-using the input expression
            // before the expression is even attempted to be evaluated, this is just here as a last precaution a
            // as new types may be added in the future.
            default:
              logger.debug("Constant expression not folded due to return type {}, complete expression: {}",
                materializedExpr.getMajorType(),
                ExpressionStringBuilder.toString(materializedExpr));
              return newCall;
          }
        }
      };

      reducedValues.add(literator.apply(output));
    }
  }
}


