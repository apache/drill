/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.planner.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionStringBuilder;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
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
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.TypeInferenceUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class DrillConstExecutor implements RelOptPlanner.Executor {
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
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {
    for (RexNode newCall : constExps) {
      LogicalExpression logEx = DrillOptiq.toDrill(new DrillParseContext(plannerSettings), null /* input rel */, newCall);

      ErrorCollectorImpl errors = new ErrorCollectorImpl();
      LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(logEx, null, errors, funcImplReg);
      if (errors.getErrorCount() != 0) {
        String message = String.format(
            "Failure while materializing expression in constant expression evaluator [%s].  Errors: %s",
            newCall.toString(), errors.toString());
        logger.error(message);
        throw new DrillRuntimeException(message);
      }

      if (NON_REDUCIBLE_TYPES.contains(materializedExpr.getMajorType().getMinorType())) {
        logger.debug("Constant expression not folded due to return type {}, complete expression: {}",
            materializedExpr.getMajorType(),
            ExpressionStringBuilder.toString(materializedExpr));
        reducedValues.add(newCall);
        continue;
      }

      ValueHolder output = InterpreterEvaluator.evaluateConstantExpr(udfUtilities, materializedExpr);
      RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

      if (materializedExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL && TypeHelper.isNull(output)) {
        SqlTypeName sqlTypeName = TypeInferenceUtils.getCalciteTypeFromDrillType(materializedExpr.getMajorType().getMinorType());
        if (sqlTypeName == null) {
          String message = String.format("Error reducing constant expression, unsupported type: %s.",
              materializedExpr.getMajorType().getMinorType());
          logger.error(message);
          throw new DrillRuntimeException(message);
        }
        reducedValues.add(rexBuilder.makeNullLiteral(sqlTypeName));
        continue;
      }

        switch(materializedExpr.getMajorType().getMinorType()) {
          case INT:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(((IntHolder)output).value),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.INTEGER, newCall.getType().isNullable()),
                false));
            break;
          case BIGINT:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(((BigIntHolder)output).value),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.BIGINT, newCall.getType().isNullable()),
                false));
            break;
          case FLOAT4:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(((Float4Holder)output).value),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.FLOAT, newCall.getType().isNullable()),
                false));
            break;
          case FLOAT8:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(((Float8Holder)output).value),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.DOUBLE, newCall.getType().isNullable()),
                false));
            break;
          case VARCHAR:
            reducedValues.add(rexBuilder.makeCharLiteral(
                new NlsString(StringFunctionHelpers.getStringFromVarCharHolder((VarCharHolder)output), null, null)));
            break;
          case BIT:
            reducedValues.add(rexBuilder.makeLiteral(
                ((BitHolder)output).value == 1 ? true : false,
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.BOOLEAN, newCall.getType().isNullable()),
                false));
            break;
          case DATE:
            reducedValues.add(rexBuilder.makeLiteral(
                new DateTime(((DateHolder) output).value, DateTimeZone.UTC).toCalendar(null),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.DATE, newCall.getType().isNullable()),
                false));
            break;
          case DECIMAL9:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(BigInteger.valueOf(((Decimal9Holder) output).value), ((Decimal9Holder)output).scale),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.DECIMAL, newCall.getType().isNullable()),
                false));
            break;
          case DECIMAL18:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(BigInteger.valueOf(((Decimal18Holder) output).value), ((Decimal18Holder)output).scale),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.DECIMAL, newCall.getType().isNullable()),
                false));
            break;
          case DECIMAL28SPARSE:
            Decimal28SparseHolder decimal28Out = (Decimal28SparseHolder)output;
            reducedValues.add(rexBuilder.makeLiteral(
                org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(
                    decimal28Out.buffer,
                    decimal28Out.start * 20,
                    5,
                    decimal28Out.scale),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.DECIMAL, newCall.getType().isNullable()),
                false
            ));
            break;
          case DECIMAL38SPARSE:
            Decimal38SparseHolder decimal38Out = (Decimal38SparseHolder)output;
            reducedValues.add(rexBuilder.makeLiteral(
                org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(
                    decimal38Out.buffer,
                    decimal38Out.start * 24,
                    6,
                    decimal38Out.scale),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.DECIMAL, newCall.getType().isNullable()),
                false));
            break;

          case TIME:
            reducedValues.add(rexBuilder.makeLiteral(
                new DateTime(((TimeHolder)output).value, DateTimeZone.UTC).toCalendar(null),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.TIME, newCall.getType().isNullable()),
                false));
            break;
          case TIMESTAMP:
            reducedValues.add(rexBuilder.makeTimestampLiteral(
                new DateTime(((TimeStampHolder)output).value, DateTimeZone.UTC).toCalendar(null), 0));
            break;
          case INTERVALYEAR:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(((IntervalYearHolder)output).value),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.INTERVAL_YEAR_MONTH, newCall.getType().isNullable()),
                false));
            break;
          case INTERVALDAY:
            IntervalDayHolder intervalDayOut = (IntervalDayHolder) output;
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(intervalDayOut.days * DateUtility.daysToStandardMillis + intervalDayOut.milliseconds),
                TypeInferenceUtils.createCalciteTypeWithNullability(typeFactory, SqlTypeName.INTERVAL_DAY_TIME, newCall.getType().isNullable()),
                false));
            break;
          // The list of known unsupported types is used to trigger this behavior of re-using the input expression
          // before the expression is even attempted to be evaluated, this is just here as a last precaution a
          // as new types may be added in the future.
          default:
            logger.debug("Constant expression not folded due to return type {}, complete expression: {}",
                materializedExpr.getMajorType(),
                ExpressionStringBuilder.toString(materializedExpr));
            reducedValues.add(newCall);
            break;
        }
    }
  }
}


