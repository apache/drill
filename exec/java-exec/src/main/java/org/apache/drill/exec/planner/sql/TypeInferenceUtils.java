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
package org.apache.drill.exec.planner.sql;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlRankFunction;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.MajorTypeInLogicalExpression;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.resolver.FunctionResolver;
import org.apache.drill.exec.resolver.FunctionResolverFactory;
import org.apache.drill.exec.resolver.TypeCastRules;

import java.util.List;

public class TypeInferenceUtils {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeInferenceUtils.class);

  public static final TypeProtos.MajorType UNKNOWN_TYPE = TypeProtos.MajorType.getDefaultInstance();
  private static final ImmutableMap<TypeProtos.MinorType, SqlTypeName> DRILL_TO_CALCITE_TYPE_MAPPING
      = ImmutableMap.<TypeProtos.MinorType, SqlTypeName> builder()
      .put(TypeProtos.MinorType.INT, SqlTypeName.INTEGER)
      .put(TypeProtos.MinorType.BIGINT, SqlTypeName.BIGINT)
      .put(TypeProtos.MinorType.FLOAT4, SqlTypeName.FLOAT)
      .put(TypeProtos.MinorType.FLOAT8, SqlTypeName.DOUBLE)
      .put(TypeProtos.MinorType.VARCHAR, SqlTypeName.VARCHAR)
      .put(TypeProtos.MinorType.BIT, SqlTypeName.BOOLEAN)
      .put(TypeProtos.MinorType.DATE, SqlTypeName.DATE)
      .put(TypeProtos.MinorType.DECIMAL9, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.DECIMAL18, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.DECIMAL28SPARSE, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.DECIMAL38SPARSE, SqlTypeName.DECIMAL)
      .put(TypeProtos.MinorType.TIME, SqlTypeName.TIME)
      .put(TypeProtos.MinorType.TIMESTAMP, SqlTypeName.TIMESTAMP)
      .put(TypeProtos.MinorType.VARBINARY, SqlTypeName.VARBINARY)
      .put(TypeProtos.MinorType.INTERVALYEAR, SqlTypeName.INTERVAL_YEAR_MONTH)
      .put(TypeProtos.MinorType.INTERVALDAY, SqlTypeName.INTERVAL_DAY_TIME)
      .put(TypeProtos.MinorType.MAP, SqlTypeName.MAP)
      .put(TypeProtos.MinorType.LIST, SqlTypeName.ARRAY)
      .put(TypeProtos.MinorType.LATE, SqlTypeName.ANY)

      // These are defined in the Drill type system but have been turned off for now
      // .put(TypeProtos.MinorType.TINYINT, SqlTypeName.TINYINT)
      // .put(TypeProtos.MinorType.SMALLINT, SqlTypeName.SMALLINT)
      // Calcite types currently not supported by Drill, nor defined in the Drill type list:
      //      - CHAR, SYMBOL, MULTISET, DISTINCT, STRUCTURED, ROW, OTHER, CURSOR, COLUMN_LIST
      .build();

  private static final ImmutableMap<SqlTypeName, TypeProtos.MinorType> CALCITE_TO_DRILL_MAPPING
      = ImmutableMap.<SqlTypeName, TypeProtos.MinorType> builder()
      .put(SqlTypeName.INTEGER, TypeProtos.MinorType.INT)
      .put(SqlTypeName.BIGINT, TypeProtos.MinorType.BIGINT)
      .put(SqlTypeName.FLOAT, TypeProtos.MinorType.FLOAT4)
      .put(SqlTypeName.DOUBLE, TypeProtos.MinorType.FLOAT8)
      .put(SqlTypeName.VARCHAR, TypeProtos.MinorType.VARCHAR)
      .put(SqlTypeName.BOOLEAN, TypeProtos.MinorType.BIT)
      .put(SqlTypeName.DATE, TypeProtos.MinorType.DATE)
      .put(SqlTypeName.TIME, TypeProtos.MinorType.TIME)
      .put(SqlTypeName.TIMESTAMP, TypeProtos.MinorType.TIMESTAMP)
      .put(SqlTypeName.VARBINARY, TypeProtos.MinorType.VARBINARY)
      .put(SqlTypeName.INTERVAL_YEAR_MONTH, TypeProtos.MinorType.INTERVALYEAR)
      .put(SqlTypeName.INTERVAL_DAY_TIME, TypeProtos.MinorType.INTERVALDAY)

      // SqlTypeName.CHAR is the type for Literals in Calcite, Drill treats Literals as VARCHAR also
      .put(SqlTypeName.CHAR, TypeProtos.MinorType.VARCHAR)

      // The following types are not added due to a variety of reasons:
      // (1) Disabling decimal type
      //.put(SqlTypeName.DECIMAL, TypeProtos.MinorType.DECIMAL9)
      //.put(SqlTypeName.DECIMAL, TypeProtos.MinorType.DECIMAL18)
      //.put(SqlTypeName.DECIMAL, TypeProtos.MinorType.DECIMAL28SPARSE)
      //.put(SqlTypeName.DECIMAL, TypeProtos.MinorType.DECIMAL38SPARSE)

      // (2) These 2 types are defined in the Drill type system but have been turned off for now
      // .put(SqlTypeName.TINYINT, TypeProtos.MinorType.TINYINT)
      // .put(SqlTypeName.SMALLINT, TypeProtos.MinorType.SMALLINT)

      // (3) Calcite types currently not supported by Drill, nor defined in the Drill type list:
      //      - SYMBOL, MULTISET, DISTINCT, STRUCTURED, ROW, OTHER, CURSOR, COLUMN_LIST
      // .put(SqlTypeName.MAP, TypeProtos.MinorType.MAP)
      // .put(SqlTypeName.ARRAY, TypeProtos.MinorType.LIST)
      .build();

  private static final ImmutableMap<String, SqlReturnTypeInference> funcNameToInference = ImmutableMap.<String, SqlReturnTypeInference> builder()
      .put("DATE_PART", DrillDatePartSqlReturnTypeInference.INSTANCE)
      .put("SUM", DrillSumSqlReturnTypeInference.INSTANCE)
      .put("COUNT", DrillCountSqlReturnTypeInference.INSTANCE)
      .put("CONCAT", DrillConcatSqlReturnTypeInference.INSTANCE)
      .put("LENGTH", DrillLengthSqlReturnTypeInference.INSTANCE)
      .put("LPAD", DrillPadTrimSqlReturnTypeInference.INSTANCE)
      .put("RPAD", DrillPadTrimSqlReturnTypeInference.INSTANCE)
      .put("LTRIM", DrillPadTrimSqlReturnTypeInference.INSTANCE)
      .put("RTRIM", DrillPadTrimSqlReturnTypeInference.INSTANCE)
      .put("BTRIM", DrillPadTrimSqlReturnTypeInference.INSTANCE)
      .put("TRIM", DrillPadTrimSqlReturnTypeInference.INSTANCE)
      .put("CONVERT_TO", DrillConvertToSqlReturnTypeInference.INSTANCE)
      .put("EXTRACT", DrillExtractSqlReturnTypeInference.INSTANCE)
      .put("SQRT", DrillSqrtSqlReturnTypeInference.INSTANCE)
      .put("CAST", DrillCastSqlReturnTypeInference.INSTANCE)
      .put("FLATTEN", DrillDeferToExecSqlReturnTypeInference.INSTANCE)
      .put("KVGEN", DrillDeferToExecSqlReturnTypeInference.INSTANCE)
      .put("CONVERT_FROM", DrillDeferToExecSqlReturnTypeInference.INSTANCE)

      // Window Functions
      // RANKING
      .put(SqlKind.CUME_DIST.name(), DrillRankingSqlReturnTypeInference.INSTANCE_DOUBLE)
      .put(SqlKind.DENSE_RANK.name(), DrillRankingSqlReturnTypeInference.INSTANCE_BIGINT)
      .put(SqlKind.PERCENT_RANK.name(), DrillRankingSqlReturnTypeInference.INSTANCE_DOUBLE)
      .put(SqlKind.RANK.name(), DrillRankingSqlReturnTypeInference.INSTANCE_BIGINT)
      .put(SqlKind.ROW_NUMBER.name(), DrillRankingSqlReturnTypeInference.INSTANCE_BIGINT)

      // NTILE
      .put("NTILE", DrillNTILESqlReturnTypeInference.INSTANCE)

      // LEAD, LAG
      .put("LEAD", DrillLeadLagSqlReturnTypeInference.INSTANCE)
      .put("LAG", DrillLeadLagSqlReturnTypeInference.INSTANCE)

      // FIRST_VALUE, LAST_VALUE
      .put("FIRST_VALUE", DrillFirstLastValueSqlReturnTypeInference.INSTANCE)
      .put("LAST_VALUE", DrillFirstLastValueSqlReturnTypeInference.INSTANCE)

      // Functions rely on DrillReduceAggregatesRule for expression simplification as opposed to getting evaluated directly
      .put(SqlAvgAggFunction.Subtype.AVG.name(), DrillAvgAggSqlReturnTypeInference.INSTANCE)
      .put(SqlAvgAggFunction.Subtype.STDDEV_POP.name(), DrillAvgAggSqlReturnTypeInference.INSTANCE)
      .put(SqlAvgAggFunction.Subtype.STDDEV_SAMP.name(), DrillAvgAggSqlReturnTypeInference.INSTANCE)
      .put(SqlAvgAggFunction.Subtype.VAR_POP.name(), DrillAvgAggSqlReturnTypeInference.INSTANCE)
      .put(SqlAvgAggFunction.Subtype.VAR_SAMP.name(), DrillAvgAggSqlReturnTypeInference.INSTANCE)
      .build();

  /**
   * Given a Drill's TypeProtos.MinorType, return a Calcite's corresponding SqlTypeName
   */
  public static SqlTypeName getCalciteTypeFromDrillType(final TypeProtos.MinorType type) {
    if(!DRILL_TO_CALCITE_TYPE_MAPPING.containsKey(type)) {
      return SqlTypeName.ANY;
    }

    return DRILL_TO_CALCITE_TYPE_MAPPING.get(type);
  }

  /**
   * Given a Calcite's RelDataType, return a Drill's corresponding TypeProtos.MinorType
   */
  public static TypeProtos.MinorType getDrillTypeFromCalciteType(final RelDataType relDataType) {
    final SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
    return getDrillTypeFromCalciteType(sqlTypeName);
  }

  /**
   * Given a Calcite's SqlTypeName, return a Drill's corresponding TypeProtos.MinorType
   */
  public static TypeProtos.MinorType getDrillTypeFromCalciteType(final SqlTypeName sqlTypeName) {
    if(!CALCITE_TO_DRILL_MAPPING.containsKey(sqlTypeName)) {
      return TypeProtos.MinorType.LATE;
    }

    return CALCITE_TO_DRILL_MAPPING.get(sqlTypeName);
  }

  /**
   * Give the name and DrillFuncHolder list, return the inference mechanism.
   */
  public static SqlReturnTypeInference getDrillSqlReturnTypeInference(
      final String name,
      final List<DrillFuncHolder> functions) {

    final String nameCap = name.toUpperCase();
    if(funcNameToInference.containsKey(nameCap)) {
      return funcNameToInference.get(nameCap);
    } else {
      return new DrillDefaultSqlReturnTypeInference(functions);
    }
  }

  private static class DrillDefaultSqlReturnTypeInference implements SqlReturnTypeInference {
    private final List<DrillFuncHolder> functions;

    public DrillDefaultSqlReturnTypeInference(List<DrillFuncHolder> functions) {
      this.functions = functions;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      if (functions.isEmpty()) {
        return factory.createTypeWithNullability(
            factory.createSqlType(SqlTypeName.ANY),
            true);
      }

      // The following logic is just a safe play:
      // Even if any of the input arguments has ANY type,
      // it "might" still be possible to determine the return type based on other non-ANY types
      for (RelDataType type : opBinding.collectOperandTypes()) {
        if (getDrillTypeFromCalciteType(type) == TypeProtos.MinorType.LATE) {
          // This code for boolean output type is added for addressing DRILL-1729
          // In summary, if we have a boolean output function in the WHERE-CLAUSE,
          // this logic can validate and execute user queries seamlessly
          boolean allBooleanOutput = true;
          for (DrillFuncHolder function : functions) {
            if (function.getReturnType().getMinorType() != TypeProtos.MinorType.BIT) {
              allBooleanOutput = false;
              break;
            }
          }

          if(allBooleanOutput) {
            return factory.createTypeWithNullability(
                factory.createSqlType(SqlTypeName.BOOLEAN), true);
          } else {
            return factory.createTypeWithNullability(
                factory.createSqlType(SqlTypeName.ANY),
                true);
          }
        }
      }

      final DrillFuncHolder func = resolveDrillFuncHolder(opBinding, functions);
      final RelDataType returnType = getReturnType(opBinding, func);
      return returnType.getSqlTypeName() == SqlTypeName.VARBINARY
          ? createCalciteTypeWithNullability(factory, SqlTypeName.ANY, returnType.isNullable())
              : returnType;
    }

    private static RelDataType getReturnType(final SqlOperatorBinding opBinding, final DrillFuncHolder func) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();

      // least restrictive type (nullable ANY type)
      final RelDataType nullableAnyType = factory.createTypeWithNullability(
          factory.createSqlType(SqlTypeName.ANY),
          true);

      final TypeProtos.MajorType returnType = func.getReturnType();
      if (UNKNOWN_TYPE.equals(returnType)) {
        return nullableAnyType;
      }

      final TypeProtos.MinorType minorType = returnType.getMinorType();
      final SqlTypeName sqlTypeName = getCalciteTypeFromDrillType(minorType);
      if (sqlTypeName == null) {
        return nullableAnyType;
      }

      final boolean isNullable;
      switch (returnType.getMode()) {
        case REPEATED:
        case OPTIONAL:
          isNullable = true;
          break;

        case REQUIRED:
          switch (func.getNullHandling()) {
            case INTERNAL:
              isNullable = false;
              break;

            case NULL_IF_NULL:
              boolean isNull = false;
              for (int i = 0; i < opBinding.getOperandCount(); ++i) {
                if (opBinding.getOperandType(i).isNullable()) {
                  isNull = true;
                  break;
                }
              }

              isNullable = isNull;
              break;
            default:
              throw new UnsupportedOperationException();
          }
          break;

        default:
          throw new UnsupportedOperationException();
      }

      return createCalciteTypeWithNullability(
          factory,
          sqlTypeName,
          isNullable);
    }
  }

  private static class DrillDeferToExecSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillDeferToExecSqlReturnTypeInference INSTANCE = new DrillDeferToExecSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      return factory.createTypeWithNullability(
          factory.createSqlType(SqlTypeName.ANY),
          true);
    }
  }

  private static class DrillSumSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillSumSqlReturnTypeInference INSTANCE = new DrillSumSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      // If there is group-by and the imput type is Non-nullable,
      // the output is Non-nullable;
      // Otherwise, the output is nullable.
      final boolean isNullable = opBinding.getGroupCount() == 0
          || opBinding.getOperandType(0).isNullable();

      if(getDrillTypeFromCalciteType(opBinding.getOperandType(0)) == TypeProtos.MinorType.LATE) {
        return createCalciteTypeWithNullability(
            factory,
            SqlTypeName.ANY,
            isNullable);
      }

      final RelDataType operandType = opBinding.getOperandType(0);
      final TypeProtos.MinorType inputMinorType = getDrillTypeFromCalciteType(operandType);
      if(TypeCastRules.getLeastRestrictiveType(Lists.newArrayList(inputMinorType, TypeProtos.MinorType.BIGINT))
          == TypeProtos.MinorType.BIGINT) {
        return createCalciteTypeWithNullability(
            factory,
            SqlTypeName.BIGINT,
            isNullable);
      } else if(TypeCastRules.getLeastRestrictiveType(Lists.newArrayList(inputMinorType, TypeProtos.MinorType.FLOAT8))
          == TypeProtos.MinorType.FLOAT8) {
        return createCalciteTypeWithNullability(
            factory,
            SqlTypeName.DOUBLE,
            isNullable);
      } else {
        throw UserException
            .functionError()
            .message(String.format("%s does not support operand types (%s)",
                opBinding.getOperator().getName(),
                opBinding.getOperandType(0).getSqlTypeName()))
            .build(logger);
      }
    }
  }

  private static class DrillCountSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillCountSqlReturnTypeInference INSTANCE = new DrillCountSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final SqlTypeName type = SqlTypeName.BIGINT;
      return createCalciteTypeWithNullability(
          factory,
          type,
          false);
    }
  }

  private static class DrillConcatSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillConcatSqlReturnTypeInference INSTANCE = new DrillConcatSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();

      boolean isNullable = true;
      int precision = 0;
      for(RelDataType relDataType : opBinding.collectOperandTypes()) {
        if(!relDataType.isNullable()) {
          isNullable = false;
        }

        // If the underlying columns cannot offer information regarding the precision (i.e., the length) of the VarChar,
        // Drill uses the largest to represent it
        if(relDataType.getPrecision() == TypeHelper.VARCHAR_DEFAULT_CAST_LEN
            || relDataType.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED) {
          precision = TypeHelper.VARCHAR_DEFAULT_CAST_LEN;
        } else {
          precision += relDataType.getPrecision();
        }
      }

      return factory.createTypeWithNullability(
          factory.createSqlType(SqlTypeName.VARCHAR, precision),
          isNullable);
    }
  }

  private static class DrillLengthSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillLengthSqlReturnTypeInference INSTANCE = new DrillLengthSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final SqlTypeName sqlTypeName = SqlTypeName.BIGINT;

      // We need to check only the first argument because
      // the second one is used to represent encoding type
      final boolean isNullable = opBinding.getOperandType(0).isNullable();
      return createCalciteTypeWithNullability(
          factory,
          sqlTypeName,
          isNullable);
    }
  }

  private static class DrillPadTrimSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillPadTrimSqlReturnTypeInference INSTANCE = new DrillPadTrimSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final SqlTypeName sqlTypeName = SqlTypeName.VARCHAR;

      for(int i = 0; i < opBinding.getOperandCount(); ++i) {
        if(opBinding.getOperandType(i).isNullable()) {
          return createCalciteTypeWithNullability(
              factory, sqlTypeName, true);
        }
      }

      return createCalciteTypeWithNullability(
          factory, sqlTypeName, false);
    }
  }

  private static class DrillConvertToSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillConvertToSqlReturnTypeInference INSTANCE = new DrillConvertToSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final SqlTypeName type = SqlTypeName.VARBINARY;

      return createCalciteTypeWithNullability(
          factory, type, opBinding.getOperandType(0).isNullable());
    }
  }

  private static class DrillExtractSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillExtractSqlReturnTypeInference INSTANCE = new DrillExtractSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final TimeUnit timeUnit = opBinding.getOperandType(0).getIntervalQualifier().getStartUnit();
      final boolean isNullable = opBinding.getOperandType(1).isNullable();

      final SqlTypeName sqlTypeName = getSqlTypeNameForTimeUnit(timeUnit.name());
      return createCalciteTypeWithNullability(
          factory,
          sqlTypeName,
          isNullable);
    }
  }

  private static class DrillSqrtSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillSqrtSqlReturnTypeInference INSTANCE = new DrillSqrtSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final boolean isNullable = opBinding.getOperandType(0).isNullable();
      return createCalciteTypeWithNullability(
          factory,
          SqlTypeName.DOUBLE,
          isNullable);
    }
  }

  private static class DrillDatePartSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillDatePartSqlReturnTypeInference INSTANCE = new DrillDatePartSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();

      final SqlNode firstOperand = ((SqlCallBinding) opBinding).operand(0);
      if(!(firstOperand instanceof SqlCharStringLiteral)) {
        return createCalciteTypeWithNullability(factory,
            SqlTypeName.ANY,
            opBinding.getOperandType(1).isNullable());
      }

      final String part = ((SqlCharStringLiteral) firstOperand)
          .getNlsString()
          .getValue()
          .toUpperCase();

      final SqlTypeName sqlTypeName = getSqlTypeNameForTimeUnit(part);
      final boolean isNullable = opBinding.getOperandType(1).isNullable();
      return createCalciteTypeWithNullability(
          factory,
          sqlTypeName,
          isNullable);
    }
  }

  private static class DrillCastSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillCastSqlReturnTypeInference INSTANCE = new DrillCastSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final boolean isNullable = opBinding
          .getOperandType(0)
          .isNullable();

      RelDataType ret = factory.createTypeWithNullability(
          opBinding.getOperandType(1),
          isNullable);
      if (opBinding instanceof SqlCallBinding) {
        SqlCallBinding callBinding = (SqlCallBinding) opBinding;
        SqlNode operand0 = callBinding.operand(0);

        // dynamic parameters and null constants need their types assigned
        // to them using the type they are casted to.
        if(((operand0 instanceof SqlLiteral)
            && (((SqlLiteral) operand0).getValue() == null))
                || (operand0 instanceof SqlDynamicParam)) {
          callBinding.getValidator().setValidatedNodeType(
              operand0,
              ret);
        }
      }

      return ret;
    }
  }

  private static class DrillRankingSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillRankingSqlReturnTypeInference INSTANCE_BIGINT = new DrillRankingSqlReturnTypeInference(SqlTypeName.BIGINT);
    private static final DrillRankingSqlReturnTypeInference INSTANCE_DOUBLE = new DrillRankingSqlReturnTypeInference(SqlTypeName.DOUBLE);

    private final SqlTypeName returnType;
    private DrillRankingSqlReturnTypeInference(final SqlTypeName returnType) {
      this.returnType = returnType;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      return createCalciteTypeWithNullability(
          opBinding.getTypeFactory(),
          returnType,
          false);
    }
  }

  private static class DrillNTILESqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillNTILESqlReturnTypeInference INSTANCE = new DrillNTILESqlReturnTypeInference();
    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      return createCalciteTypeWithNullability(
          opBinding.getTypeFactory(),
          SqlTypeName.INTEGER,
          opBinding.getOperandType(0).isNullable());
    }
  }

  private static class DrillLeadLagSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillLeadLagSqlReturnTypeInference INSTANCE = new DrillLeadLagSqlReturnTypeInference();
    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      return createCalciteTypeWithNullability(
          opBinding.getTypeFactory(),
          opBinding.getOperandType(0).getSqlTypeName(),
          true);
    }
  }

  private static class DrillFirstLastValueSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillFirstLastValueSqlReturnTypeInference INSTANCE = new DrillFirstLastValueSqlReturnTypeInference();
    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      return opBinding.getOperandType(0);
    }
  }

  private static class DrillAvgAggSqlReturnTypeInference implements SqlReturnTypeInference {
    private static final DrillAvgAggSqlReturnTypeInference INSTANCE = new DrillAvgAggSqlReturnTypeInference();
    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final boolean isNullable = opBinding.getGroupCount() == 0 || opBinding.hasFilter() || opBinding.getOperandType(0).isNullable();
      return createCalciteTypeWithNullability(
          opBinding.getTypeFactory(),
          SqlTypeName.DOUBLE,
          isNullable);
    }
  }

  private static DrillFuncHolder resolveDrillFuncHolder(final SqlOperatorBinding opBinding, final List<DrillFuncHolder> functions) {
    final FunctionCall functionCall = convertSqlOperatorBindingToFunctionCall(opBinding);
    final FunctionResolver functionResolver = FunctionResolverFactory.getResolver(functionCall);
    final DrillFuncHolder func = functionResolver.getBestMatch(functions, functionCall);

    // Throw an exception
    // if no DrillFuncHolder matched for the given list of operand types
    if(func == null) {
      String operandTypes = "";
      for(int i = 0; i < opBinding.getOperandCount(); ++i) {
        operandTypes += opBinding.getOperandType(i).getSqlTypeName();
        if(i < opBinding.getOperandCount() - 1) {
          operandTypes += ",";
        }
      }

      throw UserException
          .functionError()
          .message(String.format("%s does not support operand types (%s)",
              opBinding.getOperator().getName(),
              operandTypes))
          .build(logger);
    }
    return func;
  }

  /**
   * For Extract and date_part functions, infer the return types based on timeUnit
   */
  public static SqlTypeName getSqlTypeNameForTimeUnit(String timeUnit) {
    switch (timeUnit.toUpperCase()){
      case "YEAR":
      case "MONTH":
      case "DAY":
      case "HOUR":
      case "MINUTE":
        return SqlTypeName.BIGINT;
      case "SECOND":
        return SqlTypeName.DOUBLE;
      default:
        throw UserException
            .functionError()
            .message("extract function supports the following time units: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND")
            .build(logger);
    }
  }

  /**
   * Given a {@link SqlTypeName} and nullability, create a RelDataType from the RelDataTypeFactory
   *
   * @param typeFactory RelDataTypeFactory used to create the RelDataType
   * @param sqlTypeName the given SqlTypeName
   * @param isNullable  the nullability of the created RelDataType
   * @return RelDataType Type of call
   */
  public static RelDataType createCalciteTypeWithNullability(RelDataTypeFactory typeFactory,
                                                             SqlTypeName sqlTypeName,
                                                             boolean isNullable) {
    RelDataType type;
    if (sqlTypeName == SqlTypeName.INTERVAL_DAY_TIME) {
      type = typeFactory.createSqlIntervalType(
          new SqlIntervalQualifier(
              TimeUnit.DAY,
              TimeUnit.MINUTE,
              SqlParserPos.ZERO));
    } else if (sqlTypeName == SqlTypeName.INTERVAL_YEAR_MONTH) {
      type = typeFactory.createSqlIntervalType(
          new SqlIntervalQualifier(
              TimeUnit.YEAR,
              TimeUnit.MONTH,
              SqlParserPos.ZERO));
    } else if (sqlTypeName == SqlTypeName.VARCHAR) {
      type = typeFactory.createSqlType(sqlTypeName, TypeHelper.VARCHAR_DEFAULT_CAST_LEN);
    } else {
      type = typeFactory.createSqlType(sqlTypeName);
    }
    return typeFactory.createTypeWithNullability(type, isNullable);
  }

  /**
   * Given a SqlOperatorBinding, convert it to FunctionCall
   * @param  opBinding    the given SqlOperatorBinding
   * @return FunctionCall the converted FunctionCall
   */
  public static FunctionCall convertSqlOperatorBindingToFunctionCall(final SqlOperatorBinding opBinding) {
    final List<LogicalExpression> args = Lists.newArrayList();

    for (int i = 0; i < opBinding.getOperandCount(); ++i) {
      final RelDataType type = opBinding.getOperandType(i);
      final TypeProtos.MinorType minorType = getDrillTypeFromCalciteType(type);
      final TypeProtos.MajorType majorType;
      if (type.isNullable()) {
        majorType = Types.optional(minorType);
      } else {
        majorType = Types.required(minorType);
      }

      args.add(new MajorTypeInLogicalExpression(majorType));
    }

    final String drillFuncName = FunctionCallFactory.replaceOpWithFuncName(opBinding.getOperator().getName());
    final FunctionCall functionCall = new FunctionCall(
        drillFuncName,
        args,
        ExpressionPosition.UNKNOWN);
    return functionCall;
  }

  /**
   * This class is not intended to be instantiated
   */
  private TypeInferenceUtils() {

  }
}