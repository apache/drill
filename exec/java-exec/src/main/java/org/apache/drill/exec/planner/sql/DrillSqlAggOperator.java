/**
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
package org.apache.drill.exec.planner.sql;

import com.google.common.collect.Lists;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import org.apache.drill.common.expression.DumbLogicalExpression;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;
import org.apache.drill.exec.resolver.FunctionResolver;
import org.apache.drill.exec.resolver.FunctionResolverFactory;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.apache.drill.exec.util.AssertionUtil;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class DrillSqlAggOperator extends SqlAggFunction {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlAggOperator.class);

  private static final MajorType NONE = MajorType.getDefaultInstance();

  private final List<DrillFuncHolder> functions;

  public DrillSqlAggOperator(String name, ArrayList<DrillFuncHolder> functions, int argCount) {
    super(name, new SqlIdentifier(name, SqlParserPos.ZERO), SqlKind.OTHER_FUNCTION, DynamicReturnType.INSTANCE,
        null, new Checker(argCount), SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.functions = functions;
  }

  private static MajorType getMajorType(RelDataType relDataType) {
    final MinorType minorType = DrillConstExecutor.getDrillTypeFromCalcite(relDataType);
    if (relDataType.isNullable()) {
      return Types.optional(minorType);
    } else {
      return Types.required(minorType);
    }
  }

  private RelDataType getReturnType(final SqlOperatorBinding opBinding, DrillFuncHolder func) {
    final RelDataTypeFactory factory = opBinding.getTypeFactory();
    // least restrictive type (nullable ANY type)
    final RelDataType anyType = factory.createSqlType(SqlTypeName.ANY);
    final RelDataType nullableAnyType = factory.createTypeWithNullability(anyType, true);

    final MajorType returnType = func.getReturnType();
    if (NONE.equals(returnType)) {
      return nullableAnyType;
    }

    final MinorType minorType = returnType.getMinorType();
    final SqlTypeName sqlTypeName = DrillConstExecutor.DRILL_TO_CALCITE_TYPE_MAPPING.get(minorType);
    if (sqlTypeName == null) {
      return factory.createTypeWithNullability(nullableAnyType, true);
    }

    final RelDataType relReturnType;
    switch (sqlTypeName) {
      case INTERVAL_DAY_TIME:
        relReturnType = factory.createSqlIntervalType(
            new SqlIntervalQualifier(
            TimeUnit.DAY,
            TimeUnit.MINUTE,
            SqlParserPos.ZERO));
        break;
      case INTERVAL_YEAR_MONTH:
        relReturnType = factory.createSqlIntervalType(
            new SqlIntervalQualifier(
            TimeUnit.YEAR,
            TimeUnit.MONTH,
            SqlParserPos.ZERO));
        break;
      default:
        relReturnType = factory.createSqlType(sqlTypeName);
    }

    switch(returnType.getMode()) {
      case OPTIONAL:
        return factory.createTypeWithNullability(relReturnType, true);
      case REQUIRED:
       return relReturnType;

      case REPEATED:
        return relReturnType;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    for (RelDataType type : opBinding.collectOperandTypes()) {
      if (type.getSqlTypeName() == SqlTypeName.ANY) {
        return opBinding.getTypeFactory()
            .createTypeWithNullability(opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY), true);
      }
    }

    final RelDataTypeFactory factory = opBinding.getTypeFactory();
    final FunctionResolver functionResolver = FunctionResolverFactory.getResolver();
    final List<LogicalExpression> args = Lists.newArrayList();
    for(final RelDataType type : opBinding.collectOperandTypes()) {
      if (type.getSqlTypeName() == SqlTypeName.ANY || type.getSqlTypeName() == SqlTypeName.DECIMAL) {
        return opBinding.getTypeFactory()
            .createTypeWithNullability(opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY), true);
      }

      final MajorType majorType = getMajorType(type);
      args.add(new DumbLogicalExpression(majorType));
    }
    final FunctionCall functionCall = new FunctionCall(opBinding.getOperator().getName(), args, ExpressionPosition.UNKNOWN);
    final DrillFuncHolder func = functionResolver.getBestMatch(functions, functionCall);

    RelDataType returnType = getReturnType(opBinding, func);
    if(returnType.getSqlTypeName() == SqlTypeName.VARCHAR) {
      final boolean isNullable = returnType.isNullable();
      returnType = factory.createSqlType(SqlTypeName.VARCHAR, DrillSqlOperator.MAX_VARCHAR_LENGTH);

      if(isNullable) {
        returnType = factory.createTypeWithNullability(returnType, true);
      }
    }

    return returnType;
  }
}
