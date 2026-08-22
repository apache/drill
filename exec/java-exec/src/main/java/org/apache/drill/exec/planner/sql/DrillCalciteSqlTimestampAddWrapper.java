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
package org.apache.drill.exec.planner.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

/**
 * Wrapper for Calcite's TIMESTAMPADD function that provides custom type inference.
 * Fixes Calcite 1.35 issue where DATE types incorrectly get precision added,
 * causing "typeName.allowsPrecScale(true, false): DATE" assertion errors.
 */
public class DrillCalciteSqlTimestampAddWrapper extends SqlFunction implements DrillCalciteSqlWrapper {
  private final SqlFunction operator;

  private static final SqlReturnTypeInference TIMESTAMP_ADD_INFERENCE = opBinding -> {
    RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

    // Get operand types
    RelDataType intervalType = opBinding.getOperandType(0);
    RelDataType datetimeType = opBinding.getOperandType(2);

    // Extract time unit from interval qualifier
    org.apache.calcite.avatica.util.TimeUnit timeUnit =
        intervalType.getIntervalQualifier().getStartUnit();

    SqlTypeName returnTypeName;
    int precision = -1;

    // Match logic from DrillConvertletTable.timestampAddConvertlet()
    switch (timeUnit) {
      case DAY:
      case WEEK:
      case MONTH:
      case QUARTER:
      case YEAR:
      case NANOSECOND:
        returnTypeName = datetimeType.getSqlTypeName();
        // Only set precision for types that support it (TIMESTAMP, TIME)
        if (returnTypeName == SqlTypeName.TIMESTAMP || returnTypeName == SqlTypeName.TIME) {
          precision = 3;
        }
        break;
      case MICROSECOND:
      case MILLISECOND:
        returnTypeName = SqlTypeName.TIMESTAMP;
        precision = 3;
        break;
      case SECOND:
      case MINUTE:
      case HOUR:
        if (datetimeType.getSqlTypeName() == SqlTypeName.TIME) {
          returnTypeName = SqlTypeName.TIME;
        } else {
          returnTypeName = SqlTypeName.TIMESTAMP;
        }
        precision = 3;
        break;
      default:
        returnTypeName = datetimeType.getSqlTypeName();
        precision = datetimeType.getPrecision();
    }

    RelDataType returnType;
    if (precision >= 0 && (returnTypeName == SqlTypeName.TIMESTAMP || returnTypeName == SqlTypeName.TIME)) {
      returnType = typeFactory.createSqlType(returnTypeName, precision);
    } else {
      returnType = typeFactory.createSqlType(returnTypeName);
    }

    // Apply nullability
    boolean isNullable = opBinding.getOperandType(1).isNullable() ||
                        opBinding.getOperandType(2).isNullable();
    return typeFactory.createTypeWithNullability(returnType, isNullable);
  };

  public DrillCalciteSqlTimestampAddWrapper(SqlFunction wrappedFunction) {
    super(wrappedFunction.getName(),
        wrappedFunction.getSqlIdentifier(),
        wrappedFunction.getKind(),
        TIMESTAMP_ADD_INFERENCE,
        wrappedFunction.getOperandTypeInference(),
        wrappedFunction.getOperandTypeChecker(),
        wrappedFunction.getParamTypes(),
        wrappedFunction.getFunctionType());
    this.operator = wrappedFunction;
  }

  @Override
  public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    return operator.rewriteCall(validator, call);
  }

  @Override
  public SqlOperator getOperator() {
    return operator;
  }

  @Override
  public boolean validRexOperands(int count, Litmus litmus) {
    return true;
  }

  @Override
  public String getAllowedSignatures(String opNameToUse) {
    return operator.getAllowedSignatures(opNameToUse);
  }

  @Override
  public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    return operator.getMonotonicity(call);
  }

  @Override
  public boolean isDeterministic() {
    return operator.isDeterministic();
  }

  @Override
  public boolean isDynamicFunction() {
    return operator.isDynamicFunction();
  }

  @Override
  public SqlSyntax getSyntax() {
    return operator.getSyntax();
  }

  @Override
  public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    operator.unparse(writer, call, leftPrec, rightPrec);
  }
}
