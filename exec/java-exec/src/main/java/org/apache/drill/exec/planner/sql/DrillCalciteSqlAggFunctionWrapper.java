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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

public class DrillCalciteSqlAggFunctionWrapper extends SqlAggFunction implements DrillCalciteSqlWrapper {
  private final SqlAggFunction operator;
  private final List<SqlOperator> sqlOperators;
  private final RelDataType relDataType;
  private final SqlOperandTypeChecker operandTypeChecker = new Checker();

  @Override
  public SqlOperator getOperator() {
    return operator;
  }

  public DrillCalciteSqlAggFunctionWrapper(SqlAggFunction sqlAggFunction, List<SqlOperator> sqlOperators, RelDataType relDataType) {
    super(sqlAggFunction.getName(),
        sqlAggFunction.getSqlIdentifier(),
        sqlAggFunction.getKind(),
        sqlAggFunction.getReturnTypeInference(),
        sqlAggFunction.getOperandTypeInference(),
        sqlAggFunction.getOperandTypeChecker(),
        sqlAggFunction.getFunctionType(),
        sqlAggFunction.requiresOrder(),
        sqlAggFunction.requiresOver());
    this.operator = sqlAggFunction;
    this.sqlOperators = sqlOperators;
    this.relDataType = relDataType;
  }

  public DrillCalciteSqlAggFunctionWrapper(SqlAggFunction sqlAggFunction, List<SqlOperator> sqlOperators) {
    this(sqlAggFunction, sqlOperators, null);
  }

    @Override
    public boolean validRexOperands(int count, boolean fail) {
        return true;
    }

    @Override
    public String getAllowedSignatures(String opNameToUse) {
        return operator.getAllowedSignatures(opNameToUse);
    }

    @Override
    public SqlOperandTypeInference getOperandTypeInference() {
        return operator.getOperandTypeInference();
    }

    @Override
    public boolean isAggregator() {
        return operator.isAggregator();
    }

    @Override
    public boolean allowsFraming() {
        return operator.allowsFraming();
    }

    @Override
    public SqlReturnTypeInference getReturnTypeInference() {
        return operator.getReturnTypeInference();
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
    public boolean requiresDecimalExpansion() {
        return operator.requiresDecimalExpansion();
    }

    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        return operator.argumentMustBeScalar(ordinal);
    }

    @Override
    public SqlOperandTypeChecker getOperandTypeChecker() {
        return operandTypeChecker;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return operandTypeChecker.getOperandCountRange();
    }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    if(sqlOperators.isEmpty()) {
      if(relDataType != null) {
        return relDataType;
      }

      return opBinding.getTypeFactory()
          .createTypeWithNullability(opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY), true);
    }
    return sqlOperators.get(0).inferReturnType(opBinding);
  }

    @Override
    public boolean checkOperandTypes(
            SqlCallBinding callBinding,
            boolean throwOnFailure) {
        return true;
    }

    @Override
    public SqlSyntax getSyntax() {
        return operator.getSyntax();
    }

    @Override
    public List<String> getParamNames() {
        return operator.getParamNames();
    }


    @Override
    public String getSignatureTemplate(final int operandsCount) {
        return operator.getSignatureTemplate(operandsCount);
    }

    @Override
    public boolean isQuantifierAllowed() {
        return operator.isQuantifierAllowed();
    }

    @Override
    public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call) {
      return operator.deriveType(validator,
          scope,
          call);
    }
}
