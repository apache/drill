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

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

public class DrillCalciteSqlFunctionWrapper extends SqlFunction implements DrillCalciteSqlWrapper  {
  public final SqlFunction operator;
  private SqlOperandTypeChecker operandTypeChecker = new Checker();
  private List<SqlOperator> sqlOperators;

  public DrillCalciteSqlFunctionWrapper(final SqlFunction wrappedFunction, List<SqlOperator> sqlOperators) {
    super(wrappedFunction.getName(),
        wrappedFunction.getSqlIdentifier(),
        wrappedFunction.getKind(),
        wrappedFunction.getReturnTypeInference(),
        wrappedFunction.getOperandTypeInference(),
        wrappedFunction.getOperandTypeChecker(),
        wrappedFunction.getParamTypes(),
        wrappedFunction.getFunctionType());
    this.operator = wrappedFunction;
    this.sqlOperators = sqlOperators;
  }

  @Override
  public SqlOperator getOperator() {
    return operator;
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
  public boolean requiresOrder() {
    return operator.requiresOrder();
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
    final RelDataTypeFactory factory = opBinding.getTypeFactory();
    final String name = opBinding.getOperator().getName().toUpperCase();
    if(name.equals("EXTRACT")) {
      RelDataType returnType = DrillExtractConvertlet.inferReturnType(
          factory,
          opBinding.getOperandType(0).getIntervalQualifier().getStartUnit(),
          opBinding.getOperandType(1).isNullable());
      return returnType;
    } else if(name.equals("SUBSTRING")) {
      RelDataType type = factory.createSqlType(SqlTypeName.VARCHAR, DrillSqlOperator.MAX_VARCHAR_LENGTH);
      if(opBinding.getOperandType(0).isNullable()) {
        type =  factory.createTypeWithNullability(type, true);
      }
      return type;
    }

    if(!sqlOperators.isEmpty()) {
      return sqlOperators.get(0).inferReturnType(opBinding);
    }

    return operator.inferReturnType(opBinding);
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

  @Override
  public String toString() {
    return operator.toString();
  }

  @Override
  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    operator.unparse(writer, call, leftPrec, rightPrec);
  }
}
