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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class DrillCalciteSqlOperatorWrapper extends SqlOperator implements DrillCalciteSqlWrapper {
  public final SqlOperator operator;
  private SqlOperandTypeChecker operandTypeChecker = new Checker();

  public DrillCalciteSqlOperatorWrapper(SqlOperator operator) {
    super(
        operator.getName(),
        operator.getKind(),
        operator.getLeftPrec(),
        operator.getRightPrec(),
        operator.getReturnTypeInference(),
        operator.getOperandTypeInference(),
        operator.getOperandTypeChecker());
    this.operator = operator;
  }

  @Override
  public SqlOperator getOperator() {
    return operator;
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
  public SqlSyntax getSyntax() {
    return operator.getSyntax();
  }

  @Override
  public SqlCall createCall(
      SqlLiteral functionQualifier,
      SqlParserPos pos,
      SqlNode... operands) {
    return operator.createCall(functionQualifier, pos, operands);
  }

  @Override
  public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    return operator.rewriteCall(validator, call);
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    return operator.inferReturnType(opBinding);
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
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    return true;
  }

  @Override
  public boolean validRexOperands(int count, boolean fail) {
    return true;
  }

  @Override
  public String getSignatureTemplate(final int operandsCount) {
    return operator.getSignatureTemplate(operandsCount);
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