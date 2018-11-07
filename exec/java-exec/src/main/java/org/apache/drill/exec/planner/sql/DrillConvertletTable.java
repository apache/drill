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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.drill.exec.planner.sql.parser.DrillCalciteWrapperUtility;

public class DrillConvertletTable implements SqlRexConvertletTable {

  public static HashMap<SqlOperator, SqlRexConvertlet> map = new HashMap<>();

  public static SqlRexConvertletTable INSTANCE = new DrillConvertletTable();

  private static final SqlRexConvertlet SQRT_CONVERTLET = (cx, call) -> {
    RexNode operand = cx.convertExpression(call.operand(0));
    return cx.getRexBuilder().makeCall(SqlStdOperatorTable.SQRT, operand);
  };

  // Rewrites COALESCE function into CASE WHEN IS NOT NULL operand1 THEN operand1...
  private static final SqlRexConvertlet COALESCE_CONVERTLET = (cx, call) -> {
    int operandsCount = call.operandCount();
    if (operandsCount == 1) {
      return cx.convertExpression(call.operand(0));
    } else {
      List<RexNode> caseOperands = new ArrayList<>();
      for (int i = 0; i < operandsCount - 1; i++) {
        RexNode caseOperand = cx.convertExpression(call.operand(i));
        caseOperands.add(cx.getRexBuilder().makeCall(
            SqlStdOperatorTable.IS_NOT_NULL, caseOperand));
        caseOperands.add(caseOperand);
      }
      caseOperands.add(cx.convertExpression(call.operand(operandsCount - 1)));
      return cx.getRexBuilder().makeCall(SqlStdOperatorTable.CASE, caseOperands);
    }
  };

  // Custom convertlet to avoid rewriting TIMESTAMP_DIFF by Calcite,
  // since Drill does not support Reinterpret function and does not handle
  // all Calcite interval representations correctly.
  private static final SqlRexConvertlet TIMESTAMP_DIFF_CONVERTLET = (cx, call) -> {
    SqlLiteral unitLiteral = call.operand(0);
    SqlIntervalQualifier qualifier =
        new SqlIntervalQualifier(unitLiteral.symbolValue(TimeUnit.class), null, SqlParserPos.ZERO);

    List<RexNode> operands = Arrays.asList(
        cx.convertExpression(qualifier),
        cx.convertExpression(call.operand(1)),
        cx.convertExpression(call.operand(2)));

    RelDataTypeFactory typeFactory = cx.getTypeFactory();

    RelDataType returnType = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.BIGINT),
        cx.getValidator().getValidatedNodeType(call.operand(1)).isNullable()
            || cx.getValidator().getValidatedNodeType(call.operand(2)).isNullable());

    return cx.getRexBuilder().makeCall(returnType,
        SqlStdOperatorTable.TIMESTAMP_DIFF, operands);
  };

  static {
    // Use custom convertlet for EXTRACT function
    map.put(SqlStdOperatorTable.EXTRACT, DrillExtractConvertlet.INSTANCE);
    // SQRT needs it's own convertlet because calcite overrides it to POWER(x, 0.5)
    // which is not suitable for Infinity value case
    map.put(SqlStdOperatorTable.SQRT, SQRT_CONVERTLET);
    map.put(SqlStdOperatorTable.COALESCE, COALESCE_CONVERTLET);
    map.put(SqlStdOperatorTable.TIMESTAMP_DIFF, TIMESTAMP_DIFF_CONVERTLET);
    map.put(SqlStdOperatorTable.AVG, new DrillAvgVarianceConvertlet(SqlKind.AVG));
    map.put(SqlStdOperatorTable.STDDEV_POP, new DrillAvgVarianceConvertlet(SqlKind.STDDEV_POP));
    map.put(SqlStdOperatorTable.STDDEV_SAMP, new DrillAvgVarianceConvertlet(SqlKind.STDDEV_SAMP));
    map.put(SqlStdOperatorTable.STDDEV, new DrillAvgVarianceConvertlet(SqlKind.STDDEV_SAMP));
    map.put(SqlStdOperatorTable.VAR_POP, new DrillAvgVarianceConvertlet(SqlKind.VAR_POP));
    map.put(SqlStdOperatorTable.VAR_SAMP, new DrillAvgVarianceConvertlet(SqlKind.VAR_SAMP));
    map.put(SqlStdOperatorTable.VARIANCE, new DrillAvgVarianceConvertlet(SqlKind.VAR_SAMP));
  }

  /*
   * Lookup the hash table to see if we have a custom convertlet for a given
   * operator, if we don't use StandardConvertletTable.
   */
  @Override
  public SqlRexConvertlet get(SqlCall call) {
    SqlRexConvertlet convertlet;
    if(call.getOperator() instanceof DrillCalciteSqlWrapper) {
      final SqlOperator wrapper = call.getOperator();
      final SqlOperator wrapped = DrillCalciteWrapperUtility.extractSqlOperatorFromWrapper(call.getOperator());
      if ((convertlet = map.get(wrapped)) != null) {
        return convertlet;
      }

      ((SqlBasicCall) call).setOperator(wrapped);
      SqlRexConvertlet sqlRexConvertlet = StandardConvertletTable.INSTANCE.get(call);
      ((SqlBasicCall) call).setOperator(wrapper);
      return sqlRexConvertlet;
    }

    if ((convertlet = map.get(call.getOperator())) != null) {
      return convertlet;
    }

    return StandardConvertletTable.INSTANCE.get(call);
  }

  private DrillConvertletTable() {
  }
}
