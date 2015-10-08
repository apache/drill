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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.util.Util;

/*
 * This class is adapted from calcite's AvgVarianceConvertlet. The difference being
 * we add a cast to double before we perform the division. The reason we have a separate implementation
 * from calcite's code is because while injecting a similar cast, calcite will look
 * at the output type of the aggregate function which will be 'ANY' at that point and will
 * inject a cast to 'ANY' which does not solve the problem.
 */
public class DrillAvgVarianceConvertlet implements SqlRexConvertlet {

  private final SqlAvgAggFunction.Subtype subtype;
  private static final DrillSqlOperator CastHighOp = new DrillSqlOperator("CastHigh", 1, false);

  public DrillAvgVarianceConvertlet(SqlAvgAggFunction.Subtype subtype) {
    this.subtype = subtype;
  }

  public RexNode convertCall(SqlRexContext cx, SqlCall call) {
    assert call.operandCount() == 1;
    final SqlNode arg = call.operand(0);
    final SqlNode expr;
    switch (subtype) {
      case AVG:
        expr = expandAvg(arg);
        break;
      case STDDEV_POP:
        expr = expandVariance(arg, true, true);
        break;
      case STDDEV_SAMP:
        expr = expandVariance(arg, false, true);
        break;
      case VAR_POP:
        expr = expandVariance(arg, true, false);
        break;
      case VAR_SAMP:
        expr = expandVariance(arg, false, false);
        break;
      default:
        throw Util.unexpected(subtype);
    }
    return cx.convertExpression(expr);
  }

  private SqlNode expandAvg(
      final SqlNode arg) {
    final SqlParserPos pos = SqlParserPos.ZERO;
    final SqlNode sum =
        SqlStdOperatorTable.SUM.createCall(pos, arg);
    final SqlNode count =
        SqlStdOperatorTable.COUNT.createCall(pos, arg);
    final SqlNode sumAsDouble =
        CastHighOp.createCall(pos, sum);
    return SqlStdOperatorTable.DIVIDE.createCall(
        pos, sumAsDouble, count);
  }

  private SqlNode expandVariance(
      final SqlNode arg,
      boolean biased,
      boolean sqrt) {
    /* stddev_pop(x) ==>
     *   power(
     *    (sum(x * x) - sum(x) * sum(x) / count(x))
     *    / count(x),
     *    .5)

     * stddev_samp(x) ==>
     *  power(
     *    (sum(x * x) - sum(x) * sum(x) / count(x))
     *    / (count(x) - 1),
     *    .5)

     * var_pop(x) ==>
     *    (sum(x * x) - sum(x) * sum(x) / count(x))
     *    / count(x)

     * var_samp(x) ==>
     *    (sum(x * x) - sum(x) * sum(x) / count(x))
     *    / (count(x) - 1)
     */
    final SqlParserPos pos = SqlParserPos.ZERO;

    // cast the argument to double
    final SqlNode castHighArg = CastHighOp.createCall(pos, arg);
    final SqlNode argSquared =
        SqlStdOperatorTable.MULTIPLY.createCall(pos, castHighArg, castHighArg);
    final SqlNode sumArgSquared =
        SqlStdOperatorTable.SUM.createCall(pos, argSquared);
    final SqlNode sum =
        SqlStdOperatorTable.SUM.createCall(pos, castHighArg);
    final SqlNode sumSquared =
        SqlStdOperatorTable.MULTIPLY.createCall(pos, sum, sum);
    final SqlNode count =
        SqlStdOperatorTable.COUNT.createCall(pos, castHighArg);
    final SqlNode avgSumSquared =
        SqlStdOperatorTable.DIVIDE.createCall(
            pos, sumSquared, count);
    final SqlNode diff =
        SqlStdOperatorTable.MINUS.createCall(
            pos, sumArgSquared, avgSumSquared);
    final SqlNode denominator;
    if (biased) {
      denominator = count;
    } else {
      final SqlNumericLiteral one =
          SqlLiteral.createExactNumeric("1", pos);
      denominator =
          SqlStdOperatorTable.MINUS.createCall(
              pos, count, one);
    }
    final SqlNode diffAsDouble =
        CastHighOp.createCall(pos, diff);
    final SqlNode div =
        SqlStdOperatorTable.DIVIDE.createCall(
            pos, diffAsDouble, denominator);
    SqlNode result = div;
    if (sqrt) {
      final SqlNumericLiteral half =
          SqlLiteral.createExactNumeric("0.5", pos);
      result =
          SqlStdOperatorTable.POWER.createCall(pos, div, half);
    }
    return result;
  }
}
