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

import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;

public class DrillExtractConvertlet implements SqlRexConvertlet {

  public final static DrillExtractConvertlet INSTANCE = new DrillExtractConvertlet();

  private DrillExtractConvertlet() {
  }

  /*
   * Custom convertlet to handle extract functions. Optiq rewrites
   * extract functions as divide and modulo functions, based on the
   * data type. We cannot do that in Drill since we don't know the data type
   * till we start scanning. So we don't rewrite extract and treat it as
   * a regular function.
   */
  @Override
  public RexNode convertCall(SqlRexContext cx, SqlCall call) {
    final RexBuilder rexBuilder = cx.getRexBuilder();
    final List<SqlNode> operands = call.getOperandList();
    final List<RexNode> exprs = new LinkedList<>();

    String timeUnit = ((SqlIntervalQualifier) operands.get(0)).timeUnitRange.toString();

    RelDataTypeFactory typeFactory = cx.getTypeFactory();

    //RelDataType nullableReturnType =

    for (SqlNode node: operands) {
       exprs.add(cx.convertExpression(node));
    }

    final RelDataType returnType;
    if(call.getOperator() == SqlStdOperatorTable.EXTRACT) {
      // Legacy code:
      // The return type is wrong!
      // Legacy code choose SqlTypeName.BIGINT simply to avoid conflicting against Calcite's inference mechanism
      // (, which chose BIGINT in validation phase already)
      // Determine NULL-able using 2nd argument's Null-able.
      returnType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), exprs.get(1).getType().isNullable());
    } else {
      // Determine NULL-able using 2nd argument's Null-able.
      returnType = typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(
              TypeInferenceUtils.getSqlTypeNameForTimeUnit(timeUnit)),
          exprs.get(1).getType().isNullable());
    }

    return rexBuilder.makeCall(returnType, call.getOperator(), exprs);
  }
}

