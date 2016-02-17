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

import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
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

    RelDataTypeFactory typeFactory = cx.getTypeFactory();
    for (SqlNode node: operands) {
       exprs.add(cx.convertExpression(node));
    }
    TimeUnit timeUnit = ((SqlIntervalQualifier) call.getOperandList().get(0)).getStartUnit();
    boolean isNullable = exprs.get(1).getType().isNullable();
    RelDataType returnType = inferReturnType(typeFactory, timeUnit, isNullable);
    return rexBuilder.makeCall(returnType, call.getOperator(), exprs);
  }

  public static RelDataType inferReturnType(RelDataTypeFactory factory, TimeUnit timeUnit, boolean isNullable) {
    final SqlTypeName sqlTypeName;
    switch (timeUnit){
      case YEAR:
      case MONTH:
      case DAY:
      case HOUR:
      case MINUTE:
        sqlTypeName = SqlTypeName.BIGINT;
        break;
      case SECOND:
        sqlTypeName = SqlTypeName.DOUBLE;
        break;
      default:
        throw new UnsupportedOperationException("extract function supports the following time units: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND");
    }

    final RelDataType type = factory.createSqlType(sqlTypeName);
      if(isNullable) {
      return factory.createTypeWithNullability(type, true);
    } else {
      return type;
    }
  }
}

