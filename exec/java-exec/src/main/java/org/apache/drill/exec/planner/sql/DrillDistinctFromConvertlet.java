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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;

public class DrillDistinctFromConvertlet implements SqlRexConvertlet {

  public final static DrillDistinctFromConvertlet INSTANCE = new DrillDistinctFromConvertlet();

  private DrillDistinctFromConvertlet() {
  }

  /*
   * Custom convertlet to handle "is distinct from" and "is not distinct from" functions.
   * Calcite rewrites these functions as if and equals functions and Drill needs them as it is without rewriting.
   */
  @Override
  public RexNode convertCall(SqlRexContext cx, SqlCall call) {
    final List<RexNode> exprs = new LinkedList<>();

    for (SqlNode node : call.getOperandList()) {
      exprs.add(cx.convertExpression(node));
    }

    final RexBuilder rexBuilder = cx.getRexBuilder();

    // The result of IS [NOT] DISTINCT FROM is NOT NULL because it can only return TRUE or FALSE.
    final RelDataType returnType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);

    return rexBuilder.makeCall(returnType, call.getOperator(), exprs);
  }
}

