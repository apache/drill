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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql2rel.SqlRexContext;
import org.eigenbase.sql2rel.SqlRexConvertlet;

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

    //RelDataType nullableReturnType =

    for (SqlNode node: operands) {
       exprs.add(cx.convertExpression(node));
    }

    // Determine NULL-able using 2nd argument's Null-able.
    RelDataType returnType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), exprs.get(1).getType().isNullable());

    return rexBuilder.makeCall(returnType, call.getOperator(), exprs);
  }
}

