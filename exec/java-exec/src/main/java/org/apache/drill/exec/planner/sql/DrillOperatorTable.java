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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.Arrays;
import java.util.List;

/**
 * Implementation of {@link SqlOperatorTable} that contains standard operators and functions provided through
 * {@link #inner SqlStdOperatorTable}, and Drill User Defined Functions.
 */
public class DrillOperatorTable extends SqlStdOperatorTable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillOperatorTable.class);

  private static final SqlOperatorTable inner = SqlStdOperatorTable.instance();
  private List<SqlOperator> operators;
  private ArrayListMultimap<String, SqlOperator> opMap = ArrayListMultimap.create();

  public DrillOperatorTable(FunctionImplementationRegistry registry) {
    operators = Lists.newArrayList();
    operators.addAll(inner.getOperatorList());

    registry.register(this);
  }

  public void add(String name, SqlOperator op) {
    operators.add(op);
    opMap.put(name.toLowerCase(), op);
  }

  @Override
  public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category,
                                      SqlSyntax syntax, List<SqlOperator> operatorList) {
    // first look in Calcite functions
    inner.lookupOperatorOverloads(opName, category, syntax, operatorList);

    // if no function is found, check in Drill UDFs
    System.out.println("LOOKING FOR: " + opName.getSimple() + " SIMPLE: " + opName.isSimple() + " SYNTAX: " + syntax);
    if (operatorList.isEmpty() && syntax == SqlSyntax.FUNCTION && opName.isSimple()) {
      List<SqlOperator> drillOps = opMap.get(opName.getSimple().toLowerCase());
      if (drillOps != null) {
        operatorList.addAll(drillOps);
      }
    }
    System.out.println("OPERATOR LOOKUP: " + operatorList);
    for (SqlOperator operator : operatorList) {
      System.out.println("OPERATOR: " + operator.getAllowedSignatures());
    }
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return operators;
  }

  // Get the list of SqlOperator's with the given name.
  public List<SqlOperator> getSqlOperator(String name) {
    return opMap.get(name.toLowerCase());
  }
}
