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
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.SqlSyntax;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

import java.util.List;

public class DrillOperatorTable extends SqlStdOperatorTable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillOperatorTable.class);

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
    opMap.put(name, op);
  }

  @Override
  public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax, List<SqlOperator> operatorList) {
    if (syntax == SqlSyntax.FUNCTION) {

      // add optiq.
      inner.lookupOperatorOverloads(opName, category, syntax, operatorList);

      if(!operatorList.isEmpty()){
        return;
      }

      List<SqlOperator> drillOps = opMap.get(opName.getSimple().toLowerCase());
      if(drillOps != null){
        operatorList.addAll(drillOps);
      }

    } else {
      inner.lookupOperatorOverloads(opName, category, syntax, operatorList);
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
