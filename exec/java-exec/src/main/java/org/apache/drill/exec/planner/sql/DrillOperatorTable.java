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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.SqlSyntax;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.hive12.common.collect.Sets;

public class DrillOperatorTable extends SqlStdOperatorTable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillOperatorTable.class);

  private static final SqlOperatorTable inner = SqlStdOperatorTable.instance();
  private List<SqlOperator> operators;
  private ArrayListMultimap<String, SqlOperator> opMap = ArrayListMultimap.create();

  public DrillOperatorTable(FunctionImplementationRegistry registry) {
    operators = Lists.newArrayList();
    operators.addAll(inner.getOperatorList());

    for (Map.Entry<String, Collection<DrillFuncHolder>> function : registry.getDrillRegistry().getMethods().asMap().entrySet()) {
      Set<Integer> argCounts = Sets.newHashSet();
      String name = function.getKey().toUpperCase();
      for (DrillFuncHolder f : function.getValue()) {
        if (argCounts.add(f.getParamCount())) {
          SqlOperator op = null;
          if (f.isAggregating()) {
            op = new DrillSqlAggOperator(name, f.getParamCount());
          } else {
            op = new DrillSqlOperator(name, f.getParamCount());
          }
          operators.add(op);
          opMap.put(function.getKey(), op);
        }
      }
    }

    // TODO: add hive functions.
  }

  @Override
  public List<SqlOperator> lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax) {
    if (syntax == SqlSyntax.FUNCTION) {
      List<SqlOperator> drillOps = opMap.get(opName.getSimple());
      if (drillOps == null || drillOps.isEmpty())
        return inner.lookupOperatorOverloads(opName, category, syntax);

      List<SqlOperator> optiqOps = inner.lookupOperatorOverloads(opName, category, syntax);
      if (optiqOps.isEmpty())
        return drillOps;

      // combine the two.
      List<SqlOperator> both = Lists.newArrayList();
      both.addAll(optiqOps);
      both.addAll(drillOps);

      return both;

    } else {
      return inner.lookupOperatorOverloads(opName, category, syntax);
    }
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return operators;
  }
}
