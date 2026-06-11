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
package org.apache.drill.exec.planner.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.HashMap;
import java.util.Map;

/**
 * Expands GROUP BY items that reference a SELECT-list alias into the underlying
 * expression, e.g. rewrites
 *
 * <pre>SELECT length(n_name) AS len, n_regionkey AS k ... GROUP BY len, k</pre>
 *
 * into
 *
 * <pre>SELECT length(n_name) AS len, n_regionkey AS k ... GROUP BY length(n_name), n_regionkey</pre>
 *
 * <p>Drill enables GROUP BY by alias through {@code SqlConformance.isGroupByAlias()},
 * but Calcite 1.42 no longer performs this expansion for the GROUP BY clause
 * (it still does for HAVING). Doing it here, before validation, restores Drill's
 * historical behavior. A GROUP BY item is only rewritten when it is a simple
 * identifier matching a SELECT alias; a matching real column would still be
 * grouped on the equivalent expression.
 */
public class GroupByAliasRewriter extends SqlShuttle {

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) call;
      SqlNodeList group = select.getGroup();
      if (group != null && !group.getList().isEmpty()) {
        Map<String, SqlNode> aliasToExpr = collectAliases(select.getSelectList());
        if (!aliasToExpr.isEmpty()) {
          boolean changed = false;
          for (int i = 0; i < group.size(); i++) {
            SqlNode item = group.get(i);
            if (item instanceof SqlIdentifier && ((SqlIdentifier) item).isSimple()) {
              SqlNode expr = aliasToExpr.get(((SqlIdentifier) item).getSimple());
              // Only expand when the alias maps to something other than the
              // identifier itself (avoid rewriting plain "col AS col").
              if (expr != null && !(expr instanceof SqlIdentifier
                  && ((SqlIdentifier) expr).isSimple()
                  && ((SqlIdentifier) expr).getSimple().equals(((SqlIdentifier) item).getSimple()))) {
                group.set(i, expr.clone(item.getParserPosition()));
                changed = true;
              }
            }
          }
          if (changed) {
            select.setGroupBy(group);
          }
        }
      }
    }
    return super.visit(call);
  }

  /**
   * Builds a map of SELECT-list alias name to its defining expression for items
   * of the form {@code <expr> AS <alias>}.
   */
  private static Map<String, SqlNode> collectAliases(SqlNodeList selectList) {
    Map<String, SqlNode> aliases = new HashMap<>();
    if (selectList == null) {
      return aliases;
    }
    for (SqlNode item : selectList) {
      if (item instanceof SqlCall && item.getKind() == SqlKind.AS) {
        SqlCall as = (SqlCall) item;
        SqlNode expr = as.operand(0);
        SqlNode aliasNode = as.operand(1);
        if (aliasNode instanceof SqlIdentifier && ((SqlIdentifier) aliasNode).isSimple()) {
          aliases.put(((SqlIdentifier) aliasNode).getSimple(), expr);
        }
      }
    }
    return aliases;
  }
}
