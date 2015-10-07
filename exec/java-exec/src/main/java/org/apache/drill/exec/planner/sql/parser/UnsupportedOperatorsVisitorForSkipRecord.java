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
package org.apache.drill.exec.planner.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;

public class UnsupportedOperatorsVisitorForSkipRecord extends UnsupportedOperatorsVisitor {

  protected UnsupportedOperatorsVisitorForSkipRecord(QueryContext context) {
    super(context);
  }

  public static UnsupportedOperatorsVisitor createVisitor(QueryContext context) {
    return new UnsupportedOperatorsVisitorForSkipRecord(context);
  }

  @Override
  public SqlNode visit(SqlCall sqlCall) {
    if(sqlCall instanceof SqlSelect) {
      final SqlSelect sqlSelect = (SqlSelect) sqlCall;

      if(sqlSelect.isDistinct()) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
            "Select Distinct is not supported in skipping records\n" +
            "See Apache Drill JIRA: DRILL-");
        throw new UnsupportedOperationException();
      }

      if(sqlSelect.getGroup() != null
          && !sqlSelect.getGroup().getList().isEmpty()) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
            "Group By is not supported in skipping records\n" +
            "See Apache Drill JIRA: DRILL-");
        throw new UnsupportedOperationException();
      }

      if(sqlSelect.getOrderList() != null
          && !sqlSelect.getOrderList().getList().isEmpty()) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
            "Order By is not supported in skipping records\n" +
            "See Apache Drill JIRA: DRILL-");
        throw new UnsupportedOperationException();
      }

      if(sqlSelect.getWindowList() != null
          && !sqlSelect.getWindowList().getList().isEmpty()) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
            "Window function is not supported in skipping records\n" +
            "See Apache Drill JIRA: DRILL-");
        throw new UnsupportedOperationException();
      }

      if(sqlSelect.getFrom().getKind() == SqlKind.JOIN) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
            "Join is not supported in skipping records\n" +
            "See Apache Drill JIRA: DRILL-");
        throw new UnsupportedOperationException();
      }

      for(SqlNode nodeInSelectList : sqlSelect.getSelectList()) {
        if(nodeInSelectList.getKind() == SqlKind.AS
            && (((SqlCall) nodeInSelectList).getOperandList().get(0).getKind() == SqlKind.OVER)) {
          nodeInSelectList = ((SqlCall) nodeInSelectList).getOperandList().get(0);
        }

        if(nodeInSelectList.getKind() == SqlKind.OVER) {
          unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
              "Window function is not supported in skipping records\n" +
              "See Apache Drill JIRA: DRILL-");
          throw new UnsupportedOperationException();
        }
      }
    }
    return super.visit(sqlCall);
  }
}