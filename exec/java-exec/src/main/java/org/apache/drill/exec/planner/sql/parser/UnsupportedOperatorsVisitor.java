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

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.UnsupportedOperatorCollector;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;

import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.SqlDataTypeSpec;

import java.util.List;

import com.google.common.collect.Lists;

public class UnsupportedOperatorsVisitor extends SqlShuttle {
  private QueryContext context;
  private static List<String> disabledType = Lists.newArrayList();
  private static List<String> disabledOperators = Lists.newArrayList();

  static {
    disabledType.add(SqlTypeName.TINYINT.name());
    disabledType.add(SqlTypeName.SMALLINT.name());
    disabledType.add(SqlTypeName.REAL.name());
    disabledOperators.add("CARDINALITY");
  }

  private UnsupportedOperatorCollector unsupportedOperatorCollector;

  private UnsupportedOperatorsVisitor(QueryContext context) {
    this.context = context;
    this.unsupportedOperatorCollector = new UnsupportedOperatorCollector();
  }

  public static UnsupportedOperatorsVisitor createVisitor(QueryContext context) {
    return new UnsupportedOperatorsVisitor(context);
  }

  public void convertException() throws SqlUnsupportedException {
    unsupportedOperatorCollector.convertException();
  }

  @Override
  public SqlNode visit(SqlDataTypeSpec type) {
    for(String strType : disabledType) {
      if(type.getTypeName().getSimple().equalsIgnoreCase(strType)) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.DATA_TYPE,
            type.getTypeName().getSimple() + " is not supported\n" +
            "See Apache Drill JIRA: DRILL-1959");
        throw new UnsupportedOperationException();
      }
    }

    return type;
  }

  @Override
  public SqlNode visit(SqlCall sqlCall) {
    // Inspect the window functions
    if(sqlCall instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) sqlCall;

      for(SqlNode nodeInSelectList : sqlSelect.getSelectList()) {
        // If the window function is used with an alias,
        // enter the first operand of AS operator
        if(nodeInSelectList.getKind() == SqlKind.AS
            && (((SqlCall) nodeInSelectList).getOperandList().get(0).getKind() == SqlKind.OVER)) {
          nodeInSelectList = ((SqlCall) nodeInSelectList).getOperandList().get(0);
        }

        if(nodeInSelectList.getKind() == SqlKind.OVER) {
          // Throw exceptions if window functions are disabled
          if(!context.getOptions().getOption(ExecConstants.ENABLE_WINDOW_FUNCTIONS).bool_val) {
            unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
                "Window functions are disabled\n" +
                "See Apache Drill JIRA: DRILL-2559");
            throw new UnsupportedOperationException();
          }

          // DRILL-3182, DRILL-3195
          SqlCall over = (SqlCall) nodeInSelectList;
          if(over.getOperandList().get(0) instanceof SqlCall) {
            SqlCall function = (SqlCall) over.getOperandList().get(0);

            // DRILL-3182
            // Window function with DISTINCT qualifier is temporarily disabled
            if(function.getFunctionQuantifier() != null
                && function.getFunctionQuantifier().getValue() == SqlSelectKeyword.DISTINCT) {
              unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
                  "DISTINCT for window aggregate functions is not currently supported\n" +
                  "See Apache Drill JIRA: DRILL-3182");
              throw new UnsupportedOperationException();
            }
          }
        }
      }
    }

    // DRILL-3188
    // Disable frame which is other than the default
    // (i.e., BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    if(sqlCall instanceof SqlWindow) {
      SqlWindow window = (SqlWindow) sqlCall;

      SqlNode lowerBound = window.getLowerBound();
      SqlNode upperBound = window.getUpperBound();

      // If no frame is specified
      // it is a default frame
      boolean isSupported = (lowerBound == null && upperBound == null);

      // When OVER clause contain an ORDER BY clause the following frames are equivalent to the default frame:
      // RANGE UNBOUNDED PRECEDING
      // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      if(window.getOrderList().size() != 0
          && !window.isRows()
          && SqlWindow.isUnboundedPreceding(lowerBound)
          && (upperBound == null || SqlWindow.isCurrentRow(upperBound))) {
        isSupported = true;
      }

      // When OVER clause doesn't contain an ORDER BY clause, the following are equivalent to the default frame:
      // RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      // ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      if(window.getOrderList().size() == 0
          && SqlWindow.isUnboundedPreceding(lowerBound)
          && SqlWindow.isUnboundedFollowing(upperBound)) {
        isSupported = true;
      }

      if(!isSupported) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
            "This type of window frame is currently not supported \n" +
            "See Apache Drill JIRA: DRILL-3188");
        throw new UnsupportedOperationException();
      }

      // DRILL-3189: Disable DISALLOW PARTIAL
      if(!window.isAllowPartial()) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
            "Disallowing partial windows is currently not supported \n" +
            "See Apache Drill JIRA: DRILL-3189");
        throw new UnsupportedOperationException();
      }
    }

    // Disable unsupported Intersect, Except
    if(sqlCall.getKind() == SqlKind.INTERSECT || sqlCall.getKind() == SqlKind.EXCEPT) {
      unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
          sqlCall.getOperator().getName() + " is not supported\n" +
          "See Apache Drill JIRA: DRILL-1921");
      throw new UnsupportedOperationException();
    }

    // Disable unsupported JOINs
    if(sqlCall.getKind() == SqlKind.JOIN) {
      SqlJoin join = (SqlJoin) sqlCall;

      // Block Natural Join
      if(join.isNatural()) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
            "NATURAL JOIN is not supported\n" +
            "See Apache Drill JIRA: DRILL-1986");
        throw new UnsupportedOperationException();
      }

      // Block Cross Join
      if(join.getJoinType() == JoinType.CROSS) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
            "CROSS JOIN is not supported\n" +
            "See Apache Drill JIRA: DRILL-1921");
        throw new UnsupportedOperationException();
      }
    }

    // Disable Function
    for(String strOperator : disabledOperators) {
      if(sqlCall.getOperator().isName(strOperator)) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
            sqlCall.getOperator().getName() + " is not supported\n" +
            "See Apache Drill JIRA: DRILL-2115");
        throw new UnsupportedOperationException();
      }
    }

    // Disable complex functions being present in any place other than Select-Clause
    if(sqlCall instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) sqlCall;
      if(sqlSelect.hasOrderBy()) {
        for (SqlNode sqlNode : sqlSelect.getOrderList()) {
          if(containsFlatten(sqlNode)) {
            unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
                "Flatten function is not supported in Order By\n" +
                "See Apache Drill JIRA: DRILL-2181");
            throw new UnsupportedOperationException();
          }
        }
      }

      if(sqlSelect.getGroup() != null) {
        for(SqlNode sqlNode : sqlSelect.getGroup()) {
          if(containsFlatten(sqlNode)) {
            unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
                "Flatten function is not supported in Group By\n" +
                "See Apache Drill JIRA: DRILL-2181");
            throw new UnsupportedOperationException();
          }
        }
      }

      if(sqlSelect.isDistinct()) {
        for(SqlNode column : sqlSelect.getSelectList()) {
          if(column.getKind() ==  SqlKind.AS) {
            if(containsFlatten(((SqlCall) column).getOperandList().get(0))) {
              unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
                  "Flatten function is not supported in Distinct\n" +
                  "See Apache Drill JIRA: DRILL-2181");
              throw new UnsupportedOperationException();
            }
          } else {
            if(containsFlatten(column)) {
              unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
                  "Flatten function is not supported in Distinct\n" +
                  "See Apache Drill JIRA: DRILL-2181");
              throw new UnsupportedOperationException();
            }
          }
        }
      }
    }

    if(sqlCall.getOperator() instanceof SqlCountAggFunction) {
      for(SqlNode sqlNode : sqlCall.getOperandList()) {
        if(containsFlatten(sqlNode)) {
          unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
              "Flatten function in aggregate functions is not supported\n" +
              "See Apache Drill JIRA: DRILL-2181");
          throw new UnsupportedOperationException();
        }
      }
    }

    return sqlCall.getOperator().acceptCall(this, sqlCall);
  }

  private boolean containsFlatten(SqlNode sqlNode) throws UnsupportedOperationException {
    return sqlNode instanceof SqlCall
        && ((SqlCall) sqlNode).getOperator().getName().toLowerCase().equals("flatten");
  }

  /**
   * Disable multiple partitions in a SELECT-CLAUSE
   * If multiple partitions are defined in the query,
   * SqlUnsupportedException would be thrown to inform
   * @param sqlSelect SELECT-CLAUSE in the query
   */
  private void detectMultiplePartitions(SqlSelect sqlSelect) {
    for(SqlNode nodeInSelectList : sqlSelect.getSelectList()) {
      // If the window function is used with an alias,
      // enter the first operand of AS operator
      if(nodeInSelectList.getKind() == SqlKind.AS
          && (((SqlCall) nodeInSelectList).getOperandList().get(0).getKind() == SqlKind.OVER)) {
        nodeInSelectList = ((SqlCall) nodeInSelectList).getOperandList().get(0);
      }

      if(nodeInSelectList.getKind() != SqlKind.OVER) {
        continue;
      }

      // This is used to keep track of the window function which has been defined
      SqlNode definedWindow = null;
      SqlNode window = ((SqlCall) nodeInSelectList).operand(1);

      // Partition window is referenced as a SqlIdentifier,
      // which is defined in the window list
      if(window instanceof SqlIdentifier) {
        // Expand the SqlIdentifier as the expression defined in the window list
        for(SqlNode sqlNode : sqlSelect.getWindowList()) {
          if(((SqlWindow) sqlNode).getDeclName().equalsDeep(window, false)) {
            window = sqlNode;
            break;
          }
        }

        assert !(window instanceof SqlIdentifier) : "Identifier should have been expanded as a window defined in the window list";
      }

      // In a SELECT-SCOPE, only a partition can be defined
      if(definedWindow == null) {
        definedWindow = window;
      } else {
        if(!definedWindow.equalsDeep(window, false)) {
          unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
              "Multiple window definitions in a single SELECT list is not currently supported \n" +
              "See Apache Drill JIRA: DRILL-3196");
          throw new UnsupportedOperationException();
        }
      }
    }
  }
}