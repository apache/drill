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
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlJoin;
import org.eigenbase.sql.JoinType;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.util.SqlShuttle;
import org.eigenbase.sql.SqlDataTypeSpec;
import org.eigenbase.sql.SqlSetOperator;
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
    // Disable unsupported Intersect, Except
    if(sqlCall.getKind() == SqlKind.INTERSECT || sqlCall.getKind() == SqlKind.EXCEPT) {
      unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
          sqlCall.getOperator().getName() + " is not supported\n" +
          "See Apache Drill JIRA: DRILL-1921");
      throw new UnsupportedOperationException();
    }

    // Disable unsupported Union
    if(sqlCall.getKind() == SqlKind.UNION) {
      SqlSetOperator op = (SqlSetOperator) sqlCall.getOperator();
      if(!op.isAll()) {
        unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.RELATIONAL,
            sqlCall.getOperator().getName() + " is not supported\n" +
            "See Apache Drill JIRA: DRILL-1921");
        throw new UnsupportedOperationException();
      }
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

    // Throw exceptions if window functions are disabled
    if(sqlCall.getOperator().getKind().equals(SqlKind.OVER)
        && !context.getOptions().getOption(ExecConstants.ENABLE_WINDOW_FUNCTIONS).bool_val) {
      unsupportedOperatorCollector.setException(SqlUnsupportedException.ExceptionType.FUNCTION,
          "Window functions are disabled\n" +
          "See Apache Drill JIRA: DRILL-2559");
      throw new UnsupportedOperationException();
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

    return sqlCall.getOperator().acceptCall(this, sqlCall);
  }
}