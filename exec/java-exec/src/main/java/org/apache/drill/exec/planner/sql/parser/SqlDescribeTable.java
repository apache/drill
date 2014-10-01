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

import java.util.List;

import org.apache.calcite.tools.Planner;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.DescribeTableHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.Lists;

/**
 * Sql parser tree node to represent statement:
 * { DESCRIBE | DESC } tblname [col_name | wildcard ]
 */
public class SqlDescribeTable extends DrillSqlCall {

  private final SqlIdentifier table;
  private final SqlIdentifier column;
  private final SqlNode columnQualifier;

  public static final SqlSpecialOperator OPERATOR =
    new SqlSpecialOperator("DESCRIBE_TABLE", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlDescribeTable(pos, (SqlIdentifier) operands[0], (SqlIdentifier) operands[1], operands[2]);
    }
  };

  public SqlDescribeTable(SqlParserPos pos, SqlIdentifier table, SqlIdentifier column, SqlNode columnQualifier) {
    super(pos);
    this.table = table;
    this.column = column;
    this.columnQualifier = columnQualifier;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> opList = Lists.newArrayList();
    opList.add(table);
    opList.add(column);
    opList.add(columnQualifier);
    return opList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DESCRIBE");
    writer.keyword("TABLE");
    table.unparse(writer, leftPrec, rightPrec);
    if (column != null) {
      column.unparse(writer, leftPrec, rightPrec);
    }
    if (columnQualifier != null) {
      columnQualifier.unparse(writer, leftPrec, rightPrec);
    }
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return new DescribeTableHandler(config);
  }

  public SqlIdentifier getTable() { return table; }
  public SqlIdentifier getColumn() { return column; }
  public SqlNode getColumnQualifier() { return columnQualifier; }

}
