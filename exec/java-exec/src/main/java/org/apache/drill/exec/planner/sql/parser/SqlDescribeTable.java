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

import com.google.common.collect.Lists;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Sql parser tree node to represent statement:
 * { DESCRIBE | DESC } tblname [col_name | wildcard ]
 */
public class SqlDescribeTable extends SqlCall {

  private final SqlIdentifier table;
  private final SqlIdentifier column;
  private final SqlNode columnQualifier;

  public static final SqlSpecialOperator OPERATOR =
    new SqlSpecialOperator("DESCRIBE_TABLE", SqlKind.OTHER);

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
    if (column != null) opList.add(column);
    if (columnQualifier != null) opList.add(columnQualifier);
    return opList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DESCRIBE");
    writer.keyword("TABLE");
    table.unparse(writer, leftPrec, rightPrec);
    if (column != null) column.unparse(writer, leftPrec, rightPrec);
    if (columnQualifier != null) columnQualifier.unparse(writer, leftPrec, rightPrec);
  }

  public SqlIdentifier getTable() { return table; }
  public SqlIdentifier getColumn() { return column; }
  public SqlNode getColumnQualifier() { return columnQualifier; }
}
