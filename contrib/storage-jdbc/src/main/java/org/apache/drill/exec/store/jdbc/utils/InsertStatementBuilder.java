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
package org.apache.drill.exec.store.jdbc.utils;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

public class InsertStatementBuilder {

  private final List<SqlNode> sqlRows = new ArrayList<>();
  private final List<SqlNode> sqlRowValues = new ArrayList<>();
  private final SqlDialect dialect;
  private final List<String> tableIdentifier;

  public InsertStatementBuilder(List<String> tableIdentifier, SqlDialect dialect) {
    this.dialect = dialect;
    this.tableIdentifier = tableIdentifier;
  }

  public void addRowValue(SqlNode value) {
    sqlRowValues.add(value);
  }

  public void endRecord() {
    sqlRows.add(SqlInternalOperators.ANONYMOUS_ROW.createCall(
      SqlParserPos.ZERO, sqlRowValues.toArray(new SqlNode[0])));
    resetRow();
  }

  public void resetRow() {
    sqlRowValues.clear();
  }

  public String buildInsertQuery() {
    SqlCall values = SqlStdOperatorTable.VALUES.createCall(
      SqlParserPos.ZERO, sqlRows.toArray(new SqlNode[0]));
    resetRow();
    sqlRows.clear();
    SqlInsert sqlInsert = new SqlInsert(
      SqlParserPos.ZERO,
      SqlNodeList.EMPTY,
      new SqlIdentifier(tableIdentifier, SqlParserPos.ZERO),
      values,
      null);

    return sqlInsert.toSqlString(dialect).getSql();
  }
}
