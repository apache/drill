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
import com.google.common.collect.ImmutableList;
import net.hydromatic.optiq.tools.Planner;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.sql.handlers.CreateTableHandler;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParserPos;

import java.util.List;

public class SqlCreateTable extends DrillSqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_TABLE", SqlKind.OTHER);

  private SqlIdentifier tblName;
  private SqlNodeList fieldList;
  private SqlNode query;

  public SqlCreateTable(SqlParserPos pos, SqlIdentifier tblName, SqlNodeList fieldList, SqlNode query) {
    super(pos);
    this.tblName = tblName;
    this.fieldList = fieldList;
    this.query = query;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    if (fieldList == null) return ImmutableList.of(tblName, fieldList);
    else return ImmutableList.of(tblName, query, fieldList);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("TABLE");
    tblName.unparse(writer, leftPrec, rightPrec);
    if (fieldList != null && fieldList.size() > 0) {
      writer.keyword("(");
      fieldList.get(0).unparse(writer, leftPrec, rightPrec);
      for(int i=1; i<fieldList.size(); i++) {
        writer.keyword(",");
        fieldList.get(i).unparse(writer, leftPrec, rightPrec);
      }
      writer.keyword(")");
    }
    writer.keyword("AS");
    query.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public AbstractSqlHandler getSqlHandler(Planner planner, QueryContext context) {
    return new CreateTableHandler(planner, context);
  }

  public List<String> getSchemaPath() {
    if (tblName.isSimple()) {
      return ImmutableList.of();
    }

    return tblName.names.subList(0, tblName.names.size() - 1);
  }

  public String getName() {
    if (tblName.isSimple())
      return tblName.getSimple();

    return tblName.names.get(tblName.names.size() - 1);
  }

  public List<String> getFieldNames() {
    if (fieldList == null) return ImmutableList.of();

    List<String> columnNames = Lists.newArrayList();
    for(SqlNode node : fieldList.getList()) {
      columnNames.add(node.toString());
    }
    return columnNames;
  }

  public SqlNode getQuery() { return query; }
}
