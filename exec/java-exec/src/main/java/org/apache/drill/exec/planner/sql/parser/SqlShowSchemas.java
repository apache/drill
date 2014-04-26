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
import net.hydromatic.optiq.tools.Planner;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.sql.handlers.DefaultSqlHandler;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Sql parse tree node to represent statement:
 * SHOW {DATABASES | SCHEMAS} [LIKE 'pattern' | WHERE expr]
 */
public class SqlShowSchemas extends SqlCall {

  private final SqlNode likePattern;
  private final SqlNode whereClause;

  public static final SqlSpecialOperator OPERATOR =
    new SqlSpecialOperator("SHOW_SCHEMAS", SqlKind.OTHER);

  public SqlShowSchemas(SqlParserPos pos, SqlNode likePattern, SqlNode whereClause) {
    super(pos);
    this.likePattern = likePattern;
    this.whereClause = whereClause;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> opList = Lists.newArrayList();
    if (likePattern != null) opList.add(likePattern);
    if (whereClause != null) opList.add(whereClause);
    return opList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW");
    writer.keyword("SCHEMAS");
    if (likePattern != null) {
      writer.keyword("LIKE");
      likePattern.unparse(writer, leftPrec, rightPrec);
    }
    if (whereClause != null) whereClause.unparse(writer, leftPrec, rightPrec);
  }

  public SqlNode getLikePattern() { return likePattern; }
  public SqlNode getWhereClause() { return whereClause; }
}
