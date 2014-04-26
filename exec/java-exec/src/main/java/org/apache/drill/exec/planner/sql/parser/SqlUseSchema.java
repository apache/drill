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

import com.google.common.collect.ImmutableList;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Sql parser tree node to represent <code>USE SCHEMA</code> statement.
 */
public class SqlUseSchema extends SqlCall {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("USE_SCHEMA", SqlKind.OTHER);
  private SqlIdentifier schema;

  public SqlUseSchema(SqlParserPos pos, SqlIdentifier schema) {
    super(pos);
    this.schema = schema;
    assert schema != null;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of((SqlNode)schema);
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("USE");
    schema.unparse(writer, leftPrec, rightPrec);
  }

  /**
   * Get the schema name. A schema identifier can contain more than one level of schema.
   * Ex: "dfs.home" identifier contains two levels "dfs" and "home".
   * @return schemas combined with "."
   */
  public String getSchema() {
    return schema.toString();
  }
}
