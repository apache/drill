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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.RefreshMetadataHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Sql parse tree node to represent statement:
 * REFRESH TABLE METADATA tblname
 */
public class SqlRefreshMetadata extends DrillSqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("REFRESH_TABLE_METADATA", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlRefreshMetadata(pos, (SqlIdentifier) operands[0]);
    }
  };

  private SqlIdentifier tblName;

  public SqlRefreshMetadata(SqlParserPos pos, SqlIdentifier tblName){
    super(pos);
    this.tblName = tblName;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(tblName);
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("REFRESH");
    writer.keyword("TABLE");
    writer.keyword("METADATA");
    tblName.unparse(writer, leftPrec, rightPrec);
  }

  public String getName() {
    if (tblName.isSimple()) {
      return tblName.getSimple();
    }

    return tblName.names.get(tblName.names.size() - 1);
  }

  public List<String> getSchemaPath() {
    if (tblName.isSimple()) {
      return ImmutableList.of();
    }

    return tblName.names.subList(0, tblName.names.size() - 1);
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return new RefreshMetadataHandler(config);
  }
}
