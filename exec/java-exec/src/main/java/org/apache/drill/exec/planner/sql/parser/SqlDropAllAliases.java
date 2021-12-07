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
package org.apache.drill.exec.planner.sql.parser;

import lombok.Builder;
import lombok.Getter;
import lombok.val;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.DropAllAliasesHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;

import java.util.ArrayList;
import java.util.List;

@Getter
public class SqlDropAllAliases extends DrillSqlCall {

  private final SqlNode isPublic;
  private final SqlNode aliasKind;
  private final SqlNode user;

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DROP_ALIAS", SqlKind.OTHER_DDL) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return SqlDropAllAliases.builder()
        .pos(pos)
        .aliasKind(operands[0])
        .isPublic(operands[1])
        .user(operands[2])
        .build();
    }
  };

  @Builder
  private SqlDropAllAliases(SqlParserPos pos, SqlNode isPublic, SqlNode aliasKind, SqlNode user) {
    super(pos);
    this.isPublic = isPublic;
    this.aliasKind = aliasKind;
    this.user = user;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    val opList = new ArrayList<SqlNode>();
    opList.add(aliasKind);
    opList.add(isPublic);
    opList.add(user);
    return opList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DROP");
    writer.keyword("ALL");
    if (((SqlLiteral) isPublic).booleanValue()) {
      writer.keyword("PUBLIC");
    }
    writer.keyword("ALIASES");

    writer.keyword("FOR");
    writer.keyword(((SqlLiteral) aliasKind).toValue());

    if (user != null) {
      writer.keyword("AS");
      writer.keyword("USER");
      user.unparse(writer, leftPrec, rightPrec);
    }
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return new DropAllAliasesHandler(config);
  }
}
