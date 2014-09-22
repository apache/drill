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
import com.google.common.collect.Lists;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;
import org.apache.drill.exec.planner.sql.handlers.ViewHandler;
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlNodeList;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlSpecialOperator;
import org.eigenbase.sql.SqlWriter;
import org.eigenbase.sql.parser.SqlParserPos;

import java.util.List;

public class SqlCreateView extends DrillSqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_VIEW", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlCreateView(pos, (SqlIdentifier) operands[0], (SqlNodeList) operands[1], operands[2], (SqlLiteral) operands[3]);
    }
  };

  private SqlIdentifier viewName;
  private SqlNodeList fieldList;
  private SqlNode query;
  private boolean replaceView;

  public SqlCreateView(SqlParserPos pos, SqlIdentifier viewName, SqlNodeList fieldList,
      SqlNode query, SqlLiteral replaceView) {
    this(pos, viewName, fieldList, query, replaceView.booleanValue());
  }

  public SqlCreateView(SqlParserPos pos, SqlIdentifier viewName, SqlNodeList fieldList,
                       SqlNode query, boolean replaceView) {
    super(pos);
    this.viewName = viewName;
    this.query = query;
    this.replaceView = replaceView;
    this.fieldList = fieldList;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(viewName);
    ops.add(fieldList);
    ops.add(query);
    ops.add(SqlLiteral.createBoolean(replaceView, SqlParserPos.ZERO));
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (replaceView) {
      writer.keyword("OR");
      writer.keyword("REPLACE");
    }
    writer.keyword("VIEW");
    viewName.unparse(writer, leftPrec, rightPrec);
    if (fieldList != null && fieldList.size() > 0) {
      writer.keyword("(");
      fieldList.get(0).unparse(writer, leftPrec, rightPrec);
      for (int i=1; i<fieldList.size(); i++) {
        writer.keyword(",");
        fieldList.get(i).unparse(writer, leftPrec, rightPrec);
      }
      writer.keyword(")");
    }
    writer.keyword("AS");
    query.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return new ViewHandler.CreateView(config.getPlanner(), config.getContext());
  }

  public List<String> getSchemaPath() {
    if (viewName.isSimple()) {
      return ImmutableList.of();
    }

    return viewName.names.subList(0, viewName.names.size()-1);
  }

  public String getName() {
    if (viewName.isSimple()) {
      return viewName.getSimple();
    }

    return viewName.names.get(viewName.names.size() - 1);
  }

  public List<String> getFieldNames() {
    if (fieldList == null) {
      return ImmutableList.of();
    }

    List<String> fieldNames = Lists.newArrayList();
    for (SqlNode node : fieldList.getList()) {
      fieldNames.add(node.toString());
    }
    return fieldNames;
  }

  public SqlNode getQuery() { return query; }
  public boolean getReplace() { return replaceView; }

}
