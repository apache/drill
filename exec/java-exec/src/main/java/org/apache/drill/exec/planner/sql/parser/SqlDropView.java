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

import java.util.Collections;
import java.util.List;

import net.hydromatic.optiq.tools.Planner;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;
import org.apache.drill.exec.planner.sql.handlers.ViewHandler.DropView;
import org.eigenbase.relopt.hep.HepPlanner;
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlSpecialOperator;
import org.eigenbase.sql.SqlWriter;
import org.eigenbase.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

public class SqlDropView extends DrillSqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("DROP_VIEW", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      return new SqlDropView(pos, (SqlIdentifier) operands[0]);
    }
  };

  private SqlIdentifier viewName;

  public SqlDropView(SqlParserPos pos, SqlIdentifier viewName) {
    super(pos);
    this.viewName = viewName;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Collections.singletonList((SqlNode)viewName);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DROP");
    writer.keyword("VIEW");
    viewName.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return new DropView(config.getContext());
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

}
