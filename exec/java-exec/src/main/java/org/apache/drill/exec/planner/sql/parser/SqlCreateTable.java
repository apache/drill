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

import com.google.common.base.Preconditions;
import org.apache.calcite.tools.Planner;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.CreateTableHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerUtil;
import org.apache.drill.exec.util.Pointer;

public class SqlCreateTable extends DrillSqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_TABLE", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 4, "SqlCreateTable.createCall() has to get 4 operands!");
      return new SqlCreateTable(pos, (SqlIdentifier) operands[0], (SqlNodeList) operands[1], (SqlNodeList) operands[2], operands[3]);
    }
  };

  private final SqlIdentifier tblName;
  private final SqlNodeList fieldList;
  private final SqlNodeList partitionColumns;
  private final SqlNode query;

  public SqlCreateTable(SqlParserPos pos, SqlIdentifier tblName, SqlNodeList fieldList, SqlNodeList partitionColumns, SqlNode query) {
    super(pos);
    this.tblName = tblName;
    this.fieldList = fieldList;
    this.partitionColumns = partitionColumns;
    this.query = query;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> ops = Lists.newArrayList();
    ops.add(tblName);
    ops.add(fieldList);
    ops.add(partitionColumns);
    ops.add(query);
    return ops;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("TABLE");
    tblName.unparse(writer, leftPrec, rightPrec);
    if (fieldList.size() > 0) {
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, fieldList);
    }
    if (partitionColumns.size() > 0) {
      writer.keyword("PARTITION BY");
      SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, partitionColumns);
    }
    writer.keyword("AS");
    query.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return getSqlHandler(config, null);
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config, Pointer<String> textPlan) {
    assert textPlan != null : "Create table statement should have a plan";
    return new CreateTableHandler(config, textPlan);
  }

  public List<String> getSchemaPath() {
    if (tblName.isSimple()) {
      return ImmutableList.of();
    }

    return tblName.names.subList(0, tblName.names.size() - 1);
  }

  public String getName() {
    if (tblName.isSimple()) {
      return tblName.getSimple();
    }

    return tblName.names.get(tblName.names.size() - 1);
  }

  public List<String> getFieldNames() {
    List<String> columnNames = Lists.newArrayList();
    for(SqlNode node : fieldList.getList()) {
      columnNames.add(node.toString());
    }
    return columnNames;
  }

  public List<String> getPartitionColumns() {
    List<String> columnNames = Lists.newArrayList();
    for(SqlNode node : partitionColumns.getList()) {
      columnNames.add(node.toString());
    }
    return columnNames;
  }

  public SqlNode getQuery() { return query; }

}
