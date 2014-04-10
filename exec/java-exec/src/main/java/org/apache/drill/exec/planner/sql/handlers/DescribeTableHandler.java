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

package org.apache.drill.exec.planner.sql.handlers;

import com.google.common.collect.ImmutableList;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.sql.parser.DrillParserUtil;
import org.apache.drill.exec.planner.sql.parser.SqlDescribeTable;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.util.Util;

import java.util.List;

import static org.apache.drill.exec.planner.sql.parser.DrillParserUtil.CHARSET;

public class DescribeTableHandler extends DefaultSqlHandler {

  public DescribeTableHandler(Planner planner, QueryContext context) { super(planner, context); }

  /** Rewrite the parse tree as SELECT ... FROM INFORMATION_SCHEMA.COLUMNS ... */
  @Override
  public SqlNode rewrite(SqlNode sqlNode) throws RelConversionException{
    SqlDescribeTable node = unwrap(sqlNode, SqlDescribeTable.class);

    List<SqlNode> selectList = ImmutableList.of((SqlNode)new SqlIdentifier("COLUMN_NAME", SqlParserPos.ZERO),
        new SqlIdentifier("DATA_TYPE", SqlParserPos.ZERO),
        new SqlIdentifier("IS_NULLABLE", SqlParserPos.ZERO));

    SqlNode fromClause = new SqlIdentifier(
        ImmutableList.of("INFORMATION_SCHEMA", "COLUMNS"), null, SqlParserPos.ZERO, null);

    final SqlIdentifier table = node.getTable();
    final int numLevels = table.names.size();

    SqlNode schemaCondition = null;
    if (numLevels > 1) {
      schemaCondition = DrillParserUtil.createCondition(
          new SqlIdentifier("TABLE_SCHEMA", SqlParserPos.ZERO),
          SqlStdOperatorTable.EQUALS,
          SqlLiteral.createCharString(Util.sepList(table.names.subList(0, numLevels - 1), "."),
              CHARSET, SqlParserPos.ZERO)
      );
    }

    SqlNode where = DrillParserUtil.createCondition(
        new SqlIdentifier("TABLE_NAME", SqlParserPos.ZERO),
        SqlStdOperatorTable.EQUALS,
        SqlLiteral.createCharString(table.names.get(numLevels-1), CHARSET, SqlParserPos.ZERO));

    where = DrillParserUtil.createCondition(schemaCondition, SqlStdOperatorTable.AND, where);

    SqlNode columnFilter = null;
    if (node.getColumn() != null) {
      columnFilter = DrillParserUtil.createCondition(new SqlIdentifier("COLUMN_NAME", SqlParserPos.ZERO),
          SqlStdOperatorTable.EQUALS,
          SqlLiteral.createCharString(node.getColumn().toString(), CHARSET, SqlParserPos.ZERO));
    } else if (node.getColumnQualifier() != null) {
      columnFilter = DrillParserUtil.createCondition(new SqlIdentifier("COLUMN_NAME", SqlParserPos.ZERO),
          SqlStdOperatorTable.LIKE, node.getColumnQualifier());
    }

    where = DrillParserUtil.createCondition(where, SqlStdOperatorTable.AND, columnFilter);

    return new SqlSelect(SqlParserPos.ZERO, null, new SqlNodeList(selectList, SqlParserPos.ZERO),
        fromClause, where, null, null, null, null, null, null);
  }
}

