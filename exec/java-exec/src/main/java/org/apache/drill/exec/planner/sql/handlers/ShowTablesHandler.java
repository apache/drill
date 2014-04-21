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
import com.google.common.collect.Lists;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.sql.parser.DrillParserUtil;
import org.apache.drill.exec.planner.sql.parser.SqlShowTables;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParserPos;

import java.util.List;

import static org.apache.drill.exec.planner.sql.parser.DrillParserUtil.CHARSET;

public class ShowTablesHandler extends DefaultSqlHandler {

  public ShowTablesHandler(Planner planner, QueryContext context) { super(planner, context); }

  /** Rewrite the parse tree as SELECT ... FROM INFORMATION_SCHEMA.`TABLES` ... */
  @Override
  public SqlNode rewrite(SqlNode sqlNode) throws RelConversionException{
    SqlShowTables node = unwrap(sqlNode, SqlShowTables.class);
    List<SqlNode> selectList = Lists.newArrayList();
    SqlNode fromClause;
    SqlNode where = null;

    // create select columns
    selectList.add(new SqlIdentifier("TABLE_SCHEMA", SqlParserPos.ZERO));
    selectList.add(new SqlIdentifier("TABLE_NAME", SqlParserPos.ZERO));

    fromClause = new SqlIdentifier(ImmutableList.of("INFORMATION_SCHEMA", "TABLES"), SqlParserPos.ZERO);

    final SqlIdentifier db = node.getDb();
    if (db != null) {
      where = DrillParserUtil.createCondition(
          new SqlIdentifier("TABLE_SCHEMA", SqlParserPos.ZERO),
          SqlStdOperatorTable.EQUALS,
          SqlLiteral.createCharString(db.toString(), CHARSET, db.getParserPosition()));
    }

    SqlNode filter = null;
    final SqlNode likePattern = node.getLikePattern();
    if (likePattern != null) {
      filter = DrillParserUtil.createCondition(
          new SqlIdentifier("TABLE_NAME", SqlParserPos.ZERO),
          SqlStdOperatorTable.LIKE,
          likePattern);
    } else if (node.getWhereClause() != null) {
      filter = node.getWhereClause();
    }

    where = DrillParserUtil.createCondition(where, SqlStdOperatorTable.AND, filter);

    return new SqlSelect(SqlParserPos.ZERO, null, new SqlNodeList(selectList, SqlParserPos.ZERO),
        fromClause, where, null, null, null, null, null, null);
  }
}

