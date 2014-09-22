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

import java.util.List;

import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.sql.parser.DrillParserUtil;
import org.apache.drill.exec.planner.sql.parser.SqlShowSchemas;
import org.eigenbase.relopt.hep.HepPlanner;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlNodeList;
import org.eigenbase.sql.SqlSelect;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

public class ShowSchemasHandler extends DefaultSqlHandler {

  public ShowSchemasHandler(SqlHandlerConfig config) { super(config); }

  /** Rewrite the parse tree as SELECT ... FROM INFORMATION_SCHEMA.SCHEMATA ... */
  @Override
  public SqlNode rewrite(SqlNode sqlNode) throws RelConversionException{
    SqlShowSchemas node = unwrap(sqlNode, SqlShowSchemas.class);
    List<SqlNode> selectList = ImmutableList.of((SqlNode) new SqlIdentifier("SCHEMA_NAME", SqlParserPos.ZERO));

    SqlNode fromClause = new SqlIdentifier(
        ImmutableList.of("INFORMATION_SCHEMA", "SCHEMATA"), null, SqlParserPos.ZERO, null);

    SqlNode where = null;
    final SqlNode likePattern = node.getLikePattern();
    if (likePattern != null) {
      where = DrillParserUtil.createCondition(new SqlIdentifier("SCHEMA_NAME", SqlParserPos.ZERO),
          SqlStdOperatorTable.LIKE, likePattern);
    } else if (node.getWhereClause() != null) {
      where = node.getWhereClause();
    }

    return new SqlSelect(SqlParserPos.ZERO, null, new SqlNodeList(selectList, SqlParserPos.ZERO),
        fromClause, where, null, null, null, null, null, null);
  }
}
