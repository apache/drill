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

import static org.apache.drill.exec.planner.sql.parser.DrillParserUtil.CHARSET;

import java.util.List;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.RelConversionException;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.DrillParserUtil;
import org.apache.drill.exec.planner.sql.parser.SqlShowTables;
import org.apache.drill.exec.store.AbstractSchema;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.*;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class ShowTablesHandler extends DefaultSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShowTablesHandler.class);

  public ShowTablesHandler(SqlHandlerConfig config) { super(config); }

  /** Rewrite the parse tree as SELECT ... FROM INFORMATION_SCHEMA.`TABLES` ... */
  @Override
  public SqlNode rewrite(SqlNode sqlNode) throws RelConversionException, ForemanSetupException {
    SqlShowTables node = unwrap(sqlNode, SqlShowTables.class);
    List<SqlNode> selectList = Lists.newArrayList();
    SqlNode fromClause;
    SqlNode where;

    // create select columns
    selectList.add(new SqlIdentifier(SHRD_COL_TABLE_SCHEMA, SqlParserPos.ZERO));
    selectList.add(new SqlIdentifier(SHRD_COL_TABLE_NAME, SqlParserPos.ZERO));

    fromClause = new SqlIdentifier(ImmutableList.of(IS_SCHEMA_NAME, TAB_TABLES), SqlParserPos.ZERO);

    final SqlIdentifier db = node.getDb();
    String tableSchema;
    if (db != null) {
      tableSchema = db.toString();
    } else {
      // If no schema is given in SHOW TABLES command, list tables from current schema
      SchemaPlus schema = context.getNewDefaultSchema();

      if (SchemaUtilites.isRootSchema(schema)) {
        // If the default schema is a root schema, throw an error to select a default schema
        throw UserException.validationError()
            .message("No default schema selected. Select a schema using 'USE schema' command")
            .build(logger);
      }

      final AbstractSchema drillSchema = SchemaUtilites.unwrapAsDrillSchemaInstance(schema);
      tableSchema = drillSchema.getFullSchemaName();
    }

    where = DrillParserUtil.createCondition(
        new SqlIdentifier(SHRD_COL_TABLE_SCHEMA, SqlParserPos.ZERO),
        SqlStdOperatorTable.EQUALS,
        SqlLiteral.createCharString(tableSchema, CHARSET, SqlParserPos.ZERO));

    SqlNode filter = null;
    final SqlNode likePattern = node.getLikePattern();
    if (likePattern != null) {
      filter = DrillParserUtil.createCondition(
          new SqlIdentifier(SHRD_COL_TABLE_NAME, SqlParserPos.ZERO),
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
