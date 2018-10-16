/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.sql.handlers;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillAnalyzeRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.logical.DrillWriterRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlAnalyzeTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;

import java.io.IOException;
import java.util.List;

public class AnalyzeTableHandler extends DefaultSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AnalyzeTableHandler.class);

  public AnalyzeTableHandler(SqlHandlerConfig config, Pointer<String> textPlan) {
    super(config, textPlan);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode)
      throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    final SqlAnalyzeTable sqlAnalyzeTable = unwrap(sqlNode, SqlAnalyzeTable.class);

    verifyNoUnsupportedFunctions(sqlAnalyzeTable);

    SqlIdentifier tableIdentifier = sqlAnalyzeTable.getTableIdentifier();
    SqlSelect scanSql = new SqlSelect(
        SqlParserPos.ZERO, /* position */
        SqlNodeList.EMPTY, /* keyword list */
        getColumnList(sqlAnalyzeTable), /*select list */
        tableIdentifier, /* from */
        null, /* where */
        null, /* group by */
        null, /* having */
        null, /* windowDecls */
        null, /* orderBy */
        null, /* offset */
        null /* fetch */
    );

    final ConvertedRelNode convertedRelNode = validateAndConvert(rewrite(scanSql));
    final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();

    final RelNode relScan = convertedRelNode.getConvertedNode();

    final String tableName = sqlAnalyzeTable.getName();
    final AbstractSchema drillSchema = SchemaUtilites.resolveToMutableDrillSchema(
        config.getConverter().getDefaultSchema(), sqlAnalyzeTable.getSchemaPath());

    if (SqlHandlerUtil.getTableFromSchema(drillSchema, tableName) == null) {
      throw UserException.validationError()
          .message("No table with given name [%s] exists in schema [%s]", tableName, drillSchema.getFullSchemaName())
          .build(logger);
    }

    // Convert the query to Drill Logical plan and insert a writer operator on top.
    DrillRel drel = convertToDrel(relScan, drillSchema, tableName);
    Prel prel = convertToPrel(drel, validatedRowType);
    logAndSetTextPlan("Drill Physical", prel, logger);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    log("Drill Plan", plan, logger);

    return plan;
  }

  private SqlNodeList getColumnList(final SqlAnalyzeTable sqlAnalyzeTable) {
    final SqlNodeList columnList = new SqlNodeList(SqlParserPos.ZERO);

    final List<String> fields = sqlAnalyzeTable.getFieldNames();
    if (fields == null || fields.size() <= 0) {
      columnList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
    } else {
      for(String field : fields) {
        columnList.add(new SqlIdentifier(field, SqlParserPos.ZERO));
      }
    }

    return columnList;
  }

  protected DrillRel convertToDrel(RelNode relNode, AbstractSchema schema, String analyzeTableName)
      throws RelConversionException, SqlUnsupportedException {
    final DrillRel convertedRelNode = convertToDrel(relNode);

    if (convertedRelNode instanceof DrillStoreRel) {
      throw new UnsupportedOperationException();
    }

    final RelNode analyzeRel = new DrillAnalyzeRel(
        convertedRelNode.getCluster(),
        convertedRelNode.getTraitSet(),
        convertedRelNode
    );

    final RelNode writerRel = new DrillWriterRel(
        analyzeRel.getCluster(),
        analyzeRel.getTraitSet(),
        analyzeRel,
        schema.appendToStatsTable(analyzeTableName)
    );

    return new DrillScreenRel(writerRel.getCluster(), writerRel.getTraitSet(), writerRel);
  }

  // make sure no unsupported features in ANALYZE statement are used
  private static void verifyNoUnsupportedFunctions(final SqlAnalyzeTable analyzeTable) {
    // throw unsupported error for functions that are not yet implemented
    if (analyzeTable.getEstimate()) {
      throw UserException.unsupportedError()
          .message("Statistics estimation is not yet supported.")
          .build(logger);
    }

    if (analyzeTable.getPercent() != 100) {
      throw UserException.unsupportedError()
          .message("Statistics from sampling is not yet supported.")
          .build(logger);
    }
  }
}
