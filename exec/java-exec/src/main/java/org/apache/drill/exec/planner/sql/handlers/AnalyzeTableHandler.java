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

import java.io.IOException;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.dotdrill.DotDrillType;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillAnalyzeRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.logical.DrillWriterRel;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlAnalyzeTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.dfs.NamedFormatPluginConfig;
import org.apache.drill.exec.store.parquet.ParquetFormatConfig;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

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
        SqlParserPos.ZERO,              /* position */
        SqlNodeList.EMPTY,              /* keyword list */
        getColumnList(sqlAnalyzeTable), /* select list */
        tableIdentifier,                /* from */
        null,                           /* where */
        null,                           /* group by */
        null,                           /* having */
        null,                           /* windowDecls */
        null,                           /* orderBy */
        null,                           /* offset */
        null                            /* fetch */
    );

    final ConvertedRelNode convertedRelNode = validateAndConvert(rewrite(scanSql));
    final RelNode relScan = convertedRelNode.getConvertedNode();
    final String tableName = sqlAnalyzeTable.getName();
    final AbstractSchema drillSchema = SchemaUtilites.resolveToDrillSchema(
        config.getConverter().getDefaultSchema(), sqlAnalyzeTable.getSchemaPath());
    Table table = SqlHandlerUtil.getTableFromSchema(drillSchema, tableName);

    if (table == null) {
      throw UserException.validationError()
          .message("No table with given name [%s] exists in schema [%s]", tableName,
              drillSchema.getFullSchemaName())
          .build(logger);
    }

    if(! (table instanceof DrillTable)) {
      return notSupported(tableName);
    }

    if (table instanceof DrillTable) {
      DrillTable drillTable = (DrillTable) table;
      final Object selection = drillTable.getSelection();
      if (!(selection instanceof FormatSelection)) {
        return notSupported(tableName);
      }
      // Do not support non-parquet tables
      FormatSelection formatSelection = (FormatSelection) selection;
      FormatPluginConfig formatConfig = formatSelection.getFormat();
      if (!((formatConfig instanceof ParquetFormatConfig)
            || ((formatConfig instanceof NamedFormatPluginConfig)
                 && ((NamedFormatPluginConfig) formatConfig).name.equals("parquet")))) {
        return notSupported(tableName);
      }

      FileSystemPlugin plugin = (FileSystemPlugin) drillTable.getPlugin();
      DrillFileSystem fs = new DrillFileSystem(plugin.getFormatPlugin(
          formatSelection.getFormat()).getFsConf());

      String selectionRoot = formatSelection.getSelection().getSelectionRoot();
      if (!selectionRoot.contains(tableName)
          || !fs.getFileStatus(new Path(selectionRoot)).isDirectory()) {
        return notSupported(tableName);
      }
      // Do not recompute statistics, if stale
      Path statsFilePath = new Path(new Path(selectionRoot), DotDrillType.STATS.getEnding());
      if (fs.exists(statsFilePath)
          && !isStatsStale(fs, statsFilePath)) {
       return notRequired(tableName);
      }
    }
    // Convert the query to Drill Logical plan and insert a writer operator on top.
    DrillRel drel = convertToDrel(relScan, drillSchema, tableName);
    Prel prel = convertToPrel(drel);
    logAndSetTextPlan("Drill Physical", prel, logger);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    log("Drill Plan", plan, logger);

    return plan;
  }

  /* Determines if the table was modified after computing statistics based on
   * directory/file modification timestamps
   */
  private boolean isStatsStale(DrillFileSystem fs, Path statsFilePath)
      throws IOException {
    long statsFileModifyTime = fs.getFileStatus(statsFilePath).getModificationTime();
    Path parentPath = statsFilePath.getParent();
    FileStatus directoryStatus = fs.getFileStatus(parentPath);
    // Parent directory modified after stats collection?
    if (directoryStatus.getModificationTime() > statsFileModifyTime) {
      return true;
    }
    if (tableModified(fs, parentPath, statsFileModifyTime)) {
      return true;
    }
    return false;
  }

  /* Determines if the table was modified after computing statistics based on
   * directory/file modification timestamps. Recursively checks sub-directories.
   */
  private boolean tableModified(DrillFileSystem fs, Path parentPath,
                                long statsModificationTime) throws IOException {
    for (final FileStatus file : fs.listStatus(parentPath)) {
      // If directory or files within it are modified
      if (file.getModificationTime() > statsModificationTime) {
        return true;
      }
      // For a directory, we should recursively check sub-directories
      if (file.isDirectory()) {
        if (tableModified(fs, file.getPath(), statsModificationTime)) {
          return true;
        }
      }
    }
    return false;
  }

  private PhysicalPlan direct(boolean outcome, String message, Object... values){
    return DirectPlan.createDirectPlan(context, outcome, String.format(message, values));
  }

  /* Helper function to generate error - statistics not supported on non-parquet tables */
  private PhysicalPlan notSupported(String tbl){
    return direct(false, "Table %s is not supported by ANALYZE."
        + " Support is currently limited to directory-based Parquet tables.", tbl);
  }

  private PhysicalPlan notRequired(String tbl){
    return direct(false, "Table %s has not changed since last ANALYZE!", tbl);
  }

  /* Generates the column list specified in the ANALYZE statement */
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

  /* Converts to Drill logical plan */
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

  // Make sure no unsupported features in ANALYZE statement are used
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
