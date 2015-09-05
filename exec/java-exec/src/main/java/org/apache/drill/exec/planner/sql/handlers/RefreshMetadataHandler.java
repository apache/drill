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

import java.io.IOException;
import java.util.List;


import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DrillWriterRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.planner.sql.parser.SqlRefreshMetadata;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.dfs.NamedFormatPluginConfig;
import org.apache.drill.exec.store.parquet.Metadata;
import org.apache.drill.exec.store.parquet.ParquetFormatConfig;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import static org.apache.drill.exec.planner.sql.SchemaUtilites.findSchema;

public class RefreshMetadataHandler extends DefaultSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RefreshMetadataHandler.class);

  public RefreshMetadataHandler(SqlHandlerConfig config) {
    super(config);
  }

  private PhysicalPlan direct(boolean outcome, String message, Object... values){
    return DirectPlan.createDirectPlan(context, outcome, String.format(message, values));
  }

  private PhysicalPlan notSupported(String tbl){
    return direct(false, "Table %s does not support metadata refresh.  Support is currently limited to single-directory-based Parquet tables.", tbl);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    final SqlRefreshMetadata refreshTable = unwrap(sqlNode, SqlRefreshMetadata.class);

    try {

      final SchemaPlus schema = findSchema(context.getNewDefaultSchema(),
          refreshTable.getSchemaPath());

      final String tableName = refreshTable.getName();

      if (tableName.contains("*") || tableName.contains("?")) {
        return direct(false, "Glob path %s not supported for metadata refresh", tableName);
      }

      final Table table = schema.getTable(tableName);

      if(table == null){
        return direct(false, "Table %s does not exist.", tableName);
      }

      if(! (table instanceof DrillTable) ){
        return notSupported(tableName);
      }


      final DrillTable drillTable = (DrillTable) table;

      final Object selection = drillTable.getSelection();
      if( !(selection instanceof FormatSelection) ){
        return notSupported(tableName);
      }

      FormatSelection formatSelection = (FormatSelection) selection;

      FormatPluginConfig formatConfig = formatSelection.getFormat();
      if (!((formatConfig instanceof ParquetFormatConfig) ||
          ((formatConfig instanceof NamedFormatPluginConfig) && ((NamedFormatPluginConfig) formatConfig).name.equals("parquet")))) {
        return notSupported(tableName);
      }

      FileSystemPlugin plugin = (FileSystemPlugin) drillTable.getPlugin();
      DrillFileSystem fs = new DrillFileSystem(plugin.getFormatPlugin(formatSelection.getFormat()).getFsConf());

      String selectionRoot = formatSelection.getSelection().selectionRoot;
      if (!fs.getFileStatus(new Path(selectionRoot)).isDirectory()) {
        return notSupported(tableName);
      }

      Metadata.createMeta(fs, selectionRoot);
      return direct(true, "Successfully updated metadata for table %s.", tableName);

    } catch(Exception e) {
      logger.error("Failed to update metadata for table '{}'", refreshTable.getName(), e);
      return DirectPlan.createDirectPlan(context, false, String.format("Error: %s", e.getMessage()));
    }
  }


}
