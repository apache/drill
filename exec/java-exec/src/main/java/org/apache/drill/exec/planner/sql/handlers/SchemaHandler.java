/*
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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.IOUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlCreateType;
import org.apache.drill.exec.planner.sql.parser.SqlSchema;
import org.apache.drill.exec.record.metadata.schema.FsMetastoreSchemaProvider;
import org.apache.drill.exec.record.metadata.schema.PathSchemaProvider;
import org.apache.drill.exec.record.metadata.schema.SchemaProvider;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.StorageStrategy;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Parent class for CREATE / DROP SCHEMA handlers.
 * Contains common logic on how extract workspace, output error result.
 */
public abstract class SchemaHandler extends DefaultSqlHandler {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaHandler.class);

  SchemaHandler(SqlHandlerConfig config) {
    super(config);
  }

  WorkspaceSchemaFactory.WorkspaceSchema getWorkspaceSchema(List<String> tableSchema, String tableName) {
    SchemaPlus defaultSchema = config.getConverter().getDefaultSchema();
    AbstractSchema temporarySchema = SchemaUtilites.resolveToTemporarySchema(tableSchema, defaultSchema, context.getConfig());

    if (context.getSession().isTemporaryTable(temporarySchema, context.getConfig(), tableName)) {
      produceErrorResult(String.format("Indicated table [%s] is temporary table", tableName), true);
    }

    AbstractSchema drillSchema = SchemaUtilites.resolveToMutableDrillSchema(defaultSchema, tableSchema);
    Table table = SqlHandlerUtil.getTableFromSchema(drillSchema, tableName);
    if (table == null || table.getJdbcTableType() != Schema.TableType.TABLE) {
      produceErrorResult(String.format("Table [%s] was not found", tableName), true);
    }

    if (!(drillSchema instanceof WorkspaceSchemaFactory.WorkspaceSchema)) {
      produceErrorResult(String.format("Table [`%s`.`%s`] must belong to file storage plugin",
        drillSchema.getFullSchemaName(), tableName), true);
    }

    Preconditions.checkState(drillSchema instanceof WorkspaceSchemaFactory.WorkspaceSchema);
    return (WorkspaceSchemaFactory.WorkspaceSchema) drillSchema;
  }

  PhysicalPlan produceErrorResult(String message, boolean doFail) {
    if (doFail) {
      throw UserException.validationError().message(message).build(logger);
    } else {
      return DirectPlan.createDirectPlan(context, false, message);
    }
  }

  /**
   * CREATE SCHEMA command handler.
   */
  public static class Create extends SchemaHandler {

    public Create(SqlHandlerConfig config) {
      super(config);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) {

      SqlSchema.Create sqlCall = ((SqlSchema.Create) sqlNode);

      String schemaString = getSchemaString(sqlCall);
      String schemaSource = sqlCall.hasTable() ? sqlCall.getTable().toString() : sqlCall.getPath();
      try {

        SchemaProvider schemaProvider;
        if (sqlCall.hasTable()) {
          String tableName = sqlCall.getTableName();
          WorkspaceSchemaFactory.WorkspaceSchema wsSchema = getWorkspaceSchema(sqlCall.getSchemaPath(), tableName);
          schemaProvider = new FsMetastoreSchemaProvider(wsSchema, tableName);
        } else {
          schemaProvider = new PathSchemaProvider(new Path(sqlCall.getPath()));
        }

        if (schemaProvider.exists()) {
          if (SqlCreateType.OR_REPLACE == sqlCall.getSqlCreateType()) {
            schemaProvider.delete();
          } else {
            return produceErrorResult(String.format("Schema already exists for [%s]", schemaSource), true);
          }
        }

        // schema file will be created with same permission as used for persistent tables
        StorageStrategy storageStrategy = new StorageStrategy(context.getOption(
          ExecConstants.PERSISTENT_TABLE_UMASK).string_val, false);
        schemaProvider.store(schemaString, sqlCall.getProperties(), storageStrategy);
        return DirectPlan.createDirectPlan(context, true, String.format("Created schema for [%s]", schemaSource));

      } catch (IOException e) {
        throw UserException.resourceError(e)
          .message(e.getMessage())
          .addContext("Error while preparing / creating schema for [%s]", schemaSource)
          .build(logger);
      }
    }

    /**
     * If raw schema was present in create schema command, returns schema from command,
     * otherwise loads raw schema from the given file.
     *
     * @param sqlCall sql create schema call
     * @return string representation of raw schema (column names, types and nullability)
     */
    private String getSchemaString(SqlSchema.Create sqlCall) {
      if (sqlCall.hasSchema()) {
        return sqlCall.getSchema();
      }

      Path path = new Path(sqlCall.getLoad());
      try {
        FileSystem rawFs = path.getFileSystem(new Configuration());
        FileSystem fs = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), rawFs.getConf());

        if (!fs.exists(path)) {
          throw UserException.resourceError()
            .message("File with raw schema [%s] does not exist", path.toUri().getPath())
            .build(logger);
        }

        try (InputStream stream = fs.open(path)) {
          return IOUtils.toString(stream);
        }

      } catch (IOException e) {
        throw UserException.resourceError(e)
          .message("Unable to load raw schema from file %s", path.toUri().getPath())
          .build(logger);
      }
    }
  }

  /**
   * DROP SCHEMA command handler.
   */
  public static class Drop extends SchemaHandler {

    public Drop(SqlHandlerConfig config) {
      super(config);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) {
      SqlSchema.Drop sqlCall = ((SqlSchema.Drop) sqlNode);

      String tableName = sqlCall.getTableName();
      WorkspaceSchemaFactory.WorkspaceSchema wsSchema = getWorkspaceSchema(sqlCall.getSchemaPath(), tableName);

      try {

        SchemaProvider schemaProvider = new FsMetastoreSchemaProvider(wsSchema, tableName);

        if (!schemaProvider.exists()) {
          return produceErrorResult(String.format("Schema [%s] does not exist in table [%s] root directory",
            SchemaProvider.DEFAULT_SCHEMA_NAME, sqlCall.getTable()), !sqlCall.ifExists());
        }

        schemaProvider.delete();

        return DirectPlan.createDirectPlan(context, true,
          String.format("Dropped schema for table [%s]", sqlCall.getTable()));

      } catch (IOException e) {
        throw UserException.resourceError(e)
          .message(e.getMessage())
          .addContext("Error while accessing table location or deleting schema for [%s]", sqlCall.getTable())
          .build(logger);
      }
    }
  }

}
