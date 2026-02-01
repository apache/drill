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

import java.io.IOException;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.util.DrillStringUtils;
import org.apache.drill.exec.dotdrill.MaterializedView;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.SchemaUtilities;
import org.apache.drill.exec.planner.sql.parser.SqlCreateMaterializedView;
import org.apache.drill.exec.planner.sql.parser.SqlCreateType;
import org.apache.drill.exec.planner.sql.parser.SqlDropMaterializedView;
import org.apache.drill.exec.planner.sql.parser.SqlRefreshMaterializedView;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

/**
 * Handlers for materialized view DDL commands: CREATE, DROP, and REFRESH MATERIALIZED VIEW.
 */
public abstract class MaterializedViewHandler extends DefaultSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MaterializedViewHandler.class);

  protected QueryContext context;

  public MaterializedViewHandler(SqlHandlerConfig config) {
    super(config);
    this.context = config.getContext();
  }

  /**
   * Handler for CREATE MATERIALIZED VIEW DDL command.
   */
  public static class CreateMaterializedView extends MaterializedViewHandler {

    public CreateMaterializedView(SqlHandlerConfig config) {
      super(config);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
      SqlCreateMaterializedView createMV = unwrap(sqlNode, SqlCreateMaterializedView.class);

      final String newViewName = DrillStringUtils.removeLeadingSlash(createMV.getName());

      // Disallow temporary tables usage in materialized view definition
      config.getConverter().disallowTemporaryTables();

      // Store the SQL as the view definition
      final String viewSql = createMV.getQuery().toSqlString(null, true).getSql();
      final ConvertedRelNode convertedRelNode = validateAndConvert(createMV.getQuery());
      final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
      final RelNode queryRelNode = convertedRelNode.getConvertedNode();

      final RelNode newViewRelNode = SqlHandlerUtil.resolveNewTableRel(true, createMV.getFieldNames(),
          validatedRowType, queryRelNode);

      final SchemaPlus defaultSchema = context.getNewDefaultSchema();
      final AbstractSchema drillSchema = SchemaUtilities.resolveToMutableDrillSchema(defaultSchema,
          createMV.getSchemaPath());

      final String schemaPath = drillSchema.getFullSchemaName();

      // Check view creation possibility
      if (!checkMaterializedViewCreationPossibility(drillSchema, createMV, context)) {
        return DirectPlan.createDirectPlan(context, false,
            String.format("A table or view with given name [%s] already exists in schema [%s]",
                newViewName, schemaPath));
      }

      // Create the materialized view definition
      final MaterializedView materializedView = new MaterializedView(newViewName, viewSql,
          newViewRelNode.getRowType(), SchemaUtilities.getSchemaPathAsList(defaultSchema));

      // Create the materialized view (this will also populate the data)
      final boolean replaced = drillSchema.createMaterializedView(materializedView);

      final String summary = String.format("Materialized view '%s' %s successfully in '%s' schema",
          newViewName, replaced ? "replaced" : "created", schemaPath);

      return DirectPlan.createDirectPlan(context, true, summary);
    }

    /**
     * Validates if materialized view can be created in indicated schema.
     */
    private boolean checkMaterializedViewCreationPossibility(AbstractSchema drillSchema,
                                                              SqlCreateMaterializedView createMV,
                                                              QueryContext context) {
      final String schemaPath = drillSchema.getFullSchemaName();
      final String viewName = createMV.getName();
      final Table table = SqlHandlerUtil.getTableFromSchema(drillSchema, viewName);

      final boolean isTable = (table != null && table.getJdbcTableType() != Schema.TableType.VIEW)
          || context.getSession().isTemporaryTable(drillSchema, context.getConfig(), viewName);
      final boolean isView = (table != null && table.getJdbcTableType() == Schema.TableType.VIEW);
      // Check if it's a materialized view by checking table type
      final boolean isMaterializedView = table != null &&
          "MATERIALIZED_VIEW".equals(table.getJdbcTableType().jdbcName);

      SqlCreateType createType = createMV.getSqlCreateType();
      switch (createType) {
        case SIMPLE:
          if (isTable) {
            throw UserException.validationError()
                .message("A non-view table with given name [%s] already exists in schema [%s]",
                    viewName, schemaPath)
                .build(logger);
          } else if (isView) {
            throw UserException.validationError()
                .message("A view with given name [%s] already exists in schema [%s]", viewName, schemaPath)
                .build(logger);
          } else if (isMaterializedView) {
            throw UserException.validationError()
                .message("A materialized view with given name [%s] already exists in schema [%s]",
                    viewName, schemaPath)
                .build(logger);
          }
          break;
        case OR_REPLACE:
          if (isTable) {
            throw UserException.validationError()
                .message("A non-view table with given name [%s] already exists in schema [%s]",
                    viewName, schemaPath)
                .build(logger);
          } else if (isView) {
            throw UserException.validationError()
                .message("A regular view with given name [%s] already exists in schema [%s]. " +
                    "Cannot replace a regular view with a materialized view.", viewName, schemaPath)
                .build(logger);
          }
          // Allow replacing existing materialized view
          break;
        case IF_NOT_EXISTS:
          if (isTable || isView || isMaterializedView) {
            return false;
          }
          break;
      }
      return true;
    }
  }

  /**
   * Handler for DROP MATERIALIZED VIEW DDL command.
   */
  public static class DropMaterializedView extends MaterializedViewHandler {

    public DropMaterializedView(SqlHandlerConfig config) {
      super(config);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) throws IOException, ForemanSetupException {
      SqlDropMaterializedView dropMV = unwrap(sqlNode, SqlDropMaterializedView.class);
      final String viewName = DrillStringUtils.removeLeadingSlash(dropMV.getName());
      final AbstractSchema drillSchema = SchemaUtilities.resolveToMutableDrillSchema(
          context.getNewDefaultSchema(), dropMV.getSchemaPath());

      final String schemaPath = drillSchema.getFullSchemaName();

      final Table viewToDrop = SqlHandlerUtil.getTableFromSchema(drillSchema, viewName);

      if (dropMV.checkViewExistence()) {
        if (viewToDrop == null) {
          return DirectPlan.createDirectPlan(context, false,
              String.format("Materialized view [%s] not found in schema [%s].", viewName, schemaPath));
        }
      } else {
        if (viewToDrop == null) {
          throw UserException.validationError()
              .message("Unknown materialized view [%s] in schema [%s].", viewName, schemaPath)
              .build(logger);
        }
      }

      // Drop the materialized view (definition file and data directory)
      drillSchema.dropMaterializedView(viewName);

      return DirectPlan.createDirectPlan(context, true,
          String.format("Materialized view [%s] deleted successfully from schema [%s].", viewName, schemaPath));
    }
  }

  /**
   * Handler for REFRESH MATERIALIZED VIEW DDL command.
   */
  public static class RefreshMaterializedView extends MaterializedViewHandler {

    public RefreshMaterializedView(SqlHandlerConfig config) {
      super(config);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException,
        IOException, ForemanSetupException {
      SqlRefreshMaterializedView refreshMV = unwrap(sqlNode, SqlRefreshMaterializedView.class);
      final String viewName = DrillStringUtils.removeLeadingSlash(refreshMV.getName());
      final AbstractSchema drillSchema = SchemaUtilities.resolveToMutableDrillSchema(
          context.getNewDefaultSchema(), refreshMV.getSchemaPath());

      final String schemaPath = drillSchema.getFullSchemaName();

      final Table viewToRefresh = SqlHandlerUtil.getTableFromSchema(drillSchema, viewName);

      if (viewToRefresh == null) {
        throw UserException.validationError()
            .message("Materialized view [%s] not found in schema [%s].", viewName, schemaPath)
            .build(logger);
      }

      // Refresh the materialized view data
      drillSchema.refreshMaterializedView(viewName);

      return DirectPlan.createDirectPlan(context, true,
          String.format("Materialized view [%s] refreshed successfully in schema [%s].", viewName, schemaPath));
    }
  }
}
