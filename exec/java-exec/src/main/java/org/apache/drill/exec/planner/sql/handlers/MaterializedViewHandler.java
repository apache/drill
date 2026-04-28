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

import org.apache.calcite.plan.RelTraitSet;
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
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillWriterRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.SchemaUtilities;
import org.apache.drill.exec.planner.sql.parser.SqlCreateMaterializedView;
import org.apache.drill.exec.planner.sql.parser.SqlCreateType;
import org.apache.drill.exec.planner.sql.parser.SqlDropMaterializedView;
import org.apache.drill.exec.planner.sql.parser.SqlRefreshMaterializedView;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;

/**
 * Handlers for materialized view DDL commands: CREATE, DROP, and REFRESH MATERIALIZED VIEW.
 * <p>
 * CREATE and DROP return DirectPlan with ok/summary output.
 * REFRESH executes the MV query and writes data to Parquet, returning write statistics.
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
   * <p>
   * Creates the MV definition file. The data will be materialized on first query
   * or can be explicitly populated via REFRESH MATERIALIZED VIEW.
   */
  public static class CreateMaterializedView extends MaterializedViewHandler {

    public CreateMaterializedView(SqlHandlerConfig config) {
      super(config);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException,
        IOException, ForemanSetupException {
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
      // Use the actual schema path where the MV is created (not the session's default schema)
      final MaterializedView materializedView = new MaterializedView(newViewName, viewSql,
          newViewRelNode.getRowType(), drillSchema.getSchemaPath());

      // Create the materialized view definition file
      final boolean replaced = drillSchema.createMaterializedView(materializedView);

      String message = replaced
          ? String.format("Materialized view '%s' replaced successfully in '%s' schema", newViewName, schemaPath)
          : String.format("Materialized view '%s' created successfully in '%s' schema", newViewName, schemaPath);

      logger.info("Created materialized view [{}] in schema [{}]", newViewName, schemaPath);
      return DirectPlan.createDirectPlan(context, true, message);
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

      // Check if it's a materialized view
      final boolean isMaterializedView = table != null &&
          table.getJdbcTableType() == Schema.TableType.MATERIALIZED_VIEW;
      final boolean isView = (table != null && table.getJdbcTableType() == Schema.TableType.VIEW);
      // Regular table check excludes views and materialized views
      final boolean isTable = (table != null
          && table.getJdbcTableType() != Schema.TableType.VIEW
          && table.getJdbcTableType() != Schema.TableType.MATERIALIZED_VIEW)
          || context.getSession().isTemporaryTable(drillSchema, context.getConfig(), viewName);

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

      // Check if the table exists and is actually a materialized view
      final boolean isMaterializedView = viewToDrop != null &&
          viewToDrop.getJdbcTableType() == Schema.TableType.MATERIALIZED_VIEW;

      if (dropMV.checkViewExistence()) {
        if (viewToDrop == null || !isMaterializedView) {
          return DirectPlan.createDirectPlan(context, false,
              String.format("Materialized view [%s] not found in schema [%s].", viewName, schemaPath));
        }
      } else {
        if (viewToDrop == null || !isMaterializedView) {
          if (viewToDrop != null) {
            throw UserException.validationError()
                .message("[%s] is not a materialized view in schema [%s].", viewName, schemaPath)
                .build(logger);
          }
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
   * <p>
   * Re-executes the MV's defining query and writes the results to Parquet files
   * in the MV's data directory. Returns write statistics (like CTAS).
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

      // Get the existing materialized view definition
      final MaterializedView mv = drillSchema.getMaterializedView(viewName);
      if (mv == null) {
        throw UserException.validationError()
            .message("Materialized view [%s] not found in schema [%s].", viewName, schemaPath)
            .build(logger);
      }

      // Clear existing data directory and mark INCOMPLETE while refresh is in progress
      drillSchema.refreshMaterializedView(viewName);

      // Parse and validate the MV's SQL definition
      SqlNode mvQuery = config.getConverter().parse(mv.getSql());
      final ConvertedRelNode convertedRelNode = validateAndConvert(mvQuery);
      final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
      final RelNode queryRelNode = convertedRelNode.getConvertedNode();

      try {
        // Get the writer entry for the MV data directory
        CreateTableEntry createTableEntry = drillSchema.createMaterializedViewDataWriter(viewName);

        // Convert to Drill logical plan with writer
        DrillRel drel = convertToDrel(queryRelNode, createTableEntry, validatedRowType);

        // Convert to physical plan
        Prel prel = convertToPrel(drel, validatedRowType);
        logAndSetTextPlan("Materialized View Refresh Physical", prel, logger);

        PhysicalOperator pop = convertToPop(prel);
        PhysicalPlan plan = convertToPlan(pop, queryRelNode);

        // Mark COMPLETE after the plan is successfully created.
        // TODO: Ideally this should be called after plan execution completes
        // via a post-execution callback, so that the status is only COMPLETE
        // once data files are fully written.
        drillSchema.completeMaterializedViewRefresh(viewName);

        logger.info("Refreshing materialized view [{}] in schema [{}]", viewName, schemaPath);
        return plan;

      } catch (SqlUnsupportedException e) {
        throw UserException.unsupportedError(e)
            .message("Failed to create physical plan for materialized view refresh")
            .build(logger);
      }
    }

    /**
     * Convert to Drill logical plan with a writer on top.
     */
    private DrillRel convertToDrel(RelNode relNode, CreateTableEntry createTableEntry, RelDataType queryRowType)
        throws SqlUnsupportedException {
      final DrillRel convertedRelNode = convertToRawDrel(relNode);

      // Put a non-trivial topProject to ensure the final output field name is preserved
      final DrillRel topPreservedNameProj = queryRowType.getFieldCount() == convertedRelNode.getRowType().getFieldCount()
          ? addRenamedProject(convertedRelNode, queryRowType) : convertedRelNode;

      final RelTraitSet traits = convertedRelNode.getCluster().traitSet().plus(DrillRel.DRILL_LOGICAL);
      final DrillWriterRel writerRel = new DrillWriterRel(convertedRelNode.getCluster(),
          traits, topPreservedNameProj, createTableEntry);
      return new DrillScreenRel(writerRel.getCluster(), writerRel.getTraitSet(), writerRel);
    }
  }
}
