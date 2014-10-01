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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import org.apache.drill.exec.dotdrill.View;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlCreateView;
import org.apache.drill.exec.planner.sql.parser.SqlDropView;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory.WorkspaceSchema;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

import com.google.common.collect.ImmutableList;

public abstract class ViewHandler extends AbstractSqlHandler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ViewHandler.class);

  protected Planner planner;
  protected QueryContext context;

  public ViewHandler(Planner planner, QueryContext context) {
    this.planner = planner;
    this.context = context;
  }

  /** Handler for Create View DDL command */
  public static class CreateView extends ViewHandler {

    public CreateView(Planner planner, QueryContext context) {
      super(planner, context);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
      SqlCreateView createView = unwrap(sqlNode, SqlCreateView.class);

      try {
        // Store the viewSql as view def SqlNode is modified as part of the resolving the new table definition below.
        final String viewSql = createView.getQuery().toString();

        final RelNode newViewRelNode =
            SqlHandlerUtil.resolveNewTableRel(true, planner, createView.getFieldNames(), createView.getQuery());

        SchemaPlus defaultSchema = context.getNewDefaultSchema();
        SchemaPlus schema = findSchema(context.getRootSchema(), defaultSchema, createView.getSchemaPath());
        AbstractSchema drillSchema = getDrillSchema(schema);

        String schemaPath = drillSchema.getFullSchemaName();
        if (!drillSchema.isMutable()) {
          return DirectPlan.createDirectPlan(context, false, String.format("Unable to create view. " +
            "Schema [%s] is immutable. ", schemaPath));
        }

        // find current workspace schema path
        List<String> workspaceSchemaPath = ImmutableList.of();
        if (!isRootSchema(defaultSchema)) {
          workspaceSchemaPath = getDrillSchema(defaultSchema).getSchemaPath();
        }

        View view = new View(createView.getName(), viewSql, newViewRelNode.getRowType(), workspaceSchemaPath);

        final String viewName = view.getName();
        final Table existingTable = SqlHandlerUtil.getTableFromSchema(drillSchema, viewName);

        if (existingTable != null) {
          if (existingTable.getJdbcTableType() != Schema.TableType.VIEW) {
            // existing table is not a view
            throw new ValidationException(
                String.format("A non-view table with given name [%s] already exists in schema [%s]",
                    viewName, schemaPath));
          }

          if (existingTable.getJdbcTableType() == Schema.TableType.VIEW && !createView.getReplace()) {
            // existing table is a view and create view has no "REPLACE" clause
            throw new ValidationException(
                String.format("A view with given name [%s] already exists in schema [%s]",
                    view.getName(), schemaPath));
          }
        }

        boolean replaced;
        if (drillSchema instanceof WorkspaceSchema) {
          replaced = ((WorkspaceSchema) drillSchema).createView(view);
        } else {
          return DirectPlan.createDirectPlan(context, false, "Schema provided was not a workspace schema.");
        }

        String summary = String.format("View '%s' %s successfully in '%s' schema",
            createView.getName(), replaced ? "replaced" : "created", schemaPath);

        return DirectPlan.createDirectPlan(context, true, summary);
      } catch(Exception e) {
        logger.error("Failed to create view '{}'", createView.getName(), e);
        return DirectPlan.createDirectPlan(context, false, String.format("Error: %s", e.getMessage()));
      }
    }
  }

  /** Handler for Drop View DDL command. */
  public static class DropView extends ViewHandler {
    public DropView(QueryContext context) {
      super(null, context);
    }

    @Override
    public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
      SqlDropView dropView = unwrap(sqlNode, SqlDropView.class);

      try {
        SchemaPlus schema = findSchema(context.getRootSchema(), context.getNewDefaultSchema(), dropView.getSchemaPath());
        AbstractSchema drillSchema = getDrillSchema(schema);

        String schemaPath = drillSchema.getFullSchemaName();
        if (!drillSchema.isMutable()) {
          return DirectPlan.createDirectPlan(context, false, String.format("Schema '%s' is not a mutable schema. " +
              "Views don't exist in this schema", schemaPath));
        }

        if (drillSchema instanceof WorkspaceSchema) {
          ((WorkspaceSchema) drillSchema).dropView(dropView.getName());;
        } else {
          return DirectPlan.createDirectPlan(context, false, "Schema provided was not a workspace schema.");
        }

        return DirectPlan.createDirectPlan(context, true,
            String.format("View '%s' deleted successfully from '%s' schema", dropView.getName(), schemaPath));
      } catch(Exception e) {
        logger.debug("Failed to delete view {}", dropView.getName(), e);
        return DirectPlan.createDirectPlan(context, false, String.format("Error: %s", e.getMessage()));
      }
    }
  }

}
