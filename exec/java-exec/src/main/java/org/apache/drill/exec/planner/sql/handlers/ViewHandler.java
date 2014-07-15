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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.ViewTable;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.exec.dotdrill.View;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.logical.DrillViewTable;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.parser.SqlCreateView;
import org.apache.drill.exec.planner.sql.parser.SqlDropView;
import org.apache.drill.exec.planner.types.DrillFixedRelDataTypeImpl;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory.WorkspaceSchema;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlNode;

public abstract class ViewHandler extends AbstractSqlHandler{
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
    public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException {
      SqlCreateView createView = unwrap(sqlNode, SqlCreateView.class);

      try {
        SchemaPlus defaultSchema = context.getNewDefaultSchema();
        SchemaPlus schema = findSchema(context.getRootSchema(), defaultSchema, createView.getSchemaPath());
        AbstractSchema drillSchema = getDrillSchema(schema);

        String schemaPath = drillSchema.getFullSchemaName();
        if (!drillSchema.isMutable())
          return DirectPlan.createDirectPlan(context, false, String.format("Current schema '%s' is not a mutable schema. " +
              "Can't create views in this schema.", schemaPath));

        // find current workspace schema path
        List<String> workspaceSchemaPath = ImmutableList.of();
        if (!isRootSchema(defaultSchema)) {
          workspaceSchemaPath = getDrillSchema(defaultSchema).getSchemaPath();
        }

        String viewSql = createView.getQuery().toString();

        SqlNode validatedQuery = planner.validate(createView.getQuery());
        RelNode validatedRelNode = planner.convert(validatedQuery);

        // If view's field list is specified then its size should match view's query field list size.
        RelDataType queryRowType = validatedRelNode.getRowType();

        List<String> viewFieldNames = createView.getFieldNames();
        if (viewFieldNames.size() > 0) {
          // number of fields match.
          if (viewFieldNames.size() != queryRowType.getFieldCount())
            return DirectPlan.createDirectPlan(context, false,
                "View's field list and View's query field list have different counts.");

          // make sure View's query field list has no "*"
          for(String field : queryRowType.getFieldNames()) {
            if (field.equals("*"))
              return DirectPlan.createDirectPlan(context, false,
                  "View's query field list has a '*', which is invalid when View's field list is specified.");
          }

          queryRowType = new DrillFixedRelDataTypeImpl(planner.getTypeFactory(), viewFieldNames);
        }

        View view = new View(createView.getName(), viewSql, queryRowType, workspaceSchemaPath);

        boolean replaced;
        if (drillSchema instanceof WorkspaceSchema) {
          WorkspaceSchema workspaceSchema = (WorkspaceSchema) drillSchema;
          if (!createView.getReplace() && workspaceSchema.viewExists(view.getName())) {
            return DirectPlan.createDirectPlan(context, false, "View with given name already exists in current schema");
          }
          replaced = ((WorkspaceSchema) drillSchema).createView(view);
        }else{
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
    public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException {
      SqlDropView dropView = unwrap(sqlNode, SqlDropView.class);

      try {
        SchemaPlus schema = findSchema(context.getRootSchema(), context.getNewDefaultSchema(), dropView.getSchemaPath());
        AbstractSchema drillSchema = getDrillSchema(schema);

        String schemaPath = drillSchema.getFullSchemaName();
        if (!drillSchema.isMutable())
          return DirectPlan.createDirectPlan(context, false, String.format("Schema '%s' is not a mutable schema. " +
              "Views don't exist in this schema", schemaPath));

        if (drillSchema instanceof WorkspaceSchema) {
          ((WorkspaceSchema) drillSchema).dropView(dropView.getName());;
        }else{
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
