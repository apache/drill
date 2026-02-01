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
package org.apache.drill.exec.planner.logical;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.drill.exec.dotdrill.MaterializedView;
import org.apache.drill.exec.ops.ViewExpansionContext;
import org.apache.drill.exec.planner.sql.conversion.DrillViewExpander;

/**
 * Represents a materialized view in the Drill query planning.
 * <p>
 * Unlike regular views which expand to their definition query, materialized views
 * read from pre-computed data stored in the workspace directory.
 * <p>
 * A materialized view stores:
 * <ul>
 *   <li>Definition file (.materialized_view.drill) - JSON with name, SQL, schema info</li>
 *   <li>Data directory - Parquet files with the pre-computed results</li>
 * </ul>
 */
public class DrillMaterializedViewTable implements TranslatableTable, DrillViewInfoProvider {

  private final MaterializedView materializedView;
  private final String viewOwner;
  private final ViewExpansionContext viewExpansionContext;
  private final String workspaceLocation;

  public DrillMaterializedViewTable(MaterializedView materializedView, String viewOwner,
                                    ViewExpansionContext viewExpansionContext, String workspaceLocation) {
    this.materializedView = materializedView;
    this.viewOwner = viewOwner;
    this.viewExpansionContext = viewExpansionContext;
    this.workspaceLocation = workspaceLocation;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return materializedView.getRowType(typeFactory);
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  /**
   * Converts this materialized view to a RelNode for query planning.
   * <p>
   * Unlike regular views, materialized views expand to their definition SQL
   * which is then converted to a RelNode. The data is actually read from
   * the materialized data directory, not computed fresh.
   * <p>
   * For now, we expand the view definition since the data is in Parquet format
   * in a directory with the same name as the view. The storage plugin will
   * handle reading the actual data.
   */
  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    DrillViewExpander viewExpander = viewExpansionContext.getViewExpander();
    ViewExpansionContext.ViewExpansionToken token = null;
    try {
      RelDataType rowType = relOptTable.getRowType();
      RelNode rel;

      if (viewExpansionContext.isImpersonationEnabled()) {
        token = viewExpansionContext.reserveViewExpansionToken(viewOwner);
        rel = expandViewForImpersonatedUser(viewExpander, materializedView.getWorkspaceSchemaPath(),
            token.getSchemaTree());
      } else {
        rel = viewExpander.expandView(rowType, materializedView.getSql(),
            materializedView.getWorkspaceSchemaPath(), Collections.emptyList()).rel;
      }

      return rel;
    } finally {
      if (token != null) {
        token.release();
      }
    }
  }

  protected RelNode expandViewForImpersonatedUser(DrillViewExpander context,
                                                  List<String> workspaceSchemaPath,
                                                  SchemaPlus tokenSchemaTree) {
    return context.expandView(materializedView.getSql(), tokenSchemaTree, workspaceSchemaPath).rel;
  }

  @Override
  public TableType getJdbcTableType() {
    // Report as TABLE since materialized views store actual data
    // This distinguishes them from regular views (VIEW type)
    return TableType.TABLE;
  }

  @Override
  public String getViewSql() {
    return materializedView.getSql();
  }

  /**
   * @return the materialized view definition
   */
  public MaterializedView getMaterializedView() {
    return materializedView;
  }

  /**
   * @return the owner of this materialized view
   */
  public String getViewOwner() {
    return viewOwner;
  }

  /**
   * @return path to the data storage location for this materialized view
   */
  public String getDataStoragePath() {
    return workspaceLocation + "/" + materializedView.getDataStoragePath();
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(String column,
      SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
    return true;
  }

  @Override
  public boolean isRolledUp(String column) {
    return false;
  }
}
