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
 * A materialized view stores:
 * <ul>
 *   <li>Definition file (.materialized_view.drill) - JSON with name, SQL, schema info</li>
 *   <li>Data directory ({name}_mv_data/) - Parquet files with pre-computed results</li>
 * </ul>
 * <p>
 * <b>Behavior:</b>
 * <ul>
 *   <li>Before REFRESH: queries expand the SQL definition (like a view)</li>
 *   <li>After REFRESH: queries scan from pre-computed Parquet data</li>
 * </ul>
 *
 * @see org.apache.drill.exec.dotdrill.MaterializedView
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
   * If the MV has been refreshed (data exists), scans from the pre-computed Parquet data.
   * Otherwise, expands the SQL definition like a regular view.
   */
  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    DrillViewExpander viewExpander = viewExpansionContext.getViewExpander();
    ViewExpansionContext.ViewExpansionToken token = null;
    try {
      RelDataType rowType = relOptTable.getRowType();
      RelNode rel;

      // Check if materialized data exists (REFRESH has been called)
      boolean hasData = materializedView.getRefreshStatus() == MaterializedView.RefreshStatus.COMPLETE;

      // Build the SQL to execute - either scan data or expand definition
      String sqlToExpand;
      if (hasData) {
        // Scan from the pre-computed data directory
        sqlToExpand = buildDataScanSql();
      } else {
        // No data yet - expand the SQL definition like a view
        sqlToExpand = materializedView.getSql();
      }

      // Always use the workspace schema path for context - needed for table resolution
      List<String> schemaPath = materializedView.getWorkspaceSchemaPath();

      if (viewExpansionContext.isImpersonationEnabled()) {
        token = viewExpansionContext.reserveViewExpansionToken(viewOwner);
        rel = viewExpander.expandView(sqlToExpand, token.getSchemaTree(), schemaPath).rel;
      } else {
        // When scanning data, pass null for rowType to let Parquet schema be inferred
        // When expanding SQL definition, use the MV's row type
        RelDataType typeHint = hasData ? null : rowType;
        rel = viewExpander.expandView(typeHint, sqlToExpand, schemaPath, Collections.emptyList()).rel;
      }

      return rel;
    } finally {
      if (token != null) {
        token.release();
      }
    }
  }

  /**
   * Builds SQL to scan the materialized data directory.
   * The data is stored in {workspace}/{mvName}_mv_data/ directory.
   * We explicitly select the MV's columns to ensure proper schema matching.
   */
  private String buildDataScanSql() {
    String dataTableName = materializedView.getName() + "_mv_data";

    // Build explicit column list from the MV's field definitions
    List<String> fieldNames = materializedView.getFields().stream()
        .map(f -> f.getName())
        .collect(java.util.stream.Collectors.toList());
    if (fieldNames.isEmpty()) {
      // Fallback to SELECT * if no fields defined (shouldn't happen for non-dynamic MVs)
      return "SELECT * FROM `" + dataTableName + "`";
    }

    StringBuilder sql = new StringBuilder("SELECT ");
    for (int i = 0; i < fieldNames.size(); i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append("`").append(fieldNames.get(i)).append("`");
    }
    sql.append(" FROM `").append(dataTableName).append("`");
    return sql.toString();
  }

  @Override
  public TableType getJdbcTableType() {
    // Report as MATERIALIZED_VIEW type
    return TableType.MATERIALIZED_VIEW;
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
