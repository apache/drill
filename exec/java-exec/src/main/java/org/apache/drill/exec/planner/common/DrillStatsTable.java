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
package org.apache.drill.exec.planner.common;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.server.DrillbitContext;

/**
 * Wraps the stats table info including schema and tableName. Also materializes stats from storage and keeps them in
 * memory.
 */
public class DrillStatsTable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillStatsTable.class);

  /**
   * List of columns in stats table.
   */
  public static final String COL_COLUMN = "column";
  public static final String COL_COMPUTED = "computed";
  public static final String COL_STATCOUNT = "statcount";
  public static final String COL_NDV = "ndv";

  private final String schemaName;
  private final String tableName;

  private final Map<String, Long> ndv = Maps.newHashMap();
  private double rowCount = -1;

  private boolean materialized = false;

  public DrillStatsTable(String schemaName, String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  /**
   * Get number of distinct values of given column. If stats are not present for the given column, a null is returned.
   *
   * Note: returned data may not be accurate. Accuracy depends on whether the table data has changed after the
   * stats are computed.
   *
   * @param col
   * @return
   */
  public Double getNdv(String col) {
    Preconditions.checkState(materialized, "Stats are not yet materialized.");

    final String upperCol = col.toUpperCase();
    final Long ndvCol = ndv.get(upperCol);
    if (ndvCol != null) {
      return Math.min(ndvCol, rowCount);
    }

    return null;
  }

  /**
   * Get row count of the table. Returns null if stats are not present.
   *
   * Note: returned data may not be accurate. Accuracy depends on whether the table data has changed after the
   * stats are computed.
   *
   * @return
   */
  public Double getRowCount() {
    Preconditions.checkState(materialized, "Stats are not yet materialized.");
    return rowCount > 0 ? rowCount : null;
  }

  /**
   * Read the stats from storage and keep them in memory.
   * @param context
   * @throws Exception
   */
  public void materialize(final QueryContext context) throws Exception {
    if (materialized) {
      return;
    }

    final String fullTableName = "`" + schemaName + "`.`" + tableName + "`";
    final String sql = "SELECT a.* FROM " + fullTableName + " AS a INNER JOIN " +
        "(SELECT `" + COL_COLUMN + "`, max(`" + COL_COMPUTED +"`) AS `" + COL_COMPUTED + "` " +
        "FROM " + fullTableName + " GROUP BY `" + COL_COLUMN + "`) AS b " +
        "ON a.`" + COL_COLUMN + "` = b.`" + COL_COLUMN +"` and a.`" + COL_COMPUTED + "` = b.`" + COL_COMPUTED + "`";

    final DrillbitContext dc = context.getDrillbitContext();
    try(final DrillClient client = new DrillClient(dc.getConfig(), dc.getClusterCoordinator(), dc.getAllocator())) {
      /*final Listener listener = new Listener(dc.getAllocator());

      client.connect();
      client.runQuery(UserBitShared.QueryType.SQL, sql, listener);

      listener.waitForCompletion();

      for (Map<String, String> r : listener.results) {
        ndv.put(r.get(COL_COLUMN).toUpperCase(), Long.valueOf(r.get(COL_NDV)));
        rowCount = Math.max(rowCount, Long.valueOf(r.get(COL_STATCOUNT)));
      }*/
    }

    materialized = true;
  }

  /**
   * materialize on nodes that have an attached stats table
   */
  public static class StatsMaterializationVisitor extends RelVisitor {
    private QueryContext context;

    public static void materialize(final RelNode relNode, final QueryContext context) {
      new StatsMaterializationVisitor(context).go(relNode);
    }

    private StatsMaterializationVisitor(QueryContext context) {
      this.context = context;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
      if (node instanceof TableScan) {
        try {
          final DrillTable drillTable = node.getTable().unwrap(DrillTable.class);
          final DrillStatsTable statsTable = drillTable.getStatsTable();
          if (statsTable != null) {
            statsTable.materialize(context);
          }
        } catch (Exception e) {
          // Log a warning and proceed. We don't want to fail a query.
          logger.warn("Failed to materialize the stats. Continuing without stats.", e);
        }
      }
      super.visit(node, ordinal, parent);
    }
  }
}
