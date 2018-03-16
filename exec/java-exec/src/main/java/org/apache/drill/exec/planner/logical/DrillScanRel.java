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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.exec.util.Utilities;

/**
 * GroupScan of a Drill table.
 */
public class DrillScanRel extends DrillScanRelBase implements DrillRel {
  private final static int STAR_COLUMN_COST = 10000;
  private PlannerSettings settings;
  private List<SchemaPath> columns;
  private final boolean partitionFilterPushdown;
  final private RelDataType rowType;

  /** Creates a DrillScan. */
  public DrillScanRel(final RelOptCluster cluster, final RelTraitSet traits,
                      final RelOptTable table) {
    this(cluster, traits, table, false);
  }
  /** Creates a DrillScan. */
  public DrillScanRel(final RelOptCluster cluster, final RelTraitSet traits,
                      final RelOptTable table, boolean partitionFilterPushdown) {
    // By default, scan does not support project pushdown.
    // Decision whether push projects into scan will be made solely in DrillPushProjIntoScanRule.
    this(cluster, traits, table, table.getRowType(), getProjectedColumns(table, true), partitionFilterPushdown);
    this.settings = PrelUtil.getPlannerSettings(cluster.getPlanner());
  }

  /** Creates a DrillScan. */
  public DrillScanRel(final RelOptCluster cluster, final RelTraitSet traits,
                      final RelOptTable table, final RelDataType rowType, final List<SchemaPath> columns) {
    this(cluster, traits, table, rowType, columns, false);
  }

  /** Creates a DrillScan. */
  public DrillScanRel(final RelOptCluster cluster, final RelTraitSet traits,
                      final RelOptTable table, final RelDataType rowType, final List<SchemaPath> columns, boolean partitionFilterPushdown) {
    super(cluster, traits, table, columns);
    this.settings = PrelUtil.getPlannerSettings(cluster.getPlanner());
    this.rowType = rowType;
    Preconditions.checkNotNull(columns);
    this.columns = columns;
    this.partitionFilterPushdown = partitionFilterPushdown;
  }

  /** Creates a DrillScanRel for a particular GroupScan */
  public DrillScanRel(final RelOptCluster cluster, final RelTraitSet traits,
                      final RelOptTable table, final GroupScan groupScan, final RelDataType rowType, final List<SchemaPath> columns) {
    this(cluster, traits, table, groupScan, rowType, columns, false);
  }

  /** Creates a DrillScanRel for a particular GroupScan */
  public DrillScanRel(final RelOptCluster cluster, final RelTraitSet traits,
                      final RelOptTable table, final GroupScan groupScan, final RelDataType rowType, final List<SchemaPath> columns, boolean partitionFilterPushdown) {
    super(cluster, traits, groupScan, table);
    this.rowType = rowType;
    this.columns = columns;
    this.settings = PrelUtil.getPlannerSettings(cluster.getPlanner());
    this.partitionFilterPushdown = partitionFilterPushdown;
  }

//
//  private static GroupScan getCopy(GroupScan scan){
//    try {
//      return (GroupScan) scan.getNewWithChildren((List<PhysicalOperator>) (Object) Collections.emptyList());
//    } catch (ExecutionSetupException e) {
//      throw new DrillRuntimeException("Unexpected failure while coping node.", e);
//    }
//  }

  public List<SchemaPath> getColumns() {
    return this.columns;
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    Scan.Builder builder = Scan.builder();
    builder.storageEngine(drillTable.getStorageEngineName());
    builder.selection(new JSONOptions(drillTable.getSelection()));
    implementor.registerSource(drillTable);
    return builder.build();
  }

  public static DrillScanRel convert(Scan scan, ConversionContext context) {
    return new DrillScanRel(context.getCluster(), context.getLogicalTraits(),
        context.getTable(scan));
  }

  @Override
  public RelDataType deriveRowType() {
    return this.rowType;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("groupscan", getGroupScan().getDigest());
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return getGroupScan().getScanStats(settings).getRecordCount();
  }

  /// TODO: this method is same as the one for ScanPrel...eventually we should consolidate
  /// this and few other methods in a common base class which would be extended
  /// by both logical and physical rels.
  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, RelMetadataQuery mq) {
    final ScanStats stats = getGroupScan().getScanStats(settings);
    int columnCount = getRowType().getFieldCount();
    double ioCost = 0;
    boolean isStarQuery = Utilities.isStarQuery(columns);

    if (isStarQuery) {
      columnCount = STAR_COLUMN_COST;
    }

    // double rowCount = RelMetadataQuery.getRowCount(this);
    double rowCount = stats.getRecordCount();
    if (rowCount < 1) {
      rowCount = 1;
    }

    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return planner.getCostFactory().makeCost(rowCount * columnCount, stats.getCpuCost(), stats.getDiskCost());
    }

    double cpuCost = rowCount * columnCount; // for now, assume cpu cost is proportional to row count and number of columns

    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(rowCount, cpuCost, ioCost, 0);
  }

  public boolean partitionFilterPushdown() {
    return this.partitionFilterPushdown;
  }

  private static List<SchemaPath> getProjectedColumns(final RelOptTable table, boolean isSelectStar) {
    List<String> columnNames = table.getRowType().getFieldNames();
    List<SchemaPath> projectedColumns = new ArrayList<SchemaPath>(columnNames.size());

    for (String columnName : columnNames) {
       projectedColumns.add(SchemaPath.getSimplePath(columnName));
    }

    // If the row-type doesn't contain the STAR keyword, then insert it
    // as we are dealing with a  SELECT_STAR query.
    if (isSelectStar && !Utilities.isStarQuery(projectedColumns)) {
      projectedColumns.add(SchemaPath.STAR_COLUMN);
    }

    return projectedColumns;
  }

}
