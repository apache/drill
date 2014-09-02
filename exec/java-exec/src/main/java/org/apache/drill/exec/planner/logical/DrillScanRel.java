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
package org.apache.drill.exec.planner.logical;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;

/**
 * GroupScan of a Drill table.
 */
public class DrillScanRel extends DrillScanRelBase implements DrillRel {
  private final static int STAR_COLUMN_COST = 10000;

  final private RelDataType rowType;
  private GroupScan groupScan;

  /** Creates a DrillScan. */
  public DrillScanRel(RelOptCluster cluster, RelTraitSet traits,
      RelOptTable table) {
    // By default, scan does not support project pushdown.
    // Decision whether push projects into scan will be made solely in DrillPushProjIntoScanRule.
    this(cluster, traits, table, table.getRowType(), AbstractGroupScan.ALL_COLUMNS);
  }

  /** Creates a DrillScan. */
  public DrillScanRel(RelOptCluster cluster, RelTraitSet traits,
      RelOptTable table, RelDataType rowType, List<SchemaPath> columns) {
    super(DRILL_LOGICAL, cluster, traits, table);
    this.rowType = rowType;
    columns = columns == null || columns.size() == 0 ? GroupScan.ALL_COLUMNS : columns;
    try {
      this.groupScan = drillTable.getGroupScan().clone(columns);
    } catch (IOException e) {
      throw new DrillRuntimeException("Failure creating scan.", e);
    }
  }
//
//  private static GroupScan getCopy(GroupScan scan){
//    try {
//      return (GroupScan) scan.getNewWithChildren((List<PhysicalOperator>) (Object) Collections.emptyList());
//    } catch (ExecutionSetupException e) {
//      throw new DrillRuntimeException("Unexpected failure while coping node.", e);
//    }
//  }


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
    return super.explainTerms(pw).item("groupscan", groupScan.getDigest());
  }

  @Override
  public double getRows() {
    return this.groupScan.getScanStats().getRecordCount();
  }

  /// TODO: this method is same as the one for ScanPrel...eventually we should consolidate
  /// this and few other methods in a common base class which would be extended
  /// by both logical and physical rels.
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    ScanStats stats = groupScan.getScanStats();
    int columnCount = getRowType().getFieldCount();
    double ioCost = 0;
    boolean isStarQuery = Iterables.tryFind(getRowType().getFieldNames(), new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return Preconditions.checkNotNull(input).equals("*");
      }
    }).isPresent();

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

    double cpuCost = rowCount * columnCount; // for now, assume cpu cost is proportional to row count.
    // Even though scan is reading from disk, in the currently generated plans all plans will
    // need to read the same amount of data, so keeping the disk io cost 0 is ok for now.
    // In the future we might consider alternative scans that go against projections or
    // different compression schemes etc that affect the amount of data read. Such alternatives
    // would affect both cpu and io cost.

    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(rowCount, cpuCost, ioCost, 0);
  }

  public GroupScan getGroupScan() {
    return groupScan;
  }

}
