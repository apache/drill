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
import java.util.List;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.torel.ConversionContext;
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
  final private RelDataType rowType;
  private GroupScan groupScan;

  /** Creates a DrillScan. */
  public DrillScanRel(RelOptCluster cluster, RelTraitSet traits,
      RelOptTable table) {
    // By default, scan does not support project pushdown.
    // Decision whether push projects into scan will be made solely in DrillPushProjIntoScanRule.
    this(cluster, traits, table, table.getRowType(), null);
  }

  /** Creates a DrillScan. */
  public DrillScanRel(RelOptCluster cluster, RelTraitSet traits,
      RelOptTable table, RelDataType rowType, List<SchemaPath> columns) {
    super(DRILL_LOGICAL, cluster, traits, table);
    this.rowType = rowType;

    try {
      if (columns == null || columns.isEmpty()) {
        this.groupScan = this.drillTable.getGroupScan();
      } else {
        this.groupScan = this.drillTable.getGroupScan().clone(columns);
      }
    } catch (IOException e) {
      this.groupScan = null;
      e.printStackTrace();
    }

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
  public double getRows() {
    return this.groupScan.getSize().getRecordCount();
  }

  /// TODO: this method is same as the one for ScanPrel...eventually we should consolidate
  /// this and few other methods in a common base class which would be extended
  /// by both logical and physical rels.
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    Size scanSize = this.groupScan.getSize();
    int columnCount = this.getRowType().getFieldCount();   
    
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      OperatorCost scanCost = this.groupScan.getCost();
      return planner.getCostFactory().makeCost(scanSize.getRecordCount() * columnCount, scanCost.getCpu(), scanCost.getDisk());
    }
    
    // double rowCount = RelMetadataQuery.getRowCount(this);
    double rowCount = scanSize.getRecordCount();
    
    double cpuCost = rowCount * columnCount; // for now, assume cpu cost is proportional to row count. 
    // Even though scan is reading from disk, in the currently generated plans all plans will
    // need to read the same amount of data, so keeping the disk io cost 0 is ok for now.  
    // In the future we might consider alternative scans that go against projections or 
    // different compression schemes etc that affect the amount of data read. Such alternatives
    // would affect both cpu and io cost. 
    double ioCost = 0;
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(rowCount, cpuCost, ioCost, 0);   
  }  
  
  public GroupScan getGroupScan() {
    return groupScan;
  }

}
