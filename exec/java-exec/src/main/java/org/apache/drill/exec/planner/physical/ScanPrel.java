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
package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.eigenbase.rel.AbstractRelNode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;

public class ScanPrel extends AbstractRelNode implements DrillScanPrel {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(ScanPrel.class);

  protected final GroupScan groupScan;
  private final RelDataType rowType;

  public ScanPrel(RelOptCluster cluster, RelTraitSet traits,
      GroupScan groupScan, RelDataType rowType) {
    super(cluster, traits);
    this.groupScan = groupScan;
    this.rowType = rowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ScanPrel(this.getCluster(), traitSet, this.groupScan,
        this.rowType);
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return new ScanPrel(this.getCluster(), this.getTraitSet(), this.groupScan,
        this.rowType);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator)
      throws IOException {
    return groupScan;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    OperatorCost scanCost = this.groupScan.getCost();
    Size scanSize = this.groupScan.getSize();
    int columnCount = this.getRowType().getFieldCount();

    // FIXME: Use the new cost model
    return this
        .getCluster()
        .getPlanner()
        .getCostFactory()
        .makeCost(scanSize.getRecordCount() * columnCount, scanCost.getCpu(),
            scanCost.getNetwork() * scanCost.getDisk());
  }

  @Override
  public GroupScan getGroupScan() {
    return groupScan;
  }

  public static ScanPrel create(RelNode old, RelTraitSet traitSets,
      GroupScan scan, RelDataType rowType) {
    return new ScanPrel(old.getCluster(), traitSets, scan, rowType);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("groupscan", groupScan.getDigest());
  }

  @Override
  public RelDataType deriveRowType() {
    return this.rowType;
  }

}
