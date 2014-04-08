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
import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

public class ScanPrel extends DrillScanRelBase implements Prel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanPrel.class);

  protected final GroupScan scan;

  private ScanPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable tbl, GroupScan scan) {
    super(DRILL_PHYSICAL, cluster, traits, tbl);
    this.scan = scan;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ScanPrel(this.getCluster(), traitSet, this.getTable(), this.scan);
  }


  @Override
  protected Object clone() throws CloneNotSupportedException {
    return new ScanPrel(this.getCluster(), this.getTraitSet(), this.getTable(), this.scan);
  }


  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return scan;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    OperatorCost scanCost = this.scan.getCost();
    Size scanSize = this.scan.getSize();
    // FIXME: Use the new cost model
    return this.getCluster().getPlanner().getCostFactory()
        .makeCost(scanSize.getRecordCount(),
            scanCost.getCpu(),
            scanCost.getNetwork() * scanCost.getDisk()); 
  }

  public GroupScan getGroupScan() {
    return scan;
  }

  public static ScanPrel create(DrillScanRelBase old, RelTraitSet traitSets, GroupScan scan) {
    return new ScanPrel(old.getCluster(), traitSets, old.getTable(), scan);
  }


  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("groupscan", scan.getDigest());
  }

}
