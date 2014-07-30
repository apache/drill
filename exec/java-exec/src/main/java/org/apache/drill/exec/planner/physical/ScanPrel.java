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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
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
    this.groupScan = getCopy(groupScan);
    this.rowType = rowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ScanPrel(this.getCluster(), traitSet, groupScan,
        this.rowType);
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return new ScanPrel(this.getCluster(), this.getTraitSet(), getCopy(groupScan),
        this.rowType);
  }

  private static GroupScan getCopy(GroupScan scan){
    try {
      return (GroupScan) scan.getNewWithChildren((List<PhysicalOperator>) (Object) Collections.emptyList());
    } catch (ExecutionSetupException e) {
      throw new DrillRuntimeException("Unexpected failure while coping node.", e);
    }
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator)
      throws IOException {
    return creator.addMetadata(this, groupScan);
  }

  @Override
  public GroupScan getGroupScan() {
    return groupScan;
  }

  public static ScanPrel create(RelNode old, RelTraitSet traitSets,
      GroupScan scan, RelDataType rowType) {
    return new ScanPrel(old.getCluster(), traitSets, getCopy(scan), rowType);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("groupscan", groupScan.getDigest());
  }

  @Override
  public RelDataType deriveRowType() {
    return this.rowType;
  }

  @Override
  public double getRows() {
    return this.groupScan.getScanStats().getRecordCount();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    ScanStats stats = this.groupScan.getScanStats();
    int columnCount = this.getRowType().getFieldCount();

    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return planner.getCostFactory().makeCost(stats.getRecordCount() * columnCount, stats.getCpuCost(), stats.getDiskCost());
    }

    // double rowCount = RelMetadataQuery.getRowCount(this);
    double rowCount = stats.getRecordCount();

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

  @Override
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitScan(this, value);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }
}
