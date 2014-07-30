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

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Pair;

import com.google.common.collect.Lists;

public class HashJoinPrel  extends JoinPrel {

  public HashJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType) throws InvalidRelException {
    super(cluster, traits, left, right, condition, joinType);

    RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys);
  }


  @Override
  public JoinRelBase copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    try {
      return new HashJoinPrel(this.getCluster(), traitSet, left, right, conditionExpr, joinType);
    }catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    double probeRowCount = RelMetadataQuery.getRowCount(this.getLeft());
    double buildRowCount = RelMetadataQuery.getRowCount(this.getRight());

    // cpu cost of hashing the join keys for the build side
    double cpuCostBuild = DrillCostBase.HASH_CPU_COST * getRightKeys().size() * buildRowCount;
    // cpu cost of hashing the join keys for the probe side
    double cpuCostProbe = DrillCostBase.HASH_CPU_COST * getLeftKeys().size() * probeRowCount;

    // cpu cost of evaluating each leftkey=rightkey join condition
    double joinConditionCost = DrillCostBase.COMPARE_CPU_COST * this.getLeftKeys().size();

    double cpuCost = joinConditionCost * (buildRowCount + probeRowCount) + cpuCostBuild + cpuCostProbe;
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(buildRowCount + probeRowCount, cpuCost, 0, 0);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    final List<String> fields = getRowType().getFieldNames();
    assert isUnique(fields);
    final int leftCount = left.getRowType().getFieldCount();
    final List<String> leftFields = fields.subList(0, leftCount);
    final List<String> rightFields = fields.subList(leftCount, fields.size());

    PhysicalOperator leftPop = ((Prel)left).getPhysicalOperator(creator);
    PhysicalOperator rightPop = ((Prel)right).getPhysicalOperator(creator);

    JoinRelType jtype = this.getJoinType();

    List<JoinCondition> conditions = Lists.newArrayList();

    for (Pair<Integer, Integer> pair : Pair.zip(leftKeys, rightKeys)) {
      conditions.add(new JoinCondition("==", new FieldReference(leftFields.get(pair.left)), new FieldReference(rightFields.get(pair.right))));
    }

    HashJoinPOP hjoin = new HashJoinPOP(leftPop, rightPop, conditions, jtype);
    return creator.addMetadata(this, hjoin);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

}
