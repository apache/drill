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
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.planner.common.DrillJoinRelBase;
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

import com.beust.jcommander.internal.Lists;

public class HashJoinPrel  extends DrillJoinRelBase implements Prel {

  public HashJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType) throws InvalidRelException {
    super(cluster, traits, left, right, condition, joinType);

    RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys);
  }


  @Override
  public JoinRelBase copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType) {
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

    PhysicalOperator leftPop = implementInput(creator, 0, left);
    PhysicalOperator rightPop = implementInput(creator, leftCount, right);

    //Currently, only accepts "NONE" or "SV2". For other, requires SelectionVectorRemover
    leftPop = PrelUtil.removeSvIfRequired(leftPop, SelectionVectorMode.NONE, SelectionVectorMode.TWO_BYTE);

    //Currently, only accepts "NONE" or "SV2". For other, requires SelectionVectorRemover
    rightPop = PrelUtil.removeSvIfRequired(rightPop, SelectionVectorMode.NONE, SelectionVectorMode.TWO_BYTE);

    JoinRelType jtype = this.getJoinType();

    List<JoinCondition> conditions = Lists.newArrayList();

    for (Pair<Integer, Integer> pair : Pair.zip(leftKeys, rightKeys)) {
      conditions.add(new JoinCondition("==", new FieldReference(leftFields.get(pair.left)), new FieldReference(rightFields.get(pair.right))));
    }

    HashJoinPOP hjoin = new HashJoinPOP(leftPop, rightPop, conditions, jtype);

    return hjoin;
  }

  public List<Integer> getLeftKeys() {
    return this.leftKeys;
  }

  public List<Integer> getRightKeys() {
    return this.rightKeys;
  }

  /**
   * Check to make sure that the fields of the inputs are the same as the output field names.  If not, insert a project renaming them.
   * @param implementor
   * @param i
   * @param offset
   * @param input
   * @return
   */
  private PhysicalOperator implementInput(PhysicalPlanCreator creator, int offset, RelNode input) throws IOException {
    final PhysicalOperator inputOp = ((Prel) input).getPhysicalOperator(creator);
    assert uniqueFieldNames(input.getRowType());
    final List<String> fields = getRowType().getFieldNames();
    final List<String> inputFields = input.getRowType().getFieldNames();
    final List<String> outputFields = fields.subList(offset, offset + inputFields.size());
    if (!outputFields.equals(inputFields)) {
      // Ensure that input field names are the same as output field names.
      // If there are duplicate field names on left and right, fields will get
      // lost.
      return rename(creator, inputOp, inputFields, outputFields);
    } else {
      return inputOp;
    }
  }

  private PhysicalOperator rename(PhysicalPlanCreator creator, PhysicalOperator inputOp, List<String> inputFields, List<String> outputFields) {
    List<NamedExpression> exprs = Lists.newArrayList();

    //Currently, Project only accepts "NONE". For other, requires SelectionVectorRemover
    inputOp = PrelUtil.removeSvIfRequired(inputOp, SelectionVectorMode.NONE);

    for (Pair<String, String> pair : Pair.zip(inputFields, outputFields)) {
      exprs.add(new NamedExpression(new FieldReference(pair.left), new FieldReference(pair.right)));
    }

    Project proj = new Project(exprs, inputOp);

    return proj;
  }


}
