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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.planner.common.DrillJoinRelBase;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Pair;

import com.beust.jcommander.internal.Lists;

public class MergeJoinPrel  extends DrillJoinRelBase implements Prel {

  //private final JoinCondition[] joinConditions; // Drill's representation of join conditions
  
  /** Creates a MergeJoiPrel. */
  public MergeJoinPrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType) throws InvalidRelException {
    super(cluster, traits, left, right, condition, joinType);

    RexNode remaining = RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys);
    if (!remaining.isAlwaysTrue()) {
      throw new InvalidRelException("MergeJoinPrel only supports equi-join");
    }
    //this.joinConditions = joinConditions;
  }

  
  @Override 
  public JoinRelBase copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType) {
    try {
      return new MergeJoinPrel(this.getCluster(), traitSet, left, right, conditionExpr, joinType);
    }catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override  
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {    
    PhysicalOperator leftPop = ((Prel) getLeft()).getPhysicalOperator(creator);

    //Currently, only accepts "NONE" or "SV2". For other, requires SelectionVectorRemover
    if (leftPop.getSVMode().equals(SelectionVectorMode.FOUR_BYTE)) {
      leftPop = new SelectionVectorRemover(leftPop);
      creator.addPhysicalOperator(leftPop);
    }

    PhysicalOperator rightPop = ((Prel) getRight()).getPhysicalOperator(creator);

    //Currently, only accepts "NONE" or "SV2". For other, requires SelectionVectorRemover
    if (rightPop.getSVMode().equals(SelectionVectorMode.FOUR_BYTE)) {
      rightPop = new SelectionVectorRemover(rightPop);
      creator.addPhysicalOperator(rightPop);
    }
    
    JoinRelType jtype = this.getJoinType();
    
    final List<String> fields = getRowType().getFieldNames();
    assert isUnique(fields);
    final int leftCount = left.getRowType().getFieldCount();
    final List<String> leftFields = fields.subList(0, leftCount);
    final List<String> rightFields = fields.subList(leftCount, fields.size());
    
    List<JoinCondition> conditions = Lists.newArrayList();
    
    for (Pair<Integer, Integer> pair : Pair.zip(leftKeys, rightKeys)) {
      conditions.add(new JoinCondition("==", new FieldReference(leftFields.get(pair.left)), new FieldReference(rightFields.get(pair.right))));
    }
    MergeJoinPOP mjoin = new MergeJoinPOP(leftPop, rightPop, conditions, jtype);
    creator.addPhysicalOperator(mjoin);
   
    return mjoin;
  }

  public List<Integer> getLeftKeys() {
    return this.leftKeys;
  }
  
  public List<Integer> getRightKeys() {
    return this.rightKeys;
  }
  
//  public JoinCondition[] getJoinConditions() {
//    return joinConditions;
//  }
}
