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
package org.apache.drill.exec.planner.common;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

import com.google.common.collect.Lists;

/**
 * Base class for logical and physical Joins implemented in Drill.
 */
public abstract class DrillJoinRelBase extends JoinRelBase implements DrillRelNode {
  protected List<Integer> leftKeys = Lists.newArrayList();
  protected List<Integer> rightKeys = Lists.newArrayList() ;
  private final double joinRowFactor;

  public DrillJoinRelBase(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType) throws InvalidRelException {
    super(cluster, traits, left, right, condition, joinType, Collections.<String> emptySet());
    this.joinRowFactor = PrelUtil.getPlannerSettings(cluster.getPlanner()).getRowCountEstimateFactor();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if(condition.isAlwaysTrue()){
      return ((DrillCostFactory)planner.getCostFactory()).makeInfiniteCost();
    }
    return super.computeSelfCost(planner);
  }

  @Override
  public double getRows() {
    return joinRowFactor * Math.max(this.getLeft().getRows(), this.getRight().getRows());
  }

  /**
   * Returns whether there are any elements in common between left and right.
   */
  private static <T> boolean intersects(List<T> left, List<T> right) {
    return new HashSet<>(left).removeAll(right);
  }

  protected boolean uniqueFieldNames(RelDataType rowType) {
    return isUnique(rowType.getFieldNames());
  }

  protected static <T> boolean isUnique(List<T> list) {
    return new HashSet<>(list).size() == list.size();
  }

  public List<Integer> getLeftKeys() {
    return this.leftKeys;
  }

  public List<Integer> getRightKeys() {
    return this.rightKeys;
  }

}
