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

import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.physical.impl.join.JoinUtils;
import org.apache.drill.exec.physical.impl.join.JoinUtils.JoinCategory;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.Lists;

/**
 * Base class for logical and physical Joins implemented in Drill.
 */
public abstract class DrillJoinRelBase extends Join implements DrillRelNode {
  protected List<Integer> leftKeys = Lists.newArrayList();
  protected List<Integer> rightKeys = Lists.newArrayList();

  /**
   * The join key positions for which null values will not match. null values only match for the
   * "is not distinct from" condition.
   */
  protected List<Boolean> filterNulls = Lists.newArrayList();

  private final double joinRowFactor;

  public DrillJoinRelBase(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType){
    super(cluster, traits, left, right, condition, joinType, Collections.<String> emptySet());
    this.joinRowFactor = PrelUtil.getPlannerSettings(cluster.getPlanner()).getRowCountEstimateFactor();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    JoinCategory category = JoinUtils.getJoinCategory(left, right, condition, leftKeys, rightKeys, filterNulls);
    if (category == JoinCategory.CARTESIAN || category == JoinCategory.INEQUALITY) {
      if (PrelUtil.getPlannerSettings(planner).isNestedLoopJoinEnabled()) {
        if (PrelUtil.getPlannerSettings(planner).isNlJoinForScalarOnly()) {
          if (hasScalarSubqueryInput()) {
            return computeLogicalJoinCost(planner);
          } else {
            /*
             *  Why do we return non-infinite cost for CartsianJoin with non-scalar subquery, when LOPT planner is enabled?
             *   - We do not want to turn on the two Join permutation rule : PushJoinPastThroughJoin.LEFT, RIGHT.
             *   - As such, we may end up with filter on top of join, which will cause CanNotPlan in LogicalPlanning, if we
             *   return infinite cost.
             *   - Such filter on top of join might be pushed into JOIN, when LOPT planner is called.
             *   - Return non-infinite cost will give LOPT planner a chance to try to push the filters.
             */
            if (PrelUtil.getPlannerSettings(planner).isHepOptEnabled()) {
             return computeCartesianJoinCost(planner);
            } else {
              return ((DrillCostFactory)planner.getCostFactory()).makeInfiniteCost();
            }
          }
        } else {
          return computeLogicalJoinCost(planner);
        }
      }
      return ((DrillCostFactory)planner.getCostFactory()).makeInfiniteCost();
    }

    return computeLogicalJoinCost(planner);
  }

  @Override
  public double getRows() {
    if (this.condition.isAlwaysTrue()) {
      return joinRowFactor * this.getLeft().getRows() * this.getRight().getRows();
    } else {
      return joinRowFactor * Math.max(this.getLeft().getRows(), this.getRight().getRows());
    }
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

  protected  RelOptCost computeCartesianJoinCost(RelOptPlanner planner) {
    final double probeRowCount = RelMetadataQuery.getRowCount(this.getLeft());
    final double buildRowCount = RelMetadataQuery.getRowCount(this.getRight());

    final DrillCostFactory costFactory = (DrillCostFactory) planner.getCostFactory();

    final double mulFactor = 10000; // This is a magic number,
                                    // just to make sure Cartesian Join is more expensive
                                    // than Non-Cartesian Join.

    final int keySize = 1 ;  // assume having 1 join key, when estimate join cost.
    final DrillCostBase cost = (DrillCostBase) computeHashJoinCostWithKeySize(planner, keySize).multiplyBy(mulFactor);

    // Cartesian join row count will be product of two inputs. The other factors come from the above estimated DrillCost.
    return costFactory.makeCost(
        buildRowCount * probeRowCount,
        cost.getCpu(),
        cost.getIo(),
        cost.getNetwork(),
        cost.getMemory() );

  }

  protected RelOptCost computeLogicalJoinCost(RelOptPlanner planner) {
    // During Logical Planning, although we don't care much about the actual physical join that will
    // be chosen, we do care about which table - bigger or smaller - is chosen as the right input
    // of the join since that is important at least for hash join and we don't currently have
    // hybrid-hash-join that can swap the inputs dynamically.  The Calcite planner's default cost of a join
    // is the same whether the bigger table is used as left input or right. In order to overcome that,
    // we will use the Hash Join cost as the logical cost such that cardinality of left and right inputs
    // is considered appropriately.
    return computeHashJoinCost(planner);
  }

  protected RelOptCost computeHashJoinCost(RelOptPlanner planner) {
      return computeHashJoinCostWithKeySize(planner, this.getLeftKeys().size());
  }

  /**
   *
   * @param planner  : Optimization Planner.
   * @param keySize  : the # of join keys in join condition. Left key size should be equal to right key size.
   * @return         : RelOptCost
   */
  private RelOptCost computeHashJoinCostWithKeySize(RelOptPlanner planner, int keySize) {
    double probeRowCount = RelMetadataQuery.getRowCount(this.getLeft());
    double buildRowCount = RelMetadataQuery.getRowCount(this.getRight());

    // cpu cost of hashing the join keys for the build side
    double cpuCostBuild = DrillCostBase.HASH_CPU_COST * keySize * buildRowCount;
    // cpu cost of hashing the join keys for the probe side
    double cpuCostProbe = DrillCostBase.HASH_CPU_COST * keySize * probeRowCount;

    // cpu cost of evaluating each leftkey=rightkey join condition
    double joinConditionCost = DrillCostBase.COMPARE_CPU_COST * keySize;

    double factor = PrelUtil.getPlannerSettings(planner).getOptions()
        .getOption(ExecConstants.HASH_JOIN_TABLE_FACTOR_KEY).float_val;
    long fieldWidth = PrelUtil.getPlannerSettings(planner).getOptions()
        .getOption(ExecConstants.AVERAGE_FIELD_WIDTH_KEY).num_val;

    // table + hashValues + links
    double memCost =
        (
            (fieldWidth * keySize) +
                IntHolder.WIDTH +
                IntHolder.WIDTH
        ) * buildRowCount * factor;

    double cpuCost = joinConditionCost * (probeRowCount) // probe size determine the join condition comparison cost
        + cpuCostBuild + cpuCostProbe ;

    DrillCostFactory costFactory = (DrillCostFactory) planner.getCostFactory();

    return costFactory.makeCost(buildRowCount + probeRowCount, cpuCost, 0, 0, memCost);
  }

  private boolean hasScalarSubqueryInput() {
    if (JoinUtils.isScalarSubquery(this.getLeft())
        || JoinUtils.isScalarSubquery(this.getRight())) {
      return true;
    }

    return false;
  }

}
