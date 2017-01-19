/*******************************************************************************
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
 ******************************************************************************/

package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Exchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.BroadcastExchange;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.planner.common.DrillJoinRelBase;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class BroadcastExchangePrel extends ExchangePrel{

  public BroadcastExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet, input);
    assert input.getConvention() == Prel.DRILL_PHYSICAL;
  }

  /**
   * In a BroadcastExchange, each sender is sending data to N receivers (for costing
   * purposes we assume it is also sending to itself).
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    RelNode child = this.getInput();

    int numEndPoints = PrelUtil.getSettings(getCluster()).numEndPoints();
    final double broadcastFactor = PrelUtil.getSettings(getCluster()).getBroadcastFactor();
    final double inputRows = RelMetadataQuery.getRowCount(child);
    if (!DrillRelOptUtil.guessRows(this)
        && PrelUtil.getSettings(getCluster()).useNewBroadcastEstimate()) {
      RelNode root = planner.getRoot();
      RelNode parent = getParent(root, null, child);
      if (parent instanceof DrillJoinRelBase) {
        double leftRows, rightRows;
        if (child instanceof RelSubset) {
          if (findNode(((DrillJoinRelBase) parent).getLeft(), child)) {
            leftRows = RelMetadataQuery.getRowCount(((DrillJoinRelBase) parent).getLeft());
            rightRows = RelMetadataQuery.getRowCount(((DrillJoinRelBase) parent).getRight());
            if (Math.min(leftRows, rightRows) > 100000) {
              numEndPoints = (int) (Math.max(1.0, Math.round(rightRows / leftRows)));
            }
          } else if (findNode(((DrillJoinRelBase) parent).getRight(), child)) {
            leftRows = RelMetadataQuery.getRowCount(((DrillJoinRelBase) parent).getLeft());
            rightRows = RelMetadataQuery.getRowCount(((DrillJoinRelBase) parent).getRight());
            if (Math.min(leftRows, rightRows) > 100000) {
              numEndPoints = (int) (Math.max(1.0, Math.round(leftRows / rightRows)));
            }
          }
        }
      }
    }
    final int  rowWidth = child.getRowType().getFieldCount() * DrillCostBase.AVG_FIELD_WIDTH;
    final double cpuCost = broadcastFactor * DrillCostBase.SVR_CPU_COST * inputRows;
    final double networkCost = broadcastFactor * DrillCostBase.BYTE_NETWORK_COST * inputRows * rowWidth * numEndPoints;

    return new DrillCostBase(inputRows, cpuCost, 0, networkCost);
  }

  private boolean findNode(RelNode node, RelNode target) {
    if (node == target
            || (target instanceof RelSubset
            && ((((RelSubset) target).getOriginal() == node)
            || ((RelSubset) target).getBest() == node))) {
      return true;
    }
    if (node instanceof RelSubset) {
      if (((RelSubset) node).getBest() != null) {
        return findNode(((RelSubset) node).getBest(), target);
      } else if (((RelSubset) node).getOriginal() != null) {
        return findNode(((RelSubset) node).getOriginal(), target);
      }
    } else {
      for (RelNode child : node.getInputs()) {
        if (findNode(child, target)) {
          return true;
        }
      }
    }
    return false;
  }
  private RelNode getParent(RelNode node, RelNode parent, RelNode target) {
    if (node == target
        || (target instanceof RelSubset
            && ((((RelSubset) target).getOriginal() == node)
                || ((RelSubset) target).getBest() == node))) {
      return parent;
    }
    // Skip subset/single rels when determining parent node
    if (node instanceof RelSubset) {
      if (((RelSubset) node).getBest() != null) {
        return getParent(((RelSubset) node).getBest(), parent, target);
      } else if (((RelSubset) node).getOriginal() != null) {
        return getParent(((RelSubset) node).getOriginal(), parent, target);
      }
    } else if (node instanceof SingleRel) {
      return getParent(((SingleRel) node).getInput(), parent, target);
    } else {
      for (RelNode child : node.getInputs()) {
        RelNode par = getParent(child, node, target);
        if (par != null) {
          return par;
        }
      }
    }
    return null;
  }
  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BroadcastExchangePrel(getCluster(), traitSet, sole(inputs));
  }

  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    //Currently, only accepts "NONE". For other, requires SelectionVectorRemover
    if (!childPOP.getSVMode().equals(SelectionVectorMode.NONE)) {
      childPOP = new SelectionVectorRemover(childPOP);
    }

    BroadcastExchange g = new BroadcastExchange(childPOP);
    return creator.addMetadata(this, g);
  }

}
