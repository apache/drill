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

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Util;

public class DrillPushFilterPastProjectRule extends RelOptRule {

  public final static RelOptRule INSTANCE = new DrillPushFilterPastProjectRule();

  private RexCall findItemOrFlatten(
      final RexNode node,
      final List<RexNode> projExprs) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
        public Void visitCall(RexCall call) {
          if ("item".equals(call.getOperator().getName().toLowerCase()) ||
            "flatten".equals(call.getOperator().getName().toLowerCase())) {
            throw new Util.FoundOne(call); /* throw exception to interrupt tree walk (this is similar to
                                              other utility methods in RexUtil.java */
          }
          return super.visitCall(call);
        }

        public Void visitInputRef(RexInputRef inputRef) {
          final int index = inputRef.getIndex();
          RexNode n = projExprs.get(index);
          if (n instanceof RexCall) {
            RexCall r = (RexCall) n;
            if ("item".equals(r.getOperator().getName().toLowerCase()) ||
                "flatten".equals(r.getOperator().getName().toLowerCase())) {
              throw new Util.FoundOne(r);
            }
          }

          return super.visitInputRef(inputRef);
        }
      };
      node.accept(visitor);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (RexCall) e.getNode();
    }
  }

  protected DrillPushFilterPastProjectRule() {
    super(
        operand(
            LogicalFilter.class,
            operand(LogicalProject.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    Filter filterRel = call.rel(0);
    Project projRel = call.rel(1);

    // get a conjunctions of the filter condition. For each conjunction, if it refers to ITEM or FLATTEN expression
    // then we could not pushed down. Otherwise, it's qualified to be pushed down.
    final List<RexNode> predList = RelOptUtil.conjunctions(filterRel.getCondition());

    final List<RexNode> qualifiedPredList = Lists.newArrayList();
    final List<RexNode> unqualifiedPredList = Lists.newArrayList();


    for (final RexNode pred : predList) {
      if (findItemOrFlatten(pred, projRel.getProjects()) == null) {
        qualifiedPredList.add(pred);
      } else {
        unqualifiedPredList.add(pred);
      }
    }

    final RexNode qualifedPred =RexUtil.composeConjunction(filterRel.getCluster().getRexBuilder(), qualifiedPredList, true);

    if (qualifedPred == null) {
      return;
    }

    // convert the filter to one that references the child of the project
    RexNode newCondition =
        RelOptUtil.pushFilterPastProject(qualifedPred, projRel);

    Filter newFilterRel = LogicalFilter.create(projRel.getInput(), newCondition);

    Project newProjRel =
        (Project) RelOptUtil.createProject(
            newFilterRel,
            projRel.getNamedProjects(),
            false);

    final RexNode unqualifiedPred = RexUtil.composeConjunction(filterRel.getCluster().getRexBuilder(), unqualifiedPredList, true);

    if (unqualifiedPred == null) {
      call.transformTo(newProjRel);
    } else {
      // if there are filters not qualified to be pushed down, then we have to put those filters on top of
      // the new Project operator.
      // Filter -- unqualified filters
      //   \
      //    Project
      //     \
      //      Filter  -- qualified filters
      Filter filterNotPushed = LogicalFilter.create(newProjRel, unqualifiedPred);
      call.transformTo(filterNotPushed);
    }
  }

}
