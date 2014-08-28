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

import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.rules.PushProjector;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexOver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DrillPushProjectPastFilterRule extends RelOptRule {

  private final static Logger logger = LoggerFactory.getLogger(DrillPushProjectPastFilterRule.class);

  public final static RelOptRule INSTANCE = new DrillPushProjectPastFilterRule(new PushProjector.ExprCondition() {
    @Override
    public boolean test(RexNode expr) {
      if (expr instanceof RexCall) {
        RexCall call = (RexCall)expr;
        return "ITEM".equals(call.getOperator().getName());
      }
      return false;
    }
  });

  /**
   * Expressions that should be preserved in the projection
   */
  private final PushProjector.ExprCondition preserveExprCondition;

  private DrillPushProjectPastFilterRule(PushProjector.ExprCondition preserveExprCondition) {
    super(RelOptHelper.any(ProjectRel.class, FilterRel.class));
    this.preserveExprCondition = preserveExprCondition;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    ProjectRel origProj;
    FilterRel filterRel;

    if (call.rels.length == 2) {
      origProj = call.rel(0);
      filterRel = call.rel(1);
    } else {
      origProj = null;
      filterRel = call.rel(0);
    }
    RelNode rel = filterRel.getChild();
    RexNode origFilter = filterRel.getCondition();

    if ((origProj != null) && RexOver.containsOver(origProj.getProjects(), null)) {
      // Cannot push project through filter if project contains a windowed
      // aggregate -- it will affect row counts. Abort this rule
      // invocation; pushdown will be considered after the windowed
      // aggregate has been implemented. It's OK if the filter contains a
      // windowed aggregate.
      return;
    }

    PushProjector pushProjector = createPushProjector(origProj, origFilter, rel, preserveExprCondition);
    RelNode topProject = pushProjector.convertProject(null);

    if (topProject != null) {
      call.transformTo(topProject);
    }
  }

  protected PushProjector createPushProjector(ProjectRel origProj, RexNode origFilter, RelNode rel,
                                              PushProjector.ExprCondition preserveExprCondition) {
    return new PushProjector(origProj, origFilter,rel, preserveExprCondition);
  }

}
