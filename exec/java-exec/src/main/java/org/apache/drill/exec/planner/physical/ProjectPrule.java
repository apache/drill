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

import org.apache.drill.exec.planner.common.BaseProjectRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

public class ProjectPrule extends RelOptRule {
  public static final RelOptRule INSTANCE = new ProjectPrule();

  private ProjectPrule() {
    super(RelOptHelper.some(BaseProjectRel.class, DrillRel.DRILL_LOGICAL, RelOptHelper.any(RelNode.class)), "ProjectPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final BaseProjectRel project = (BaseProjectRel) call.rel(0);
    final RelNode input = call.rel(1);
    final RelTraitSet traits = project.getTraitSet().replace(Prel.DRILL_PHYSICAL);
    final RelNode convertedInput = convert(input, traits);
    call.transformTo(new ProjectPrel(project.getCluster(), traits, convertedInput, project.getProjects(), project.getRowType()));
  }
}
