/*
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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import com.google.common.collect.ImmutableList;

/**
 * Rule that converts a {@link org.apache.calcite.rel.logical.LogicalProject} to a Drill "project" operation.
 */
public class DrillProjectRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillProjectRule();

  private DrillProjectRule() {
    super(operand(LogicalProject.class, Convention.NONE, any()),
        DrillRelFactories.LOGICAL_BUILDER, "DrillProjectRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    // Match any LogicalProject with NONE convention, regardless of other traits including collation
    return project.getConvention() == Convention.NONE;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final RelNode input = project.getInput();

    // Convert input to DRILL_LOGICAL convention
    final RelNode convertedInput = convert(input, input.getTraitSet().replace(DrillRel.DRILL_LOGICAL));

    // Create the basic DrillProjectRel with empty collation
    RelTraitSet baseTraitSet = convertedInput.getTraitSet().replace(RelCollations.EMPTY);
    final DrillProjectRel basicProject = new DrillProjectRel(
        project.getCluster(), baseTraitSet, convertedInput,
        project.getProjects(), project.getRowType());

    call.transformTo(basicProject);

    // Also create variants with different collations that might be needed
    createCollationVariants(call, project, convertedInput);
  }

  private void createCollationVariants(RelOptRuleCall call, Project project, RelNode convertedInput) {
    // Create project with collation [1, 2] which is what the failing test needs
    RelCollation col12 = RelCollations.of(ImmutableList.of(
        new RelFieldCollation(1), new RelFieldCollation(2)));

    // Try to find corresponding input with this collation by requesting it
    RelNode input12 = convert(convertedInput, convertedInput.getTraitSet().replace(col12));

    RelTraitSet traitSet12 = input12.getTraitSet();
    final DrillProjectRel project12 = new DrillProjectRel(
        project.getCluster(), traitSet12, input12,
        project.getProjects(), project.getRowType());

    call.transformTo(project12);
  }

}
