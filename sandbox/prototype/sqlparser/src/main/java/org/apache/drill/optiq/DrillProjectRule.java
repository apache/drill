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
package org.apache.drill.optiq;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

/**
 * Rule that converts a {@link org.eigenbase.rel.ProjectRel} to a Drill
 * "project" operation.
 */
public class DrillProjectRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillProjectRule();

  private DrillProjectRule() {
    super(
        new RelOptRuleOperand(
            ProjectRel.class,
            Convention.NONE,
            new RelOptRuleOperand(RelNode.class, ANY)),
        "DrillProjectRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectRel project = (ProjectRel) call.getRels()[0];
    final RelNode input = call.getRels()[1];
    final RelTraitSet traits = project.getTraitSet().plus(DrillRel.CONVENTION);
    final RelNode convertedInput = convert(input, traits);
    call.transformTo(
        new DrillProjectRel(project.getCluster(), traits, convertedInput,
            project.getProjectExps(), project.getRowType()));
  }
}

// End DrillProjectRule.java
