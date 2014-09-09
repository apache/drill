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


import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelFactories.ProjectFactory;
import org.eigenbase.rel.rules.MergeProjectRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexNode;

public class DrillMergeProjectRule extends MergeProjectRule {

  private FunctionImplementationRegistry functionRegistry;
  private static DrillMergeProjectRule INSTANCE = null;

  public static DrillMergeProjectRule getInstance(boolean force, ProjectFactory pFactory, FunctionImplementationRegistry functionRegistry) {
    if (INSTANCE == null) {
      INSTANCE = new DrillMergeProjectRule(force, pFactory, functionRegistry);
    }
    return INSTANCE;
  }

  private DrillMergeProjectRule(boolean force, ProjectFactory pFactory, FunctionImplementationRegistry functionRegistry) {
    super(force, pFactory);
   this.functionRegistry = functionRegistry;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    ProjectRelBase topProject = call.rel(0);
    ProjectRelBase bottomProject = call.rel(1);

    // We have a complex output type do not fire the merge project rule
    if (checkComplexOutput(topProject) || checkComplexOutput(bottomProject)) {
      return false;
    }

    return true;
  }

  private boolean checkComplexOutput(ProjectRelBase project) {
    for (RexNode expr: project.getChildExps()) {
      if (expr instanceof RexCall) {
        if (functionRegistry.isFunctionComplexOutput(((RexCall) expr).getOperator().getName())) {
          return true;
        }
      }
    }
    return false;
  }
}
