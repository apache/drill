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

import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.trace.EigenbaseTrace;

import java.util.logging.Logger;

/**
 * Rule that converts a {@link JoinRel} to a {@link DrillJoinRel}, which is implemented by Drill "join" operation.
 */
public class DrillJoinRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillJoinRule();
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private DrillJoinRule() {
    super(
        RelOptHelper.some(JoinRel.class, Convention.NONE, RelOptHelper.any(RelNode.class), RelOptHelper.any(RelNode.class)),
        "DrillJoinRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final JoinRel join = (JoinRel) call.rel(0);
    final RelNode left = call.rel(1);
    final RelNode right = call.rel(2);
    final RelTraitSet traits = join.getTraitSet().plus(DrillRel.CONVENTION);

    final RelNode convertedLeft = convert(left, traits);
    final RelNode convertedRight = convert(right, traits);
    try {
      call.transformTo(new DrillJoinRel(join.getCluster(), traits, convertedLeft, convertedRight, join.getCondition(),
          join.getJoinType()));
    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }
  }
}
