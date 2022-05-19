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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.exec.physical.impl.join.JoinUtils;

/**
 * Converts join with distinct right input to semi-join.
 */
public class DrillDistinctJoinToSemiJoinRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillDistinctJoinToSemiJoinRule();

  public DrillDistinctJoinToSemiJoinRule() {
    super(RelOptHelper.any(Project.class, Join.class),
      DrillRelFactories.LOGICAL_BUILDER, "DrillDistinctJoinToSemiJoinRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelMetadataQuery mq = call.getMetadataQuery();
    Project project = call.rel(0);
    Join join = call.rel(1);
    ImmutableBitSet bits = RelOptUtil.InputFinder.bits(project.getProjects(), null);
    ImmutableBitSet rightBits = ImmutableBitSet.range(
      join.getLeft().getRowType().getFieldCount(),
      join.getRowType().getFieldCount());
    JoinInfo joinInfo = join.analyzeCondition();
    // can convert to semi-join if all of these are true
    // - non-cartesian join
    // - projecting only columns from left input
    // - join has only equality conditions
    // - all columns in condition from the right input are unique
    return !JoinUtils.checkCartesianJoin(join)
      && !bits.intersects(rightBits)
      && joinInfo.isEqui()
      && SqlFunctions.isTrue(mq.areColumnsUnique(join.getRight(), joinInfo.rightSet()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Project project = call.rel(0);
    Join join = call.rel(1);
    RelBuilder relBuilder = call.builder();
    RelNode relNode = relBuilder.push(join.getLeft())
      .push(join.getRight())
      .semiJoin(join.getCondition())
      .project(project.getProjects())
      .build();
    call.transformTo(relNode);
  }
}
