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

import org.apache.drill.exec.planner.common.DrillUnionRelBase;
import org.eigenbase.rel.UnionRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that converts a {@link UnionRel} to a {@link DrillUnionRelBase}, implemented by a "union" operation.
 */
public class DrillUnionRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillUnionRule();

  private DrillUnionRule() {
    super(RelOptHelper.any(UnionRel.class, Convention.NONE), "DrillUnionRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final UnionRel union = (UnionRel) call.rel(0);
    final RelTraitSet traits = union.getTraitSet().plus(DrillRel.DRILL_LOGICAL);
    final List<RelNode> convertedInputs = new ArrayList<>();
    for (RelNode input : union.getInputs()) {
      final RelNode convertedInput = convert(input, traits);
      convertedInputs.add(convertedInput);
    }
    call.transformTo(new DrillUnionRel(union.getCluster(), traits, convertedInputs, union.all));
  }
}
