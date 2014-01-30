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

import org.eigenbase.rel.SortRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;

/**
 * Rule that converts an {@link SortRel} to a {@link DrillSortRel}, implemented by a Drill "order" operation.
 */
public class DrillSortRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillSortRule();

  private DrillSortRule() {
    super(RelOptHelper.some(SortRel.class, Convention.NONE, RelOptHelper.any(RelNode.class)), "DrillSortRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final SortRel sort = call.rel(0);
    return sort.offset == null && sort.fetch == null;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {

    final SortRel sort = call.rel(0);

    final RelNode input = call.rel(1);
    final RelTraitSet traits = sort.getTraitSet().plus(DrillRel.CONVENTION);

    final RelNode convertedInput = convert(input, input.getTraitSet().plus(DrillRel.CONVENTION));
    call.transformTo(new DrillSortRel(sort.getCluster(), traits, convertedInput, sort.getCollation()));
  }
}
