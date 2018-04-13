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
package org.apache.drill.exec.planner.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.exec.planner.logical.RelOptHelper;

import java.math.BigDecimal;

public class LimitUnionExchangeTransposeRule extends Prule{
  public static final RelOptRule INSTANCE = new LimitUnionExchangeTransposeRule();

  private LimitUnionExchangeTransposeRule() {
    super(RelOptHelper.some(LimitPrel.class, RelOptHelper.any(UnionExchangePrel.class)), "LimitUnionExchangeTransposeRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final LimitPrel limit = (LimitPrel) call.rel(0);

    // Two situations we do not fire this rule:
    // 1) limit has been pushed down to its child,
    // 2) the fetch() is null (indicating we have to fetch all the remaining rows starting from offset.
    return !limit.isPushDown() && limit.getFetch() != null;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LimitPrel limit = (LimitPrel) call.rel(0);
    final UnionExchangePrel unionExchangePrel = (UnionExchangePrel) call.rel(1);

    RelNode child = unionExchangePrel.getInput();

    final int offset = limit.getOffset() != null ? Math.max(0, RexLiteral.intValue(limit.getOffset())) : 0;
    final int fetch = Math.max(0, RexLiteral.intValue(limit.getFetch()));

    // child Limit uses conservative approach:  use offset 0 and fetch = parent limit offset + parent limit fetch.
    final RexNode childFetch = limit.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(offset + fetch));

    final RelNode limitUnderExchange = new LimitPrel(child.getCluster(), child.getTraitSet(), child, null, childFetch);
    final RelNode newUnionExch = new UnionExchangePrel(unionExchangePrel.getCluster(), unionExchangePrel.getTraitSet(), limitUnderExchange);
    final RelNode limitAboveExchange = new LimitPrel(limit.getCluster(), limit.getTraitSet(), newUnionExch, limit.getOffset(), limit.getFetch(), true);

    call.transformTo(limitAboveExchange);
  }

}
