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

import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexLiteral;

public class PushLimitToTopN  extends Prule{

  public static final RelOptRule INSTANCE = new PushLimitToTopN();

  private PushLimitToTopN() {
    super(RelOptHelper.some(LimitPrel.class, RelOptHelper.some(SingleMergeExchangePrel.class, RelOptHelper.any(SortPrel.class))), "PushLimitToTopN");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LimitPrel limit = (LimitPrel) call.rel(0);
    final SingleMergeExchangePrel smex = (SingleMergeExchangePrel) call.rel(1);
    final SortPrel sort = (SortPrel) call.rel(2);

    // First offset to include into results (inclusive). Null implies it is starting from offset 0
    int offset = limit.getOffset() != null ? Math.max(0, RexLiteral.intValue(limit.getOffset())) : 0;
    int fetch = limit.getFetch() != null?  Math.max(0, RexLiteral.intValue(limit.getFetch())) : 0;

    final TopNPrel topN = new TopNPrel(limit.getCluster(), sort.getTraitSet(), sort.getInput(), offset + fetch, sort.getCollation());
    final LimitPrel newLimit = new LimitPrel(limit.getCluster(), limit.getTraitSet(),
        new SingleMergeExchangePrel(smex.getCluster(), smex.getTraitSet(), topN, sort.getCollation()),
        limit.getOffset(), limit.getFetch());

    call.transformTo(newLimit);
  }

}
