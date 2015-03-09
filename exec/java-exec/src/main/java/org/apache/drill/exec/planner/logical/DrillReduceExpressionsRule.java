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
package org.apache.drill.exec.planner.logical;

import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.SortRel;
import org.eigenbase.rel.rules.ReduceExpressionsRule;

import java.math.BigDecimal;

public class DrillReduceExpressionsRule {

  public static final DrillReduceFilterRule FILTER_INSTANCE_DRILL =
      new DrillReduceFilterRule();

  public static final DrillReduceCalcRule CALC_INSTANCE_DRILL =
      new DrillReduceCalcRule();

  private static class DrillReduceFilterRule extends ReduceExpressionsRule.ReduceFilterRule {

    DrillReduceFilterRule() {
      super("DrillReduceExpressionsRule(Filter)");
    }

    /**
     * Drills schema flexibility requires us to override the default behavior of calcite
     * to produce an EmptyRel in the case of a constant false filter. We need to propagate
     * schema at runtime, so we cannot just produce a simple operator at planning time to
     * expose the planning time known schema. Instead we have to insert a limit 0.
     */
    @Override
    protected RelNode createEmptyRelOrEquivalent(FilterRel filter) {
      return createEmptyEmptyRelHelper(filter);
    }

  }

  private static class DrillReduceCalcRule extends ReduceExpressionsRule.ReduceCalcRule {

    DrillReduceCalcRule() {
      super("DrillReduceExpressionsRule(Calc)");
    }

    /**
     * Drills schema flexibility requires us to override the default behavior of calcite
     * to produce an EmptyRel in the case of a constant false filter. We need to propagate
     * schema at runtime, so we cannot just produce a simple operator at planning time to
     * expose the planning time known schema. Instead we have to insert a limit 0.
     */
    @Override
    protected RelNode createEmptyRelOrEquivalent(CalcRel calc) {
      return createEmptyEmptyRelHelper(calc);
    }

  }

  private static RelNode createEmptyEmptyRelHelper(SingleRel input) {
    return new SortRel(input.getCluster(), input.getTraitSet(),
        input.getChild(),
        RelCollationImpl.EMPTY,
        input.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)),
        input.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)));
  }
}
