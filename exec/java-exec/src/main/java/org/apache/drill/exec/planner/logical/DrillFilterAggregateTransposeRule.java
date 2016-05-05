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

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.tools.RelBuilder;

public class DrillFilterAggregateTransposeRule extends FilterAggregateTransposeRule{

  // Since Calcite's default FilterAggregateTransposeRule would match Filter on top of Aggregate, it potentially will match Rels with mixed CONVENTION trait.
  // Here override match method, such that the rule matchs with Rel in the same CONVENTION.

  public static final FilterAggregateTransposeRule INSTANCE = new DrillFilterAggregateTransposeRule();

  private DrillFilterAggregateTransposeRule() {
    super(Filter.class, RelBuilder.proto(Contexts.of(RelFactories.DEFAULT_FILTER_FACTORY)), Aggregate.class);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Filter filter = (Filter) call.rel(0);
    final Aggregate aggregate = (Aggregate) call.rel(1);
    return filter.getTraitSet().getTrait(ConventionTraitDef.INSTANCE) == aggregate.getTraitSet().getTrait(ConventionTraitDef.INSTANCE);
  }

}
