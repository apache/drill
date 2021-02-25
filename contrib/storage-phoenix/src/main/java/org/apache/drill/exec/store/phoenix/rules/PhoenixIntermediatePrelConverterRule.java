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
package org.apache.drill.exec.store.phoenix.rules;

import java.util.function.Predicate;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillRelFactories;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.store.enumerable.plan.VertexDrel;

final class PhoenixIntermediatePrelConverterRule extends ConverterRule {

  static final PhoenixIntermediatePrelConverterRule INSTANCE = new PhoenixIntermediatePrelConverterRule();

  private PhoenixIntermediatePrelConverterRule() {
    super(VertexDrel.class, (Predicate<RelNode>) input -> true,
        DrillRel.DRILL_LOGICAL, Prel.DRILL_PHYSICAL, DrillRelFactories.LOGICAL_BUILDER, "Phoenix_PREL_Converter");
  }

  @Override
  public RelNode convert(RelNode in) {
    return new PhoenixIntermediatePrel(
        in.getCluster(),
        in.getTraitSet().replace(getOutTrait()),
        in.getInput(0));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode rel = call.rel(0);
    if (rel.getTraitSet().contains(getInTrait())) {
      Convention c = rel.getInput(0).getTraitSet().getTrait(ConventionTraitDef.INSTANCE);
      /*
       * only accept the PhoenixConvention
       * need to avoid the NPE or ClassCastException
       */
      if (c != null && c instanceof PhoenixConvention) {
        final RelNode converted = convert(rel);
        if (converted != null) {
          call.transformTo(converted);
        }
      }
    }
  }
}
