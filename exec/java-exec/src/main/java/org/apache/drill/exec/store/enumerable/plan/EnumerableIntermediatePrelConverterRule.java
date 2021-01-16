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
package org.apache.drill.exec.store.enumerable.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillRelFactories;
import org.apache.drill.exec.planner.physical.Prel;

import java.util.function.Predicate;

public class EnumerableIntermediatePrelConverterRule extends ConverterRule {

  private final EnumerablePrelContext context;

  public EnumerableIntermediatePrelConverterRule(EnumerablePrelContext context) {
    super(VertexDrel.class, (Predicate<RelNode>) input -> true, DrillRel.DRILL_LOGICAL,
        Prel.DRILL_PHYSICAL, DrillRelFactories.LOGICAL_BUILDER,
        "EnumerableIntermediatePrelConverterRule:" + context.getPlanPrefix());
    this.context = context;
  }

  @Override
  public RelNode convert(RelNode in) {
    return new EnumerableIntermediatePrel(
        in.getCluster(),
        in.getTraitSet().replace(getOutTrait()),
        in.getInput(0),
        context);
  }
}
