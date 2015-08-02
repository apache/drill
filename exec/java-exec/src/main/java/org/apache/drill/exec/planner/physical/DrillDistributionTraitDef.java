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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;

public class DrillDistributionTraitDef extends RelTraitDef<DrillDistributionTrait>{
  public static final DrillDistributionTraitDef INSTANCE = new DrillDistributionTraitDef();

  private DrillDistributionTraitDef() {
    super();
  }

  @Override
  public boolean canConvert(
      RelOptPlanner planner, DrillDistributionTrait fromTrait, DrillDistributionTrait toTrait) {
    return true;
  }

  @Override
  public Class<DrillDistributionTrait> getTraitClass(){
    return DrillDistributionTrait.class;
  }

  @Override
  public DrillDistributionTrait getDefault() {
    return DrillDistributionTrait.DEFAULT;
  }

  @Override
  public String getSimpleName() {
    return this.getClass().getSimpleName();
  }

  // implement RelTraitDef
  @Override
  public RelNode convert(
      RelOptPlanner planner,
      RelNode rel,
      DrillDistributionTrait toDist,
      boolean allowInfiniteCostConverters) {

    DrillDistributionTrait currentDist = rel.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE);

    // Source and Target have the same trait.
    if (currentDist.equals(toDist)) {
      return rel;
    }

    // Source trait is "ANY", which is abstract type of distribution.
    // We do not want to convert from "ANY", since it's abstract.
    // Source trait should be concrete type: SINGLETON, HASH_DISTRIBUTED, etc.
    if (currentDist.equals(DrillDistributionTrait.DEFAULT) && !(rel instanceof RelSubset) ) {
        return null;
    }

    // It is only possible to apply a distribution trait to a DRILL_PHYSICAL convention.
    if (rel.getConvention() != Prel.DRILL_PHYSICAL) {
      return null;
    }

    switch(toDist.getType()){
      // UnionExchange, HashToRandomExchange, OrderedPartitionExchange and BroadcastExchange destroy the ordering property,
      // therefore RelCollation is set to default, which is EMPTY.
      case SINGLETON:
        return new UnionExchangePrel(rel.getCluster(), planner.emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toDist), rel);
      case HASH_DISTRIBUTED:
        return new HashToRandomExchangePrel(rel.getCluster(), planner.emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toDist), rel,
                                             toDist.getFields());
      case RANGE_DISTRIBUTED:
        return new OrderedPartitionExchangePrel(rel.getCluster(), planner.emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toDist), rel);
      case BROADCAST_DISTRIBUTED:
        return new BroadcastExchangePrel(rel.getCluster(), planner.emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toDist), rel);
      case ANY:
        // If target is "any", any input would satisfy "any". Return input directly.
        return rel;
      default:
        return null;
    }
  }

}
