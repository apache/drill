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

import java.util.List;

import org.apache.calcite.util.BitSets;

import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;

import com.google.common.collect.Lists;

// abstract base class for the aggregation physical rules
public abstract class AggPruleBase extends Prule {

  protected AggPruleBase(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  protected List<DistributionField> getDistributionField(DrillAggregateRel rel, boolean allFields) {
    List<DistributionField> groupByFields = Lists.newArrayList();

    for (int group : BitSets.toIter(rel.getGroupSet())) {
      DistributionField field = new DistributionField(group);
      groupByFields.add(field);

      if (!allFields && groupByFields.size() == 1) {
        // if we are only interested in 1 grouping field, pick the first one for now..
        // but once we have num distinct values (NDV) statistics, we should pick the one
        // with highest NDV.
        break;
      }
    }

    return groupByFields;
  }

  // Create 2 phase aggr plan for aggregates such as SUM, MIN, MAX
  // If any of the aggregate functions are not one of these, then we
  // currently won't generate a 2 phase plan.
  protected boolean create2PhasePlan(RelOptRuleCall call, DrillAggregateRel aggregate) {
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    RelNode child = call.rel(0).getInputs().get(0);
    boolean smallInput = child.getRows() < settings.getSliceTarget();
    if (! settings.isMultiPhaseAggEnabled() || settings.isSingleMode() || smallInput) {
      return false;
    }

    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      String name = aggCall.getAggregation().getName();
      if ( ! (name.equals("SUM") || name.equals("MIN") || name.equals("MAX") || name.equals("COUNT")
              || name.equals("$SUM0"))) {
        return false;
      }
    }
    return true;
  }
}
