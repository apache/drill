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
package org.apache.drill.exec.planner.common;

import java.util.BitSet;
import java.util.List;

import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;


/**
 * Base class for logical and physical Aggregations implemented in Drill
 */
public abstract class DrillAggregateRelBase extends AggregateRelBase implements DrillRelNode {

  public DrillAggregateRelBase(RelOptCluster cluster, RelTraitSet traits, RelNode child, BitSet groupSet,
      List<AggregateCall> aggCalls) throws InvalidRelException {
    super(cluster, traits, child, groupSet, aggCalls);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    for (AggregateCall aggCall : getAggCallList()) {
      String name = aggCall.getAggregation().getName();
      // For avg, stddev_pop, stddev_samp, var_pop and var_samp, the ReduceAggregatesRule is supposed
      // to convert them to use sum and count. Here, we make the cost of the original functions high
      // enough such that the planner does not choose them and instead chooses the rewritten functions.
      if (name.equals("AVG") || name.equals("STDDEV_POP") || name.equals("STDDEV_SAMP")
          || name.equals("VAR_POP") || name.equals("VAR_SAMP")) {
        return ((DrillCostFactory)planner.getCostFactory()).makeHugeCost();
      }
    }
    return ((DrillCostFactory)planner.getCostFactory()).makeTinyCost();
  }

}
