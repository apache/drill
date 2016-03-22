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

import java.util.BitSet;
import java.util.List;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.GroupingAggregate;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.common.DrillAggregateRelBase;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;

import com.google.common.collect.Lists;

/**
 * Aggregation implemented in Drill.
 */
public class DrillAggregateRel extends DrillAggregateRelBase implements DrillRel {
  /** Creates a DrillAggregateRel. */
  public DrillAggregateRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator, ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) throws InvalidRelException {
    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    try {
      return new DrillAggregateRel(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {

    GroupingAggregate.Builder builder = GroupingAggregate.builder();
    builder.setInput(implementor.visitChild(this, 0, getInput()));
    final List<String> childFields = getInput().getRowType().getFieldNames();
    final List<String> fields = getRowType().getFieldNames();

    for (int group : BitSets.toIter(groupSet)) {
      FieldReference fr = new FieldReference(childFields.get(group), ExpressionPosition.UNKNOWN);
      builder.addKey(fr, fr);
    }

    for (Ord<AggregateCall> aggCall : Ord.zip(aggCalls)) {
      FieldReference ref = new FieldReference(fields.get(groupSet.cardinality() + aggCall.i));
      LogicalExpression expr = toDrill(aggCall.e, childFields, implementor);
      builder.addExpr(ref, expr);
    }

    return builder.build();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    for (AggregateCall aggCall : getAggCallList()) {
      String name = aggCall.getAggregation().getName();
      // For avg, stddev_pop, stddev_samp, var_pop and var_samp, the ReduceAggregatesRule is supposed
      // to convert them to use sum and count. Here, we make the cost of the original functions high
      // enough such that the planner does not choose them and instead chooses the rewritten functions.
      if (name.equals("AVG")
              || name.equals("STDDEV_POP")
              || name.equals("STDDEV_SAMP")
              || name.equals("STDDEV")
              || name.equals("VAR_POP")
              || name.equals("VAR_SAMP")
              || name.equals("VARIANCE")) {
        return ((DrillCostBase.DrillCostFactory)planner.getCostFactory()).makeHugeCost();
      }
    }

    return computeLogicalAggCost(planner);
  }

  public static LogicalExpression toDrill(AggregateCall call, List<String> fn, DrillImplementor implementor) {
    List<LogicalExpression> args = Lists.newArrayList();
    for(Integer i : call.getArgList()) {
      args.add(new FieldReference(fn.get(i)));
    }

    // for count(1).
    if (args.isEmpty()) {
      args.add(new ValueExpressions.LongExpression(1l));
    }
    LogicalExpression expr = FunctionCallFactory.createExpression(call.getAggregation().getName().toLowerCase(), ExpressionPosition.UNKNOWN, args);
    return expr;
  }

  public static DrillAggregateRel convert(GroupingAggregate groupBy, ConversionContext value)
      throws InvalidRelException {
    throw new UnsupportedOperationException();
  }

}
