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

import net.hydromatic.linq4j.Ord;
import net.hydromatic.optiq.util.BitSets;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.GroupingAggregate;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.common.DrillAggregateRelBase;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

import com.google.common.collect.Lists;

/**
 * Aggregation implemented in Drill.
 */
public class DrillAggregateRel extends DrillAggregateRelBase implements DrillRel {
  /** Creates a DrillAggregateRel. */
  public DrillAggregateRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, BitSet groupSet,
      List<AggregateCall> aggCalls) throws InvalidRelException {
    super(cluster, traits, child, groupSet, aggCalls);
  }

  @Override
  public AggregateRelBase copy(RelTraitSet traitSet, RelNode input, BitSet groupSet, List<AggregateCall> aggCalls) {
    try {
      return new DrillAggregateRel(getCluster(), traitSet, input, getGroupSet(), aggCalls);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {

    GroupingAggregate.Builder builder = GroupingAggregate.builder();
    builder.setInput(implementor.visitChild(this, 0, getChild()));
    final List<String> childFields = getChild().getRowType().getFieldNames();
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
