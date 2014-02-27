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

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import net.hydromatic.linq4j.Ord;
import net.hydromatic.optiq.util.BitSets;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.SingleMergeExchange;
import org.apache.drill.exec.physical.config.StreamingAggregate;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

import com.beust.jcommander.internal.Lists;

public class StreamAggPrel extends AggregateRelBase implements Prel{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StreamAggPrel.class);

  public StreamAggPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, BitSet groupSet,
      List<AggregateCall> aggCalls) throws InvalidRelException {
    super(cluster, traits, child, groupSet, aggCalls);
    for (AggregateCall aggCall : aggCalls) {
      if (aggCall.isDistinct()) {
        throw new InvalidRelException("DrillAggregateRel does not support DISTINCT aggregates");
      }
    }
  }

  public AggregateRelBase copy(RelTraitSet traitSet, RelNode input, BitSet groupSet, List<AggregateCall> aggCalls) {
    try {
      return new StreamAggPrel(getCluster(), traitSet, input, getGroupSet(), aggCalls);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }
   
  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    // Prel child = (Prel) this.getChild();
    
    final List<String> childFields = getChild().getRowType().getFieldNames();
    final List<String> fields = getRowType().getFieldNames();
    List<NamedExpression> keys = Lists.newArrayList();
    List<NamedExpression> exprs = Lists.newArrayList();
    
    for (int group : BitSets.toIter(groupSet)) {
      FieldReference fr = new FieldReference(childFields.get(group), ExpressionPosition.UNKNOWN);
      keys.add(new NamedExpression(fr, fr));
    }
    
    for (Ord<AggregateCall> aggCall : Ord.zip(aggCalls)) {
      FieldReference ref = new FieldReference(fields.get(groupSet.cardinality() + aggCall.i));
      LogicalExpression expr = toDrill(aggCall.e, childFields, new DrillParseContext());
      exprs.add(new NamedExpression(expr, ref));
    }

    Prel child = (Prel) this.getChild();
    StreamingAggregate g = new StreamingAggregate(child.getPhysicalOperator(creator), keys.toArray(new NamedExpression[keys.size()]), exprs.toArray(new NamedExpression[exprs.size()]), 1.0f);
    creator.addPhysicalOperator(g);
    
    return g;    

  }
  
  private LogicalExpression toDrill(AggregateCall call, List<String> fn, DrillParseContext pContext) {
    List<LogicalExpression> args = Lists.newArrayList();
    for(Integer i : call.getArgList()){
      args.add(new FieldReference(fn.get(i)));
    }
    
    // for count(1).
    if(args.isEmpty()) args.add(new ValueExpressions.LongExpression(1l));
    LogicalExpression expr = new FunctionCall(call.getAggregation().getName().toLowerCase(), args, ExpressionPosition.UNKNOWN );
    return expr;
  }
 
}
