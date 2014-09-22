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

import com.google.common.collect.Lists;
import net.hydromatic.linq4j.Ord;
import net.hydromatic.optiq.util.BitSets;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.exec.planner.common.DrillWindowRelBase;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexLiteral;

import java.util.List;

public class DrillWindowRel extends DrillWindowRelBase implements DrillRel {
  /**
   * Creates a window relational expression.
   *
   * @param cluster Cluster
   * @param traits
   * @param child   Input relational expression
   * @param rowType Output row type
   * @param windows Windows
   */
  public DrillWindowRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      List<RexLiteral> constants,
      RelDataType rowType,
      List<Window> windows) {
    super(cluster, traits, child, constants, rowType, windows);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillWindowRel(getCluster(), traitSet, sole(inputs), constants, getRowType(), windows);
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    final LogicalOperator inputOp = implementor.visitChild(this, 0, getChild());
    org.apache.drill.common.logical.data.Window.Builder builder = new org.apache.drill.common.logical.data.Window.Builder();
    final List<String> fields = getRowType().getFieldNames();
    final List<String> childFields = getChild().getRowType().getFieldNames();
    for (Window window : windows) {

      for(RelFieldCollation orderKey : window.orderKeys.getFieldCollations()) {
        builder.addOrdering(new Order.Ordering(orderKey.getDirection(), new FieldReference(fields.get(orderKey.getFieldIndex()))));
      }

      for (int group : BitSets.toIter(window.groupSet)) {
        FieldReference fr = new FieldReference(childFields.get(group), ExpressionPosition.UNKNOWN);
        builder.addWithin(fr, fr);
      }

      int groupCardinality = window.groupSet.cardinality();
      for (Ord<AggregateCall> aggCall : Ord.zip(window.getAggregateCalls(this))) {
        FieldReference ref = new FieldReference(fields.get(groupCardinality + aggCall.i));
        LogicalExpression expr = toDrill(aggCall.e, childFields);
        builder.addAggregation(ref, expr);
      }
    }
    builder.setInput(inputOp);
    org.apache.drill.common.logical.data.Window frame = builder.build();
    return frame;
  }

  protected LogicalExpression toDrill(AggregateCall call, List<String> fn) {
    List<LogicalExpression> args = Lists.newArrayList();
    for (Integer i : call.getArgList()) {
      args.add(new FieldReference(fn.get(i)));
    }

    // for count(1).
    if (args.isEmpty()) {
      args.add(new ValueExpressions.LongExpression(1l));
    }
    LogicalExpression expr = new FunctionCall(call.getAggregation().getName().toLowerCase(), args, ExpressionPosition.UNKNOWN);
    return expr;
  }
}


