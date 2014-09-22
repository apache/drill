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

import com.google.common.collect.Lists;
import net.hydromatic.optiq.util.BitSets;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.WindowPOP;
import org.apache.drill.exec.planner.common.DrillWindowRelBase;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexLiteral;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class StreamingWindowPrel extends DrillWindowRelBase implements Prel {
  public StreamingWindowPrel(RelOptCluster cluster,
                             RelTraitSet traits,
                             RelNode child,
                             List<RexLiteral> constants,
                             RelDataType rowType,
                             Window window) {
    super(cluster, traits, child, constants, rowType, Collections.singletonList(window));
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StreamingWindowPrel(getCluster(), traitSet, sole(inputs), constants, getRowType(), windows.get(0));
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    final List<String> childFields = getChild().getRowType().getFieldNames();

    checkState(windows.size() == 1, "Only one window is expected in WindowPrel");

    Window window = windows.get(0);
    List<NamedExpression> withins = Lists.newArrayList();
    List<NamedExpression> aggs = Lists.newArrayList();
    for (int group : BitSets.toIter(window.groupSet)) {
      FieldReference fr = new FieldReference(childFields.get(group), ExpressionPosition.UNKNOWN);
      withins.add(new NamedExpression(fr, fr));
    }

    for (AggregateCall aggCall : window.getAggregateCalls(this)) {
      FieldReference ref = new FieldReference(aggCall.getName());
      LogicalExpression expr = toDrill(aggCall, childFields);
      aggs.add(new NamedExpression(expr, ref));
    }

    WindowPOP windowPOP = new WindowPOP(
        childPOP,
        withins.toArray(new NamedExpression[withins.size()]),
        aggs.toArray(new NamedExpression[aggs.size()]),
        Long.MIN_VALUE, //TODO: Get first/last to work
        Long.MIN_VALUE);

    creator.addMetadata(this, windowPOP);
    return windowPOP;
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

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
    return BatchSchema.SelectionVectorMode.ALL;
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getChild());
  }
}
