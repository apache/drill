/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.physical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.StatisticsMerge;
import org.apache.drill.exec.planner.common.DrillRelNode;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class StatsMergePrel extends SingleRel implements DrillRelNode, Prel {

  public static enum OperatorPhase {PHASE_1of1, PHASE_1of2, PHASE_2of2};
  protected OperatorPhase phase = OperatorPhase.PHASE_1of1;  // default phase
  private List<String> functions;
  protected List<AggregateCall> phase2functions = Lists.newArrayList();

  public StatsMergePrel(RelNode child, RelOptCluster cluster, List<String> functions, OperatorPhase operPhase) {
    super(cluster, child.getTraitSet(), child);
    this.functions = ImmutableList.copyOf(functions);
    this.phase = operPhase;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StatsMergePrel(sole(inputs), getCluster(), ImmutableList.copyOf(functions), phase);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator)
      throws IOException {
    Prel child = (Prel) this.getInput();
    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    StatisticsMerge g = new StatisticsMerge(childPOP, phase, functions);
    return creator.addMetadata(this, g);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value)
      throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.ALL;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

  public OperatorPhase getOperatorPhase() {
    return phase;
  }
}
