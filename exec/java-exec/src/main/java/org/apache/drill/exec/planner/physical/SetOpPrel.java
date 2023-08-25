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
package org.apache.drill.exec.planner.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.sql.SqlKind;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class SetOpPrel extends SetOp implements Prel {

  public SetOpPrel(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, SqlKind kind,
                   boolean all) throws InvalidRelException {
    super(cluster, traits, inputs, kind, all);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("kind", kind);
  }

  public SetOpPrel copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    try {
      return new SetOpPrel(this.getCluster(), traitSet, inputs, kind, all);
    }catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    List<PhysicalOperator> inputPops = Lists.newArrayList();

    for (int i = 0; i < this.getInputs().size(); i++) {
      inputPops.add( ((Prel)this.getInputs().get(i)).getPhysicalOperator(creator));
    }

    org.apache.drill.exec.physical.config.SetOp setOp = new org.apache.drill.exec.physical.config.SetOp(inputPops, kind, all);
    return creator.addMetadata(this, setOp);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(this.getInputs());
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;
  }
}
