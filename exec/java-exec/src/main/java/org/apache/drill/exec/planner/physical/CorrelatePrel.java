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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.LateralJoinPOP;
import org.apache.drill.exec.planner.common.DrillCorrelateRelBase;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema;

import java.io.IOException;
import java.util.Iterator;

public class CorrelatePrel extends DrillCorrelateRelBase implements Prel {


  protected CorrelatePrel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
                              CorrelationId correlationId, ImmutableBitSet requiredColumns, SemiJoinType semiJoinType) {
    super(cluster, traits, left, right, correlationId, requiredColumns, semiJoinType);
  }
  @Override
  public Correlate copy(RelTraitSet traitSet,
                        RelNode left, RelNode right, CorrelationId correlationId,
                        ImmutableBitSet requiredColumns, SemiJoinType joinType) {
    return new CorrelatePrel(this.getCluster(), this.getTraitSet(), left, right, correlationId, requiredColumns,
        this.getJoinType());
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {

    PhysicalOperator leftPop = ((Prel)left).getPhysicalOperator(creator);
    PhysicalOperator rightPop = ((Prel)right).getPhysicalOperator(creator);

    SemiJoinType jtype = this.getJoinType();

    LateralJoinPOP ljoin = new LateralJoinPOP(leftPop, rightPop, jtype.toJoinType());
    return creator.addMetadata(this, ljoin);
  }


  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> visitor, X value) throws E {
    return visitor.visitPrel(this, value);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getLeft(), getRight());
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

  @Override
  public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
    return BatchSchema.SelectionVectorMode.DEFAULT;
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

}
