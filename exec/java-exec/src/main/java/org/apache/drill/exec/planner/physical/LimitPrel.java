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

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Limit;
import org.apache.drill.exec.planner.common.DrillLimitRelBase;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class LimitPrel extends DrillLimitRelBase implements Prel {

  public LimitPrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, child, offset, fetch);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new LimitPrel(getCluster(), traitSet, sole(inputs), offset, fetch);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    // First offset to include into results (inclusive). Null implies it is starting from offset 0
    int first = offset != null ? Math.max(0, RexLiteral.intValue(offset)) : 0;

    // Last offset to stop including into results (exclusive), translating fetch row counts into an offset.
    // Null value implies including entire remaining result set from first offset
    Integer last = fetch != null ? Math.max(0, RexLiteral.intValue(fetch)) + first : null;

    Limit limit = new Limit(childPOP, first, last);
    return creator.addMetadata(this, limit);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getChild());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.NONE_AND_TWO;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.TWO_BYTE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

//  @Override
//  public LogicalOperator implement(DrillImplementor implementor) {
//    LogicalOperator inputOp = implementor.visitChild(this, 0, getChild());
//
//    // First offset to include into results (inclusive). Null implies it is starting from offset 0
//    int first = offset != null ? Math.max(0, RexLiteral.intValue(offset)) : 0;
//
//    // Last offset to stop including into results (exclusive), translating fetch row counts into an offset.
//    // Null value implies including entire remaining result set from first offset
//    Integer last = fetch != null ? Math.max(0, RexLiteral.intValue(fetch)) + first : null;
//    Limit limit = new Limit(first, last);
//    limit.setInput(inputOp);
//    return limit;
//  }

//  public static LimitPrel convert(Limit limit, ConversionContext context) throws InvalidRelException{
//    RelNode input = context.toRel(limit.getInput());
//    RexNode first = context.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(limit.getFirst()), context.getTypeFactory().createSqlType(SqlTypeName.INTEGER));
//    RexNode last = context.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(limit.getLast()), context.getTypeFactory().createSqlType(SqlTypeName.INTEGER));
//    return new LimitPrel(context.getCluster(), context.getLogicalTraits(), input, first, last);
//  }


}
