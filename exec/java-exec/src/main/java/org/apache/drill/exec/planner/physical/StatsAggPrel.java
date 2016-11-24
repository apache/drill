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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.StatisticsAggregate;
import org.apache.drill.exec.planner.common.DrillRelNode;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;

public class StatsAggPrel extends SingleRel implements DrillRelNode, Prel {

  public static enum OperatorPhase {PHASE_1of1, PHASE_1of2, PHASE_2of2};
  protected OperatorPhase phase = OperatorPhase.PHASE_1of1;  // default phase
  private List<String> functions;
  protected List<AggregateCall> phase2functions = Lists.newArrayList();

  public StatsAggPrel(RelNode child, RelOptCluster cluster, List<String> functions, OperatorPhase operPhase) {
    super(cluster, child.getTraitSet(), child);
    this.functions = ImmutableList.copyOf(functions);
    this.phase = operPhase;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new StatsAggPrel(sole(inputs), getCluster(), ImmutableList.copyOf(functions), phase);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator)
      throws IOException {
    Prel child = (Prel) this.getInput();
    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    StatisticsAggregate g = new StatisticsAggregate(childPOP, phase, functions);
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

  /*protected void createKeysAndExprs() {
    final List<String> childFields = getInput().getRowType().getFieldNames();
    final List<String> fields = getRowType().getFieldNames();

    for (Ord<AggregateCall> aggCall : Ord.zip(aggCalls)) {
      int aggExprOrdinal = aggCall.i;

      if (getOperatorPhase() == OperatorPhase.PHASE_1of2) {
        if (aggCall.e.getAggregation().getName().equals("STATCOUNT")
            || aggCall.e.getAggregation().getName().equals("NONNULLSTATCOUNT")   ) {
          // If we are doing a STATCOUNT/NONNULLSTATCOUNT aggregate in Phase1of2, then in Phase2of2
          // we should SUM the COUNTs,
          SqlAggFunction sumAggFun = new SqlSumCountAggFunction(aggCall.e.getType());
          AggregateCall newAggCall =
              new AggregateCall(
                  sumAggFun,
                  aggCall.e.isDistinct(),
                  Collections.singletonList(aggExprOrdinal),
                  aggCall.e.getType(),
                  aggCall.e.getName());
          phase2AggCallList.add(newAggCall);
        } else if (aggCall.e.getAggregation().getName().equals("NDV")) {
          AggregateCall newAggCall =
              new AggregateCall(
                  aggCall.e.getAggregation(),
                  aggCall.e.isDistinct(),
                  Collections.singletonList(aggExprOrdinal),
                  aggCall.e.getType(),
                  aggCall.e.getName());
          phase2AggCallList.add(newAggCall);
        }
      }
    }
  }*/
  /**
   * Specialized aggregate function for SUMing the COUNTs.  Since return type of
   * COUNT is non-nullable and return type of SUM is nullable, this class enables
   * creating a SUM whose return type is non-nullable.
   *
   */
  /*public class SqlSumCountAggFunction extends SqlAggFunction {

    private final RelDataType type;

    public SqlSumCountAggFunction(RelDataType type) {
      super("$SUM0",
              SqlKind.OTHER_FUNCTION,
              ReturnTypes.BIGINT, // use the inferred return type of SqlCountAggFunction
              null,
              OperandTypes.NUMERIC,
              SqlFunctionCategory.NUMERIC);

      this.type = type;
    }

    public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
      return ImmutableList.of(type);
    }

    public RelDataType getType() {
      return type;
    }

    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
      return type;
    }

  }

  protected LogicalExpression toDrill(AggregateCall call, List<String> fn) {
    List<LogicalExpression> args = Lists.newArrayList();
    for (Integer i : call.getArgList()) {
      args.add(FieldReference.getWithQuotedRef(fn.get(i)));
    }

    // for count(1).
    if (args.isEmpty()) {
      args.add(new ValueExpressions.LongExpression(1l));
    }
    LogicalExpression expr = new FunctionCall(call.getAggregation().getName().toLowerCase(), args, ExpressionPosition.UNKNOWN );
    return expr;
  }*/
}
