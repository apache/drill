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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import net.hydromatic.linq4j.Ord;
import net.hydromatic.optiq.util.BitSets;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.Aggregation;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.type.OperandTypes;
import org.eigenbase.sql.type.ReturnTypes;

import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public abstract class AggPrelBase extends AggregateRelBase implements Prel {

  protected static enum OperatorPhase {PHASE_1of1, PHASE_1of2, PHASE_2of2};

  protected OperatorPhase operPhase = OperatorPhase.PHASE_1of1 ; // default phase
  protected List<NamedExpression> keys = Lists.newArrayList();
  protected List<NamedExpression> aggExprs = Lists.newArrayList();
  protected List<AggregateCall> phase2AggCallList = Lists.newArrayList();

  /**
   * Specialized aggregate function for SUMing the COUNTs.  Since return type of
   * COUNT is non-nullable and return type of SUM is nullable, this class enables
   * creating a SUM whose return type is non-nullable.
   *
   */
  public class SqlSumCountAggFunction extends SqlAggFunction {

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

  public AggPrelBase(RelOptCluster cluster, RelTraitSet traits, RelNode child, BitSet groupSet,
      List<AggregateCall> aggCalls, OperatorPhase phase) throws InvalidRelException {
    super(cluster, traits, child, groupSet, aggCalls);
    this.operPhase = phase;
    createKeysAndExprs();
  }

  public OperatorPhase getOperatorPhase() {
    return operPhase;
  }

  public List<NamedExpression> getKeys() {
    return keys;
  }

  public List<NamedExpression> getAggExprs() {
    return aggExprs;
  }

  public List<AggregateCall> getPhase2AggCalls() {
    return phase2AggCallList;
  }

  protected void createKeysAndExprs() {
    final List<String> childFields = getChild().getRowType().getFieldNames();
    final List<String> fields = getRowType().getFieldNames();

    for (int group : BitSets.toIter(groupSet)) {
      FieldReference fr = new FieldReference(childFields.get(group), ExpressionPosition.UNKNOWN);
      keys.add(new NamedExpression(fr, fr));
    }

    for (Ord<AggregateCall> aggCall : Ord.zip(aggCalls)) {
      int aggExprOrdinal = groupSet.cardinality() + aggCall.i;
      FieldReference ref = new FieldReference(fields.get(aggExprOrdinal));
      LogicalExpression expr = toDrill(aggCall.e, childFields);
      NamedExpression ne = new NamedExpression(expr, ref);
      aggExprs.add(ne);

      if (getOperatorPhase() == OperatorPhase.PHASE_1of2) {
        if (aggCall.e.getAggregation().getName().equals("COUNT")) {
          // If we are doing a COUNT aggregate in Phase1of2, then in Phase2of2 we should SUM the COUNTs,
          Aggregation sumAggFun = new SqlSumCountAggFunction(aggCall.e.getType());
          AggregateCall newAggCall =
              new AggregateCall(
                  sumAggFun,
                  aggCall.e.isDistinct(),
                  Collections.singletonList(aggExprOrdinal),
                  aggCall.e.getType(),
                  aggCall.e.getName());

          phase2AggCallList.add(newAggCall);
        } else {
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
    LogicalExpression expr = new FunctionCall(call.getAggregation().getName().toLowerCase(), args, ExpressionPosition.UNKNOWN );
    return expr;
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
  public boolean needsFinalColumnReordering() {
    return true;
  }

}
