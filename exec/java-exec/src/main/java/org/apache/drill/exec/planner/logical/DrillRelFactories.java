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
package org.apache.drill.exec.planner.logical;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.exec.planner.DrillRelBuilder;

import java.util.List;
import java.util.Set;

import static org.apache.calcite.rel.core.RelFactories.DEFAULT_AGGREGATE_FACTORY;
import static org.apache.calcite.rel.core.RelFactories.DEFAULT_FILTER_FACTORY;
import static org.apache.calcite.rel.core.RelFactories.DEFAULT_JOIN_FACTORY;
import static org.apache.calcite.rel.core.RelFactories.DEFAULT_MATCH_FACTORY;
import static org.apache.calcite.rel.core.RelFactories.DEFAULT_PROJECT_FACTORY;
import static org.apache.calcite.rel.core.RelFactories.DEFAULT_SET_OP_FACTORY;
import static org.apache.calcite.rel.core.RelFactories.DEFAULT_SORT_FACTORY;
import static org.apache.calcite.rel.core.RelFactories.DEFAULT_TABLE_SCAN_FACTORY;
import static org.apache.calcite.rel.core.RelFactories.DEFAULT_VALUES_FACTORY;
import static org.apache.drill.exec.planner.logical.DrillRel.DRILL_LOGICAL;

/**
 * Contains factory implementation for creating various Drill Logical Rel nodes.
 */

public class DrillRelFactories {
  public static final RelFactories.SetOpFactory DRILL_LOGICAL_SET_OP_FACTORY =
    new DrillSetOpFactoryImpl();

  public static final RelFactories.ProjectFactory DRILL_LOGICAL_PROJECT_FACTORY =
      new DrillProjectFactoryImpl();

  public static final RelFactories.FilterFactory DRILL_LOGICAL_FILTER_FACTORY =
      new DrillFilterFactoryImpl();

  public static final RelFactories.JoinFactory DRILL_LOGICAL_JOIN_FACTORY = new DrillJoinFactoryImpl();

  public static final RelFactories.AggregateFactory DRILL_LOGICAL_AGGREGATE_FACTORY = new DrillAggregateFactoryImpl();

  public static final RelFactories.SemiJoinFactory DRILL_SEMI_JOIN_FACTORY = new SemiJoinFactoryImpl();

  private static class SemiJoinFactoryImpl implements RelFactories.SemiJoinFactory {
    public RelNode createSemiJoin(RelNode left, RelNode right,
                                  RexNode condition) {
      final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
      return DrillSemiJoinRel.create(left, right,
              condition, joinInfo.leftKeys, joinInfo.rightKeys);
    }
  }
  /**
   * A {@link RelBuilderFactory} that creates a {@link DrillRelBuilder} that will
   * create logical relational expressions for everything.
   */
  public static final RelBuilderFactory LOGICAL_BUILDER =
      DrillRelBuilder.proto(
          Contexts.of(DEFAULT_PROJECT_FACTORY,
              DEFAULT_FILTER_FACTORY,
              DEFAULT_JOIN_FACTORY,
              DRILL_SEMI_JOIN_FACTORY,
              DEFAULT_SORT_FACTORY,
              DEFAULT_AGGREGATE_FACTORY,
              DEFAULT_MATCH_FACTORY,
              DEFAULT_SET_OP_FACTORY,
              DEFAULT_VALUES_FACTORY,
              DEFAULT_TABLE_SCAN_FACTORY));

  /**
   * Implementation of {@link RelFactories.SetOpFactory} that returns
   * a vanilla {@link DrillExceptRel} or {@link DrillIntersectRel}
   * dependent on the particular kind of set operation (EXCEPT, INTERSECT)
   */
  private static class DrillSetOpFactoryImpl implements RelFactories.SetOpFactory {
    @Override
    public RelNode createSetOp(SqlKind kind, List<RelNode> inputs, boolean all) {
      switch (kind) {
      case EXCEPT:
        return DrillExceptRel.create(inputs, all);
      case INTERSECT:
        return DrillIntersectRel.create(inputs, all);
      default:
        throw new AssertionError("unsupported set op: " + kind);
      }
    }
  }

  /**
   * Implementation of {@link RelFactories.ProjectFactory} that returns a vanilla
   * {@link DrillProjectRel}.
   */
  private static class DrillProjectFactoryImpl implements RelFactories.ProjectFactory {

    @Override
    public RelNode createProject(RelNode input, List<RelHint> hints, List<? extends RexNode> childExprs,
      List<? extends String> fieldNames, Set<CorrelationId> variablesSet) {
      RelOptCluster cluster = input.getCluster();
      RelDataType rowType =
        RexUtil.createStructType(cluster.getTypeFactory(), childExprs, fieldNames, null);

      return DrillProjectRel.create(cluster, input.getTraitSet().plus(DRILL_LOGICAL), input, childExprs, rowType);
    }
  }

  /**
   * Implementation of {@link RelFactories.FilterFactory} that
   * returns a vanilla {@link DrillFilterRel}.
   */
  private static class DrillFilterFactoryImpl implements RelFactories.FilterFactory {
    @Override
    public RelNode createFilter(RelNode child, RexNode condition, Set<CorrelationId> variablesSet) {
      // Normalize nullability of RexInputRef nodes to match the input's row type
      // This is necessary for Calcite 1.37+ which has stricter type checking
      RexNode normalizedCondition = condition.accept(new RexShuttle() {
        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
          int index = inputRef.getIndex();
          if (index >= child.getRowType().getFieldCount()) {
            return inputRef;
          }
          RelDataType actualType = child.getRowType().getFieldList().get(index).getType();
          // If nullability differs, create a new RexInputRef with correct nullability
          if (inputRef.getType().isNullable() != actualType.isNullable() ||
              !inputRef.getType().equals(actualType)) {
            return new RexInputRef(index, actualType);
          }
          return inputRef;
        }
      });
      return DrillFilterRel.create(child, normalizedCondition);
    }
  }

  /**
   * Implementation of {@link RelFactories.JoinFactory} that returns a vanilla
   * {@link DrillJoinRel}.
   */
  private static class DrillJoinFactoryImpl implements RelFactories.JoinFactory {

    @Override
    public RelNode createJoin(RelNode left, RelNode right, List<RelHint> hints,
                              RexNode condition, Set<CorrelationId> variablesSet,
                              JoinRelType joinType, boolean semiJoinDone) {
      switch (joinType) {
        case SEMI:
          JoinInfo joinInfo = JoinInfo.of(left, right, condition);
          return DrillSemiJoinRel.create(left, right, condition, joinInfo.leftKeys, joinInfo.rightKeys);
        default:
          return new DrillJoinRel(left.getCluster(), left.getTraitSet().plus(DRILL_LOGICAL), left, right, condition, joinType);
      }
    }
  }

  /**
   * Implementation of {@link RelFactories.AggregateFactory} that returns a vanilla
   * {@link DrillAggregateRel}.
   */
  private static class DrillAggregateFactoryImpl implements RelFactories.AggregateFactory {

    @Override
    public RelNode createAggregate(RelNode input, List<RelHint> hints, ImmutableBitSet groupSet,
                                   com.google.common.collect.ImmutableList<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      return new DrillAggregateRel(input.getCluster(), input.getTraitSet().plus(DRILL_LOGICAL), input, groupSet, groupSets, aggCalls);
    }
  }
}
