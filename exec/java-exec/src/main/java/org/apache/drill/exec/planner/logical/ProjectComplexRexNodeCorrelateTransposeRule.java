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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.trace.CalciteTrace;
import org.apache.drill.common.exceptions.UserException;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that moves non-{@link RexFieldAccess} rex node from project below {@link Uncollect}
 * to the left side of the {@link Correlate}.
 */
public class ProjectComplexRexNodeCorrelateTransposeRule extends RelOptRule {

  public static final RelOptRule INSTANCE = new ProjectComplexRexNodeCorrelateTransposeRule();

  public ProjectComplexRexNodeCorrelateTransposeRule() {
    super(operand(LogicalCorrelate.class,
        operand(RelNode.class, any()),
        operand(Uncollect.class, operand(LogicalProject.class, any()))),
        DrillRelFactories.LOGICAL_BUILDER,
        "ProjectComplexRexNodeCorrelateTransposeRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Correlate correlate = call.rel(0);
    final Uncollect uncollect = call.rel(2);
    final LogicalProject project = call.rel(3);

    // uncollect requires project with single expression
    RexNode projectedNode = project.getProjects().iterator().next();

    // check that the expression is complex call
    if (!(projectedNode instanceof RexFieldAccess)) {
      RelBuilder builder = call.builder();
      RexBuilder rexBuilder = builder.getRexBuilder();

      builder.push(correlate.getLeft());

      // creates project with complex expr on top of the left side
      List<RexNode> leftProjExprs = new ArrayList<>();

      String complexFieldName = correlate.getRowType().getFieldNames()
            .get(correlate.getRowType().getFieldNames().size() - 1);

      List<String> fieldNames = new ArrayList<>();
      for (RelDataTypeField field : correlate.getLeft().getRowType().getFieldList()) {
        leftProjExprs.add(rexBuilder.makeInputRef(correlate.getLeft(), field.getIndex()));
        fieldNames.add(field.getName());
      }
      fieldNames.add(complexFieldName);
      List<RexNode> topProjectExpressions = new ArrayList<>(leftProjExprs);

      // adds complex expression with replaced correlation
      // to the projected list from the left
      leftProjExprs.add(projectedNode.accept(new RexFieldAccessReplacer(builder)));

      RelNode leftProject = builder.project(leftProjExprs, fieldNames)
          .build();

      CorrelationId correlationId = correlate.getCluster().createCorrel();
      RexCorrelVariable rexCorrel =
          (RexCorrelVariable) rexBuilder.makeCorrel(
              leftProject.getRowType(),
              correlationId);
      builder.push(project.getInput());
      RelNode rightProject = builder.project(
              ImmutableList.of(rexBuilder.makeFieldAccess(rexCorrel, leftProjExprs.size() - 1)),
              ImmutableList.of(complexFieldName))
          .build();

      int requiredColumnsCount = correlate.getRequiredColumns().cardinality();
      if (requiredColumnsCount != 1) {
        throw UserException.planError()
            .message("Required columns count for Correlate operator " +
                "differs from the expected value:\n" +
                "Expected columns count is %s, but actual is %s",
                1, requiredColumnsCount)
            .build(CalciteTrace.getPlannerTracer());
      }

      RelNode newUncollect = uncollect.copy(uncollect.getTraitSet(), rightProject);
      Correlate newCorrelate = correlate.copy(uncollect.getTraitSet(), leftProject, newUncollect,
          correlationId, ImmutableBitSet.of(leftProjExprs.size() - 1), correlate.getJoinType());
      builder.push(newCorrelate);

      switch(correlate.getJoinType()) {
        case LEFT:
        case INNER:
          // adds field from the right input of correlate to the top project
          topProjectExpressions.add(
              rexBuilder.makeInputRef(newCorrelate, topProjectExpressions.size() + 1));
          // fall through
        case ANTI:
        case SEMI:
          builder.project(topProjectExpressions, correlate.getRowType().getFieldNames());
      }

      call.transformTo(builder.build());
    }
  }

  /**
   * Visitor for RexNode which replaces {@link RexFieldAccess}
   * with a reference to the field used in {@link RexFieldAccess}.
   */
  private static class RexFieldAccessReplacer extends RexShuttle {
    private final RelBuilder builder;

    public RexFieldAccessReplacer(RelBuilder builder) {
      this.builder = builder;
    }

    @Override
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      return builder.field(fieldAccess.getField().getName());
    }
  }
}
