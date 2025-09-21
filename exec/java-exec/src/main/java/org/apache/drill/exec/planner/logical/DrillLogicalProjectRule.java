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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.linq4j.Ord;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.common.collect.Lists;

/**
 * Rule that ensures LogicalProject nodes can be created with LOGICAL convention.
 * This handles the case where Calcite needs a LogicalProject with LOGICAL convention
 * but can't find a way to create it from NONE convention.
 */
public class DrillLogicalProjectRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillLogicalProjectRule();

  private DrillLogicalProjectRule() {
    super(operand(LogicalProject.class, any()),
        DrillRelFactories.LOGICAL_BUILDER, "DrillLogicalProjectRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    // Match LogicalProject nodes that need conversion to DRILL_LOGICAL
    return project.getConvention() != DrillRel.DRILL_LOGICAL;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    final RelNode input = project.getInput();

    // Convert input to DRILL_LOGICAL convention, preserving collation
    final RelNode convertedInput = convert(input, input.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify());

    // Create collation mapping from input fields to output fields
    final Map<Integer, Integer> collationMap = getCollationMap(project);

    // Get input collation and map it to output collation
    final RelCollation inputCollation = project.getTraitSet().getCollation();
    final RelCollation outputCollation = convertRelCollation(inputCollation, collationMap);

    // Create traits with mapped collation
    final RelTraitSet traits = project.getTraitSet()
        .plus(DrillRel.DRILL_LOGICAL)
        .replace(outputCollation)
        .simplify();

    call.transformTo(new DrillProjectRel(
        project.getCluster(),
        traits,
        convertedInput,
        project.getProjects(),
        project.getRowType()));
  }

  private Map<Integer, Integer> getCollationMap(LogicalProject project) {
    Map<Integer, Integer> m = new HashMap<>();

    for (Ord<RexNode> node : Ord.zip(project.getProjects())) {
      // For collation, only $0 and cast($0) will keep the sort-ness after projection
      if (node.e instanceof RexInputRef) {
        m.put(((RexInputRef) node.e).getIndex(), node.i);
      } else if (node.e.isA(SqlKind.CAST)) {
        RexNode operand = ((RexCall) node.e).getOperands().get(0);
        if (operand instanceof RexInputRef) {
          m.put(((RexInputRef) operand).getIndex(), node.i);
        }
      }
    }
    return m;
  }

  private RelCollation convertRelCollation(RelCollation src, Map<Integer, Integer> inToOut) {
    if (src == null || src.getFieldCollations().isEmpty()) {
      return RelCollations.of();
    }

    List<RelFieldCollation> newFields = Lists.newArrayList();

    for (RelFieldCollation field : src.getFieldCollations()) {
      if (inToOut.containsKey(field.getFieldIndex())) {
        newFields.add(new RelFieldCollation(inToOut.get(field.getFieldIndex()),
                                          field.getDirection(), field.nullDirection));
      }
    }

    if (newFields.isEmpty()) {
      return RelCollations.of();
    } else {
      return RelCollations.of(newFields);
    }
  }
}