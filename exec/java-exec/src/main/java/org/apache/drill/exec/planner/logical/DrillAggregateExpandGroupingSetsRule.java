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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that expands GROUPING SETS, ROLLUP, and CUBE into a UNION ALL
 * of multiple aggregates, each with a single grouping set.
 *
 * This rule converts:
 *   SELECT a, b, SUM(c) FROM t GROUP BY GROUPING SETS ((a, b), (a), ())
 *
 * Into:
 *   SELECT a, b, SUM(c), 0 AS $g FROM t GROUP BY a, b
 *   UNION ALL
 *   SELECT a, null, SUM(c), 1 AS $g FROM t GROUP BY a
 *   UNION ALL
 *   SELECT null, null, SUM(c), 3 AS $g FROM t GROUP BY ()
 *
 * The $g column is the grouping ID that can be used by GROUPING() and GROUPING_ID() functions.
 */
public class DrillAggregateExpandGroupingSetsRule extends RelOptRule {

  public static final DrillAggregateExpandGroupingSetsRule INSTANCE =
      new DrillAggregateExpandGroupingSetsRule();

  private DrillAggregateExpandGroupingSetsRule() {
    super(operand(Aggregate.class, any()), DrillRelFactories.LOGICAL_BUILDER,
        "DrillAggregateExpandGroupingSetsRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);

    // Only match aggregates with multiple grouping sets
    // Also only match logical aggregates (not physical ones)
    return aggregate.getGroupSets().size() > 1
        && (aggregate instanceof DrillAggregateRel || aggregate instanceof LogicalAggregate);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelOptCluster cluster = aggregate.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    // Get the input
    final RelNode input = aggregate.getInput();
    final List<ImmutableBitSet> groupSets = aggregate.getGroupSets();
    final ImmutableBitSet fullGroupSet = aggregate.getGroupSet();
    final List<AggregateCall> aggCalls = aggregate.getAggCallList();

    // Create a separate aggregate for each grouping set
    // Process grouping sets in order of decreasing cardinality (more columns first)
    // This ensures that for UNION ALL, branches with actual data types come before
    // branches with NULL placeholders, helping with type inference
    List<RelNode> aggregates = new ArrayList<>();
    List<ImmutableBitSet> sortedGroupSets = new ArrayList<>(groupSets);
    // Sort by cardinality descending (more grouping columns first)
    sortedGroupSets.sort((a, b) -> Integer.compare(b.cardinality(), a.cardinality()));

    for (int i = 0; i < sortedGroupSets.size(); i++) {
      ImmutableBitSet groupSet = sortedGroupSets.get(i);

      // Create the aggregate for this grouping set
      Aggregate newAggregate;
      if (aggregate instanceof DrillAggregateRel) {
        newAggregate = new DrillAggregateRel(
            cluster,
            aggregate.getTraitSet(),
            input,
            groupSet,
            ImmutableList.of(groupSet),
            aggCalls);
      } else {
        newAggregate = aggregate.copy(
            aggregate.getTraitSet(),
            input,
            groupSet,
            ImmutableList.of(groupSet),
            aggCalls);
      }

      // Create a project to add NULLs for missing grouping columns
      List<RexNode> projects = new ArrayList<>();
      List<String> fieldNames = new ArrayList<>();

      // Add grouping columns (with NULLs for columns not in this grouping set)
      int aggOutputIdx = 0;
      int outputColIdx = 0; // Index in the final output row type
      for (int col : fullGroupSet) {
        if (groupSet.get(col)) {
          // Column is in this grouping set - project it directly from the aggregate output
          RexNode inputRef = rexBuilder.makeInputRef(newAggregate, aggOutputIdx);
          projects.add(inputRef);
          aggOutputIdx++;
        } else {
          // Column is NOT in this grouping set - project a NULL literal
          // Use constantNull() which should produce an untyped NULL
          projects.add(rexBuilder.constantNull());
        }
        fieldNames.add(aggregate.getRowType().getFieldList().get(outputColIdx).getName());
        outputColIdx++;
      }

      // Add aggregate result columns
      for (int j = 0; j < aggCalls.size(); j++) {
        projects.add(rexBuilder.makeInputRef(newAggregate, aggOutputIdx));
        fieldNames.add(aggregate.getRowType().getFieldList().get(fullGroupSet.cardinality() + j).getName());
        aggOutputIdx++;
      }

      // Create the project
      RelNode project = call.builder()
          .push(newAggregate)
          .project(projects, fieldNames, false)  // false = don't force alias names
          .build();

      aggregates.add(project);
    }

    // Union all the aggregates
    RelNode result;
    if (aggregates.size() == 1) {
      result = aggregates.get(0);
    } else {
      result = call.builder()
          .pushAll(aggregates)
          .union(true, aggregates.size())
          .build();
    }

    call.transformTo(result);
  }
}
