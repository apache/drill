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
 * Currently, the $g column is generated internally but stripped from the final output.
 *
 * TODO: Implement GROUPING() and GROUPING_ID() functions by:
 * 1. Detecting these functions in the SELECT list during expansion
 * 2. Rewriting them to reference the $g column (e.g., GROUPING(a) becomes bit extraction from $g)
 * 3. Preserving the $g column in the output when these functions are used
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

    // Check if we have GROUPING or GROUPING_ID functions (they would appear as aggregate calls)
    // For now, we always generate the $g column but strip it at the end
    // TODO: Detect actual GROUPING function usage and preserve $g when needed
    boolean hasGroupingFunctions = false;

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
          // Column is NOT in this grouping set - project a typed NULL literal
          // Use the expected output type from the original aggregate to create a properly typed NULL
          // This prevents type inference issues in the UNION ALL
          org.apache.calcite.rel.type.RelDataType nullType =
              aggregate.getRowType().getFieldList().get(outputColIdx).getType();
          // Use makeLiteral with null value and explicit type to create a typed NULL
          projects.add(rexBuilder.makeNullLiteral(nullType));
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

      // Add grouping ID column ($g)
      // The grouping ID is a bitmap where bit i is 1 if column i is NOT in the grouping set
      int groupingId = 0;
      int bitPosition = 0;
      for (int col : fullGroupSet) {
        if (!groupSet.get(col)) {
          groupingId |= (1 << bitPosition);
        }
        bitPosition++;
      }
      projects.add(rexBuilder.makeLiteral(groupingId, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), true));
      fieldNames.add("$g");

      // Create the project
      RelNode project = call.builder()
          .push(newAggregate)
          .project(projects, fieldNames, false)  // false = don't force alias names
          .build();

      aggregates.add(project);
    }

    // Union all the aggregates
    RelNode unionResult;
    if (aggregates.size() == 1) {
      unionResult = aggregates.get(0);
    } else {
      // Create DrillUnionRel directly instead of LogicalUnion
      // This allows us to set the isGroupingSetsExpansion flag immediately
      try {
        List<RelNode> convertedInputs = new ArrayList<>();
        for (RelNode agg : aggregates) {
          // Convert each input to Drill logical convention
          RelNode converted = convert(agg, agg.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify());
          convertedInputs.add(converted);
        }

        unionResult = new DrillUnionRel(cluster,
            cluster.traitSet().plus(DrillRel.DRILL_LOGICAL),
            convertedInputs,
            true /* all */,
            true /* check compatibility */,
            true /* isGroupingSetsExpansion */);
      } catch (org.apache.calcite.rel.InvalidRelException e) {
        throw new RuntimeException("Failed to create DrillUnionRel for grouping sets expansion", e);
      }
    }

    // Now strip the $g column from the final output to match the expected schema
    // We needed it in the union branches for type inference, but it shouldn't be in the final result
    // Create a project that excludes the last column ($g)
    List<RexNode> finalProjects = new ArrayList<>();
    List<String> finalFieldNames = new ArrayList<>();

    int numFields = unionResult.getRowType().getFieldCount();
    for (int i = 0; i < numFields - 1; i++) {  // Exclude last field ($g)
      finalProjects.add(rexBuilder.makeInputRef(unionResult, i));
      finalFieldNames.add(aggregate.getRowType().getFieldList().get(i).getName());
    }

    RelNode result = call.builder()
        .push(unionResult)
        .project(finalProjects, finalFieldNames, false)
        .build();

    call.transformTo(result);
  }
}
