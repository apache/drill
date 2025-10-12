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

    // Check if we have GROUPING, GROUPING_ID, or GROUP_ID functions
    // These functions need the $g column to be preserved in the output
    // We need to separate them from regular aggregate functions but preserve their original positions
    List<AggregateCall> regularAggCalls = new ArrayList<>();
    List<Integer> groupingFunctionPositions = new ArrayList<>();  // Original positions in aggCalls
    List<AggregateCall> groupingFunctionCalls = new ArrayList<>();
    boolean hasGroupingFunctions = false;

    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall aggCall = aggCalls.get(i);
      org.apache.calcite.sql.SqlKind kind = aggCall.getAggregation().getKind();
      if (kind == org.apache.calcite.sql.SqlKind.GROUPING ||
          kind == org.apache.calcite.sql.SqlKind.GROUPING_ID ||
          kind == org.apache.calcite.sql.SqlKind.GROUP_ID) {
        hasGroupingFunctions = true;
        groupingFunctionPositions.add(i);
        groupingFunctionCalls.add(aggCall);
      } else {
        regularAggCalls.add(aggCall);
      }
    }

    // Create a separate aggregate for each grouping set
    // Process grouping sets in order of decreasing cardinality (more columns first)
    // This ensures that for UNION ALL, branches with actual data types come before
    // branches with NULL placeholders, helping with type inference
    //
    // For GROUP_ID support, we need to track duplicate grouping sets and assign sequence numbers
    List<RelNode> aggregates = new ArrayList<>();
    List<ImmutableBitSet> sortedGroupSets = new ArrayList<>(groupSets);
    // Sort by cardinality descending (more grouping columns first)
    sortedGroupSets.sort((a, b) -> Integer.compare(b.cardinality(), a.cardinality()));

    // Track GROUP_ID for duplicate grouping sets
    // Map from grouping set to the count of times we've seen it so far
    java.util.Map<ImmutableBitSet, Integer> groupSetOccurrences = new java.util.HashMap<>();
    List<Integer> groupIds = new ArrayList<>();  // GROUP_ID value for each position in sortedGroupSets

    for (int i = 0; i < sortedGroupSets.size(); i++) {
      ImmutableBitSet groupSet = sortedGroupSets.get(i);

      // Track GROUP_ID: how many times have we seen this grouping set before?
      int groupId = groupSetOccurrences.getOrDefault(groupSet, 0);
      groupIds.add(groupId);
      groupSetOccurrences.put(groupSet, groupId + 1);

      // Create the aggregate for this grouping set
      // Use regularAggCalls (without GROUPING functions) because GROUPING functions
      // will be evaluated later using the $g column
      Aggregate newAggregate;
      if (aggregate instanceof DrillAggregateRel) {
        newAggregate = new DrillAggregateRel(
            cluster,
            aggregate.getTraitSet(),
            input,
            groupSet,
            ImmutableList.of(groupSet),
            regularAggCalls);
      } else {
        newAggregate = aggregate.copy(
            aggregate.getTraitSet(),
            input,
            groupSet,
            ImmutableList.of(groupSet),
            regularAggCalls);
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

      // Add aggregate result columns (only regular aggregates, not GROUPING functions)
      // We'll use the alias from the original aggregate call
      for (int j = 0; j < regularAggCalls.size(); j++) {
        projects.add(rexBuilder.makeInputRef(newAggregate, aggOutputIdx));
        AggregateCall regCall = regularAggCalls.get(j);
        String fieldName = regCall.getName() != null ? regCall.getName() : ("$f" + (fullGroupSet.cardinality() + j));
        fieldNames.add(fieldName);
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

      // Add GROUP_ID column ($group_id) - sequence number for duplicate grouping sets
      int currentGroupId = groupIds.get(i);
      projects.add(rexBuilder.makeLiteral(currentGroupId, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), true));
      fieldNames.add("$group_id");

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

    // Create final project
    // If there are GROUPING functions, we need to:
    // 1. Add grouping columns
    // 2. Add aggregate results in their ORIGINAL order, inserting GROUPING function
    //    expressions at their original positions
    // 3. Do NOT include the $g and $group_id columns themselves
    // If there are NO GROUPING functions, just strip the $g and $group_id columns
    List<RexNode> finalProjects = new ArrayList<>();
    List<String> finalFieldNames = new ArrayList<>();

    int numFields = unionResult.getRowType().getFieldCount();

    // Add grouping columns (they come first in the output)
    for (int i = 0; i < fullGroupSet.cardinality(); i++) {
      finalProjects.add(rexBuilder.makeInputRef(unionResult, i));
      finalFieldNames.add(unionResult.getRowType().getFieldList().get(i).getName());
    }

    // If we have GROUPING functions, we need to interleave regular aggregates and GROUPING functions
    // in their original positions
    if (hasGroupingFunctions) {
      // Each GROUPING function call needs to be converted to an expression that
      // extracts the appropriate bits from the $g column or references $group_id column
      RexNode gColumnRef = rexBuilder.makeInputRef(unionResult, numFields - 2); // $g is second to last
      RexNode groupIdColumnRef = rexBuilder.makeInputRef(unionResult, numFields - 1); // $group_id is last

      // Build a map from original positions to GROUPING function calls
      java.util.Map<Integer, AggregateCall> groupingFuncMap = new java.util.HashMap<>();
      for (int i = 0; i < groupingFunctionPositions.size(); i++) {
        groupingFuncMap.put(groupingFunctionPositions.get(i), groupingFunctionCalls.get(i));
      }

      // Now add aggregate columns in their original order
      int regularAggIndex = fullGroupSet.cardinality(); // Index in unionResult for next regular aggregate
      for (int origPos = 0; origPos < aggCalls.size(); origPos++) {
        if (groupingFuncMap.containsKey(origPos)) {
          // This position had a GROUPING function - create the expression
          AggregateCall groupingCall = groupingFuncMap.get(origPos);
          org.apache.calcite.sql.SqlKind kind = groupingCall.getAggregation().getKind();
          String funcName = groupingCall.getAggregation().getName();

          if ("GROUPING".equals(funcName)) {
            // GROUPING(column) - extracts a single bit from $g
            // Returns 1 if the column is aggregated (NULL in output), 0 otherwise
            if (groupingCall.getArgList().size() != 1) {
              throw new RuntimeException("GROUPING function expects exactly 1 argument");
            }

            int columnIndex = groupingCall.getArgList().get(0);
            // Find the position of this column in the full group set
            int bitPosition = 0;
            for (int col : fullGroupSet) {
              if (col == columnIndex) {
                break;
              }
              bitPosition++;
            }

            // Extract the bit: (g / 2^bitPosition) % 2
            // This is equivalent to (g >> bitPosition) & 1 but uses operators available in Calcite
            RexNode divisor = rexBuilder.makeLiteral(
                1 << bitPosition,  // 2^bitPosition
                typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER),
                true);

            RexNode divided = rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE,
                gColumnRef,
                divisor);

            RexNode extractBit = rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.MOD,
                divided,
                rexBuilder.makeLiteral(2, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), true));

            finalProjects.add(extractBit);
            String fieldName = groupingCall.getName() != null ? groupingCall.getName() : "EXPR$" + finalFieldNames.size();
            finalFieldNames.add(fieldName);

          } else if ("GROUPING_ID".equals(funcName)) {
            // GROUPING_ID(col1, col2, ...) - combines multiple bits from $g
            // Returns a bitmap where bit i corresponds to column i
            if (groupingCall.getArgList().isEmpty()) {
              throw new RuntimeException("GROUPING_ID function expects at least 1 argument");
            }

            // Create a mask and shift expression to extract the relevant bits
            RexNode result = null;
            for (int i = 0; i < groupingCall.getArgList().size(); i++) {
              int columnIndex = groupingCall.getArgList().get(i);

              // Find the position of this column in the full group set
              int bitPosition = 0;
              for (int col : fullGroupSet) {
                if (col == columnIndex) {
                  break;
                }
                bitPosition++;
              }

              // Extract the bit: (g / 2^bitPosition) % 2
              // This is equivalent to (g >> bitPosition) & 1 but uses operators available in Calcite
              RexNode divisor = rexBuilder.makeLiteral(
                  1 << bitPosition,  // 2^bitPosition
                  typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER),
                  true);

              RexNode divided = rexBuilder.makeCall(
                  org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE,
                  gColumnRef,
                  divisor);

              RexNode extractBit = rexBuilder.makeCall(
                  org.apache.calcite.sql.fun.SqlStdOperatorTable.MOD,
                  divided,
                  rexBuilder.makeLiteral(2, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), true));

              // Shift to correct position in result: bit * 2^(args.size() - 1 - i)
              // This is equivalent to bit << (args.size() - 1 - i)
              int resultBitPos = groupingCall.getArgList().size() - 1 - i;
              RexNode bitInPosition;
              if (resultBitPos > 0) {
                RexNode multiplier = rexBuilder.makeLiteral(
                    1 << resultBitPos,  // 2^resultBitPos
                    typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER),
                    true);
                bitInPosition = rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY,
                    extractBit,
                    multiplier);
              } else {
                bitInPosition = extractBit;
              }

              // Combine with previous bits using addition (instead of OR, since bits don't overlap)
              if (result == null) {
                result = bitInPosition;
              } else {
                result = rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS,
                    result,
                    bitInPosition);
              }
            }

            finalProjects.add(result);
            String fieldName = groupingCall.getName() != null ? groupingCall.getName() : "EXPR$" + finalFieldNames.size();
            finalFieldNames.add(fieldName);

          } else if ("GROUP_ID".equals(funcName)) {
            // GROUP_ID() - returns sequence number for duplicate grouping sets
            // Simply reference the $group_id column we added earlier
            finalProjects.add(groupIdColumnRef);
            String fieldName = groupingCall.getName() != null ? groupingCall.getName() : "EXPR$" + finalFieldNames.size();
            finalFieldNames.add(fieldName);
          }
        } else {
          // This position had a regular aggregate - reference it from unionResult
          finalProjects.add(rexBuilder.makeInputRef(unionResult, regularAggIndex));
          finalFieldNames.add(unionResult.getRowType().getFieldList().get(regularAggIndex).getName());
          regularAggIndex++;
        }
      }
    } else {
      // No GROUPING functions - just add all regular aggregate columns
      // Strip both $g (numFields - 2) and $group_id (numFields - 1) columns
      for (int i = fullGroupSet.cardinality(); i < numFields - 2; i++) {
        finalProjects.add(rexBuilder.makeInputRef(unionResult, i));
        finalFieldNames.add(unionResult.getRowType().getFieldList().get(i).getName());
      }
    }

    RelNode result = call.builder()
        .push(unionResult)
        .project(finalProjects, finalFieldNames, false)
        .build();

    call.transformTo(result);
  }
}
