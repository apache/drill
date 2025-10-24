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
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Planner rule that expands GROUPING SETS, ROLLUP, and CUBE into a UNION ALL
 * of multiple aggregates, each with a single grouping set.
 * <p>
 * This rule converts:
 *   SELECT a, b, SUM(c) FROM t GROUP BY GROUPING SETS ((a, b), (a), ())
 * <p>
 * Into:
 *   SELECT a, b, SUM(c), 0 AS $g FROM t GROUP BY a, b
 *   UNION ALL
 *   SELECT a, null, SUM(c), 1 AS $g FROM t GROUP BY a
 *   UNION ALL
 *   SELECT null, null, SUM(c), 3 AS $g FROM t GROUP BY ()
 * <p>
 * The $g column is the grouping ID that can be used by GROUPING() and GROUPING_ID() functions.
 * Currently, the $g column is generated internally but stripped from the final output.
 */
public class DrillAggregateExpandGroupingSetsRule extends RelOptRule {

  public static final DrillAggregateExpandGroupingSetsRule INSTANCE =
      new DrillAggregateExpandGroupingSetsRule();
  public static final String GROUPING_ID_COLUMN_NAME = "$g";
  public static final String GROUP_ID_COLUMN_NAME = "$group_id";
  public static final String EXPRESSION_COLUMN_PLACEHOLDER = "EXPR$";

  private DrillAggregateExpandGroupingSetsRule() {
    super(operand(Aggregate.class, any()), DrillRelFactories.LOGICAL_BUILDER,
        "DrillAggregateExpandGroupingSetsRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    return aggregate.getGroupSets().size() > 1
        && (aggregate instanceof DrillAggregateRel || aggregate instanceof LogicalAggregate);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelOptCluster cluster = aggregate.getCluster();

    GroupingFunctionAnalysis analysis = analyzeGroupingFunctions(aggregate.getAggCallList());
    GroupingSetOrderingResult ordering = sortAndAssignGroupIds(aggregate.getGroupSets());

    List<RelNode> perGroupAggregates = new ArrayList<>();
    for (int i = 0; i < ordering.sortedGroupSets.size(); i++) {
      perGroupAggregates.add(
          createAggregateForGroupingSet(call, aggregate, ordering.sortedGroupSets.get(i),
              ordering.groupIds.get(i), analysis.regularAggCalls));
    }

    RelNode unionResult = buildUnion(cluster, perGroupAggregates);
    RelNode result = buildFinalProject(call, unionResult, aggregate, analysis);

    call.transformTo(result);
  }

  /**
   * Encapsulates analysis results of aggregate calls to determine
   * which are regular aggregates and which are grouping-related
   * functions (GROUPING, GROUPING_ID, GROUP_ID).
   */
  private static class GroupingFunctionAnalysis {
    final boolean hasGroupingFunctions;
    final List<AggregateCall> regularAggCalls;
    final List<AggregateCall> groupingFunctionCalls;
    final List<Integer> groupingFunctionPositions;

    GroupingFunctionAnalysis(List<AggregateCall> regularAggCalls,
        List<AggregateCall> groupingFunctionCalls,
        List<Integer> groupingFunctionPositions) {
      this.hasGroupingFunctions = !groupingFunctionPositions.isEmpty();
      this.regularAggCalls = regularAggCalls;
      this.groupingFunctionCalls = groupingFunctionCalls;
      this.groupingFunctionPositions = groupingFunctionPositions;
    }
  }

  /**
   * Holds the sorted grouping sets (largest first) and their assigned group IDs.
   */
  private static class GroupingSetOrderingResult {
    final List<ImmutableBitSet> sortedGroupSets;
    final List<Integer> groupIds;
    GroupingSetOrderingResult(List<ImmutableBitSet> sortedGroupSets, List<Integer> groupIds) {
      this.sortedGroupSets = sortedGroupSets;
      this.groupIds = groupIds;
    }
  }

  /**
   * Analyzes aggregate calls to identify which ones are GROUPING-related functions.
   *
   * @param aggCalls list of aggregate calls in the original aggregate
   * @return structure classifying grouping and non-grouping calls
   */
  private GroupingFunctionAnalysis analyzeGroupingFunctions(List<AggregateCall> aggCalls) {
    List<AggregateCall> regularAggCalls = new ArrayList<>();
    List<AggregateCall> groupingFunctionCalls = new ArrayList<>();
    List<Integer> groupingFunctionPositions = new ArrayList<>();

    for (int i = 0; i < aggCalls.size(); i++) {
      AggregateCall aggCall = aggCalls.get(i);
      SqlKind kind = aggCall.getAggregation().getKind();
      switch (kind) {
      case GROUPING:
      case GROUPING_ID:
      case GROUP_ID:
        groupingFunctionPositions.add(i);
        groupingFunctionCalls.add(aggCall);
        break;
      default:
        regularAggCalls.add(aggCall);
      }
    }

    return new GroupingFunctionAnalysis(regularAggCalls,
        groupingFunctionCalls, groupingFunctionPositions);
  }

  /**
   * Sorts the given grouping sets in descending order of their cardinality
   * and assigns group IDs to each grouping set based on their occurrences.
   *
   * @param groupSets a list of grouping sets represented as ImmutableBitSet instances
   * @return a GroupingSetOrderingResult containing the sorted grouping sets and their assigned group IDs
   */
  private GroupingSetOrderingResult sortAndAssignGroupIds(List<ImmutableBitSet> groupSets) {
    List<ImmutableBitSet> sortedGroupSets = new ArrayList<>(groupSets);
    sortedGroupSets.sort((a, b) -> Integer.compare(b.cardinality(), a.cardinality()));

    Map<ImmutableBitSet, Integer> groupSetOccurrences = new HashMap<>();
    List<Integer> groupIds = new ArrayList<>();

    for (ImmutableBitSet groupSet : sortedGroupSets) {
      int groupId = groupSetOccurrences.getOrDefault(groupSet, 0);
      groupIds.add(groupId);
      groupSetOccurrences.put(groupSet, groupId + 1);
    }

    return new GroupingSetOrderingResult(sortedGroupSets, groupIds);
  }

  /**
   * Creates a new aggregate relational node for a specific grouping set. This method constructs
   * the necessary aggregation logic and ensures proper handling of grouping columns, aggregate
   * calls, and additional metadata such as grouping and group IDs.
   *
   * @param call the RelOptRuleCall instance being processed
   * @param originalAgg the original aggregate relational expression
   * @param groupSet the grouping set to be handled in the new aggregate
   * @param groupId the unique identifier associated with this specific grouping set
   * @param regularAggCalls the list of regular aggregate calls to be included in the aggregate
   * @return a RelNode representing the newly created aggregate for the specified grouping set
   */
  private RelNode createAggregateForGroupingSet(
      RelOptRuleCall call,
      Aggregate originalAgg,
      ImmutableBitSet groupSet,
      int groupId,
      List<AggregateCall> regularAggCalls) {

    ImmutableBitSet fullGroupSet = originalAgg.getGroupSet();
    RelOptCluster cluster = originalAgg.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    RelNode input = originalAgg.getInput();

    Aggregate newAggregate;
    if (originalAgg instanceof DrillAggregateRel) {
      newAggregate = new DrillAggregateRel(cluster, originalAgg.getTraitSet(), input,
          groupSet, ImmutableList.of(groupSet), regularAggCalls);
    } else {
      newAggregate = originalAgg.copy(originalAgg.getTraitSet(), input, groupSet,
          ImmutableList.of(groupSet), regularAggCalls);
    }

    List<RexNode> projects = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    int aggOutputIdx = 0;
    int outputColIdx = 0;

    // Populate grouping columns (nulls for omitted columns)
    for (int col : fullGroupSet) {
      if (groupSet.get(col)) {
        projects.add(rexBuilder.makeInputRef(newAggregate, aggOutputIdx++));
      } else {
        RelDataType nullType = originalAgg.getRowType().getFieldList().get(outputColIdx).getType();
        projects.add(rexBuilder.makeNullLiteral(nullType));
      }
      fieldNames.add(originalAgg.getRowType().getFieldList().get(outputColIdx++).getName());
    }

    // Add regular aggregates
    for (AggregateCall regCall : regularAggCalls) {
      projects.add(rexBuilder.makeInputRef(newAggregate, aggOutputIdx++));
      fieldNames.add(regCall.getName() != null ? regCall.getName() : "agg$" + aggOutputIdx);
    }

    // Add grouping ID ($g)
    int groupingId = computeGroupingId(fullGroupSet, groupSet);
    projects.add(rexBuilder.makeLiteral(groupingId,
        typeFactory.createSqlType(SqlTypeName.INTEGER), true));
    fieldNames.add(GROUPING_ID_COLUMN_NAME);

    // Add group ID ($group_id)
    projects.add(rexBuilder.makeLiteral(groupId,
        typeFactory.createSqlType(SqlTypeName.INTEGER), true));
    fieldNames.add(GROUP_ID_COLUMN_NAME);

    return call.builder().push(newAggregate).project(projects, fieldNames, false).build();
  }

  private int computeGroupingId(ImmutableBitSet fullGroupSet, ImmutableBitSet groupSet) {
    int id = 0;
    int bit = 0;
    for (int col : fullGroupSet) {
      if (!groupSet.get(col)) {
        id |= (1 << bit);
      }
      bit++;
    }
    return id;
  }

  /**
   * Builds a union of the given aggregate relational nodes. If there is only one
   * aggregate node, it returns that node directly. Otherwise, it constructs a
   * union relational expression containing all the provided aggregate nodes.
   *
   * @param cluster the optimization cluster in which the relational node resides
   * @param aggregates a list of aggregate relational nodes to be combined into a union
   * @return the resultant union relational node if multiple nodes are provided;
   *         otherwise, the single aggregate node from the input list
   * @throws RuntimeException if union creation fails due to invalid relational state
   */
  private RelNode buildUnion(RelOptCluster cluster, List<RelNode> aggregates) {
    if (aggregates.size() == 1) {
      return aggregates.get(0);
    }
    try {
      List<RelNode> convertedInputs = new ArrayList<>();
      for (RelNode agg : aggregates) {
        convertedInputs.add(convert(agg, agg.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify()));
      }
      return new DrillUnionRel(cluster,
          cluster.traitSet().plus(DrillRel.DRILL_LOGICAL),
          convertedInputs,
          true,
          true,
          true);
    } catch (InvalidRelException e) {
      throw new RuntimeException("Failed to create DrillUnionRel", e);
    }
  }

  /**
   * Builds the final projection for the result of the aggregation, incorporating the necessary
   * output columns such as the grouping functions and the aggregation results.
   *
   * @param call the RelOptRuleCall instance being processed
   * @param unionResult the relational expression resulting from the union of partial aggregates
   * @param aggregate the original Aggregate relational expression
   * @param analysis the analysis results classifying regular and grouping-related aggregate calls
   * @return the relational expression with the final projection applied
   */
  private RelNode buildFinalProject(
      RelOptRuleCall call,
      RelNode unionResult,
      Aggregate aggregate,
      GroupingFunctionAnalysis analysis) {

    RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();
    ImmutableBitSet fullGroupSet = aggregate.getGroupSet();
    List<RexNode> finalProjects = new ArrayList<>();
    List<String> finalFieldNames = new ArrayList<>();
    int numFields = unionResult.getRowType().getFieldCount();

    for (int i = 0; i < fullGroupSet.cardinality(); i++) {
      finalProjects.add(rexBuilder.makeInputRef(unionResult, i));
      finalFieldNames.add(unionResult.getRowType().getFieldList().get(i).getName());
    }

    if (analysis.hasGroupingFunctions) {
      RexNode gColumnRef = rexBuilder.makeInputRef(unionResult, numFields - 2);
      RexNode groupIdColumnRef = rexBuilder.makeInputRef(unionResult, numFields - 1);
      Map<Integer, AggregateCall> groupingFuncMap = new HashMap<>();
      for (int i = 0; i < analysis.groupingFunctionPositions.size(); i++) {
        groupingFuncMap.put(analysis.groupingFunctionPositions.get(i),
            analysis.groupingFunctionCalls.get(i));
      }

      int regularAggIndex = fullGroupSet.cardinality();
      for (int origPos = 0; origPos < aggregate.getAggCallList().size(); origPos++) {
        if (groupingFuncMap.containsKey(origPos)) {
          AggregateCall groupingCall = groupingFuncMap.get(origPos);
          String funcName = groupingCall.getAggregation().getName();
          if ("GROUPING".equals(funcName)) {
            processGrouping(groupingCall, fullGroupSet, rexBuilder, typeFactory,
                gColumnRef, finalProjects, finalFieldNames);
          } else if ("GROUPING_ID".equals(funcName)) {
            processGroupingId(groupingCall, fullGroupSet, rexBuilder, typeFactory,
                gColumnRef, finalProjects, finalFieldNames);
          } else if ("GROUP_ID".equals(funcName)) {
            finalProjects.add(groupIdColumnRef);
            String fieldName = groupingCall.getName() != null
                ? groupingCall.getName()
                : EXPRESSION_COLUMN_PLACEHOLDER + finalFieldNames.size();
            finalFieldNames.add(fieldName);
          }
        } else {
          finalProjects.add(rexBuilder.makeInputRef(unionResult, regularAggIndex));
          finalFieldNames.add(unionResult.getRowType().getFieldList().get(regularAggIndex).getName());
          regularAggIndex++;
        }
      }
    } else {
      for (int i = fullGroupSet.cardinality(); i < numFields - 2; i++) {
        finalProjects.add(rexBuilder.makeInputRef(unionResult, i));
        finalFieldNames.add(unionResult.getRowType().getFieldList().get(i).getName());
      }
    }

    return call.builder().push(unionResult).project(finalProjects, finalFieldNames, false).build();
  }

  /**
   * Processes the GROUPING aggregate function by extracting the bit representing
   * whether each column is aggregated or not and appends the computed RexNode
   * projection and the corresponding field name to the provided lists.
   *
   * @param groupingCall the GROUPING aggregate function call to process
   * @param fullGroupSet the complete set of grouping keys for the aggregation
   * @param rexBuilder the RexBuilder instance used to construct RexNode expressions
   * @param typeFactory the data type factory used for creating type-specific literals
   * @param gColumnRef the RexNode reference to the grouping column
   * @param finalProjects the list to store the constructed RexNode projections
   * @param finalFieldNames the list to store the corresponding field names
   */
  private void processGrouping(AggregateCall groupingCall,
      ImmutableBitSet fullGroupSet,
      RexBuilder rexBuilder,
      RelDataTypeFactory typeFactory,
      RexNode gColumnRef,
      List<RexNode> finalProjects,
      List<String> finalFieldNames) {

    if (groupingCall.getArgList().size() != 1) {
      throw new RuntimeException("GROUPING() expects exactly 1 argument");
    }

    int columnIndex = groupingCall.getArgList().get(0);
    int bitPosition = 0;
    for (int col : fullGroupSet) {
      if (col == columnIndex) {
        break;
      }
      bitPosition++;
    }

    RexNode divisor = rexBuilder.makeLiteral(
        1 << bitPosition, typeFactory.createSqlType(SqlTypeName.INTEGER), true);

    RexNode divided = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, gColumnRef, divisor);
    RexNode extractBit = rexBuilder.makeCall(SqlStdOperatorTable.MOD, divided,
        rexBuilder.makeLiteral(2, typeFactory.createSqlType(SqlTypeName.INTEGER), true));

    finalProjects.add(extractBit);
    String fieldName = groupingCall.getName() != null
        ? groupingCall.getName()
        : "EXPR$" + finalFieldNames.size();
    finalFieldNames.add(fieldName);
  }

  /**
   * Processes the GROUPING_ID aggregate function by computing a bitmask
   * based on the provided grouping columns and full group set. Constructs
   * the corresponding RexNode representation for the GROUPING_ID function
   * and appends it to the final projection and field names list.
   *
   * @param groupingCall the GROUPING_ID aggregate function call to process
   * @param fullGroupSet the complete set of grouping keys for the aggregation
   * @param rexBuilder the RexBuilder instance used to construct RexNode expressions
   * @param typeFactory the data type factory for creating type-specific literals
   * @param gColumnRef the RexNode reference to the group column
   * @param finalProjects the list to which the computed RexNode is added
   * @param finalFieldNames the list to which the corresponding field name is added
   */
  private void processGroupingId(AggregateCall groupingCall,
      ImmutableBitSet fullGroupSet,
      RexBuilder rexBuilder,
      RelDataTypeFactory typeFactory,
      RexNode gColumnRef,
      List<RexNode> finalProjects,
      List<String> finalFieldNames) {

    if (groupingCall.getArgList().isEmpty()) {
      throw new RuntimeException("GROUPING_ID() expects at least one argument");
    }

    RexNode result = null;
    for (int i = 0; i < groupingCall.getArgList().size(); i++) {
      int columnIndex = groupingCall.getArgList().get(i);
      int bitPosition = 0;
      for (int col : fullGroupSet) {
        if (col == columnIndex) {
          break;
        }
        bitPosition++;
      }

      RexNode divisor = rexBuilder.makeLiteral(1 << bitPosition,
          typeFactory.createSqlType(SqlTypeName.INTEGER), true);

      RexNode divided = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, gColumnRef, divisor);
      RexNode extractBit = rexBuilder.makeCall(SqlStdOperatorTable.MOD, divided,
          rexBuilder.makeLiteral(2, typeFactory.createSqlType(SqlTypeName.INTEGER), true));

      int resultBitPos = groupingCall.getArgList().size() - 1 - i;
      RexNode bitInPosition = (resultBitPos > 0)
          ? rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, extractBit,
          rexBuilder.makeLiteral(1 << resultBitPos,
              typeFactory.createSqlType(SqlTypeName.INTEGER), true))
          : extractBit;

      result = (result == null)
          ? bitInPosition
          : rexBuilder.makeCall(SqlStdOperatorTable.PLUS, result, bitInPosition);
    }

    finalProjects.add(result);
    String fieldName = groupingCall.getName() != null
        ? groupingCall.getName()
        : "EXPR$" + finalFieldNames.size();
    finalFieldNames.add(fieldName);
  }
}
