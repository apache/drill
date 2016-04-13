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

package org.apache.drill.exec.planner.logical;


import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.trace.CalciteTrace;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.DrillCalciteSqlAggFunctionWrapper;
import org.apache.drill.exec.planner.sql.DrillCalciteSqlWrapper;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import org.apache.drill.exec.planner.sql.TypeInferenceUtils;
import org.apache.drill.exec.planner.sql.parser.DrillCalciteWrapperUtility;

/**
 * Rule to reduce aggregates to simpler forms. Currently only AVG(x) to
 * SUM(x)/COUNT(x), but eventually will handle others such as STDDEV.
 */
public class DrillReduceAggregatesRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * The singleton.
   */
  public static final DrillReduceAggregatesRule INSTANCE =
      new DrillReduceAggregatesRule(operand(LogicalAggregate.class, any()));

  /**
   * Instance of rule which matches Aggregate on top of Project. This pattern is required to reduce the Quantile syntax:
   * quantile(p, x) -> tdigest_quantile(p, tdigest(x))
   * where p is a literal between 0.0 and 1.0, and x is a field reference
   */
  public static final DrillReduceAggregatesRule INSTANCE_QUANTILE =
          new DrillReduceAggregatesRule(operand(LogicalAggregate.class, operand(Project.class, any())),
                  "DrillReduceQuantile");
  public static final DrillConvertSumToSumZero INSTANCE_SUM =
      new DrillConvertSumToSumZero(operand(DrillAggregateRel.class, any()));

  private static final DrillSqlOperator CastHighOp = new DrillSqlOperator("CastHigh", 1, false,
      new SqlReturnTypeInference() {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
          return TypeInferenceUtils.createCalciteTypeWithNullability(
              opBinding.getTypeFactory(),
              SqlTypeName.ANY,
              opBinding.getOperandType(0).isNullable());
      }
  });

  //~ Constructors -----------------------------------------------------------

  protected DrillReduceAggregatesRule(RelOptRuleOperand operand) {
    super(operand);
  }

  protected DrillReduceAggregatesRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (!super.matches(call)) {
      return false;
    }
    Aggregate oldAggRel = (Aggregate) call.rels[0];
    return containsAvgStddevVarCall(oldAggRel.getAggCallList());
  }

  @Override
  public void onMatch(RelOptRuleCall ruleCall) {
    Aggregate oldAggRel = (Aggregate) ruleCall.rels[0];
    reduceAggs(ruleCall, oldAggRel);
  }

  /**
   * Returns whether any of the aggregates are calls to AVG, STDDEV_*, VAR_*.
   *
   * @param aggCallList List of aggregate calls
   */
  private boolean containsAvgStddevVarCall(List<AggregateCall> aggCallList) {
    for (AggregateCall call : aggCallList) {
      SqlAggFunction sqlAggFunction = DrillCalciteWrapperUtility.extractSqlOperatorFromWrapper(call.getAggregation());
      if (sqlAggFunction instanceof SqlAvgAggFunction
          || sqlAggFunction instanceof SqlSumAggFunction
              || sqlAggFunction.getName().equals("MEDIAN")
              || sqlAggFunction.getName().equals("QUANTILE")) {
        return true;
      }
    }
    return false;
  }

  /*
  private boolean isMatch(AggregateCall call) {
    if (call.getAggregation() instanceof SqlAvgAggFunction) {
      final SqlAvgAggFunction.Subtype subtype =
          ((SqlAvgAggFunction) call.getAggregation()).getSubtype();
      return (subtype == SqlAvgAggFunction.Subtype.AVG);
    }
    return false;
  }
 */

  /**
   * Reduces all calls to AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP in
   * the aggregates list to.
   *
   * <p>It handles newly generated common subexpressions since this was done
   * at the sql2rel stage.
   */
  private void reduceAggs(
      RelOptRuleCall ruleCall,
      Aggregate oldAggRel) {
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    final int nGroups = oldAggRel.getGroupCount();

    List<AggregateCall> newCalls = new ArrayList<AggregateCall>();
    Map<AggregateCall, RexNode> aggCallMapping =
        new HashMap<AggregateCall, RexNode>();

    List<RexNode> projList = new ArrayList<RexNode>();

    // pass through group key
    for (int i = 0; i < nGroups; ++i) {
      projList.add(
          rexBuilder.makeInputRef(
              getFieldType(oldAggRel, i),
              i));
    }

    // List of input expressions. If a particular aggregate needs more, it
    // will add an expression to the end, and we will create an extra
    // project.
    RelNode input = oldAggRel.getInput();
    List<RexNode> inputExprs = new ArrayList<RexNode>();
    for (RelDataTypeField field : input.getRowType().getFieldList()) {
      inputExprs.add(
          rexBuilder.makeInputRef(
              field.getType(), inputExprs.size()));
    }

    // create new agg function calls and rest of project list together
    for (AggregateCall oldCall : oldCalls) {
      projList.add(
          reduceAgg(
              ruleCall, oldCall, newCalls, aggCallMapping, inputExprs));
    }

    final int extraArgCount =
        inputExprs.size() - input.getRowType().getFieldCount();
    if (extraArgCount > 0) {
      input =
          RelOptUtil.createProject(
              input,
              inputExprs,
              CompositeList.of(
                  input.getRowType().getFieldNames(),
                  Collections.<String>nCopies(
                      extraArgCount,
                      null)));
    }
    Aggregate newAggRel =
        newAggregateRel(
            oldAggRel, input, newCalls);

    RelNode projectRel =
        RelOptUtil.createProject(
            newAggRel,
            projList,
            oldAggRel.getRowType().getFieldNames());

    ruleCall.transformTo(projectRel);
  }

  private RexNode reduceAgg(
      RelOptRuleCall ruleCall,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    final Aggregate oldAggRel = (Aggregate) ruleCall.rels[0];
    final SqlAggFunction sqlAggFunction = DrillCalciteWrapperUtility.extractSqlOperatorFromWrapper(oldCall.getAggregation());
    if (sqlAggFunction instanceof SqlSumAggFunction) {
      // replace original SUM(x) with
      // case COUNT(x) when 0 then null else SUM0(x) end
      return reduceSum(oldAggRel, oldCall, newCalls, aggCallMapping);
    }
    if (sqlAggFunction.getName().equals("MEDIAN")) {
      return reduceMedian(oldAggRel, oldCall, newCalls, aggCallMapping);
    }
    if (sqlAggFunction.getName().equals("QUANTILE")) {
      if (ruleCall.rels.length == 2 && ruleCall.rels[1] instanceof Project) {
        return reduceQuantile(ruleCall, oldCall, newCalls, aggCallMapping);
      }
    }
    if (sqlAggFunction instanceof SqlAvgAggFunction) {
      final SqlAvgAggFunction.Subtype subtype = ((SqlAvgAggFunction) sqlAggFunction).getSubtype();
      switch (subtype) {
      case AVG:
        // replace original AVG(x) with SUM(x) / COUNT(x)
        return reduceAvg(
            oldAggRel, oldCall, newCalls, aggCallMapping);
      case STDDEV_POP:
        // replace original STDDEV_POP(x) with
        //   SQRT(
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / COUNT(x))
        return reduceStddev(
            oldAggRel, oldCall, true, true, newCalls, aggCallMapping,
            inputExprs);
      case STDDEV_SAMP:
        // replace original STDDEV_POP(x) with
        //   SQRT(
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END)
        return reduceStddev(
            oldAggRel, oldCall, false, true, newCalls, aggCallMapping,
            inputExprs);
      case VAR_POP:
        // replace original VAR_POP(x) with
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / COUNT(x)
        return reduceStddev(
            oldAggRel, oldCall, true, false, newCalls, aggCallMapping,
            inputExprs);
      case VAR_SAMP:
        // replace original VAR_POP(x) with
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END
        return reduceStddev(
            oldAggRel, oldCall, false, false, newCalls, aggCallMapping,
            inputExprs);
      default:
        throw Util.unexpected(subtype);
      }
    } else {
      // anything else:  preserve original call
      RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
      final int nGroups = oldAggRel.getGroupCount();

      List<RelDataType> oldArgTypes = new ArrayList<>();
      List<Integer> ordinals = oldCall.getArgList();

      assert ordinals.size() <= inputExprs.size();

      for (int ordinal : ordinals) {
        oldArgTypes.add(inputExprs.get(ordinal).getType());
      }

      return rexBuilder.addAggCall(
          oldCall,
          nGroups,
          oldAggRel.indicator,
          newCalls,
          aggCallMapping,
          oldArgTypes);
    }
  }

  private RexNode reduceAvg(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping) {
    final PlannerSettings plannerSettings = (PlannerSettings) oldAggRel.getCluster().getPlanner().getContext();
    final boolean isInferenceEnabled = plannerSettings.isTypeInferenceEnabled();
    final int nGroups = oldAggRel.getGroupCount();
    RelDataTypeFactory typeFactory =
        oldAggRel.getCluster().getTypeFactory();
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    int iAvgInput = oldCall.getArgList().get(0);
    RelDataType avgInputType =
        getFieldType(
            oldAggRel.getInput(),
            iAvgInput);
    RelDataType sumType =
        typeFactory.createTypeWithNullability(
            avgInputType,
            avgInputType.isNullable() || nGroups == 0);
    // SqlAggFunction sumAgg = new SqlSumAggFunction(sumType);
    SqlAggFunction sumAgg = new SqlSumEmptyIsZeroAggFunction();
    AggregateCall sumCall =
        new AggregateCall(
            sumAgg,
            oldCall.isDistinct(),
            oldCall.getArgList(),
            sumType,
            null);
    final SqlCountAggFunction countAgg = (SqlCountAggFunction) SqlStdOperatorTable.COUNT;
    final RelDataType countType = countAgg.getReturnType(typeFactory);
    AggregateCall countCall =
        new AggregateCall(
            countAgg,
            oldCall.isDistinct(),
            oldCall.getArgList(),
            countType,
            null);

    RexNode tmpsumRef =
        rexBuilder.addAggCall(
            sumCall,
            nGroups,
            oldAggRel.indicator,
            newCalls,
            aggCallMapping,
            ImmutableList.of(avgInputType));

    RexNode tmpcountRef =
        rexBuilder.addAggCall(
            countCall,
            nGroups,
            oldAggRel.indicator,
            newCalls,
            aggCallMapping,
            ImmutableList.of(avgInputType));

    RexNode n = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            tmpcountRef, rexBuilder.makeExactLiteral(BigDecimal.ZERO)),
            rexBuilder.constantNull(),
            tmpsumRef);

    // NOTE:  these references are with respect to the output
    // of newAggRel
    /*
    RexNode numeratorRef =
        rexBuilder.makeCall(CastHighOp,
          rexBuilder.addAggCall(
              sumCall,
              nGroups,
              newCalls,
              aggCallMapping,
              ImmutableList.of(avgInputType))
        );
    */
    RexNode numeratorRef = rexBuilder.makeCall(CastHighOp,  n);

    RexNode denominatorRef =
        rexBuilder.addAggCall(
            countCall,
            nGroups,
            oldAggRel.indicator,
            newCalls,
            aggCallMapping,
            ImmutableList.of(avgInputType));
    if(isInferenceEnabled) {
      return rexBuilder.makeCall(
          new DrillSqlOperator(
              "divide",
              2,
              true,
              oldCall.getType()),
          numeratorRef,
          denominatorRef);
    } else {
      final RexNode divideRef =
          rexBuilder.makeCall(
              SqlStdOperatorTable.DIVIDE,
              numeratorRef,
              denominatorRef);
      return rexBuilder.makeCast(
          typeFactory.createSqlType(SqlTypeName.ANY), divideRef);
    }
  }

  private RexNode reduceMedian(
          Aggregate oldAggRel,
          AggregateCall oldCall,
          List<AggregateCall> newCalls,
          Map<AggregateCall, RexNode> aggCallMapping) {
    final PlannerSettings plannerSettings = (PlannerSettings) oldAggRel.getCluster().getPlanner().getContext();
    final boolean isInferenceEnabled = plannerSettings.isTypeInferenceEnabled();
    final int nGroups = oldAggRel.getGroupCount();
    RelDataTypeFactory typeFactory = oldAggRel.getCluster().getTypeFactory();
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    int arg = oldCall.getArgList().get(0);
    RelDataType argType = getFieldType(oldAggRel.getInput(), arg);
    final RelDataType tDigestType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARBINARY), true);
    SqlAggFunction tDigestAgg = new DrillTDigestAggFunction();
    if(isInferenceEnabled) {
      tDigestAgg = new DrillCalciteSqlAggFunctionWrapper(tDigestAgg, tDigestType);
    }
    AggregateCall tDigestCall =
            new AggregateCall(
                    tDigestAgg,
                    oldCall.isDistinct(),
                    oldCall.getArgList(),
                    tDigestType,
                    null);

    // NOTE:  these references are with respect to the output
    // of newAggRel
    RexNode medianRef =
            rexBuilder.addAggCall(
                    tDigestCall,
                    nGroups,
                    oldAggRel.indicator,
                    newCalls,
                    aggCallMapping,
                    ImmutableList.of(argType));
    DrillSqlOperator op = new DrillSqlOperator("TDIGEST_MEDIAN", 1, true);
    return rexBuilder.makeCall(op, medianRef);
  }

  private RexNode reduceQuantile(
          RelOptRuleCall ruleCall,
          AggregateCall oldCall,
          List<AggregateCall> newCalls,
          Map<AggregateCall, RexNode> aggCallMapping) {

    final Aggregate oldAggRel = (Aggregate) ruleCall.rels[0];
    final Project project = (Project) ruleCall.rels[1];
    final PlannerSettings plannerSettings = (PlannerSettings) oldAggRel.getCluster().getPlanner().getContext();
    final boolean isInferenceEnabled = plannerSettings.isTypeInferenceEnabled();
    final int nGroups = oldAggRel.getGroupCount();
    RelDataTypeFactory typeFactory = oldAggRel.getCluster().getTypeFactory();
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    int literalArg = oldCall.getArgList().get(0);
    int aggArg = oldCall.getArgList().get(1);
    List<RexNode> rexNodes = project.getChildExps();
    String message = "QUANTILE requires a numeric value between 0.0 and 1.0 as the first parameter";
    Preconditions.checkArgument(rexNodes.get(literalArg) instanceof RexLiteral, message);
    RexLiteral literal = (RexLiteral) rexNodes.get(literalArg);
    Preconditions.checkArgument(literal.getValue() instanceof BigDecimal, message);
    BigDecimal value = (BigDecimal) literal.getValue();
    Preconditions.checkArgument(value.compareTo(new BigDecimal(0)) >= 0, message);
    Preconditions.checkArgument(value.compareTo(new BigDecimal(1)) <= 0, message);
    RelDataType argType = getFieldType(oldAggRel.getInput(), aggArg);
    final RelDataType tDigestType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARBINARY), true);
    SqlAggFunction tDigestAgg = new DrillTDigestAggFunction();
    if(isInferenceEnabled) {
      tDigestAgg = new DrillCalciteSqlAggFunctionWrapper(tDigestAgg, tDigestType);
    }
    AggregateCall tDigestCall =
            new AggregateCall(
                    tDigestAgg,
                    oldCall.isDistinct(),
                    ImmutableList.of(oldCall.getArgList().get(1)),
                    tDigestType,
                    null);

    RexNode tDigestRef =
            rexBuilder.addAggCall(
                    tDigestCall,
                    nGroups,
                    oldAggRel.indicator,
                    newCalls,
                    aggCallMapping,
                    ImmutableList.of(argType));

    DrillSqlOperator op = new DrillSqlOperator("TDIGEST_QUANTILE", 2, true);
    return rexBuilder.makeCall(op, literal, tDigestRef);
  }

  private static class DrillTDigestAggFunction extends SqlAggFunction {

    protected DrillTDigestAggFunction() {
      super("TDIGEST",
              SqlKind.OTHER_FUNCTION,
              ReturnTypes.ARG0, // use the inferred return type of SqlCountAggFunction
              null,
              OperandTypes.BINARY,
              SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }
  }

  private RexNode reduceSum(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping) {
    final PlannerSettings plannerSettings = (PlannerSettings) oldAggRel.getCluster().getPlanner().getContext();
    final boolean isInferenceEnabled = plannerSettings.isTypeInferenceEnabled();
    final int nGroups = oldAggRel.getGroupCount();
    RelDataTypeFactory typeFactory =
        oldAggRel.getCluster().getTypeFactory();
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    int arg = oldCall.getArgList().get(0);
    RelDataType argType =
        getFieldType(
            oldAggRel.getInput(),
            arg);
    final RelDataType sumType;
    final SqlAggFunction sumZeroAgg;
    if(isInferenceEnabled) {
      sumType = oldCall.getType();
      sumZeroAgg = new DrillCalciteSqlAggFunctionWrapper(
          new SqlSumEmptyIsZeroAggFunction(), sumType);
    } else {
      sumType =
          typeFactory.createTypeWithNullability(
              argType, argType.isNullable());
      sumZeroAgg = new SqlSumEmptyIsZeroAggFunction();
    }
    AggregateCall sumZeroCall =
        new AggregateCall(
            sumZeroAgg,
            oldCall.isDistinct(),
            oldCall.getArgList(),
            sumType,
            null);
    final SqlCountAggFunction countAgg = (SqlCountAggFunction) SqlStdOperatorTable.COUNT;
    final RelDataType countType = countAgg.getReturnType(typeFactory);
    AggregateCall countCall =
        new AggregateCall(
            countAgg,
            oldCall.isDistinct(),
            oldCall.getArgList(),
            countType,
            null);

    // NOTE:  these references are with respect to the output
    // of newAggRel
    RexNode sumZeroRef =
        rexBuilder.addAggCall(
            sumZeroCall,
            nGroups,
            oldAggRel.indicator,
            newCalls,
            aggCallMapping,
            ImmutableList.of(argType));
    if (!oldCall.getType().isNullable()) {
      // If SUM(x) is not nullable, the validator must have determined that
      // nulls are impossible (because the group is never empty and x is never
      // null). Therefore we translate to SUM0(x).
      return sumZeroRef;
    }
    RexNode countRef =
        rexBuilder.addAggCall(
            countCall,
            nGroups,
            oldAggRel.indicator,
            newCalls,
            aggCallMapping,
            ImmutableList.of(argType));
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            countRef, rexBuilder.makeExactLiteral(BigDecimal.ZERO)),
            rexBuilder.constantNull(),
            sumZeroRef);
  }

  private RexNode reduceStddev(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      boolean biased,
      boolean sqrt,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    // stddev_pop(x) ==>
    //   power(
    //     (sum(x * x) - sum(x) * sum(x) / count(x))
    //     / count(x),
    //     .5)
    //
    // stddev_samp(x) ==>
    //   power(
    //     (sum(x * x) - sum(x) * sum(x) / count(x))
    //     / nullif(count(x) - 1, 0),
    //     .5)
    final PlannerSettings plannerSettings = (PlannerSettings) oldAggRel.getCluster().getPlanner().getContext();
    final boolean isInferenceEnabled = plannerSettings.isTypeInferenceEnabled();
    final int nGroups = oldAggRel.getGroupCount();
    RelDataTypeFactory typeFactory =
        oldAggRel.getCluster().getTypeFactory();
    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    assert oldCall.getArgList().size() == 1 : oldCall.getArgList();
    final int argOrdinal = oldCall.getArgList().get(0);
    final RelDataType argType =
        getFieldType(
            oldAggRel.getInput(),
            argOrdinal);

    // final RexNode argRef = inputExprs.get(argOrdinal);
    RexNode argRef = rexBuilder.makeCall(CastHighOp, inputExprs.get(argOrdinal));
    inputExprs.set(argOrdinal, argRef);

    final RexNode argSquared =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY, argRef, argRef);
    final int argSquaredOrdinal = lookupOrAdd(inputExprs, argSquared);

    final RelDataType sumType =
        typeFactory.createTypeWithNullability(
            argType,
            true);
    final AggregateCall sumArgSquaredAggCall =
        new AggregateCall(
            new SqlSumAggFunction(sumType),
            oldCall.isDistinct(),
            ImmutableIntList.of(argSquaredOrdinal),
            sumType,
            null);
    final RexNode sumArgSquared =
        rexBuilder.addAggCall(
            sumArgSquaredAggCall,
            nGroups,
            oldAggRel.indicator,
            newCalls,
            aggCallMapping,
            ImmutableList.of(argType));

    final AggregateCall sumArgAggCall =
        new AggregateCall(
            new SqlSumAggFunction(sumType),
            oldCall.isDistinct(),
            ImmutableIntList.of(argOrdinal),
            sumType,
            null);
    final RexNode sumArg =
          rexBuilder.addAggCall(
              sumArgAggCall,
              nGroups,
              oldAggRel.indicator,
              newCalls,
              aggCallMapping,
              ImmutableList.of(argType));

    final RexNode sumSquaredArg =
          rexBuilder.makeCall(
              SqlStdOperatorTable.MULTIPLY, sumArg, sumArg);

    final SqlCountAggFunction countAgg = (SqlCountAggFunction) SqlStdOperatorTable.COUNT;
    final RelDataType countType = countAgg.getReturnType(typeFactory);
    final AggregateCall countArgAggCall =
        new AggregateCall(
            countAgg,
            oldCall.isDistinct(),
            oldCall.getArgList(),
            countType,
            null);
    final RexNode countArg =
        rexBuilder.addAggCall(
            countArgAggCall,
            nGroups,
            oldAggRel.indicator,
            newCalls,
            aggCallMapping,
            ImmutableList.of(argType));

    final RexNode avgSumSquaredArg =
        rexBuilder.makeCall(
            SqlStdOperatorTable.DIVIDE,
            sumSquaredArg, countArg);

    final RexNode diff =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MINUS,
            sumArgSquared, avgSumSquaredArg);

    final RexNode denominator;
    if (biased) {
      denominator = countArg;
    } else {
      final RexLiteral one =
          rexBuilder.makeExactLiteral(BigDecimal.ONE);
      final RexNode nul =
          rexBuilder.makeNullLiteral(countArg.getType().getSqlTypeName());
      final RexNode countMinusOne =
          rexBuilder.makeCall(
              SqlStdOperatorTable.MINUS, countArg, one);
      final RexNode countEqOne =
          rexBuilder.makeCall(
              SqlStdOperatorTable.EQUALS, countArg, one);
      denominator =
          rexBuilder.makeCall(
              SqlStdOperatorTable.CASE,
              countEqOne, nul, countMinusOne);
    }

    final SqlOperator divide;
    if(isInferenceEnabled) {
      divide = new DrillSqlOperator(
          "divide",
          2,
          true,
          oldCall.getType());
    } else {
      divide = SqlStdOperatorTable.DIVIDE;
    }

    final RexNode div =
        rexBuilder.makeCall(
            divide, diff, denominator);

    RexNode result = div;
    if (sqrt) {
      final RexNode half =
          rexBuilder.makeExactLiteral(new BigDecimal("0.5"));
      result =
          rexBuilder.makeCall(
              SqlStdOperatorTable.POWER, div, half);
    }

    if(isInferenceEnabled) {
      return result;
    } else {
     /*
      * Currently calcite's strategy to infer the return type of aggregate functions
      * is wrong because it uses the first known argument to determine output type. For
      * instance if we are performing stddev on an integer column then it interprets the
      * output type to be integer which is incorrect as it should be double. So based on
      * this if we add cast after rewriting the aggregate we add an additional cast which
      * would cause wrong results. So we simply add a cast to ANY.
      */
      return rexBuilder.makeCast(
          typeFactory.createSqlType(SqlTypeName.ANY), result);
    }
  }

  /**
   * Finds the ordinal of an element in a list, or adds it.
   *
   * @param list    List
   * @param element Element to lookup or add
   * @param <T>     Element type
   * @return Ordinal of element in list
   */
  private static <T> int lookupOrAdd(List<T> list, T element) {
    int ordinal = list.indexOf(element);
    if (ordinal == -1) {
      ordinal = list.size();
      list.add(element);
    }
    return ordinal;
  }

  /**
   * Do a shallow clone of oldAggRel and update aggCalls. Could be refactored
   * into Aggregate and subclasses - but it's only needed for some
   * subclasses.
   *
   * @param oldAggRel AggregateRel to clone.
   * @param inputRel  Input relational expression
   * @param newCalls  New list of AggregateCalls
   * @return shallow clone with new list of AggregateCalls.
   */
  protected Aggregate newAggregateRel(
      Aggregate oldAggRel,
      RelNode inputRel,
      List<AggregateCall> newCalls) {
    return LogicalAggregate.create(inputRel, oldAggRel.indicator, oldAggRel.getGroupSet(), oldAggRel.getGroupSets(), newCalls);
  }

  private RelDataType getFieldType(RelNode relNode, int i) {
    final RelDataTypeField inputField =
        relNode.getRowType().getFieldList().get(i);
    return inputField.getType();
  }

  private static class DrillConvertSumToSumZero extends RelOptRule {
    protected static final Logger tracer = CalciteTrace.getPlannerTracer();

    public DrillConvertSumToSumZero(RelOptRuleOperand operand) {
      super(operand);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      DrillAggregateRel oldAggRel = (DrillAggregateRel) call.rels[0];
      for (AggregateCall aggregateCall : oldAggRel.getAggCallList()) {
        final SqlAggFunction sqlAggFunction = DrillCalciteWrapperUtility.extractSqlOperatorFromWrapper(aggregateCall.getAggregation());
        if(sqlAggFunction instanceof SqlSumAggFunction
            && !aggregateCall.getType().isNullable()) {
          // If SUM(x) is not nullable, the validator must have determined that
          // nulls are impossible (because the group is never empty and x is never
          // null). Therefore we translate to SUM0(x).
          return true;
        }
      }
      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final DrillAggregateRel oldAggRel = (DrillAggregateRel) call.rels[0];

      final Map<AggregateCall, RexNode> aggCallMapping = Maps.newHashMap();
      final List<AggregateCall> newAggregateCalls = Lists.newArrayList();
      for (AggregateCall oldAggregateCall : oldAggRel.getAggCallList()) {
        final SqlAggFunction sqlAggFunction = DrillCalciteWrapperUtility.extractSqlOperatorFromWrapper(
            oldAggregateCall.getAggregation());
        if(sqlAggFunction instanceof SqlSumAggFunction
            && !oldAggregateCall.getType().isNullable()) {
          final RelDataType argType = oldAggregateCall.getType();
          final RelDataType sumType = oldAggRel.getCluster().getTypeFactory()
              .createTypeWithNullability(argType, argType.isNullable());
          final SqlAggFunction sumZeroAgg = new DrillCalciteSqlAggFunctionWrapper(
              new SqlSumEmptyIsZeroAggFunction(), sumType);
          AggregateCall sumZeroCall =
              new AggregateCall(
                  sumZeroAgg,
                  oldAggregateCall.isDistinct(),
                  oldAggregateCall.getArgList(),
                  sumType,
                  null);
          oldAggRel.getCluster().getRexBuilder()
              .addAggCall(sumZeroCall,
                  oldAggRel.getGroupCount(),
                  oldAggRel.indicator,
                  newAggregateCalls,
                  aggCallMapping,
                  ImmutableList.of(argType));
        } else {
          newAggregateCalls.add(oldAggregateCall);
        }
      }

      try {
        call.transformTo(new DrillAggregateRel(
            oldAggRel.getCluster(),
            oldAggRel.getTraitSet(),
            oldAggRel.getInput(),
            oldAggRel.indicator,
            oldAggRel.getGroupSet(),
            oldAggRel.getGroupSets(),
            newAggregateCalls));
      } catch (InvalidRelException e) {
        tracer.warning(e.toString());
      }
    }
  }
}

