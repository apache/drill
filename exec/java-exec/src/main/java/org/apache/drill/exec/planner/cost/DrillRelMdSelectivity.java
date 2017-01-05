/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.planner.cost;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;
import org.apache.drill.exec.planner.common.DrillJoinRelBase;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DrillTranslatableTable;

import java.util.ArrayList;
import java.util.List;

public class DrillRelMdSelectivity extends RelMdSelectivity {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RelMdSelectivity.class);

  private static final DrillRelMdSelectivity INSTANCE =
      new DrillRelMdSelectivity();

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.SELECTIVITY.method, INSTANCE);

  @Override
  public Double getSelectivity(RelNode rel, RexNode predicate) {
    if (rel instanceof TableScan) {
      return getScanSelectivity((TableScan) rel, predicate);
    } else if (rel instanceof DrillJoinRelBase) {
      return getJoinSelectivity(((DrillJoinRelBase) rel), predicate);
    } else if (rel instanceof SingleRel && !DrillRelOptUtil.guessRows(rel)) {
        return RelMetadataQuery.getSelectivity(((SingleRel) rel).getInput(), predicate);
    } else if (rel instanceof RelSubset && !DrillRelOptUtil.guessRows(rel)) {
      if (((RelSubset) rel).getBest() != null) {
        return RelMetadataQuery.getSelectivity(((RelSubset)rel).getBest(), predicate);
      } else if (((RelSubset)rel).getOriginal() != null) {
        return RelMetadataQuery.getSelectivity(((RelSubset)rel).getOriginal(), predicate);
      } else {
        return super.getSelectivity(rel, predicate);
      }
    } else {
      return super.getSelectivity(rel, predicate);
    }
  }

  private Double getJoinSelectivity(DrillJoinRelBase rel, RexNode predicate) {
    double sel = 1.0;
    // determine which filters apply to the left vs right
    RexNode leftPred = null;
    RexNode rightPred = null;
    JoinRelType joinType = rel.getJoinType();
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    int[] adjustments = new int[rel.getRowType().getFieldCount()];

    if (DrillRelOptUtil.guessRows(rel)) {
      return super.getSelectivity(rel, predicate);
    }

    if (predicate != null) {
      RexNode pred;
      List<RexNode> leftFilters = new ArrayList<RexNode>();
      List<RexNode> rightFilters = new ArrayList<RexNode>();
      List<RexNode> joinFilters = new ArrayList<RexNode>();
      List<RexNode> predList = RelOptUtil.conjunctions(predicate);

      RelOptUtil.classifyFilters(
          rel,
          predList,
          joinType,
          joinType == JoinRelType.INNER,
          !joinType.generatesNullsOnLeft(),
          !joinType.generatesNullsOnRight(),
          joinFilters,
          leftFilters,
          rightFilters);
      leftPred =
          RexUtil.composeConjunction(rexBuilder, leftFilters, true);
      rightPred =
          RexUtil.composeConjunction(rexBuilder, rightFilters, true);
      for (RelNode child : rel.getInputs()) {
        RexNode modifiedPred = null;

        if (child == rel.getLeft()) {
          pred = leftPred;
        } else {
          pred = rightPred;
        }
        if (pred != null) {
          // convert the predicate to reference the types of the children
          modifiedPred =
              pred.accept(new RelOptUtil.RexInputConverter(
              rexBuilder,
              null,
              child.getRowType().getFieldList(),
              adjustments));
        }
        sel *= RelMetadataQuery.getSelectivity(child, modifiedPred);
      }
      sel *= RelMdUtil.guessSelectivity(
          RexUtil.composeConjunction(rexBuilder, joinFilters, true));
    }
    return sel;
  }

  private Double getScanSelectivity(TableScan scan, RexNode predicate) {
    DrillTable table = scan.getTable().unwrap(DrillTable.class);
    if (table == null) {
      table = scan.getTable().unwrap(DrillTranslatableTable.class).getDrillTable();
    }
    if (table == null || table.getStatsTable() == null) {
      return super.getSelectivity(scan, predicate);
    } else {
      return getScanSelectivityInternal(table, predicate, scan.getRowType());
    }
  }

  private Double getScanSelectivityInternal(DrillTable table, RexNode predicate,
      RelDataType type) {
    double sel = 1.0;

    if ((predicate == null) || predicate.isAlwaysTrue()) {
      return sel;
    }

    for (RexNode pred : RelOptUtil.conjunctions(predicate)) {
      double orSel = 0;
      for (RexNode orPred : RelOptUtil.disjunctions(pred)) {
        //CALCITE guess
        Double guess = RelMdUtil.guessSelectivity(pred);
        if (orPred.isA(SqlKind.EQUALS)) {
          if (orPred instanceof RexCall) {
            int colIdx = -1;
            RexInputRef op = findRexInputRef(orPred);
            if (op != null) {
              colIdx = op.hashCode();
            }
            if (colIdx != -1 && colIdx < type.getFieldNames().size()) {
              String col = type.getFieldNames().get(colIdx);
              if (table.getStatsTable() != null
                  && table.getStatsTable().getNdv(col) != null) {
                orSel += 1.00 / table.getStatsTable().getNdv(col);
              }
            } else {
              orSel += guess;
              if (logger.isDebugEnabled()) {
                logger.warn(String.format("No input reference $[%s] found for predicate [%s]",
                    Integer.toString(colIdx), orPred.toString()));
              }
            }
          }
        } else {
          //Use the CALCITE guess. TODO: Use histograms for COMPARISON operator
          orSel += guess;
        }
      }
      sel *= orSel;
    }
    return sel;
  }

  public static RexInputRef findRexInputRef(final RexNode node) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
            public Void visitCall(RexCall call) {
              for (RexNode child : call.getOperands()) {
                child.accept(this);
              }
              return super.visitCall(call);
            }

            public Void visitInputRef(RexInputRef inputRef) {
              throw new Util.FoundOne(inputRef);
            }
          };
      node.accept(visitor);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (RexInputRef) e.getNode();
    }
  }
}
