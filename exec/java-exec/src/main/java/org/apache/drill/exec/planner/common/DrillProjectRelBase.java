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
package org.apache.drill.exec.planner.common;

import java.util.HashSet;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Pair;

import com.google.common.collect.Lists;

/**
 *
 * Base class for logical and physical Project implemented in Drill
 */
public abstract class DrillProjectRelBase extends ProjectRelBase implements DrillRelNode {
  protected DrillProjectRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps,
      RelDataType rowType) {
    super(cluster, traits, child, exps, rowType, Flags.BOXED);
    assert getConvention() == convention;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    // cost is proportional to the number of rows and number of columns being projected
    double rowCount = RelMetadataQuery.getRowCount(this);
    double cpuCost = DrillCostBase.PROJECT_CPU_COST * getRowType().getFieldCount();
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(rowCount, cpuCost, 0, 0);
  }

  private List<Pair<RexNode, String>> projects() {
    return Pair.zip(exps, getRowType().getFieldNames());
  }

  // By default, the project will not allow duplicate columns, caused by expanding from * column.
  // For example, if we have T1_*, T1_Col1, T1_Col2, Col1 and Col2 will have two copies if we expand
  // * into a list of regular columns.  For the intermediate project, the duplicate columns are not
  // necessary; it will impact performance.
  protected List<NamedExpression> getProjectExpressions(DrillParseContext context) {
    List<NamedExpression> expressions = Lists.newArrayList();

    HashSet<String> starColPrefixes = new HashSet<String>();

    // To remove duplicate columns caused by expanding from * column, we'll keep track of
    // all the prefix in the project expressions. If a regular column C1 have the same prefix, that
    // regular column is not included in the project expression, since at execution time, * will be
    // expanded into a list of column, including column C1.
    for (String fieldName : getRowType().getFieldNames()) {
      if (StarColumnHelper.isPrefixedStarColumn(fieldName)) {
        starColPrefixes.add(StarColumnHelper.extractStarColumnPrefix(fieldName));
      }
    }

    for (Pair<RexNode, String> pair : projects()) {
      if (! StarColumnHelper.subsumeRegColumn(starColPrefixes, pair.right)) {
        LogicalExpression expr = DrillOptiq.toDrill(context, getChild(), pair.left);
        expressions.add(new NamedExpression(expr, FieldReference.getWithQuotedRef(pair.right)));
      }
    }
    return expressions;
  }

}
