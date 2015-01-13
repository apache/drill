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

import java.util.HashMap;
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

  protected List<NamedExpression> getProjectExpressions(DrillParseContext context) {
    List<NamedExpression> expressions = Lists.newArrayList();

    HashMap<String, String> starColPrefixes = new HashMap<String, String>();

    // T1.* will subsume T1.*0, but will not subsume any regular column/expression.
    // Select *, col1, *, col2 : the intermediate will output one set of regular columns expanded from star with prefix,
    // plus col1 and col2 without prefix.
    // This will allow us to differentiate the regular expanded from *, and the regular column referenced in the query.
    for (Pair<RexNode, String> pair : projects()) {
      if (StarColumnHelper.isPrefixedStarColumn(pair.right)) {
        String prefix = StarColumnHelper.extractStarColumnPrefix(pair.right);

        if (! starColPrefixes.containsKey(prefix)) {
          starColPrefixes.put(prefix, pair.right);
        }
      }
    }

    for (Pair<RexNode, String> pair : projects()) {
      if (! StarColumnHelper.subsumeColumn(starColPrefixes, pair.right)) {
        LogicalExpression expr = DrillOptiq.toDrill(context, getChild(), pair.left);
        expressions.add(new NamedExpression(expr, FieldReference.getWithQuotedRef(pair.right)));
      }
    }
    return expressions;
  }

}
