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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.List;
import java.util.Set;

/**
 * Contains factory implementation for creating various Drill Logical Rel nodes.
 */

public class DrillRelFactories {

  public static final RelFactories.ProjectFactory DRILL_LOGICAL_PROJECT_FACTORY =
      new DrillProjectFactoryImpl();

  public static final RelFactories.FilterFactory DRILL_LOGICAL_FILTER_FACTORY =
      new DrillFilterFactoryImpl();

  public static final RelFactories.JoinFactory DRILL_LOGICAL_JOIN_FACTORY = new DrillJoinFactoryImpl();


  /**
   * Implementation of {@link RelFactories.ProjectFactory} that returns a vanilla
   * {@link org.apache.calcite.rel.logical.LogicalProject}.
   */
  private static class DrillProjectFactoryImpl implements RelFactories.ProjectFactory {
    @Override
    public RelNode createProject(RelNode child,
                                 List<? extends RexNode> childExprs, List<String> fieldNames) {
      final RelOptCluster cluster = child.getCluster();
      final RelDataType rowType = RexUtil.createStructType(cluster.getTypeFactory(), childExprs, fieldNames);
      final RelNode project = DrillProjectRel.create(cluster, child.getTraitSet(), child, childExprs, rowType);

      return project;
    }
  }


  /**
   * Implementation of {@link RelFactories.FilterFactory} that
   * returns a vanilla {@link LogicalFilter}.
   */
  private static class DrillFilterFactoryImpl implements RelFactories.FilterFactory {
    @Override
    public RelNode createFilter(RelNode child, RexNode condition) {
      return DrillFilterRel.create(child, condition);
    }
  }


  /**
   * Implementation of {@link RelFactories.JoinFactory} that returns a vanilla
   * {@link org.apache.calcite.rel.logical.LogicalJoin}.
   */
  private static class DrillJoinFactoryImpl implements RelFactories.JoinFactory {
    @Override
    public RelNode createJoin(RelNode left, RelNode right,
                              RexNode condition, JoinRelType joinType,
                              Set<String> variablesStopped, boolean semiJoinDone) {
      return new DrillJoinRel(left.getCluster(), left.getTraitSet(), left, right, condition, joinType);
    }
  }

}
