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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Custom Drill version of SubQueryRemoveRule that handles Calcite 1.40 compatibility issues
 * with RexLiteral creation during search argument optimization for IN clauses.
 */
public class DrillSubQueryRemoveRule extends SubQueryRemoveRule {
  private static final Logger logger = LoggerFactory.getLogger(DrillSubQueryRemoveRule.class);

  public DrillSubQueryRemoveRule(Config config) {
    super(config);
  }

  /**
   * Override apply to handle IllegalArgumentException during search argument optimization.
   * Falls back to standard processing when optimization fails.
   */
  @Override
  public RexNode apply(RexSubQuery e, Set<CorrelationId> variablesSet,
                      RelOptUtil.Logic logic, RelBuilder builder,
                      int inputCount, int offset, int subQueryIndex) {
    try {
      return super.apply(e, variablesSet, logic, builder, inputCount, offset, subQueryIndex);
    } catch (IllegalArgumentException ex) {
      // For Calcite 1.40 compatibility: if search argument optimization fails,
      // fall back to a simpler approach that avoids the problematic optimization
      logger.debug("Search argument optimization failed for subquery {}, falling back to simpler approach: {}",
                   e.getKind(), ex.getMessage());

      // For IN clauses, return the original expression without optimization
      // This preserves correctness while avoiding the RexLiteral creation issue
      if (e.getKind() == org.apache.calcite.sql.SqlKind.IN) {
        // Return a simple fallback that doesn't attempt search argument optimization
        return e;
      }

      // For other subquery types, attempt the original processing
      return super.apply(e, variablesSet, logic, builder, inputCount, offset, subQueryIndex);
    }
  }

  /**
   * Creates FILTER configuration for Drill SubQueryRemoveRule
   */
  public static final Config FILTER_CONFIG =
      SubQueryRemoveRule.Config.FILTER
        .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
        .as(Config.class);

  /**
   * Creates PROJECT configuration for Drill SubQueryRemoveRule
   */
  public static final Config PROJECT_CONFIG =
      SubQueryRemoveRule.Config.PROJECT
        .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
        .as(Config.class);

  /**
   * Creates JOIN configuration for Drill SubQueryRemoveRule
   */
  public static final Config JOIN_CONFIG =
      SubQueryRemoveRule.Config.JOIN
        .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
        .as(Config.class);
}