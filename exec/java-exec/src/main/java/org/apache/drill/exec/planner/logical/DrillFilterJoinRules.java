package org.apache.drill.exec.planner.logical;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

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

public class DrillFilterJoinRules {
  /** Predicate that always returns true for any filter in OUTER join, and only true
   * for EQUAL or IS_DISTINCT_FROM over RexInputRef in INNER join. With this predicate,
   * the filter expression that return true will be kept in the JOIN OP.
   * Example:  INNER JOIN,   L.C1 = R.C2 and L.C3 + 100 = R.C4 + 100 will be kepted in JOIN.
   *                         L.C5 < R.C6 will be pulled up into Filter above JOIN.
   *           OUTER JOIN,   Keep any filter in JOIN.
  */
  public static final FilterJoinRule.Predicate EQUAL_IS_DISTINCT_FROM =
      new FilterJoinRule.Predicate() {
        public boolean apply(Join join, JoinRelType joinType, RexNode exp) {
          if (joinType != JoinRelType.INNER) {
            return true;  // In OUTER join, we could not pull-up the filter.
                          // All we can do is keep the filter with JOIN, and
                          // then decide whether the filter could be pushed down
                          // into LEFT/RIGHT.
          }

          List<RexNode> tmpLeftKeys = Lists.newArrayList();
          List<RexNode> tmpRightKeys = Lists.newArrayList();
          List<RelDataTypeField> sysFields = Lists.newArrayList();

          RexNode remaining = RelOptUtil.splitJoinCondition(sysFields, join.getLeft(), join.getRight(), exp, tmpLeftKeys, tmpRightKeys, null, null);

          if (remaining.isAlwaysTrue()) {
            return true;
          }

          return false;
        }
      };


  /** Rule that pushes predicates from a Filter into the Join below them. */
  public static final FilterJoinRule DRILL_FILTER_ON_JOIN =
      new FilterJoinRule.FilterIntoJoinRule(true, RelFactories.DEFAULT_FILTER_FACTORY,
          RelFactories.DEFAULT_PROJECT_FACTORY, EQUAL_IS_DISTINCT_FROM);


  /** Rule that pushes predicates in a Join into the inputs to the join. */
  public static final FilterJoinRule DRILL_JOIN =
      new FilterJoinRule.JoinConditionPushRule(RelFactories.DEFAULT_FILTER_FACTORY,
          RelFactories.DEFAULT_PROJECT_FACTORY, EQUAL_IS_DISTINCT_FROM);

}
