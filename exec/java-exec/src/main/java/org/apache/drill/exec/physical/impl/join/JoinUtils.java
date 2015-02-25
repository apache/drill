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

package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.common.logical.data.JoinCondition;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptUtil;

import java.util.List;

public class JoinUtils {
  public static enum JoinComparator {
    NONE, // No comparator
    EQUALS, // Equality comparator
    IS_NOT_DISTINCT_FROM // 'IS NOT DISTINCT FROM' comparator
  }

  // Check the comparator for the join condition. Note that a similar check is also
  // done in JoinPrel; however we have to repeat it here because a physical plan
  // may be submitted directly to Drill.
  public static JoinComparator checkAndSetComparison(JoinCondition condition,
      JoinComparator comparator) {
    if (condition.getRelationship().equalsIgnoreCase("EQUALS") ||
        condition.getRelationship().equals("==") /* older json plans still have '==' */) {
      if (comparator == JoinComparator.NONE ||
          comparator == JoinComparator.EQUALS) {
        return JoinComparator.EQUALS;
      } else {
        throw new IllegalArgumentException("This type of join does not support mixed comparators.");
      }
    } else if (condition.getRelationship().equalsIgnoreCase("IS_NOT_DISTINCT_FROM")) {
      if (comparator == JoinComparator.NONE ||
          comparator == JoinComparator.IS_NOT_DISTINCT_FROM) {
        return JoinComparator.IS_NOT_DISTINCT_FROM;
      } else {
        throw new IllegalArgumentException("This type of join does not support mixed comparators.");
      }
    }
    throw new IllegalArgumentException("Invalid comparator supplied to this join.");
  }

    /**
     * Check if the given RelNode contains any Cartesian join.
     * Return true if find one. Otherwise, return false.
     *
     * @param relNode   the RelNode to be inspected.
     * @param leftKeys  a list used for the left input into the join which has
     *                  equi-join keys. It can be empty or not (but not null),
     *                  this method will clear this list before using it.
     * @param rightKeys a list used for the right input into the join which has
     *                  equi-join keys. It can be empty or not (but not null),
     *                  this method will clear this list before using it.
     * @return          Return true if the given relNode contains Cartesian join.
     *                  Otherwise, return false
     */
  public static boolean checkCartesianJoin(RelNode relNode, List<Integer> leftKeys, List<Integer> rightKeys) {
    if (relNode instanceof JoinRelBase) {
      leftKeys.clear();
      rightKeys.clear();

      JoinRelBase joinRel = (JoinRelBase) relNode;
      RelNode left = joinRel.getLeft();
      RelNode right = joinRel.getRight();

      RelOptUtil.splitJoinCondition(left, right, joinRel.getCondition(), leftKeys, rightKeys);
      if(leftKeys.isEmpty() || rightKeys.isEmpty()) {
        return true;
      }
    }

    for (RelNode child : relNode.getInputs()) {
      if(checkCartesianJoin(child, leftKeys, rightKeys)) {
        return true;
      }
    }

    return false;
  }
}
