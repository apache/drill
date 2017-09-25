/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.drill.exec.planner.physical.visitor;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.drill.exec.planner.physical.HashJoinPrel;
import org.apache.drill.exec.planner.physical.JoinPrel;
import org.apache.drill.exec.planner.physical.Prel;

import java.util.List;

/**
 * Visit Prel tree. Find all the HashJoinPrel nodes and set the flag to swap the Left/Right for HashJoinPrel when
 * <ol>
 * <li>It's inner join</li>
 * <li>left maxRowCount < right maxRowCount</li>
 * <li>right maxRowCount > maxHashTableSize</li>
 * </ol>
 * Resets the flag to swap the Left/Right for HashJoinPrel when
 * <ol>
 * <li>It's inner join</li>
 * <li>left maxRowCount > right maxRowCount</li>
 * <li>left maxRowCount > maxHashTableSize</li>
 * </ol>
 * The purpose of this visitor is to prevent planner from putting much more larger dataset in the RIGHT side,
 * which may cause OOM.
 *
 * @see org.apache.drill.exec.planner.physical.HashJoinPrel
 */
public class SwapHashJoinRegardingMaxRowCountVisitor extends BasePrelVisitor<Prel, Double, RuntimeException> {

  private static final SwapHashJoinRegardingMaxRowCountVisitor INSTANCE = new SwapHashJoinRegardingMaxRowCountVisitor();

  public static Prel resolveSwapHashJoin(Prel prel, Double maxHashTableSize) {
    return prel.accept(INSTANCE, maxHashTableSize);
  }

  @Override
  public Prel visitPrel(Prel prel, Double value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for (Prel child : prel) {
      child = child.accept(this, value);
      children.add(child);
    }

    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitJoin(JoinPrel prel, Double value) throws RuntimeException {
    RelMetadataQuery mq = RelMetadataQuery.instance();
    JoinPrel join = (JoinPrel) visitPrel(prel, value);

    if (prel instanceof HashJoinPrel) {
      HashJoinPrel hashJoinPrel = (HashJoinPrel) join;

      if (join.getJoinType() == JoinRelType.INNER) {
        Double rightMaxRowCount = mq.getMaxRowCount(join.getRight());
        Double leftMaxRowCount = mq.getMaxRowCount(join.getLeft());

        // checks that the getMaxRowCount value for both inputs isn't available
        if (rightMaxRowCount == null
            || leftMaxRowCount == null
            || rightMaxRowCount == Double.POSITIVE_INFINITY
            || leftMaxRowCount == Double.POSITIVE_INFINITY) {
          return join;
        }

        boolean isBuildGreaterProbe = rightMaxRowCount > leftMaxRowCount;

        if (!hashJoinPrel.isSwapped() && isBuildGreaterProbe && rightMaxRowCount > value) {
          hashJoinPrel.setSwapped(true);
        } else if (hashJoinPrel.isSwapped() && !isBuildGreaterProbe && leftMaxRowCount > value) {
          hashJoinPrel.setSwapped(false);
        }
      }
    }

    return join;
  }
}
