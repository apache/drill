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
package org.apache.drill.exec.physical.config;

import java.util.List;

import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.calcite.rel.core.JoinRelType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.exec.physical.base.AbstractJoinPop;

@JsonTypeName("hash-join")
public class HashJoinPOP extends AbstractJoinPop {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashJoinPOP.class);

  @JsonCreator
  public HashJoinPOP(@JsonProperty("left") PhysicalOperator left, @JsonProperty("right") PhysicalOperator right,
                       @JsonProperty("conditions") List<JoinCondition> conditions,
                       @JsonProperty("joinType") JoinRelType joinType) {
        super(left, right, joinType, null, conditions);
        Preconditions.checkArgument(joinType != null, "Join type is missing for HashJoin Pop");
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
        Preconditions.checkArgument(children.size() == 2);
        HashJoinPOP newHashJoin = new HashJoinPOP(children.get(0), children.get(1), conditions, joinType);
        newHashJoin.setMaxAllocation(getMaxAllocation());
        return newHashJoin;
  }

  public HashJoinPOP flipIfRight() {
      if (joinType == JoinRelType.RIGHT) {
            List<JoinCondition> flippedConditions = Lists.newArrayList();
            for (JoinCondition c : conditions) {
                flippedConditions.add(c.flip());
            }
            return new HashJoinPOP(right, left, flippedConditions, JoinRelType.LEFT);
      } else {
            return this;
      }
  }

  @Override
  public int getOperatorType() {
        return CoreOperatorType.HASH_JOIN_VALUE;
    }

  /**
   *
   * @param maxAllocation The max memory allocation to be set
   */
  @Override
  public void setMaxAllocation(long maxAllocation) {
        this.maxAllocation = maxAllocation;
    }

  /**
   * The Hash Aggregate operator supports spilling
   * @return true (unless a single partition is forced)
   * @param queryContext
   */
  @Override
  public boolean isBufferedOperator(QueryContext queryContext) {
    // In case forced to use a single partition - do not consider this a buffered op (when memory is divided)
    return queryContext == null ||
      1 < (int)queryContext.getOptions().getOption(ExecConstants.HASHJOIN_NUM_PARTITIONS_VALIDATOR);
  }
}
