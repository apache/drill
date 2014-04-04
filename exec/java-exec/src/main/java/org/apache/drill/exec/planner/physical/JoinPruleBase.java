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

package org.apache.drill.exec.planner.physical;

import java.util.List;

import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.rex.RexNode;

import com.google.common.collect.Lists;

// abstract base class for the join physical rules  
public abstract class JoinPruleBase extends RelOptRule {

  protected JoinPruleBase(RelOptRuleOperand operand, String description) {
    super(operand, description);   
  }

  protected boolean checkPreconditions(DrillJoinRel join, RelNode left, RelNode right) {
    if (join.getCondition().isAlwaysTrue()) {
      // this indicates a cartesian join which is not supported by existing rules
      return false;
    }
    
    List<Integer> leftKeys = Lists.newArrayList();
    List<Integer> rightKeys = Lists.newArrayList() ;
    RexNode remaining = RelOptUtil.splitJoinCondition(left, right, join.getCondition(), leftKeys, rightKeys);
    if (!remaining.isAlwaysTrue() && (leftKeys.size() == 0 || rightKeys.size() == 0)) {
      // this is a non-equijoin which is not supported by existing rules
      return false;
    }
    return true;
  }
  
  protected List<DistributionField> getDistributionField(List<Integer> keys) {
    List<DistributionField> distFields = Lists.newArrayList();

    for (int key : keys) {
      distFields.add(new DistributionField(key));
    }
     
    return distFields;
  }  
  
}
