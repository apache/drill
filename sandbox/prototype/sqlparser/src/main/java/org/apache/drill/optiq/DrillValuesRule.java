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
package org.apache.drill.optiq;

import org.eigenbase.rel.ValuesRel;
import org.eigenbase.relopt.*;

/**
 * Rule that converts a {@link ValuesRel} to a Drill
 * "values" operation.
 */
public class DrillValuesRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillValuesRule();

  private DrillValuesRule() {
    super(
        new RelOptRuleOperand(
            ValuesRel.class,
            Convention.NONE),
        "DrillValuesRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ValuesRel values = (ValuesRel) call.getRels()[0];
    final RelTraitSet traits = values.getTraitSet().plus(DrillRel.CONVENTION);
    call.transformTo(
        new DrillValuesRel(values.getCluster(), values.getRowType(),
            values.getTuples(), traits));
  }
}

// End DrillValuesRule.java
