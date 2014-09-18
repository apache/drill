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

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.relopt.volcano.RelSubset;

public abstract class SubsetTransformer<T extends RelNode, E extends Exception> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SubsetTransformer.class);

  public abstract RelNode convertChild(T current, RelNode child) throws E;

  private final RelOptRuleCall call;

  public SubsetTransformer(RelOptRuleCall call) {
    this.call = call;
  }

  public RelTraitSet newTraitSet(RelTrait... traits) {
    RelTraitSet set = call.getPlanner().emptyTraitSet();
    for (RelTrait t : traits) {
      set = set.plus(t);
    }
    return set;

  }

  boolean go(T n, RelNode candidateSet) throws E {
    if ( !(candidateSet instanceof RelSubset) ) {
      return false;
    }

    boolean transform = false;

    for (RelNode rel : ((RelSubset)candidateSet).getRelList()) {
      if (!isDefaultDist(rel)) {
        RelNode out = convertChild(n, rel);
        if (out != null) {
          call.transformTo(out);
          transform = true;

        }
      }
    }

    return transform;
  }

  private boolean isDefaultDist(RelNode n) {
    return n.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE).equals(DrillDistributionTrait.DEFAULT);
  }

}
