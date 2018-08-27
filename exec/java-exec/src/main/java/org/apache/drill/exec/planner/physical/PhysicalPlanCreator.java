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
package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.common.logical.PlanProperties.PlanPropertiesBuilder;
import org.apache.drill.common.logical.PlanProperties.PlanType;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.physical.explain.PrelSequencer.OpId;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;


public class PhysicalPlanCreator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PhysicalPlanCreator.class);

  private final Map<Prel, OpId> opIdMap;

  private List<PhysicalOperator> popList;
  private final QueryContext context;
  PhysicalPlan plan = null;

  public PhysicalPlanCreator(QueryContext context, Map<Prel, OpId> opIdMap) {
    this.context = context;
    this.opIdMap = opIdMap;
    popList = Lists.newArrayList();
  }

  public QueryContext getContext() {
    return context;
  }

  public PhysicalOperator addMetadata(Prel originalPrel, PhysicalOperator op){
    op.setOperatorId(opIdMap.get(originalPrel).getAsSingleInt());
    op.setCost(originalPrel.estimateRowCount(originalPrel.getCluster().getMetadataQuery()));
    return op;
  }

  public PhysicalPlan build(Prel rootPrel, boolean forceRebuild) {

    if (plan != null && !forceRebuild) {
      return plan;
    }

    PlanPropertiesBuilder propsBuilder = PlanProperties.builder();
    propsBuilder.type(PlanType.APACHE_DRILL_PHYSICAL);
    propsBuilder.version(1);
    propsBuilder.resultMode(ResultMode.EXEC);
    propsBuilder.generator(PhysicalPlanCreator.class.getName(), "");


    try {
      // invoke getPhysicalOperator on the root Prel which will recursively invoke it
      // on the descendants and we should have a well-formed physical operator tree
      PhysicalOperator rootPOP = rootPrel.getPhysicalOperator(this);
      if (rootPOP != null) {
        assert (popList.size() > 0); //getPhysicalOperator() is supposed to populate this list
        plan = new PhysicalPlan(propsBuilder.build(), popList);
      }

    } catch (IOException e) {
      plan = null;
      throw new UnsupportedOperationException("Physical plan created failed with error : " + e.toString());
    }

    return plan;
  }

}
