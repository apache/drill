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
package org.apache.drill.exec.planner;

import java.util.List;

import org.apache.drill.common.physical.PhysicalPlan;
import org.apache.drill.common.physical.pop.base.PhysicalOperator;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.foreman.QueryWorkUnit;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;

/**
 * Parallelization is based on available nodes with source or target data.  Nodes that are "overloaded" are excluded from execution.
 */
public class SimpleExecPlanner implements ExecPlanner{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleExecPlanner.class);
  
  private FragmentingPhysicalVisitor fragmenter = new FragmentingPhysicalVisitor();
  private SimpleParallelizer parallelizer = new SimpleParallelizer();

  @Override
  public QueryWorkUnit getWorkUnit(QueryContext context, PhysicalPlan plan, int maxWidth) throws FragmentSetupException {
    
    // get the root physical operator and split the plan into sub fragments.
    PhysicalOperator root = plan.getSortedOperators(false).iterator().next();
    FragmentNode fragmentRoot = root.accept(fragmenter, null);
    
    // generate a planning set and collect stats.
    FragmentPlanningSet planningSet = new FragmentPlanningSet(context);
    FragmentStatsCollector statsCollector = new FragmentStatsCollector(planningSet);
    statsCollector.collectStats(fragmentRoot);
    
    return parallelizer.getFragments(context, fragmentRoot, planningSet, maxWidth);
    
    
  }
}
