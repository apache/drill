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
package org.apache.drill.exec.planner;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.MakeFragmentsVisitor;
import org.apache.drill.exec.planner.fragment.PlanningSet;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.planner.fragment.StatsCollector;
import org.apache.drill.exec.work.QueryWorkUnit;

/**
 * Parallelization is based on available nodes with source or target data.  Nodes that are "overloaded" are excluded from execution.
 */
public class SimpleExecPlanner implements ExecPlanner{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleExecPlanner.class);
  
  private MakeFragmentsVisitor fragmenter = new MakeFragmentsVisitor();
  private SimpleParallelizer parallelizer = new SimpleParallelizer();

  @Override
  public QueryWorkUnit getWorkUnit(QueryContext context, PhysicalPlan plan, int maxWidth) throws ExecutionSetupException {
    
    // get the root physical operator and split the plan into sub fragments.
    PhysicalOperator root = plan.getSortedOperators(false).iterator().next();
    Fragment fragmentRoot = root.accept(fragmenter, null);
    
    // generate a planning set and collect stats.
    PlanningSet planningSet = StatsCollector.collectStats(fragmentRoot);

    int maxWidthPerEndpoint = context.getConfig().getInt(ExecConstants.MAX_WIDTH_PER_ENDPOINT);
    
    return parallelizer.getFragments(context.getCurrentEndpoint(), context.getQueryId(), context.getActiveEndpoints(),
            context.getPlanReader(), fragmentRoot, planningSet, maxWidth, maxWidthPerEndpoint);
    
    
  }
}
