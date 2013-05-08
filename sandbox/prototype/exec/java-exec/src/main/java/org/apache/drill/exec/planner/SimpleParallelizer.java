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

import java.util.Collection;
import java.util.List;

import org.apache.drill.common.physical.pop.base.PhysicalOperator;
import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.foreman.QueryWorkUnit;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.FragmentMaterializer.IndexedFragmentNode;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

public class SimpleParallelizer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleParallelizer.class);

  private final FragmentMaterializer materializer = new FragmentMaterializer();

  /**
   * Generate a set of assigned fragments based on the provided planningSet. Do not allow parallelization stages to go
   * beyond the global max width.
   * 
   * @param context
   *          The current QueryContext.
   * @param planningSet
   *          The set of queries with collected statistics that we'll work with.
   * @param globalMaxWidth
   *          The maximum level or paralellization any stage of the query can do. Note that while this might be the
   *          number of active Drillbits, realistically, this could be well beyond that number of we want to do things
   *          like speed results return.
   * @return The list of generatoe PlanFragment protobuf objects to be assigned out to the individual nodes.
   * @throws FragmentSetupException
   */
  public QueryWorkUnit getFragments(QueryContext context, FragmentNode rootNode, FragmentPlanningSet planningSet,
      int globalMaxWidth) throws FragmentSetupException {
    assignEndpoints(context.getActiveEndpoints(), planningSet, globalMaxWidth);
    return generateWorkUnit(context.getQueryId(), context.getMapper(), rootNode, planningSet);
  }

  private QueryWorkUnit generateWorkUnit(long queryId, ObjectMapper mapper, FragmentNode rootNode,
      FragmentPlanningSet planningSet) throws FragmentSetupException {

    List<PlanFragment> fragments = Lists.newArrayList();

    PlanFragment rootFragment = null;

    // now we generate all the individual plan fragments and associated assignments. Note, we need all endpoints
    // assigned before we can materialize, so we start a new loop here rather than utilizing the previous one.
    for (FragmentWrapper info : planningSet) {

      FragmentNode node = info.getNode();
      FragmentStats stats = node.getStats();
      PhysicalOperator abstractRoot = node.getRoot();
      boolean isRootNode = rootNode == node;

      if (isRootNode && info.getWidth() != 1)
        throw new FragmentSetupException(
            String
                .format(
                    "Failure while trying to setup fragment.  The root fragment must always have parallelization one.  In the current case, the width was set to %d.",
                    info.getWidth()));
      // a fragment is self driven if it doesn't rely on any other exchanges.
      boolean selfDriven = node.getReceivingExchangePairs().size() == 0;

      // Create a minorFragment for each major fragment.
      for (int minorFragmentId = 0; minorFragmentId < info.getWidth(); minorFragmentId++) {
        IndexedFragmentNode iNode = new IndexedFragmentNode(minorFragmentId, info);
        PhysicalOperator root = abstractRoot.accept(materializer, iNode);

        // get plan as JSON
        String plan;
        try {
          plan = mapper.writeValueAsString(root);
        } catch (JsonProcessingException e) {
          throw new FragmentSetupException("Failure while trying to convert fragment into json.", e);
        }

        PlanFragment fragment = PlanFragment.newBuilder() //
            .setCpuCost(stats.getCpuCost()) //
            .setDiskCost(stats.getDiskCost()) //
            .setMemoryCost(stats.getMemoryCost()) //
            .setNetworkCost(stats.getNetworkCost()) //
            .setFragmentJson(plan) //
            .setMinorFragmentId(minorFragmentId) //
            .setMajorFragmentId(info.getMajorFragmentId()).setQueryId(queryId) //
            .setAssignment(info.getAssignedEndpoint(minorFragmentId)).setSelfDriven(selfDriven).build();

        if (isRootNode) {
          rootFragment = fragment;
        } else {
          fragments.add(fragment);
        }
      }
    }

    return new QueryWorkUnit(rootFragment, fragments);

  }

  private void assignEndpoints(Collection<DrillbitEndpoint> allNodes, FragmentPlanningSet planningSet,
      int globalMaxWidth) {
    // First we determine the amount of parallelization for a fragment. This will be between 1 and maxWidth based on
    // cost. (Later could also be based on cluster operation.) then we decide endpoints based on affinity (later this
    // could be based on endpoint load)
    for (FragmentWrapper info : planningSet) {

      FragmentStats stats = info.getStats();

      // figure out width.
      int width = Math.min(stats.getMaxWidth(), globalMaxWidth);
      float diskCost = stats.getDiskCost();

      // TODO: right now we'll just assume that each task is cost 1 so we'll set the breadth at the lesser of the number
      // of tasks or the maximum width of the fragment.
      if (diskCost < width) {
        width = (int) diskCost;
      }

      if (width < 1) width = 1;
      info.setWidth(width);

      // figure out endpoint assignments. also informs the exchanges about their respective endpoints.
      info.assignEndpoints(allNodes);
    }
  }
}
