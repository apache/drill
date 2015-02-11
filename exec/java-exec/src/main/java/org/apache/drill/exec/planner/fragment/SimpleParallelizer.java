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
package org.apache.drill.exec.planner.fragment;

import java.util.Collection;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.DrillStringUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.fragment.Materializer.IndexedFragmentNode;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.foreman.ForemanException;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * The simple parallelizer determines the level of parallelization of a plan based on the cost of the underlying
 * operations.  It doesn't take into account system load or other factors.  Based on the cost of the query, the
 * parallelization for each major fragment will be determined.  Once the amount of parallelization is done, assignment
 * is done based on round robin assignment ordered by operator affinity (locality) to available execution Drillbits.
 */
public class SimpleParallelizer {



  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleParallelizer.class);
  private final Materializer materializer = new Materializer();
  private final long parallelizationThreshold;
  private final int maxWidthPerNode;
  private final int maxGlobalWidth;
  private double affinityFactor;

  public SimpleParallelizer(QueryContext context) {
    long sliceTarget = context.getOptions().getOption(ExecConstants.SLICE_TARGET).num_val;
    this.parallelizationThreshold = sliceTarget > 0 ? sliceTarget : 1;
    this.maxWidthPerNode = context.getOptions().getOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY).num_val.intValue();
    this.maxGlobalWidth = context.getOptions().getOption(ExecConstants.MAX_WIDTH_GLOBAL_KEY).num_val.intValue();
    this.affinityFactor = context.getOptions().getOption(ExecConstants.AFFINITY_FACTOR_KEY).float_val.intValue();
  }

  public SimpleParallelizer(long parallelizationThreshold, int maxWidthPerNode, int maxGlobalWidth, double affinityFactor) {
    this.parallelizationThreshold = parallelizationThreshold;
    this.maxWidthPerNode = maxWidthPerNode;
    this.maxGlobalWidth = maxGlobalWidth;
    this.affinityFactor = affinityFactor;
  }


  /**
   * Generate a set of assigned fragments based on the provided planningSet. Do not allow parallelization stages to go
   * beyond the global max width.
   *
   * @param foremanNode     The driving/foreman node for this query.  (this node)
   * @param queryId         The queryId for this query.
   * @param activeEndpoints The list of endpoints to consider for inclusion in planning this query.
   * @param reader          Tool used to read JSON plans
   * @param rootNode        The root node of the PhysicalPlan that we will parallelizing.
   * @param planningSet     The set of queries with collected statistics that we'll work with.
   * @return The list of generated PlanFragment protobuf objects to be assigned out to the individual nodes.
   * @throws ForemanException
   */
  public QueryWorkUnit getFragments(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId, Collection<DrillbitEndpoint> activeEndpoints,
      PhysicalPlanReader reader, Fragment rootNode, PlanningSet planningSet, UserSession session) throws ExecutionSetupException {
    assignEndpoints(activeEndpoints, planningSet);
    return generateWorkUnit(options, foremanNode, queryId, reader, rootNode, planningSet, session);
  }

  private QueryWorkUnit generateWorkUnit(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId, PhysicalPlanReader reader, Fragment rootNode,
                                         PlanningSet planningSet, UserSession session) throws ExecutionSetupException {

    List<PlanFragment> fragments = Lists.newArrayList();

    PlanFragment rootFragment = null;
    FragmentRoot rootOperator = null;

    long queryStartTime = System.currentTimeMillis();
    int timeZone = DateUtility.getIndex(System.getProperty("user.timezone"));

    // now we generate all the individual plan fragments and associated assignments. Note, we need all endpoints
    // assigned before we can materialize, so we start a new loop here rather than utilizing the previous one.
    for (Wrapper wrapper : planningSet) {
      Fragment node = wrapper.getNode();
      Stats stats = node.getStats();
      final PhysicalOperator physicalOperatorRoot = node.getRoot();
      boolean isRootNode = rootNode == node;

      if (isRootNode && wrapper.getWidth() != 1) {
        throw new ForemanSetupException(
            String.format(
                "Failure while trying to setup fragment.  The root fragment must always have parallelization one.  In the current case, the width was set to %d.",
                wrapper.getWidth()));
      }
      // a fragment is self driven if it doesn't rely on any other exchanges.
      boolean isLeafFragment = node.getReceivingExchangePairs().size() == 0;

      // Create a minorFragment for each major fragment.
      for (int minorFragmentId = 0; minorFragmentId < wrapper.getWidth(); minorFragmentId++) {
        IndexedFragmentNode iNode = new IndexedFragmentNode(minorFragmentId, wrapper);
        wrapper.resetAllocation();
        PhysicalOperator op = physicalOperatorRoot.accept(materializer, iNode);
        Preconditions.checkArgument(op instanceof FragmentRoot);
        FragmentRoot root = (FragmentRoot) op;

        // get plan as JSON
        String plan;
        String optionsData;
        try {
          plan = reader.writeJson(root);
          optionsData = reader.writeJson(options);
        } catch (JsonProcessingException e) {
          throw new ForemanSetupException("Failure while trying to convert fragment into json.", e);
        }

        FragmentHandle handle = FragmentHandle //
            .newBuilder() //
            .setMajorFragmentId(wrapper.getMajorFragmentId()) //
            .setMinorFragmentId(minorFragmentId) //
            .setQueryId(queryId) //
            .build();
        PlanFragment fragment = PlanFragment.newBuilder() //
            .setForeman(foremanNode) //
            .setFragmentJson(plan) //
            .setHandle(handle) //
            .setAssignment(wrapper.getAssignedEndpoint(minorFragmentId)) //
            .setLeafFragment(isLeafFragment) //
            .setQueryStartTime(queryStartTime)
            .setTimeZone(timeZone)//
            .setMemInitial(wrapper.getInitialAllocation())//
            .setMemMax(wrapper.getMaxAllocation())
            .setOptionsJson(optionsData)
            .setCredentials(session.getCredentials())
            .build();

        if (isRootNode) {
          logger.debug("Root fragment:\n {}", DrillStringUtils.unescapeJava(fragment.toString()));
          rootFragment = fragment;
          rootOperator = root;
        } else {
          logger.debug("Remote fragment:\n {}", DrillStringUtils.unescapeJava(fragment.toString()));
          fragments.add(fragment);
        }
      }
    }

    return new QueryWorkUnit(rootOperator, rootFragment, fragments);
  }

  private void assignEndpoints(Collection<DrillbitEndpoint> allNodes, PlanningSet planningSet) throws PhysicalOperatorSetupException {
    // for each node, set the width based on the parallelization threshold and cluster width.
    for (Wrapper wrapper : planningSet) {

      Stats stats = wrapper.getStats();

      // Use max cost of all operators in this fragment; this is consistent with the
      // calculation that ExcessiveExchangeRemover uses
      double targetSlices = stats.getMaxCost()/parallelizationThreshold;
      int targetIntSlices = (int) Math.ceil(targetSlices);

      // figure out width.
      int width = Math.min(targetIntSlices, Math.min(stats.getMaxWidth(), maxGlobalWidth));


      width = Math.min(width, maxWidthPerNode*allNodes.size());

      if (width < 1) {
        width = 1;
      }
//      logger.debug("Setting width {} on fragment {}", width, wrapper);
      wrapper.setWidth(width);
      // figure out endpoint assignments. also informs the exchanges about their respective endpoints.
      wrapper.assignEndpoints(allNodes, affinityFactor);
    }
  }

}
