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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.DrillStringUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.Exchange.ParallelizationDependency;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.fragment.Fragment.ExchangeFragmentPair;
import org.apache.drill.exec.planner.fragment.Materializer.IndexedFragmentNode;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.work.QueryWorkUnit;
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

  private static final Ordering<EndpointAffinity> ENDPOINT_AFFINITY_ORDERING = Ordering.from(new Comparator<EndpointAffinity>() {
    @Override
    public int compare(EndpointAffinity o1, EndpointAffinity o2) {
      // Sort in descending order of affinity values
      return Double.compare(o2.getAffinity(), o1.getAffinity());
    }
  });

  private final long parallelizationThreshold;
  private final int maxWidthPerNode;
  private final int maxGlobalWidth;
  private final double affinityFactor;

  public SimpleParallelizer(QueryContext context) {
    OptionManager optionManager = context.getOptions();
    long sliceTarget = optionManager.getOption(ExecConstants.SLICE_TARGET).num_val;
    this.parallelizationThreshold = sliceTarget > 0 ? sliceTarget : 1;
    this.maxWidthPerNode = optionManager.getOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY).num_val.intValue();
    this.maxGlobalWidth = optionManager.getOption(ExecConstants.MAX_WIDTH_GLOBAL_KEY).num_val.intValue();
    this.affinityFactor = optionManager.getOption(ExecConstants.AFFINITY_FACTOR_KEY).float_val.intValue();
  }

  public SimpleParallelizer(long parallelizationThreshold, int maxWidthPerNode, int maxGlobalWidth, double affinityFactor) {
    this.parallelizationThreshold = parallelizationThreshold;
    this.maxWidthPerNode = maxWidthPerNode;
    this.maxGlobalWidth = maxGlobalWidth;
    this.affinityFactor = affinityFactor;
  }


  /**
   * Generate a set of assigned fragments based on the provided fragment tree. Do not allow parallelization stages
   * to go beyond the global max width.
   *
   * @param options         Option list
   * @param foremanNode     The driving/foreman node for this query.  (this node)
   * @param queryId         The queryId for this query.
   * @param activeEndpoints The list of endpoints to consider for inclusion in planning this query.
   * @param reader          Tool used to read JSON plans
   * @param rootFragment    The root node of the PhysicalPlan that we will be parallelizing.
   * @param session         UserSession of user who launched this query.
   * @return The list of generated PlanFragment protobuf objects to be assigned out to the individual nodes.
   * @throws ExecutionSetupException
   */
  public QueryWorkUnit getFragments(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId,
      Collection<DrillbitEndpoint> activeEndpoints, PhysicalPlanReader reader, Fragment rootFragment,
      UserSession session) throws ExecutionSetupException {

    final PlanningSet planningSet = new PlanningSet();

    initFragmentWrappers(rootFragment, planningSet);

    final Set<Wrapper> leafFragments = constructFragmentDependencyGraph(planningSet);

    // Start parallelizing from leaf fragments
    for (Wrapper wrapper : leafFragments) {
      parallelizeFragment(wrapper, planningSet, activeEndpoints);
    }

    return generateWorkUnit(options, foremanNode, queryId, reader, rootFragment, planningSet, session);
  }

  // For every fragment, create a Wrapper in PlanningSet.
  @VisibleForTesting
  public void initFragmentWrappers(Fragment rootFragment, PlanningSet planningSet) {
    planningSet.get(rootFragment);

    for(ExchangeFragmentPair fragmentPair : rootFragment) {
      initFragmentWrappers(fragmentPair.getNode(), planningSet);
    }
  }

  /**
   * Based on the affinity of the Exchange that separates two fragments, setup fragment dependencies.
   *
   * @param planningSet
   * @return Returns a list of leaf fragments in fragment dependency graph.
   */
  private Set<Wrapper> constructFragmentDependencyGraph(PlanningSet planningSet) {

    // Set up dependency of fragments based on the affinity of exchange that separates the fragments.
    for(Wrapper currentFragmentWrapper : planningSet) {
      ExchangeFragmentPair sendingExchange = currentFragmentWrapper.getNode().getSendingExchangePair();
      if (sendingExchange != null) {
        ParallelizationDependency dependency = sendingExchange.getExchange().getParallelizationDependency();
        Wrapper receivingFragmentWrapper = planningSet.get(sendingExchange.getNode());

        if (dependency == ParallelizationDependency.RECEIVER_DEPENDS_ON_SENDER) {
          receivingFragmentWrapper.addFragmentDependency(currentFragmentWrapper);
        } else if (dependency == ParallelizationDependency.SENDER_DEPENDS_ON_RECEIVER) {
          currentFragmentWrapper.addFragmentDependency(receivingFragmentWrapper);
        }
      }
    }

    // Identify leaf fragments. Leaf fragments are fragments that have no other fragments depending on them for
    // parallelization info. First assume all fragments are leaf fragments. Go through the fragments one by one and
    // remove the fragment on which the current fragment depends on.
    final Set<Wrapper> roots = Sets.newHashSet();
    for(Wrapper w : planningSet) {
      roots.add(w);
    }

    for(Wrapper wrapper : planningSet) {
      final List<Wrapper> fragmentDependencies = wrapper.getFragmentDependencies();
      if (fragmentDependencies != null && fragmentDependencies.size() > 0) {
        for(Wrapper dependency : fragmentDependencies) {
          if (roots.contains(dependency)) {
            roots.remove(dependency);
          }
        }
      }
    }

    return roots;
  }

  /**
   * Helper method for parallelizing a given fragment. Dependent fragments are parallelized first before
   * parallelizing the given fragment.
   */
  private void parallelizeFragment(Wrapper fragmentWrapper, PlanningSet planningSet,
      Collection<DrillbitEndpoint> activeEndpoints) throws PhysicalOperatorSetupException {
    // If the fragment is already parallelized, return.
    if (fragmentWrapper.isEndpointsAssignmentDone()) {
      return;
    }

    // First parallelize fragments on which this fragment depends on.
    final List<Wrapper> fragmentDependencies = fragmentWrapper.getFragmentDependencies();
    if (fragmentDependencies != null && fragmentDependencies.size() > 0) {
      for(Wrapper dependency : fragmentDependencies) {
        parallelizeFragment(dependency, planningSet, activeEndpoints);
      }
    }

    Fragment fragment = fragmentWrapper.getNode();

    // Step 1: Find stats. Stats include various factors including cost of physical operators, parallelizability of
    // work in physical operator and affinity of physical operator to certain nodes.
    fragment.getRoot().accept(new StatsCollector(planningSet), fragmentWrapper);

    // Step 2: Find the parallelization width of fragment

    final Stats stats = fragmentWrapper.getStats();
    final ParallelizationInfo parallelizationInfo = stats.getParallelizationInfo();

    // 2.1 Use max cost of all operators in this fragment; this is consistent with the
    //     calculation that ExcessiveExchangeRemover uses
    // 2.1. Find the parallelization based on cost
    int width = (int) Math.ceil(stats.getMaxCost() / parallelizationThreshold);

    // 2.2. Cap the parallelization width by fragment level width limit and system level per query width limit
    width = Math.min(width, Math.min(parallelizationInfo.getMaxWidth(), maxGlobalWidth));

    // 2.3. Cap the parallelization width by system level per node width limit
    width = Math.min(width, maxWidthPerNode * activeEndpoints.size());

    // 2.4. Make sure width is at least the min width enforced by operators
    width = Math.max(parallelizationInfo.getMinWidth(), width);

    // 2.4. Make sure width is at most the max width enforced by operators
    width = Math.min(parallelizationInfo.getMaxWidth(), width);

    // 2.5 Finally make sure the width is at least one
    width = Math.max(1, width);

    fragmentWrapper.setWidth(width);

    List<DrillbitEndpoint> assignedEndpoints = findEndpoints(activeEndpoints,
        parallelizationInfo.getEndpointAffinityMap(), fragmentWrapper.getWidth());
    fragmentWrapper.assignEndpoints(assignedEndpoints);
  }

  // Assign endpoints based on the given endpoint list, affinity map and width.
  private List<DrillbitEndpoint> findEndpoints(Collection<DrillbitEndpoint> activeEndpoints,
      Map<DrillbitEndpoint, EndpointAffinity> endpointAffinityMap, final int width)
      throws PhysicalOperatorSetupException {

    final List<DrillbitEndpoint> endpoints = Lists.newArrayList();

    if (endpointAffinityMap.size() > 0) {
      // Get EndpointAffinity list sorted in descending order of affinity values
      List<EndpointAffinity> sortedAffinityList = ENDPOINT_AFFINITY_ORDERING.immutableSortedCopy(endpointAffinityMap.values());

      // Find the number of mandatory nodes (nodes with +infinity affinity).
      int numRequiredNodes = 0;
      for(EndpointAffinity ep : sortedAffinityList) {
        if (ep.isAssignmentRequired()) {
          numRequiredNodes++;
        } else {
          // As the list is sorted in descending order of affinities, we don't need to go beyond the first occurrance
          // of non-mandatory node
          break;
        }
      }

      if (width < numRequiredNodes) {
        throw new PhysicalOperatorSetupException("Can not parallelize the fragment as the parallelization width is " +
            "less than the number of mandatory nodes (nodes with +INFINITE affinity).");
      }

      // Find the maximum number of slots which should go to endpoints with affinity (See DRILL-825 for details)
      int affinedSlots =
          Math.max(1, (int) (affinityFactor * width / activeEndpoints.size())) * sortedAffinityList.size();

      // Make sure affined slots is at least the number of mandatory nodes
      affinedSlots = Math.max(affinedSlots, numRequiredNodes);

      // Cap the affined slots to max parallelization width
      affinedSlots = Math.min(affinedSlots, width);

      Iterator<EndpointAffinity> affinedEPItr = Iterators.cycle(sortedAffinityList);

      // Keep adding until we have selected "affinedSlots" number of endpoints.
      while(endpoints.size() < affinedSlots) {
        EndpointAffinity ea = affinedEPItr.next();
        endpoints.add(ea.getEndpoint());
      }
    }

    // add remaining endpoints if required
    if (endpoints.size() < width) {
      // Get a list of endpoints that are not part of the affinity endpoint list
      List<DrillbitEndpoint> endpointsWithNoAffinity;
      final Set<DrillbitEndpoint> endpointsWithAffinity = endpointAffinityMap.keySet();

      if (endpointAffinityMap.size() > 0) {
        endpointsWithNoAffinity = Lists.newArrayList();
        for (DrillbitEndpoint ep : activeEndpoints) {
          if (!endpointsWithAffinity.contains(ep)) {
            endpointsWithNoAffinity.add(ep);
          }
        }
      } else {
        endpointsWithNoAffinity = Lists.newArrayList(activeEndpoints); // Need to create a copy instead of an
        // immutable copy, because we need to shuffle the list (next statement) and Collections.shuffle() doesn't
        // support immutable copy as input.
      }

      // round robin with random start.
      Collections.shuffle(endpointsWithNoAffinity, ThreadLocalRandom.current());
      Iterator<DrillbitEndpoint> otherEPItr =
          Iterators.cycle(endpointsWithNoAffinity.size() > 0 ? endpointsWithNoAffinity : endpointsWithAffinity);
      while (endpoints.size() < width) {
        endpoints.add(otherEPItr.next());
      }
    }

    return endpoints;
  }

  private QueryWorkUnit generateWorkUnit(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId,
      PhysicalPlanReader reader, Fragment rootNode, PlanningSet planningSet,
      UserSession session) throws ExecutionSetupException {
    List<PlanFragment> fragments = Lists.newArrayList();

    PlanFragment rootFragment = null;
    FragmentRoot rootOperator = null;

    long queryStartTime = System.currentTimeMillis();
    int timeZone = DateUtility.getIndex(System.getProperty("user.timezone"));

    // now we generate all the individual plan fragments and associated assignments. Note, we need all endpoints
    // assigned before we can materialize, so we start a new loop here rather than utilizing the previous one.
    for (Wrapper wrapper : planningSet) {
      Fragment node = wrapper.getNode();
      final PhysicalOperator physicalOperatorRoot = node.getRoot();
      boolean isRootNode = rootNode == node;

      if (isRootNode && wrapper.getWidth() != 1) {
        throw new ForemanSetupException(String.format("Failure while trying to setup fragment. " +
                "The root fragment must always have parallelization one. In the current case, the width was set to %d.",
                wrapper.getWidth()));
      }
      // a fragment is self driven if it doesn't rely on any other exchanges.
      boolean isLeafFragment = node.getReceivingExchangePairs().size() == 0;

      // Create a minorFragment for each major fragment.
      for (int minorFragmentId = 0; minorFragmentId < wrapper.getWidth(); minorFragmentId++) {
        IndexedFragmentNode iNode = new IndexedFragmentNode(minorFragmentId, wrapper);
        wrapper.resetAllocation();
        PhysicalOperator op = physicalOperatorRoot.accept(Materializer.INSTANCE, iNode);
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
}
