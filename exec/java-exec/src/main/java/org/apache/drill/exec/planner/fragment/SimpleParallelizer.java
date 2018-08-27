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
package org.apache.drill.exec.planner.fragment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.DrillStringUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.Exchange.ParallelizationDependency;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Receiver;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.fragment.Fragment.ExchangeFragmentPair;
import org.apache.drill.exec.planner.fragment.Materializer.IndexedFragmentNode;
import org.apache.drill.exec.proto.BitControl.Collector;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.BitControl.QueryContextInformation;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.options.OptionList;

import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.QueryWorkUnit.MinorFragmentDefn;
import org.apache.drill.exec.work.foreman.ForemanSetupException;


/**
 * The simple parallelizer determines the level of parallelization of a plan based on the cost of the underlying
 * operations.  It doesn't take into account system load or other factors.  Based on the cost of the query, the
 * parallelization for each major fragment will be determined.  Once the amount of parallelization is done, assignment
 * is done based on round robin assignment ordered by operator affinity (locality) to available execution Drillbits.
 */
public class SimpleParallelizer implements ParallelizationParameters {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleParallelizer.class);

  private final long parallelizationThreshold;
  private final int maxWidthPerNode;
  private final int maxGlobalWidth;
  private final double affinityFactor;

  public SimpleParallelizer(QueryContext context) {
    OptionManager optionManager = context.getOptions();
    long sliceTarget = optionManager.getOption(ExecConstants.SLICE_TARGET_OPTION);
    this.parallelizationThreshold = sliceTarget > 0 ? sliceTarget : 1;
    double cpu_load_average = optionManager.getOption(ExecConstants.CPU_LOAD_AVERAGE);
    final long maxWidth = optionManager.getOption(ExecConstants.MAX_WIDTH_PER_NODE);
    // compute the maxwidth
    this.maxWidthPerNode = ExecConstants.MAX_WIDTH_PER_NODE.computeMaxWidth(cpu_load_average,maxWidth);
    this.maxGlobalWidth = optionManager.getOption(ExecConstants.MAX_WIDTH_GLOBAL_KEY).num_val.intValue();
    this.affinityFactor = optionManager.getOption(ExecConstants.AFFINITY_FACTOR_KEY).float_val.intValue();
  }

  public SimpleParallelizer(long parallelizationThreshold, int maxWidthPerNode, int maxGlobalWidth, double affinityFactor) {
    this.parallelizationThreshold = parallelizationThreshold;
    this.maxWidthPerNode = maxWidthPerNode;
    this.maxGlobalWidth = maxGlobalWidth;
    this.affinityFactor = affinityFactor;
  }

  @Override
  public long getSliceTarget() {
    return parallelizationThreshold;
  }

  @Override
  public int getMaxWidthPerNode() {
    return maxWidthPerNode;
  }

  @Override
  public int getMaxGlobalWidth() {
    return maxGlobalWidth;
  }

  @Override
  public double getAffinityFactor() {
    return affinityFactor;
  }

  /**
   * Generate a set of assigned fragments based on the provided fragment tree. Do not allow parallelization stages
   * to go beyond the global max width.
   *
   * @param options         Option list
   * @param foremanNode     The driving/foreman node for this query.  (this node)
   * @param queryId         The queryId for this query.
   * @param activeEndpoints The list of endpoints to consider for inclusion in planning this query.
   * @param rootFragment    The root node of the PhysicalPlan that we will be parallelizing.
   * @param session         UserSession of user who launched this query.
   * @param queryContextInfo Info related to the context when query has started.
   * @return The list of generated PlanFragment protobuf objects to be assigned out to the individual nodes.
   * @throws ExecutionSetupException
   */
  public QueryWorkUnit getFragments(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId,
      Collection<DrillbitEndpoint> activeEndpoints, Fragment rootFragment,
      UserSession session, QueryContextInformation queryContextInfo) throws ExecutionSetupException {

    final PlanningSet planningSet = getFragmentsHelper(activeEndpoints, rootFragment);
    return generateWorkUnit(
        options, foremanNode, queryId, rootFragment, planningSet, session, queryContextInfo);
  }

  /**
   * Create multiple physical plans from original query planning, it will allow execute them eventually independently
   * @param options
   * @param foremanNode
   * @param queryId
   * @param activeEndpoints
   * @param reader
   * @param rootFragment
   * @param session
   * @param queryContextInfo
   * @return The {@link QueryWorkUnit}s.
   * @throws ExecutionSetupException
   */
  public List<QueryWorkUnit> getSplitFragments(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId,
      Collection<DrillbitEndpoint> activeEndpoints, PhysicalPlanReader reader, Fragment rootFragment,
      UserSession session, QueryContextInformation queryContextInfo) throws ExecutionSetupException {
    // no op
    throw new UnsupportedOperationException("Use children classes");
  }

  /**
   * Helper method to reuse the code for QueryWorkUnit(s) generation
   * @param activeEndpoints
   * @param rootFragment
   * @return A {@link PlanningSet}.
   * @throws ExecutionSetupException
   */
  protected PlanningSet getFragmentsHelper(Collection<DrillbitEndpoint> activeEndpoints, Fragment rootFragment) throws ExecutionSetupException {

    PlanningSet planningSet = new PlanningSet();

    initFragmentWrappers(rootFragment, planningSet);

    final Set<Wrapper> leafFragments = constructFragmentDependencyGraph(planningSet);

    // Start parallelizing from leaf fragments
    for (Wrapper wrapper : leafFragments) {
      parallelizeFragment(wrapper, planningSet, activeEndpoints);
    }
    planningSet.findRootWrapper(rootFragment);
    return planningSet;
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
  private static Set<Wrapper> constructFragmentDependencyGraph(PlanningSet planningSet) {

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

    // Find stats. Stats include various factors including cost of physical operators, parallelizability of
    // work in physical operator and affinity of physical operator to certain nodes.
    fragmentWrapper.getNode().getRoot().accept(new StatsCollector(planningSet), fragmentWrapper);

    fragmentWrapper.getStats().getDistributionAffinity()
        .getFragmentParallelizer()
        .parallelizeFragment(fragmentWrapper, this, activeEndpoints);
  }

  protected QueryWorkUnit generateWorkUnit(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId,
      Fragment rootNode, PlanningSet planningSet,
      UserSession session, QueryContextInformation queryContextInfo) throws ExecutionSetupException {
    List<MinorFragmentDefn> fragmentDefns = new ArrayList<>( );

    MinorFragmentDefn rootFragmentDefn = null;
    FragmentRoot rootOperator = null;
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

        FragmentHandle handle = FragmentHandle //
            .newBuilder() //
            .setMajorFragmentId(wrapper.getMajorFragmentId()) //
            .setMinorFragmentId(minorFragmentId) //
            .setQueryId(queryId) //
            .build();

        PlanFragment fragment = PlanFragment.newBuilder() //
            .setForeman(foremanNode) //
            .setHandle(handle) //
            .setAssignment(wrapper.getAssignedEndpoint(minorFragmentId)) //
            .setLeafFragment(isLeafFragment) //
            .setContext(queryContextInfo)
            .setMemInitial(wrapper.getInitialAllocation())//
            .setMemMax(wrapper.getMaxAllocation())
            .setCredentials(session.getCredentials())
            .addAllCollector(CountRequiredFragments.getCollectors(root))
            .build();

        MinorFragmentDefn fragmentDefn = new MinorFragmentDefn(fragment, root, options);

        if (isRootNode) {
          logger.debug("Root fragment:\n {}", DrillStringUtils.unescapeJava(fragment.toString()));
          rootFragmentDefn = fragmentDefn;
          rootOperator = root;
        } else {
          logger.debug("Remote fragment:\n {}", DrillStringUtils.unescapeJava(fragment.toString()));
          fragmentDefns.add(fragmentDefn);
        }
      }
    }
    Wrapper rootWrapper = planningSet.getRootWrapper();
    return new QueryWorkUnit(rootOperator, rootFragmentDefn, fragmentDefns, rootWrapper);
  }

  /**
   * Designed to setup initial values for arriving fragment accounting.
   */

  protected static class CountRequiredFragments extends AbstractPhysicalVisitor<Void, List<Collector>, RuntimeException> {
    private static final CountRequiredFragments INSTANCE = new CountRequiredFragments();

    public static List<Collector> getCollectors(PhysicalOperator root) {
      List<Collector> collectors = Lists.newArrayList();
      root.accept(INSTANCE, collectors);
      return collectors;
    }

    @Override
    public Void visitReceiver(Receiver receiver, List<Collector> collectors) throws RuntimeException {
      List<MinorFragmentEndpoint> endpoints = receiver.getProvidingEndpoints();
      List<Integer> list = new ArrayList<>(endpoints.size());
      for (MinorFragmentEndpoint ep : endpoints) {
        list.add(ep.getId());
      }

      collectors.add(Collector.newBuilder()
        .setIsSpooling(receiver.isSpooling())
        .setOppositeMajorFragmentId(receiver.getOppositeMajorFragmentId())
        .setSupportsOutOfOrder(receiver.supportsOutOfOrderExchange())
          .addAllIncomingMinorFragment(list)
          .build());
      return null;
    }

    @Override
    public Void visitOp(PhysicalOperator op, List<Collector> collectors) throws RuntimeException {
      for (PhysicalOperator o : op) {
        o.accept(this, collectors);
      }
      return null;
    }
  }
}
