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
package org.apache.drill.exec.planner.fragment.contrib;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.DrillStringUtils;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.base.Exchange;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.PlanningSet;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.planner.fragment.Wrapper;
import org.apache.drill.exec.planner.fragment.Materializer.IndexedFragmentNode;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.BitControl.QueryContextInformation;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * SimpleParallelizerMultiPlans class is an extension to SimpleParallelizer
 * to help with getting PlanFragments for split plan.
 * Split plan is essentially ability to create multiple Physical Operator plans from original Physical Operator plan
 * to be able to run plans separately.
 * Moving functionality specific to splitting the plan to this class
 * allows not to pollute parent class with non-authentic functionality
 *
 */
public class SplittingParallelizer extends SimpleParallelizer {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SplittingParallelizer.class);

  public SplittingParallelizer(QueryContext context) {
    super(context);
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
   * @return
   * @throws ExecutionSetupException
   */
  public List<QueryWorkUnit> getSplitFragments(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId,
      Collection<DrillbitEndpoint> activeEndpoints, PhysicalPlanReader reader, Fragment rootFragment,
      UserSession session, QueryContextInformation queryContextInfo) throws ExecutionSetupException {

    final PlanningSet planningSet = getFragmentsHelper(activeEndpoints, rootFragment);

    return generateWorkUnits(
        options, foremanNode, queryId, reader, rootFragment, planningSet, session, queryContextInfo);
  }

  /**
   * Split plan into multiple plans based on parallelization
   * Ideally it is applicable only to plans with two major fragments: Screen and UnionExchange
   * But there could be cases where we can remove even multiple exchanges like in case of "order by"
   * End goal is to get single major fragment: Screen with chain that ends up with a single minor fragment
   * from Leaf Exchange. This way each plan can run independently without any exchange involvement
   * @param options
   * @param foremanNode - not really applicable
   * @param queryId
   * @param reader
   * @param rootNode
   * @param planningSet
   * @param session
   * @param queryContextInfo
   * @return
   * @throws ExecutionSetupException
   */
  private List<QueryWorkUnit> generateWorkUnits(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId,
      PhysicalPlanReader reader, Fragment rootNode, PlanningSet planningSet,
      UserSession session, QueryContextInformation queryContextInfo) throws ExecutionSetupException {

    // now we generate all the individual plan fragments and associated assignments. Note, we need all endpoints
    // assigned before we can materialize, so we start a new loop here rather than utilizing the previous one.

    List<QueryWorkUnit> workUnits = Lists.newArrayList();
    int plansCount = 0;
    DrillbitEndpoint[] endPoints = null;
    long initialAllocation = 0;
    long maxAllocation = 0;

    final Iterator<Wrapper> iter = planningSet.iterator();
    while (iter.hasNext()) {
      Wrapper wrapper = iter.next();
      Fragment node = wrapper.getNode();
      boolean isLeafFragment = node.getReceivingExchangePairs().size() == 0;
      final PhysicalOperator physicalOperatorRoot = node.getRoot();
      // get all the needed info from leaf fragment
      if ( (physicalOperatorRoot instanceof Exchange) &&  isLeafFragment) {
        // need to get info about
        // number of minor fragments
        // assignedEndPoints
        // allocation
        plansCount = wrapper.getWidth();
        initialAllocation = (wrapper.getInitialAllocation() != 0 ) ? wrapper.getInitialAllocation()/plansCount : 0;
        maxAllocation = (wrapper.getMaxAllocation() != 0 ) ? wrapper.getMaxAllocation()/plansCount : 0;
        endPoints = new DrillbitEndpoint[plansCount];
        for (int mfId = 0; mfId < plansCount; mfId++) {
          endPoints[mfId] = wrapper.getAssignedEndpoint(mfId);
        }
      }
    }
    if ( plansCount == 0 ) {
      // no exchange, return list of single QueryWorkUnit
      workUnits.add(generateWorkUnit(options, foremanNode, queryId, reader, rootNode, planningSet, session, queryContextInfo));
      return workUnits;
    }

    for (Wrapper wrapper : planningSet) {
      Fragment node = wrapper.getNode();
      final PhysicalOperator physicalOperatorRoot = node.getRoot();
      if ( physicalOperatorRoot instanceof Exchange ) {
        // get to 0 MajorFragment
        continue;
      }
      boolean isRootNode = rootNode == node;

      if (isRootNode && wrapper.getWidth() != 1) {
        throw new ForemanSetupException(String.format("Failure while trying to setup fragment. " +
                "The root fragment must always have parallelization one. In the current case, the width was set to %d.",
                wrapper.getWidth()));
      }
      // this fragment is always leaf, as we are removing all the exchanges
      boolean isLeafFragment = true;

      FragmentHandle handle = FragmentHandle //
          .newBuilder() //
          .setMajorFragmentId(wrapper.getMajorFragmentId()) //
          .setMinorFragmentId(0) // minor fragment ID is going to be always 0, as plan will be split
          .setQueryId(queryId) //
          .build();

      // Create a minorFragment for each major fragment.
      for (int minorFragmentId = 0; minorFragmentId < plansCount; minorFragmentId++) {
        // those fragments should be empty
        List<PlanFragment> fragments = Lists.newArrayList();

        PlanFragment rootFragment = null;
        FragmentRoot rootOperator = null;

        IndexedFragmentNode iNode = new IndexedFragmentNode(minorFragmentId, wrapper);
        wrapper.resetAllocation();
        // two visitors here
        // 1. To remove exchange
        // 2. To reset operator IDs as exchanges were removed
        PhysicalOperator op = physicalOperatorRoot.accept(ExchangeRemoverMaterializer.INSTANCE, iNode).
            accept(OperatorIdVisitor.INSTANCE, 0);
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

        PlanFragment fragment = PlanFragment.newBuilder() //
            .setForeman(endPoints[minorFragmentId]) //
            .setFragmentJson(plan) //
            .setHandle(handle) //
            .setAssignment(endPoints[minorFragmentId]) //
            .setLeafFragment(isLeafFragment) //
            .setContext(queryContextInfo)
            .setMemInitial(initialAllocation)//
            .setMemMax(wrapper.getMaxAllocation()) // TODO - for some reason OOM is using leaf fragment max allocation divided by width
            .setOptionsJson(optionsData)
            .setCredentials(session.getCredentials())
            .addAllCollector(CountRequiredFragments.getCollectors(root))
            .build();

        if (isRootNode) {
          logger.debug("Root fragment:\n {}", DrillStringUtils.unescapeJava(fragment.toString()));
          rootFragment = fragment;
          rootOperator = root;
        } else {
          logger.debug("Remote fragment:\n {}", DrillStringUtils.unescapeJava(fragment.toString()));
          throw new ForemanSetupException(String.format("There should not be non-root/remote fragment present in plan split, but there is:",
              DrillStringUtils.unescapeJava(fragment.toString())));
         }
        // fragments should be always empty here
        workUnits.add(new QueryWorkUnit(rootOperator, rootFragment, fragments));
      }
    }
    return workUnits;
  }
}
