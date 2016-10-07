/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.planner.fragment;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import java.util.Map;
import java.util.List;
import java.util.Collection;
import java.util.HashMap;
import java.util.Collections;
import org.slf4j.Logger;

/**
 * Implementation of {@link FragmentParallelizer} where fragment has zero or more endpoints.
 * Fragment placement is done preferring data locality.
 */
public class LocalAffinityFragmentParallelizer implements FragmentParallelizer {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(LocalAffinityFragmentParallelizer.class);
    public static final LocalAffinityFragmentParallelizer INSTANCE = new LocalAffinityFragmentParallelizer();
    private static String EOL = System.getProperty("line.separator");

    // Sort a list of map entries in decreasing order by values.
    Ordering<Map.Entry<DrillbitEndpoint, Integer>> sortByValues = new Ordering<Map.Entry<DrillbitEndpoint, Integer>>() {
        @Override
        public int compare(Map.Entry<DrillbitEndpoint, Integer> left, Map.Entry<DrillbitEndpoint, Integer> right) {
            return right.getValue().compareTo(left.getValue());
        }
    };

    @Override
    public void parallelizeFragment(final Wrapper fragmentWrapper, final ParallelizationParameters parameters,
                                    final Collection<DrillbitEndpoint> activeEndpoints) throws PhysicalOperatorSetupException {
        final Stats stats = fragmentWrapper.getStats();
        final ParallelizationInfo parallelizationInfo = stats.getParallelizationInfo();
        logger.trace("LocalAffinity Fragment Parallelizer: " +  "MaxCost: {}, " +  "SliceTarget: {}, " +
                     "Parallelization MaxWidth: {}," + EOL + "Parallelization MinWidth: {}," + "MaxGlobalWidth: {}," +
                     "MaxWidthPerNode {}," + EOL + "ActiveEndPoints: {}",
                     stats.getMaxCost(), parameters.getSliceTarget(), parallelizationInfo.getMaxWidth(),
                     parameters.getMaxGlobalWidth(), parameters.getMaxWidthPerNode(), activeEndpoints);

        final Map<DrillbitEndpoint, EndpointAffinity> endpointAffinityMap =
            fragmentWrapper.getStats().getParallelizationInfo().getEndpointAffinityMap();
        int totalLocalWorkUnits = 0;
        Map<DrillbitEndpoint, Integer> localEndpointPool = new HashMap<>(); // Nodes with data locality.

        // Get the total number of work units and list of endPoints with data locality to schedule fragments on
        for (Map.Entry<DrillbitEndpoint, EndpointAffinity> epAff : endpointAffinityMap.entrySet()) {
            if (epAff.getValue().getNumLocalWorkUnits() > 0) {
                totalLocalWorkUnits += epAff.getValue().getNumLocalWorkUnits();
                localEndpointPool.put(epAff.getKey(), epAff.getValue().getNumLocalWorkUnits());
            }
        }

        // Find the parallelization width of fragment
        // 1. Find the parallelization based on cost. Use max cost of all operators in this fragment;
        int width = (int) Math.ceil(stats.getMaxCost() / parameters.getSliceTarget());

        // 2. Cap the parallelization width by fragment level width limit and system level per query width limit
        width = Math.min(width, Math.min(parallelizationInfo.getMaxWidth(), parameters.getMaxGlobalWidth()));

        // 3. Cap the parallelization width by system level per node width limit
        width = Math.min(width, parameters.getMaxWidthPerNode() * activeEndpoints.size());

        // 4. Make sure width is at least the min width enforced by operators
        width = Math.max(parallelizationInfo.getMinWidth(), width);

        // 5. Make sure width is at most the max width enforced by operators
        width = Math.min(parallelizationInfo.getMaxWidth(), width);

        // 6: Finally make sure the width is at least one
        width = Math.max(1, width);

        List<DrillbitEndpoint> assignedEndPoints = Lists.newArrayList();
        int totalAssigned = 0;

        // Sort the endpointPool based on numLocalWorkUnits. This sorting is done because we are doing
        // round robin allocation and we stop when we reach the width. We want to allocate
        // on endpoints which have higher numLocalWorkUnits first.
        List<Map.Entry<DrillbitEndpoint, Integer>> sortedEndpointPool = Lists.newArrayList(localEndpointPool.entrySet());
        Collections.sort(sortedEndpointPool, sortByValues);

        // Keep track of number of fragments allocated to each endpoint.
        Map<DrillbitEndpoint, Integer> endpointAssignments = new HashMap<>();

        // Keep track of how many more to assign to each endpoint.
        Map<DrillbitEndpoint, Integer> remainingEndpointAssignments = new HashMap<>();

        // localWidth is the width that we can allocate up to if we allocate only on nodes with locality.
        int localWidth = Math.min(width, parameters.getMaxWidthPerNode() * localEndpointPool.size());

        logger.trace("LocalAffinity Fragment Parallelizer: " +  "width: {}, " +  "totalLocalworkUnits: {}, " +
                     "localWidth: {}," + EOL + "localEndpointPool: {}",
                     width, totalLocalWorkUnits, localWidth, localEndpointPool);

        // Calculate the target allocation for each endPoint with data locality based on work it has to do
        // Assign one fragment (minimum) to these endPoints.
        for (DrillbitEndpoint ep : localEndpointPool.keySet()) {
            final int numWorkUnits = endpointAffinityMap.get(ep).getNumLocalWorkUnits();
            final int targetAllocation = Math.min(numWorkUnits,
                                                  (int) Math.ceil(localWidth * ((double)numWorkUnits/totalLocalWorkUnits)));
            assignedEndPoints.add(ep);
            totalAssigned++;
            endpointAssignments.put(ep, 1);
            remainingEndpointAssignments.put(ep, targetAllocation - 1);
            if (totalAssigned == localWidth) { // do not allocate more than local width
                break;
            }
        }

        // Keep allocating from endpoints in a round robin fashion up to min(targetAllocation, maxwidthPerNode)
        // for each endpoint with data locality and upto localWidth all together.
        while(totalAssigned < localWidth) {
            int assignedThisRound = 0;
            for (Map.Entry<DrillbitEndpoint, Integer> epEntry : sortedEndpointPool) {
                DrillbitEndpoint ep = epEntry.getKey();
                final int remainingAssignments = remainingEndpointAssignments.get(ep);
                final int currentAssignments = endpointAssignments.get(ep);
                if (remainingAssignments > 0 && currentAssignments < parameters.getMaxWidthPerNode()) {
                    assignedEndPoints.add(ep);
                    remainingEndpointAssignments.put(ep, remainingAssignments - 1);
                    totalAssigned++;
                    assignedThisRound++;
                    endpointAssignments.put(ep, currentAssignments + 1);
                }
                if (totalAssigned == localWidth) {
                    break;
                }
            }
            if (assignedThisRound == 0) {
                break;
            }
        }
        // At this point, we have taken care of allocating fragments for totalLocalWorkUnits, i.e. workUnits which
        // have data locality information.
        // For the workUnits which do not have data locality information (For the case where drillbits are not running
        // on endPoints which have data and local filesystem), allocate them from the active endpoint pool.
        // If we have already scheduled parallelizationInfo.getMaxWidth() fragments, do not schedule any more.
        // Else, figure out fragments to schedule (how many and where ?) on active end points for unAssignedWorkUnits.
        // We can assign max of upto width.
        int unAssigned = parallelizationInfo.getMaxWidth() > totalLocalWorkUnits ?
                                                       (parallelizationInfo.getMaxWidth() - totalLocalWorkUnits) : 0;
        while (totalAssigned < width && unAssigned > 0) {
            for (DrillbitEndpoint ep : activeEndpoints) {
                if (endpointAssignments.containsKey(ep) &&
                    endpointAssignments.get(ep) >= parameters.getMaxWidthPerNode()) {
                    continue;
                }
                assignedEndPoints.add(ep);
                totalAssigned++;
                unAssigned--;
                if (endpointAssignments.containsKey(ep)) {
                    endpointAssignments.put(ep, endpointAssignments.get(ep) + 1);
                } else {
                    endpointAssignments.put(ep, 1);
                }
                if (unAssigned == 0 || totalAssigned == width) {
                    break;
                }
            }
        }

        logger.trace("LocalAffinity Fragment Parallelizer: " + "Total Assigned: {}" + EOL +
                     "Endpoint Assignments: {}", totalAssigned, endpointAssignments);

        fragmentWrapper.setWidth(assignedEndPoints.size());
        fragmentWrapper.assignEndpoints(assignedEndPoints);
    }
}
