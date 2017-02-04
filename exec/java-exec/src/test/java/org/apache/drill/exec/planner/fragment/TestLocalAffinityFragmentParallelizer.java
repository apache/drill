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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import static java.lang.Integer.MAX_VALUE;
import static org.apache.drill.exec.ExecConstants.SLICE_TARGET_DEFAULT;
import static org.apache.drill.exec.planner.fragment.LocalAffinityFragmentParallelizer.INSTANCE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;


public class TestLocalAffinityFragmentParallelizer {

    // Create a set of test endpoints
    private static final DrillbitEndpoint DEP1 = newDrillbitEndpoint("node1", 30010);
    private static final DrillbitEndpoint DEP2 = newDrillbitEndpoint("node2", 30010);
    private static final DrillbitEndpoint DEP3 = newDrillbitEndpoint("node3", 30010);
    private static final DrillbitEndpoint DEP4 = newDrillbitEndpoint("node4", 30010);
    private static final DrillbitEndpoint DEP5 = newDrillbitEndpoint("node5", 30010);

    @Mocked private Fragment fragment;
    @Mocked private PhysicalOperator root;

    private static final DrillbitEndpoint newDrillbitEndpoint(String address, int port) {
        return DrillbitEndpoint.newBuilder().setAddress(address).setControlPort(port).build();
    }

    private static final ParallelizationParameters newParameters(final long threshold, final int maxWidthPerNode,
                                                                 final int maxGlobalWidth) {
        return new ParallelizationParameters() {
            @Override
            public long getSliceTarget() {
                return threshold;
            }

            @Override
            public int getMaxWidthPerNode() {
                return maxWidthPerNode;
            }

            @Override
            public int getMaxGlobalWidth() {
                return maxGlobalWidth;
            }

            /**
             * {@link LocalAffinityFragmentParallelizer} doesn't use affinity factor.
             * @return
             */
            @Override
            public double getAffinityFactor() {
                return 0.0f;
            }
        };
    }

    private final Wrapper newWrapper(double cost, int minWidth, int maxWidth, List<EndpointAffinity> endpointAffinities) {
        new NonStrictExpectations() {
            {
                fragment.getRoot(); result = root;
            }
        };

        final Wrapper fragmentWrapper = new Wrapper(fragment, 1);
        final Stats stats = fragmentWrapper.getStats();
        stats.setDistributionAffinity(DistributionAffinity.LOCAL);
        stats.addCost(cost);
        stats.addMinWidth(minWidth);
        stats.addMaxWidth(maxWidth);
        stats.addEndpointAffinities(endpointAffinities);
        return fragmentWrapper;
    }

    private void checkEndpointAssignments(List<DrillbitEndpoint> assignedEndpoints,
                                          Map<DrillbitEndpoint, Integer> expectedAssignments) throws Exception {
        Map<DrillbitEndpoint, Integer> endpointAssignments = new HashMap<>();
        // Count the number of fragments assigned to each endpoint.
        for (DrillbitEndpoint endpoint: assignedEndpoints) {
            if (endpointAssignments.containsKey(endpoint)) {
                endpointAssignments.put(endpoint, endpointAssignments.get(endpoint) + 1);
            } else {
                endpointAssignments.put(endpoint, 1);
            }
        }

        // Verify number of fragments assigned to each endpoint against the expected value.
        for (Map.Entry<DrillbitEndpoint, Integer> endpointAssignment : endpointAssignments.entrySet()) {
            assertEquals(expectedAssignments.get(endpointAssignment.getKey()).intValue(),
                         endpointAssignment.getValue().intValue());
        }
    }

    @Test
    public void testEqualLocalWorkUnitsUnderNodeLimit() throws Exception {
        final Wrapper wrapper = newWrapper(200, 1, 80,  /* cost, minWidth, maxWidth */
            ImmutableList.of( /*  endpointAffinities. */
                /* For local affinity, we only care about numLocalWorkUnits, the last column below */
                /* endpoint, affinity_value, mandatory, maxWidth, numLocalWorkUnits */
                new EndpointAffinity(DEP1, 0.15, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP2, 0.15, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP3, 0.10, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP4, 0.20, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP5, 0.20, false, MAX_VALUE, 16)
            ));
        INSTANCE.parallelizeFragment(wrapper, newParameters(1 /* sliceTarget */,
                                                            23 /* maxWidthPerNode */,
                                                            200 /* globalMaxWidth */),
                                                            ImmutableList.of(DEP1, DEP2, DEP3, DEP4, DEP5));
        // Everyone should get assigned 16 because
        // The parallelization maxWidth (80) is below the globalMaxWidth(200) and
        // localWorkUnits of all nodes is below maxWidthPerNode i.e. 23
        Map<DrillbitEndpoint, Integer> expectedAssignments = ImmutableMap.of(DEP1, 16,
                                                                             DEP2, 16,
                                                                             DEP3, 16,
                                                                             DEP4, 16,
                                                                             DEP5, 16);
        // Expect the fragment parallelization to be 80 (16 * 5)
        assertEquals(80, wrapper.getWidth());

        final List<DrillbitEndpoint> assignedEndpoints = wrapper.getAssignedEndpoints();
        assertEquals(80, assignedEndpoints.size());
        assertTrue(assignedEndpoints.contains(DEP1));
        assertTrue(assignedEndpoints.contains(DEP2));
        assertTrue(assignedEndpoints.contains(DEP3));
        assertTrue(assignedEndpoints.contains(DEP4));
        assertTrue(assignedEndpoints.contains(DEP5));

        checkEndpointAssignments(assignedEndpoints, expectedAssignments);
    }

    @Test
    public void testEqualLocalWorkUnitsAboveNodeLimit() throws Exception {
        final Wrapper wrapper = newWrapper(200, 1, 80,  /* cost, minWidth, maxWidth */
            ImmutableList.of( /*  endpointAffinities. */
                /* For local affinity, we only care about numLocalWorkUnits, the last column below */
                /* endpoint, affinity_value, mandatory, maxWidth, numLocalWorkUnits */
                new EndpointAffinity(DEP1, 0.15, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP2, 0.15, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP3, 0.10, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP4, 0.20, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP5, 0.20, false, MAX_VALUE, 16)
            ));
        INSTANCE.parallelizeFragment(wrapper, newParameters(1 /* sliceTarget */,
                                                            8 /* maxWidthPerNode */,
                                                            200 /* globalMaxWidth */),
                                                            ImmutableList.of(DEP1, DEP2, DEP3, DEP4, DEP5));
        // Everyone should get assigned 8 because
        // maxWidthPerNode is 8 and localWorkUnits of all nodes is above maxWidthPerNode.
        // Also, the parallelization maxWidth (80) is below the globalMaxWidth(200)
        Map<DrillbitEndpoint, Integer> expectedAssignments = ImmutableMap.of(DEP1, 8,
                                                                             DEP2, 8,
                                                                             DEP3, 8,
                                                                             DEP4, 8,
                                                                             DEP5, 8);
        // Expect the fragment parallelization to be 40 (8 * 5)
        assertEquals(40, wrapper.getWidth());

        final List<DrillbitEndpoint> assignedEndpoints = wrapper.getAssignedEndpoints();
        assertEquals(40, assignedEndpoints.size());
        assertTrue(assignedEndpoints.contains(DEP1));
        assertTrue(assignedEndpoints.contains(DEP2));
        assertTrue(assignedEndpoints.contains(DEP3));
        assertTrue(assignedEndpoints.contains(DEP4));
        assertTrue(assignedEndpoints.contains(DEP5));

        checkEndpointAssignments(assignedEndpoints, expectedAssignments);
    }

    @Test
    public void testUnEqualLocalWorkUnitsUnderNodeLimit() throws Exception {
        final Wrapper wrapper = newWrapper(200, 1, 80,  /* cost, minWidth, maxWidth, endpointAffinities */
            ImmutableList.of( /*  endpointAffinities. */
                /* For local affinity, we only care about numLocalWorkUnits, the last column below */
                /* endpoint, affinity_value, mandatory, maxWidth, numLocalWorkUnits */
                new EndpointAffinity(DEP1, 0.15, false, MAX_VALUE, 14),
                new EndpointAffinity(DEP2, 0.15, false, MAX_VALUE, 15),
                new EndpointAffinity(DEP3, 0.10, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP4, 0.20, false, MAX_VALUE, 17),
                new EndpointAffinity(DEP5, 0.20, false, MAX_VALUE, 18)
            ));
        INSTANCE.parallelizeFragment(wrapper, newParameters(1 /*sliceTarget */,
                                     23 /* maxWidthPerNode */,
                                     200 /* globalMaxWidth */),
                                     ImmutableList.of(DEP1, DEP2, DEP3, DEP4, DEP5));
        // All DrillbitEndpoints should get fragments same as localWorkUnits they have.
        // The parallelization maxWidth (80) is below the globalMaxWidth(200) and
        // localWorkUnits of all nodes is below maxWidthPerNode i.e. 23
        Map<DrillbitEndpoint, Integer> expectedAssignments = ImmutableMap.of(DEP1, 14,
                                                                             DEP2, 15,
                                                                             DEP3, 16,
                                                                             DEP4, 17,
                                                                             DEP5, 18);
        // Expect the fragment parallelization to be 80 (14 + 15 + 16 + 17 + 18)
        assertEquals(80, wrapper.getWidth());

        // All Drillbit Endpoints should get fragments assigned.
        final List<DrillbitEndpoint> assignedEndpoints = wrapper.getAssignedEndpoints();
        assertEquals(80, assignedEndpoints.size());
        assertTrue(assignedEndpoints.contains(DEP1));
        assertTrue(assignedEndpoints.contains(DEP2));
        assertTrue(assignedEndpoints.contains(DEP3));
        assertTrue(assignedEndpoints.contains(DEP4));
        assertTrue(assignedEndpoints.contains(DEP5));

        checkEndpointAssignments(assignedEndpoints, expectedAssignments);
    }

    @Test
    public void testUnequalLocalWorkUnitsAboveNodeLimit() throws Exception {
        final Wrapper wrapper = newWrapper(200, 1, 80,  /* cost, minWidth, maxWidth, endpointAffinities */
            ImmutableList.of( /*  endpointAffinities. */
                /* For local affinity, we only care about numLocalWorkUnits, the last column below */
                /* endpoint, affinity_value, mandatory, maxWidth, numLocalWorkUnits */
                new EndpointAffinity(DEP1, 0.15, false, MAX_VALUE, 14),
                new EndpointAffinity(DEP2, 0.15, false, MAX_VALUE, 15),
                new EndpointAffinity(DEP3, 0.10, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP4, 0.20, false, MAX_VALUE, 17),
                new EndpointAffinity(DEP5, 0.20, false, MAX_VALUE, 18)
            ));
        INSTANCE.parallelizeFragment(wrapper, newParameters(1 /*sliceTarget */,
                                                            16 /* maxWidthPerNode */,
                                                            200 /* globalMaxWidth */),
                                                            ImmutableList.of(DEP1, DEP2, DEP3, DEP4, DEP5));
        // All nodes should get 16 or less fragments assigned since maxWidthPerNode is 16.
        // Nodes with localWorkUnits less than 16 should get assigned fragments same as localWorkUnits.
        // Nodes with localWorkUnits 17 and 18 should get assigned only 16 fragments (they will be capped
        // by maxWidthPerNode).
        Map<DrillbitEndpoint, Integer> expectedAssignments = ImmutableMap.of(DEP1, 14,
                                                                             DEP2, 15,
                                                                             DEP3, 16,
                                                                             DEP4, 16,
                                                                             DEP5, 16);
        // Expect the fragment parallelization to be 77 (14 + 15 + 16 + 16 + 16)
        // maxWidthPerNode is 16.
        // The parallelization maxWidth (80) is below the globalMaxWidth(200)
        assertEquals(77, wrapper.getWidth());

        // All nodes should get fragments assigned.
        final List<DrillbitEndpoint> assignedEndpoints = wrapper.getAssignedEndpoints();
        assertEquals(77, assignedEndpoints.size());
        assertTrue(assignedEndpoints.contains(DEP1));
        assertTrue(assignedEndpoints.contains(DEP2));
        assertTrue(assignedEndpoints.contains(DEP3));
        assertTrue(assignedEndpoints.contains(DEP4));
        assertTrue(assignedEndpoints.contains(DEP5));

        checkEndpointAssignments(assignedEndpoints, expectedAssignments);
    }

    @Test
    public void testTotalWorkUnitsMoreThanGlobalMaxWidth() throws Exception {
        final Wrapper wrapper = newWrapper(200, 1, 80,  /* cost, minWidth, maxWidth, endpointAffinities */
            ImmutableList.of( /*  endpointAffinities. */
                /* For local affinity, we only care about numLocalWorkUnits, the last column below */
                /* endpoint, affinity_value, mandatory, maxWidth, numLocalWorkUnits */
                new EndpointAffinity(DEP1, 0.15, false, MAX_VALUE, 14),
                new EndpointAffinity(DEP2, 0.15, false, MAX_VALUE, 15),
                new EndpointAffinity(DEP3, 0.10, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP4, 0.20, false, MAX_VALUE, 17),
                new EndpointAffinity(DEP5, 0.20, false, MAX_VALUE, 18)
            ));
        INSTANCE.parallelizeFragment(wrapper, newParameters(1 /*sliceTarget */,
                                                            16 /* maxWidthPerNode */,
                                                            40 /* globalMaxWidth */),
                                                            ImmutableList.of(DEP1, DEP2, DEP3, DEP4, DEP5));
        // The parallelization maxWidth (80) is more than globalMaxWidth(40).
        // Expect the fragment parallelization to be 40 (7 + 8 + 8 + 8 + 9)
        // Each endpoint will get fragments proportional to its numLocalWorkUnits.
        // DEP5 should get 9 fragments (and not DEP4) since that has more localWorkUnits and we
        // favor nodes with more localWorkUnits.
        Map<DrillbitEndpoint, Integer> expectedAssignments = ImmutableMap.of(DEP1, 7,
                                                                             DEP2, 8,
                                                                             DEP3, 8,
                                                                             DEP4, 8,
                                                                             DEP5, 9);
        // Total number of fragments should be 40, globalMaxWidth.
        assertEquals(40, wrapper.getWidth());

        // All nodes should get fragments assigned.
        final List<DrillbitEndpoint> assignedEndpoints = wrapper.getAssignedEndpoints();
        assertEquals(40, assignedEndpoints.size());
        assertTrue(assignedEndpoints.contains(DEP1));
        assertTrue(assignedEndpoints.contains(DEP2));
        assertTrue(assignedEndpoints.contains(DEP3));
        assertTrue(assignedEndpoints.contains(DEP4));
        assertTrue(assignedEndpoints.contains(DEP5));

        checkEndpointAssignments(assignedEndpoints, expectedAssignments);
    }

    @Test
    public void testWithNoDataOnOneEndpoint() throws Exception {
        final Wrapper wrapper = newWrapper(200, 1, 80,  /* cost, minWidth, maxWidth, endpointAffinities */
            ImmutableList.of( /*  endpointAffinities. */
                /* For local affinity, we only care about numLocalWorkUnits, the last column below */
                /* endpoint, affinity_value, mandatory, maxWidth, numLocalWorkUnits */
                new EndpointAffinity(DEP1, 0.15, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP2, 0.15, false, MAX_VALUE, 18),
                new EndpointAffinity(DEP3, 0.10, false, MAX_VALUE, 21),
                new EndpointAffinity(DEP4, 0.20, false, MAX_VALUE, 25),
                new EndpointAffinity(DEP5, 0.20, false, MAX_VALUE, 0)
            ));
        INSTANCE.parallelizeFragment(wrapper, newParameters(1 /*sliceTarget */,
                                                            32 /* maxWidthPerNode */,
                                                            200 /* globalMaxWidth */),
                                                            ImmutableList.of(DEP1, DEP2, DEP3, DEP4, DEP5));
        // Everyone should get assigned equal to their numLocalWorkUnits.
        // The parallelization maxWidth (80) is below the globalMaxWidth(200)
        // numLocalWorkUnits of all nodes is less than maxWidthPerNode i.e. 32
        // Node with 0 localWorkUnits should not be assigned anything.
        Map<DrillbitEndpoint, Integer> expectedAssignments = ImmutableMap.of(DEP1, 16,
                                                                             DEP2, 18,
                                                                             DEP3, 21,
                                                                             DEP4, 25,
                                                                             DEP5, 0);
        // Expect the fragment parallelization to be 80 (16 + 18 + 21 + 25)
        assertEquals(80, wrapper.getWidth());

        // Only first four endpoints should be in the list.
        final List<DrillbitEndpoint> assignedEndpoints = wrapper.getAssignedEndpoints();
        assertEquals(80, assignedEndpoints.size());
        assertTrue(assignedEndpoints.contains(DEP1));
        assertTrue(assignedEndpoints.contains(DEP2));
        assertTrue(assignedEndpoints.contains(DEP3));
        assertTrue(assignedEndpoints.contains(DEP4));
        assertTrue(assignedEndpoints.contains(DEP5) == false);

        checkEndpointAssignments(assignedEndpoints, expectedAssignments);
    }

    @Test
    public void testWithNoDataOnMultipleEndpoints() throws Exception {
        final Wrapper wrapper = newWrapper(200, 1, 55,  /* cost, minWidth, maxWidth, endpointAffinities */
            ImmutableList.of( /*  endpointAffinities. */
                /* For local affinity, we only care about numLocalWorkUnits, the last column below */
                /* endpoint, affinity_value, mandatory, maxWidth, numLocalWorkUnits */
                new EndpointAffinity(DEP1, 0.15, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP2, 0.15, false, MAX_VALUE, 18),
                new EndpointAffinity(DEP3, 0.10, false, MAX_VALUE, 21),
                new EndpointAffinity(DEP4, 0.20, false, MAX_VALUE, 0),
                new EndpointAffinity(DEP5, 0.20, false, MAX_VALUE, 0)
            ));
        INSTANCE.parallelizeFragment(wrapper, newParameters(1 /*sliceTarget */,
                                                            32 /* maxWidthPerNode */,
                                                            200 /* globalMaxWidth */),
                                                            ImmutableList.of(DEP1, DEP2, DEP3, DEP4, DEP5));
        // Everyone should get assigned equal to their numLocalWorkUnits.
        // The parallelization maxWidth (80) is below the globalMaxWidth(200)
        // numLocalWorkUnits of all nodes is less than maxWidthPerNode i.e. 32
        // Nodes with 0 localWorkUnits should not be assigned anything.
        Map<DrillbitEndpoint, Integer> expectedAssignments = ImmutableMap.of(DEP1, 16,
                                                                             DEP2, 18,
                                                                             DEP3, 21,
                                                                             DEP4, 0,
                                                                             DEP5, 0);
        // Expect the fragment parallelization to be 55 (16 + 18 + 21)
        assertEquals(55, wrapper.getWidth());

        // Only first 3 nodes should be in the list.
        final List<DrillbitEndpoint> assignedEndpoints = wrapper.getAssignedEndpoints();
        assertEquals(55, assignedEndpoints.size());
        assertTrue(assignedEndpoints.contains(DEP1));
        assertTrue(assignedEndpoints.contains(DEP2));
        assertTrue(assignedEndpoints.contains(DEP3));
        assertTrue(assignedEndpoints.contains(DEP4) == false);
        assertTrue(assignedEndpoints.contains(DEP5) == false);

        checkEndpointAssignments(assignedEndpoints, expectedAssignments);
    }

    @Test
    public void testWithNoDataOnMultipleEndpointsAboveMaxWidth() throws Exception {
        final Wrapper wrapper = newWrapper(200, 1, 55,  /* cost, minWidth, maxWidth, endpointAffinities */
            ImmutableList.of( /*  endpointAffinities. */
                /* For local affinity, we only care about numLocalWorkUnits, the last column below */
                /* endpoint, affinity_value, mandatory, maxWidth, numLocalWorkUnits */
                new EndpointAffinity(DEP1, 0.15, false, MAX_VALUE, 16),
                new EndpointAffinity(DEP2, 0.15, false, MAX_VALUE, 18),
                new EndpointAffinity(DEP3, 0.10, false, MAX_VALUE, 21),
                new EndpointAffinity(DEP4, 0.20, false, MAX_VALUE, 0),
                new EndpointAffinity(DEP5, 0.20, false, MAX_VALUE, 0)
            ));
        INSTANCE.parallelizeFragment(wrapper, newParameters(1 /*sliceTarget */,
                                                            32 /* maxWidthPerNode */,
                                                            40 /* globalMaxWidth */),
                                                            ImmutableList.of(DEP1, DEP2, DEP3, DEP4, DEP5));
        // The parallelization maxWidth (80) is more than globalMaxWidth(40).
        // Expect the fragment parallelization to be 40 (12 + 14 + 12)
        // Each endpoint will get fragments proportional to its numLocalWorkUnits.
        // numLocalWorkUnits of all nodes is less than maxWidthPerNode i.e. 32
        // Nodes with 0 localWorkUnits should not be assigned anything.
        Map<DrillbitEndpoint, Integer> expectedAssignments = ImmutableMap.of(DEP1, 12,
                                                                             DEP2, 14,
                                                                             DEP3, 14,
                                                                             DEP4, 0,
                                                                             DEP5, 0);
        // Expect the fragment parallelization to be 40 because globalMaxWidth is 40
        assertEquals(40, wrapper.getWidth());

        // Only first 3 nodes should be in the list.
        final List<DrillbitEndpoint> assignedEps = wrapper.getAssignedEndpoints();
        assertEquals(40, assignedEps.size());
        assertTrue(assignedEps.contains(DEP1));
        assertTrue(assignedEps.contains(DEP2));
        assertTrue(assignedEps.contains(DEP3));
        assertTrue(assignedEps.contains(DEP4) == false);
        assertTrue(assignedEps.contains(DEP5) == false);

        checkEndpointAssignments(assignedEps, expectedAssignments);
    }

    @Test
    public void testCostBelowSliceTarget() throws Exception {
        final Wrapper wrapper = newWrapper(200, 1, 20, /* cost, minWidth, maxWidth */
            /* endPointAffinities - endpoint, affinity_value, mandatory, maxWidth, numLocalWorkUnits */
            Collections.singletonList(new EndpointAffinity(DEP1, 1.0, true, MAX_VALUE, 5)));
        INSTANCE.parallelizeFragment(wrapper, newParameters(SLICE_TARGET_DEFAULT, /* sliceTarget */
                                                            5, /* maxWidthPerNode */
                                                            20), /* globalMaxWidth */
                                                            Collections.singletonList(DEP1)); /* DrillbitEndpoints */
        // Expect the fragment parallelization to be just one because:
        // The cost (200) is below the threshold (SLICE_TARGET_DEFAULT) (which gives width of 200/10000 = ~1) and
        assertEquals(1, wrapper.getWidth());

        final List<DrillbitEndpoint> assignedEndpoints = wrapper.getAssignedEndpoints();
        assertEquals(1, assignedEndpoints.size());
        assertEquals(DEP1, assignedEndpoints.get(0));
    }

    @Test
    public void testNodeWidth() throws Exception {
        // Set the slice target to 1
        // cost, minWidth, maxWidth, endPointAffinities - endpoint, affinity_value, mandatory, maxWidth, numLocalWorkUnits
        final Wrapper wrapper = newWrapper(200, 1, 20, Collections.singletonList(new EndpointAffinity(DEP1, 1.0, true, MAX_VALUE, 5)));
        // sliceTarget, maxWidthPerNode, globalMaxWidth, DrillbitEndpoints
        INSTANCE.parallelizeFragment(wrapper, newParameters(1, 5, 20), Collections.singletonList(DEP1));

        // Expect the fragment parallelization to be 5:
        // 1. the cost (200) is above the threshold (SLICE_TARGET_DEFAULT) (which gives 200/1=200 width) and
        // 2. Max width per node is 5 (limits the width 200 to 5)
        assertEquals(5, wrapper.getWidth());

        final List<DrillbitEndpoint> assignedEndpoints = wrapper.getAssignedEndpoints();
        assertEquals(5, assignedEndpoints.size());
        for (DrillbitEndpoint ep : assignedEndpoints) {
            assertEquals(DEP1, ep);
        }
    }
}
