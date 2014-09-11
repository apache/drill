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
package org.apache.drill.exec.store.schedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

/**
 * The AssignmentCreator is responsible for assigning a set of work units to the available slices.
 */
public class AssignmentCreator<T extends CompleteWork> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AssignmentCreator.class);

  static final double[] ASSIGNMENT_CUTOFFS = { 0.99, 0.50, 0.25, 0.00 };
  private final ArrayListMultimap<Integer, T> mappings;
  private final List<DrillbitEndpoint> endpoints;



  /**
   * Given a set of endpoints to assign work to, attempt to evenly assign work based on affinity of work units to
   * Drillbits.
   *
   * @param incomingEndpoints
   *          The set of nodes to assign work to. Note that nodes can be listed multiple times if we want to have
   *          multiple slices on a node working on the task simultaneously.
   * @param units
   *          The work units to assign.
   * @return ListMultimap of Integer > List<CompleteWork> (based on their incoming order) to with
   */
  public static <T extends CompleteWork> ListMultimap<Integer, T> getMappings(List<DrillbitEndpoint> incomingEndpoints,
      List<T> units) {
    AssignmentCreator<T> creator = new AssignmentCreator<T>(incomingEndpoints, units);
    return creator.mappings;
  }

  private AssignmentCreator(List<DrillbitEndpoint> incomingEndpoints, List<T> units) {
    logger.debug("Assigning {} units to {} endpoints", units.size(), incomingEndpoints.size());
    Stopwatch watch = new Stopwatch();

    Preconditions.checkArgument(incomingEndpoints.size() <= units.size(), String.format("Incoming endpoints %d "
        + "is greater than number of row groups %d", incomingEndpoints.size(), units.size()));
    this.mappings = ArrayListMultimap.create();
    this.endpoints = Lists.newLinkedList(incomingEndpoints);

    ArrayList<T> rowGroupList = new ArrayList<>(units);
    for (double cutoff : ASSIGNMENT_CUTOFFS) {
      scanAndAssign(rowGroupList, cutoff, false, false);
    }
    scanAndAssign(rowGroupList, 0.0, true, false);
    scanAndAssign(rowGroupList, 0.0, true, true);

    logger.debug("Took {} ms to apply assignments", watch.elapsed(TimeUnit.MILLISECONDS));
    Preconditions.checkState(rowGroupList.isEmpty(), "All readEntries should be assigned by now, but some are still unassigned");
    Preconditions.checkState(!units.isEmpty());

  }

  /**
   *
   * @param mappings
   *          the mapping between fragment/endpoint and rowGroup
   * @param endpoints
   *          the list of drillbits, ordered by the corresponding fragment
   * @param workunits
   *          the list of rowGroups to assign
   * @param requiredPercentage
   *          the percentage of max bytes required to make an assignment
   * @param assignAll
   *          if true, will assign even if no affinity
   */
  private void scanAndAssign(List<T> workunits, double requiredPercentage, boolean assignAllToEmpty, boolean assignAll) {
    Collections.sort(workunits);
    int fragmentPointer = 0;
    final boolean requireAffinity = requiredPercentage > 0;
    int maxAssignments = (int) (workunits.size() / endpoints.size());

    if (maxAssignments < 1) {
      maxAssignments = 1;
    }

    for (Iterator<T> iter = workunits.iterator(); iter.hasNext();) {
      T unit = iter.next();
      for (int i = 0; i < endpoints.size(); i++) {
        int minorFragmentId = (fragmentPointer + i) % endpoints.size();
        DrillbitEndpoint currentEndpoint = endpoints.get(minorFragmentId);
        EndpointByteMap endpointByteMap = unit.getByteMap();
        boolean haveAffinity = endpointByteMap.isSet(currentEndpoint);

        if (assignAll
            || (assignAllToEmpty && !mappings.containsKey(minorFragmentId))
            || (!endpointByteMap.isEmpty() && (!requireAffinity || haveAffinity)
                && (!mappings.containsKey(minorFragmentId) || mappings.get(minorFragmentId).size() < maxAssignments) && (!requireAffinity || endpointByteMap
                .get(currentEndpoint) >= endpointByteMap.getMaxBytes() * requiredPercentage))) {

          mappings.put(minorFragmentId, unit);
          logger.debug("Assigned unit: {} to minorFragmentId: {}", unit, minorFragmentId);
          // logger.debug("Assigned rowGroup {} to minorFragmentId {} endpoint {}", rowGroupInfo.getRowGroupIndex(),
          // minorFragmentId, endpoints.get(minorFragmentId).getAddress());
          // if (bytesPerEndpoint.get(currentEndpoint) != null) {
          // // assignmentAffinityStats.update(bytesPerEndpoint.get(currentEndpoint) / rowGroupInfo.getLength());
          // } else {
          // // assignmentAffinityStats.update(0);
          // }
          iter.remove();
          fragmentPointer = (minorFragmentId + 1) % endpoints.size();
          break;
        }
      }

    }
  }

}
