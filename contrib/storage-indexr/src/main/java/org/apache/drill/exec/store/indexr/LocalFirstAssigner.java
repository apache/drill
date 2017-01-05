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
package org.apache.drill.exec.store.indexr;

import com.carrotsearch.hppc.cursors.ObjectLongCursor;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * An assigner using the strategy:
 * 1. Those works with some nodes data localizing will be definetely assigned to one of them.
 * 2. Those works without any nodes data localizing will be assigned to the least workload nodes.
 */
public class LocalFirstAssigner {
  private final List<DrillbitEndpoint> endpoints;
  private final List<ScanCompleteWork> works;

  public LocalFirstAssigner(List<DrillbitEndpoint> endpoints,
                            List<ScanCompleteWork> works) {
    // The planning by Drill parallelizer is useless here.
    //this.endpoints = new ArrayList<>(new HashSet<>(endpoints));
    this.endpoints = endpoints;
    this.works = works;
  }

  public static class EndpointAssignment implements Comparable<EndpointAssignment> {
    public final DrillbitEndpoint endpoint;
    public final List<ScanCompleteWork> works;
    private long workload;

    public EndpointAssignment(DrillbitEndpoint endpoint) {
      this.endpoint = endpoint;
      this.works = new ArrayList<>();
      this.workload = 0;
    }

    void addWork(ScanCompleteWork work) {
      works.add(work);
      workload += work.getTotalBytes();
    }

    @Override
    public int compareTo(EndpointAssignment o) {
      return Long.compare(workload, o.workload);
    }
  }

  public Map<DrillbitEndpoint, EndpointAssignment> assign() {
    Map<DrillbitEndpoint, EndpointAssignment> assignments = new HashMap<>(endpoints.size());
    for (DrillbitEndpoint endpoint : endpoints) {
      assignments.put(endpoint, new EndpointAssignment(endpoint));
    }
    List<ScanCompleteWork> notLocalizing = new ArrayList<>();

    // Round 1. Assign works to the least workload nodes which localizing their data.
    for (ScanCompleteWork work : works) {
      EndpointAssignment least = null;
      for (ObjectLongCursor<DrillbitEndpoint> cursor : work.getByteMap()) {
        // We assert all byte costs are equal.
        DrillbitEndpoint endpoint = cursor.key;
        EndpointAssignment current = assignments.get(endpoint);
        if (current == null) {
          // This node cannot be assigned.
          continue;
        }
        if (least == null || current.workload < least.workload) {
          least = current;
        }
      }
      if (least == null) {
        notLocalizing.add(work);
      } else {
        least.addWork(work);
      }
    }

    // Round 2. Assign those works without nodes localizing.
    if (!notLocalizing.isEmpty()) {
      TreeSet<EndpointAssignment> sortedAssignment = new TreeSet<>(assignments.values());
      for (ScanCompleteWork work : notLocalizing) {
        EndpointAssignment least = sortedAssignment.pollFirst();
        least.addWork(work);
        sortedAssignment.add(least);
      }
    }

    return assignments;
  }

}
