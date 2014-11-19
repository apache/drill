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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.Exchange;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Store;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.planner.fragment.Fragment.ExchangeFragmentPair;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A wrapping class that allows us to add additional information to each fragment node for planning purposes.
 */
public class Wrapper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Wrapper.class);

  private final Fragment node;
  private final int majorFragmentId;
  private int width = -1;
  private final Stats stats;
  private boolean endpointsAssigned;
  private Map<DrillbitEndpoint, EndpointAffinity> endpointAffinityMap = Maps.newHashMap();
  private long initialAllocation = 0;
  private long maxAllocation = 0;

  // a list of assigned endpoints. Technically, there could repeated endpoints in this list if we'd like to assign the
  // same fragment multiple times to the same endpoint.
  private List<DrillbitEndpoint> endpoints = Lists.newLinkedList();

  public Wrapper(Fragment node, int majorFragmentId) {
    this.majorFragmentId = majorFragmentId;
    this.node = node;
    this.stats = new Stats();
  }

  public Stats getStats() {
    return stats;
  }

  public void resetAllocation() {
    initialAllocation = 0;
    maxAllocation = 0;
  }

  public void addEndpointAffinity(List<EndpointAffinity> affinities){
    Preconditions.checkState(!endpointsAssigned);
    for(EndpointAffinity ea : affinities){
      addEndpointAffinity(ea.getEndpoint(), ea.getAffinity());
    }
  }

  public void addEndpointAffinity(DrillbitEndpoint endpoint, float affinity) {
    Preconditions.checkState(!endpointsAssigned);
    Preconditions.checkNotNull(endpoint);
    EndpointAffinity ea = endpointAffinityMap.get(endpoint);
    if (ea == null) {
      ea = new EndpointAffinity(endpoint);
      endpointAffinityMap.put(endpoint, ea);
    }

    ea.addAffinity(affinity);
  }

  public int getMajorFragmentId() {
    return majorFragmentId;
  }

  public int getWidth() {
    return width;
  }

  public void setWidth(int width) {
    Preconditions.checkState(this.width == -1);
    this.width = width;
  }

  public Fragment getNode() {
    return node;
  }

  public long getInitialAllocation() {
    return initialAllocation;
  }

  public long getMaxAllocation() {
    return maxAllocation;
  }

  public void addAllocation(PhysicalOperator pop) {
    initialAllocation += pop.getInitialAllocation();
//    logger.debug("Incrementing initialAllocation by {} to {}. Pop: {}", pop.getInitialAllocation(), initialAllocation, pop);
    maxAllocation += pop.getMaxAllocation();
  }

  private class AssignEndpointsToScanAndStore extends AbstractPhysicalVisitor<Void, List<DrillbitEndpoint>, PhysicalOperatorSetupException>{

    @Override
    public Void visitExchange(Exchange exchange, List<DrillbitEndpoint> value) throws PhysicalOperatorSetupException {
      if(exchange == node.getSendingExchange()){
        return visitOp(exchange, value);
      }
      // stop on receiver exchange.
      return null;
    }

    @Override
    public Void visitGroupScan(GroupScan groupScan, List<DrillbitEndpoint> value) throws PhysicalOperatorSetupException {
      groupScan.applyAssignments(value);
      return super.visitGroupScan(groupScan, value);
    }

    @Override
    public Void visitSubScan(SubScan subScan, List<DrillbitEndpoint> value) throws PhysicalOperatorSetupException {
      // TODO - implement this
      return visitOp(subScan, value);
    }

    @Override
    public Void visitStore(Store store, List<DrillbitEndpoint> value) throws PhysicalOperatorSetupException {
      store.applyAssignments(value);
      return super.visitStore(store, value);
    }

    @Override
    public Void visitOp(PhysicalOperator op, List<DrillbitEndpoint> value) throws PhysicalOperatorSetupException {
      return visitChildren(op, value);
    }

  }

  public void assignEndpoints(Collection<DrillbitEndpoint> allEndpoints, double affinityFactor) throws PhysicalOperatorSetupException {
    Preconditions.checkState(!endpointsAssigned);
    endpointsAssigned = true;

    if (endpointAffinityMap.size() > 0) {
      List<EndpointAffinity> affinedEPs = Lists.newArrayList(endpointAffinityMap.values());
      // get nodes with highest affinity.
      Collections.sort(affinedEPs);
      Iterator<EndpointAffinity> affinedEPItr = Iterators.cycle(Lists.reverse(affinedEPs));
      /** Maximum number of slots which should go to endpoints with affinity */
      int affinedSlots = Math.min((Math.max(1, (int) (affinityFactor*width/allEndpoints.size())) * affinedEPs.size()), width);
      while(endpoints.size() < affinedSlots) {
        EndpointAffinity ea = affinedEPItr.next();
        DrillbitEndpoint endpoint = ea.getEndpoint();
        endpoints.add(endpoint);
      }
    }
    // add other endpoints if required
    if (endpoints.size() < width) {
      List<DrillbitEndpoint> all = Lists.newArrayList(allEndpoints);
      all.removeAll(endpointAffinityMap.keySet());
      // round robin with random start.
      Collections.shuffle(all, ThreadLocalRandom.current());
      Iterator<DrillbitEndpoint> otherEPItr = Iterators.cycle(all.size() > 0 ? all : endpointAffinityMap.keySet());
      while (endpoints.size() < width) {
        endpoints.add(otherEPItr.next());
      }
    }

    // Set scan and store endpoints.
    AssignEndpointsToScanAndStore visitor = new AssignEndpointsToScanAndStore();
    node.getRoot().accept(visitor, endpoints);

    // Set the endpoints for this (one at most) sending exchange.
    if (node.getSendingExchange() != null) {
      node.getSendingExchange().setupSenders(majorFragmentId, endpoints);
    }

    // Set the endpoints for each incoming exchange within this fragment.
    for (ExchangeFragmentPair e : node.getReceivingExchangePairs()) {
      e.getExchange().setupReceivers(majorFragmentId, endpoints);
    }
  }

  @Override
  public String toString() {
    return "FragmentWrapper [majorFragmentId=" + majorFragmentId + ", width=" + width + ", stats=" + stats + "]";
  }

  public DrillbitEndpoint getAssignedEndpoint(int minorFragmentId) {
    Preconditions.checkState(endpointsAssigned);
    return this.endpoints.get(minorFragmentId);
  }

}
