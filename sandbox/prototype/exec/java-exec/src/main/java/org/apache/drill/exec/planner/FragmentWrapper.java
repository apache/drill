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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang.NotImplementedException;
import org.apache.drill.common.physical.EndpointAffinity;
import org.apache.drill.common.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.planner.FragmentNode.ExchangeFragmentPair;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class FragmentWrapper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentWrapper.class);

  private final FragmentNode node;
  private final int majorFragmentId;
  private int width = -1;
  private FragmentStats stats;
  private boolean endpointsAssigned;
  private Map<DrillbitEndpoint, EndpointAffinity> endpointAffinity = Maps.newHashMap();

  // a list of assigned endpoints. Technically, there could repeated endpoints in this list if we'd like to assign the
  // same fragment multiple times to the same endpoint.
  private List<DrillbitEndpoint> endpoints = Lists.newLinkedList();

  public FragmentWrapper(FragmentNode node, int majorFragmentId) {
    this.majorFragmentId = majorFragmentId;
    this.node = node;
  }

  public FragmentStats getStats() {
    return stats;
  }

  public void setStats(FragmentStats stats) {
    this.stats = stats;
  }

  public void addEndpointAffinity(DrillbitEndpoint endpoint, float affinity) {
    Preconditions.checkState(!endpointsAssigned);
    EndpointAffinity ea = endpointAffinity.get(endpoint);
    if (ea == null) {
      ea = new EndpointAffinity(endpoint);
      endpointAffinity.put(endpoint, ea);
    }

    ea.addAffinity(affinity);
    endpointAffinity.put(endpoint, ea);
  }

  public int getMajorFragmentId() {
    return majorFragmentId;
  }

  public int getWidth() {
    return width;
  }

  public void setWidth(int width) {
    Preconditions.checkState(width == -1);
    this.width = width;
  }

  public FragmentNode getNode() {
    return node;
  }

  public void assignEndpoints(Collection<DrillbitEndpoint> allPossible) {
    Preconditions.checkState(!endpointsAssigned);

    endpointsAssigned = true;
    
    List<EndpointAffinity> values = Lists.newArrayList();
    values.addAll(endpointAffinity.values());
    
    if(values.size() == 0){
      final int div = allPossible.size();
      int start = ThreadLocalRandom.current().nextInt(div);
      // round robin with random start.
      for(int i = start; i < start + width; i++){
        endpoints.add(values.get(i % div).getEndpoint());
      }
    }else if(values.size() < width){
      throw new NotImplementedException("Haven't implemented a scenario where we have some node affinity but the affinity list is smaller than the expected width.");
    }else{
      // get nodes with highest affinity.
      Collections.sort(values);
      values = Lists.reverse(values);
      for (int i = 0; i < width; i++) {
        endpoints.add(values.get(i).getEndpoint());
      }
    }

    node.getSendingExchange().setupSenders(endpoints);
    for (ExchangeFragmentPair e : node.getReceivingExchangePairs()) {
      e.getExchange().setupReceivers(endpoints);
    }
  }

  public DrillbitEndpoint getAssignedEndpoint(int minorFragmentId) {
    Preconditions.checkState(endpointsAssigned);
    return this.endpoints.get(minorFragmentId);
  }
}
