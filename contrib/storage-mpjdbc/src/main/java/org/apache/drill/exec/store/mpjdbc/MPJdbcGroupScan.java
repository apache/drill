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
package org.apache.drill.exec.store.mpjdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.drill.common.expression.SchemaPath;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MPJdbcGroupScan extends AbstractGroupScan {

  private MPJdbcFormatPlugin plugin;
  private MPJdbcFormatConfig pluginConfig;
  private MPJdbcScanSpec mPJdbcScanSpec;
  private List<SchemaPath> columns;
  private String userName;
  private Map<Integer, List<MPJdbcScanSpec>> endpointFragmentMapping;

  public MPJdbcGroupScan(String userName,MPJdbcFormatPlugin storagePlugin, MPJdbcScanSpec scanSpec,
      List<SchemaPath> columns) {
    super(userName);
    this.plugin = storagePlugin;
    this.pluginConfig = storagePlugin.getConfig();
    this.mPJdbcScanSpec = scanSpec;
    this.userName = userName;
    this.columns = columns == null || columns.size() == 0 ? ALL_COLUMNS
        : columns;
  }

  public MPJdbcGroupScan(MPJdbcGroupScan that) {
    super(that);
    this.columns = that.columns;
    this.plugin = that.plugin;
    this.endpointFragmentMapping = that.endpointFragmentMapping;
    this.pluginConfig = that.pluginConfig;
    this.mPJdbcScanSpec = that.mPJdbcScanSpec;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId)
      throws ExecutionSetupException {
    // TODO Auto-generated method stub
    return new MPJdbcSubScan(plugin,userName, pluginConfig,
        endpointFragmentMapping.get(minorFragmentId), columns);
  }

  @Override
  public int getMaxParallelizationWidth() {
    // TODO Auto-generated method stub
    return -1;
  }

  @Override
  public String getDigest() {
    // TODO Auto-generated method stub
    return toString();
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    MPJdbcGroupScan newScan = new MPJdbcGroupScan(userName,plugin, mPJdbcScanSpec, columns);
    return newScan;

  }

  @Override
  public ScanStats getScanStats() {
    // TODO Auto-generated method stub
    return ScanStats.TRIVIAL_TABLE;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new MPJdbcGroupScan(this);
    // TODO Auto-generated method stub
  }
  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    this.columns = columns;
    return true;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    Map<String, DrillbitEndpoint> endpointMap = new HashMap<String, DrillbitEndpoint>();
    for (DrillbitEndpoint ep : plugin.getContext().getBits()) {
      endpointMap.put(ep.getAddress(), ep);
    }

    Map<DrillbitEndpoint, EndpointAffinity> affinityMap = new HashMap<DrillbitEndpoint, EndpointAffinity>();
    DrillbitEndpoint ep = endpointMap.get(plugin.getConfig().getUri());
    if (ep != null) {
      EndpointAffinity affinity = affinityMap.get(ep);
      if (affinity == null) {
        affinityMap.put(ep, new EndpointAffinity(ep, 1));
      } else {
        affinity.addAffinity(1);
      }
    }
    return Lists.newArrayList(affinityMap.values());
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    final int numSlots = incomingEndpoints.size();
    int totalAssignmentsTobeDone = 1;
    Preconditions.checkArgument(numSlots <= totalAssignmentsTobeDone, String
        .format("Incoming endpoints %d is greater than number of chunks %d",
            numSlots, totalAssignmentsTobeDone));
    final int minPerEndpointSlot = (int) Math
        .floor((double) totalAssignmentsTobeDone / numSlots);
    final int maxPerEndpointSlot = (int) Math
        .ceil((double) totalAssignmentsTobeDone / numSlots);
    /* Map for (index,endpoint)'s */
    endpointFragmentMapping = Maps.newHashMapWithExpectedSize(numSlots);
    /* Reverse mapping for above indexes */
    Map<String, Queue<Integer>> endpointHostIndexListMap = Maps.newHashMap();
    /*
     * Initialize these two maps
     */
    for (int i = 0; i < numSlots; ++i) {
      List<MPJdbcScanSpec> val = new ArrayList<MPJdbcScanSpec>(maxPerEndpointSlot);
      val.add(this.mPJdbcScanSpec);
      endpointFragmentMapping.put(i, val);
      String hostname = incomingEndpoints.get(i).getAddress();
      Queue<Integer> hostIndexQueue = endpointHostIndexListMap.get(hostname);
      if (hostIndexQueue == null) {
        hostIndexQueue = Lists.newLinkedList();
        endpointHostIndexListMap.put(hostname, hostIndexQueue);
      }
      hostIndexQueue.add(i);
    }
  }

  public MPJdbcScanSpec getScanSpec() {
    return this.mPJdbcScanSpec;
  }
}