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
package org.apache.drill.exec.store.sys;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.store.StoragePluginRegistry;

import parquet.org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("sys")
public class SystemTableScan extends AbstractGroupScan implements SubScan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemTableScan.class);

  private final SystemTable table;
  private final SystemTablePlugin plugin;

  @JsonCreator
  public SystemTableScan( //
      @JsonProperty("table") SystemTable table, //
      @JacksonInject StoragePluginRegistry engineRegistry //
      ) throws IOException, ExecutionSetupException {
    this.table = table;
    this.plugin = (SystemTablePlugin) engineRegistry.getPlugin(SystemTablePluginConfig.INSTANCE);
  }

  public SystemTableScan(SystemTable table, SystemTablePlugin plugin){
    this.table = table;
    this.plugin = plugin;
  }

  public ScanStats getScanStats(){
    return ScanStats.TRIVIAL_TABLE;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return new SystemTableScan(table, plugin);
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    return this;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public long getInitialAllocation() {
    return initialAllocation;
  }

  @Override
  public long getMaxAllocation() {
    return maxAllocation;
  }

  @Override
  public String getDigest() {
    return "SystemTableScan: " + table.name();
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.emptyList();
  }


  @Override
  public int getOperatorType() {
    return CoreOperatorType.SYSTEM_TABLE_SCAN_VALUE;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return this;
  }

  public SystemTable getTable() {
    return table;
  }

  public SystemTablePlugin getPlugin() {
    return plugin;
  }


}
