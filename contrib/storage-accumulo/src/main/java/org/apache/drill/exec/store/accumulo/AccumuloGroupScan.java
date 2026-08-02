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
package org.apache.drill.exec.store.accumulo;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Group scan for Accumulo tables.
 *
 * <p>This class handles scan planning and fragmentation across Accumulo tablets.
 * It can be modified by optimizer rules to apply filter, projection, limit, and sort pushdowns.</p>
 *
 * <p>For user impersonation mode, this class carries a delegation token that is
 * serialized to JSON for distributed planning and passed to SubScans for execution.</p>
 */
@JsonTypeName("accumulo-scan")
public class AccumuloGroupScan extends AbstractGroupScan {

  private AccumuloStoragePluginConfig storagePluginConfig;
  private AccumuloStoragePlugin storagePlugin;
  private AccumuloScanSpec scanSpec;
  private List<SchemaPath> columns;
  private int maxRecords;

  /**
   * Delegation token for user impersonation in distributed execution.
   * When present, SubScans will use this token to create user-impersonated clients.
   */
  private DelegationTokenInfo delegationTokenInfo;

  private boolean filterPushedDown = false;
  private boolean projectionPushedDown = false;
  private boolean limitPushedDown = false;
  private boolean sortPushedDown = false;

  @JsonCreator
  public AccumuloGroupScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("scanSpec") AccumuloScanSpec scanSpec,
      @JsonProperty("storage") AccumuloStoragePluginConfig storagePluginConfig,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("maxRecords") int maxRecords,
      @JsonProperty("delegationTokenInfo") DelegationTokenInfo delegationTokenInfo,
      @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this(userName, pluginRegistry.resolve(storagePluginConfig, AccumuloStoragePlugin.class),
        scanSpec, columns, maxRecords, delegationTokenInfo);
  }

  public AccumuloGroupScan(
      String userName,
      AccumuloStoragePlugin storagePlugin,
      AccumuloScanSpec scanSpec,
      List<SchemaPath> columns,
      int maxRecords) {
    this(userName, storagePlugin, scanSpec, columns, maxRecords, null);
  }

  public AccumuloGroupScan(
      String userName,
      AccumuloStoragePlugin storagePlugin,
      AccumuloScanSpec scanSpec,
      List<SchemaPath> columns,
      int maxRecords,
      DelegationTokenInfo delegationTokenInfo) {
    super(userName);
    this.storagePlugin = storagePlugin;
    this.storagePluginConfig = storagePlugin.getConfig();
    this.scanSpec = scanSpec;
    this.columns = columns == null ? ALL_COLUMNS : columns;
    this.maxRecords = maxRecords;
    this.delegationTokenInfo = delegationTokenInfo;
  }

  /**
   * Copy constructor for cloning.
   */
  private AccumuloGroupScan(AccumuloGroupScan that) {
    super(that);
    this.storagePlugin = that.storagePlugin;
    this.storagePluginConfig = that.storagePluginConfig;
    this.scanSpec = that.scanSpec;
    this.columns = that.columns == null ? ALL_COLUMNS : that.columns;
    this.maxRecords = that.maxRecords;
    this.delegationTokenInfo = that.delegationTokenInfo;
    this.filterPushedDown = that.filterPushedDown;
    this.projectionPushedDown = that.projectionPushedDown;
    this.limitPushedDown = that.limitPushedDown;
    this.sortPushedDown = that.sortPushedDown;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    AccumuloGroupScan cloned = new AccumuloGroupScan(this);
    cloned.columns = columns;
    // Mark projection as pushed down if we're projecting specific columns
    if (columns != null && !columns.equals(ALL_COLUMNS)) {
      cloned.projectionPushedDown = true;
    }
    return cloned;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return new AccumuloGroupScan(this);
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    // TODO: Implement tablet-to-endpoint assignment for data locality
  }

  @Override
  public AccumuloSubScan getSpecificScan(int minorFragmentId) {
    // Pass delegation token to SubScan for distributed execution
    return new AccumuloSubScan(getUserName(), storagePlugin, scanSpec, columns, maxRecords, delegationTokenInfo);
  }

  @Override
  public int getMaxParallelizationWidth() {
    // TODO: Return actual number of tablets; for now return 1
    return 1;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    // TODO: Return endpoint affinities based on tablet locations
    return Collections.emptyList();
  }

  @Override
  public ScanStats getScanStats() {
    // TODO: Calculate actual scan statistics from Accumulo metadata
    long rowCount = 100000; // Estimate
    int columnCount = columns != null && !columns.equals(ALL_COLUMNS) ? columns.size() : 10;
    double cpuCost = rowCount * columnCount;

    // Adjust cost for pushdowns
    if (filterPushedDown) {
      cpuCost *= 0.5;
      rowCount /= 2;
    }
    if (projectionPushedDown) {
      // Projection reduces network I/O significantly
      cpuCost *= 0.7;
    }
    if (sortPushedDown) {
      cpuCost *= 1.2; // Slight penalty for using Scanner instead of BatchScanner
    }
    if (limitPushedDown && maxRecords > 0) {
      // Limit pushdown significantly reduces work
      rowCount = Math.min(rowCount, maxRecords);
      cpuCost = rowCount * columnCount;
    }

    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, rowCount, cpuCost, rowCount * columnCount * 8);
  }

  @Override
  @JsonIgnore
  public boolean supportsLimitPushdown() {
    return true;
  }

  @Override
  public GroupScan applyLimit(int maxRecords) {
    // If limit is already set and is more restrictive, keep the current one
    if (this.maxRecords > 0 && this.maxRecords <= maxRecords) {
      return null;
    }

    AccumuloGroupScan newScan = new AccumuloGroupScan(this);
    newScan.maxRecords = maxRecords;
    newScan.limitPushedDown = true;
    return newScan;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("scanSpec", scanSpec)
        .field("columns", columns)
        .field("maxRecords", maxRecords)
        .field("filterPushedDown", filterPushedDown)
        .field("projectionPushedDown", projectionPushedDown)
        .field("limitPushedDown", limitPushedDown)
        .field("sortPushedDown", sortPushedDown)
        .field("hasDelegationToken", delegationTokenInfo != null)
        .toString();
  }

  // Getters for Jackson serialization

  @JsonProperty("scanSpec")
  public AccumuloScanSpec getScanSpec() {
    return scanSpec;
  }

  @JsonProperty("storage")
  public AccumuloStoragePluginConfig getStoragePluginConfig() {
    return storagePluginConfig;
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("maxRecords")
  public int getMaxRecords() {
    return maxRecords;
  }

  @JsonProperty("delegationTokenInfo")
  public DelegationTokenInfo getDelegationTokenInfo() {
    return delegationTokenInfo;
  }

  @JsonIgnore
  public AccumuloStoragePlugin getStoragePlugin() {
    return storagePlugin;
  }

  @JsonIgnore
  public String getTableName() {
    return scanSpec != null ? scanSpec.getTableName() : null;
  }

  /**
   * Returns true if this scan has a delegation token for user impersonation.
   */
  @JsonIgnore
  public boolean hasDelegationToken() {
    return delegationTokenInfo != null;
  }

  // Pushdown tracking methods

  @JsonIgnore
  public boolean isFilterPushedDown() {
    return filterPushedDown;
  }

  public void setFilterPushedDown(boolean filterPushedDown) {
    this.filterPushedDown = filterPushedDown;
  }

  @JsonIgnore
  public boolean isProjectionPushedDown() {
    return projectionPushedDown;
  }

  public void setProjectionPushedDown(boolean projectionPushedDown) {
    this.projectionPushedDown = projectionPushedDown;
  }

  @JsonIgnore
  public boolean isLimitPushedDown() {
    return limitPushedDown;
  }

  public void setLimitPushedDown(boolean limitPushedDown) {
    this.limitPushedDown = limitPushedDown;
  }

  @JsonIgnore
  public boolean isSortPushedDown() {
    return sortPushedDown;
  }

  public void setSortPushedDown(boolean sortPushedDown) {
    this.sortPushedDown = sortPushedDown;
  }

  /**
   * Returns a new AccumuloGroupScan with the given scan spec.
   * Used by optimizer rules to create modified scans.
   */
  public AccumuloGroupScan cloneWithNewScanSpec(AccumuloScanSpec newScanSpec) {
    AccumuloGroupScan cloned = new AccumuloGroupScan(this);
    cloned.scanSpec = newScanSpec;
    return cloned;
  }
}
