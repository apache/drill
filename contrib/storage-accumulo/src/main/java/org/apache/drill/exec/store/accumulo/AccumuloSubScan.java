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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

/**
 * Accumulo sub-scan for a specific portion of an Accumulo table.
 *
 * <p>In the future, this will represent a scan on specific tablets.
 * For now, it represents a full table scan.</p>
 *
 * <p>For user impersonation mode, this class carries a delegation token
 * that was generated during planning and is used at execution time to
 * create an Accumulo client with the user's identity.</p>
 */
@JsonTypeName("accumulo-sub-scan")
public class AccumuloSubScan extends AbstractBase implements SubScan {

  public static final String OPERATOR_TYPE = "ACCUMULO_SUB_SCAN";

  private final AccumuloStoragePlugin storagePlugin;
  private final AccumuloScanSpec scanSpec;
  private final List<SchemaPath> columns;
  private final int maxRecords;

  /**
   * Delegation token for user impersonation in distributed execution.
   * When present, the record reader will use this token to create a
   * user-impersonated Accumulo client.
   */
  private final DelegationTokenInfo delegationTokenInfo;

  @JsonCreator
  public AccumuloSubScan(
      @JacksonInject StoragePluginRegistry registry,
      @JsonProperty("userName") String userName,
      @JsonProperty("storagePluginConfig") AccumuloStoragePluginConfig storagePluginConfig,
      @JsonProperty("scanSpec") AccumuloScanSpec scanSpec,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("maxRecords") int maxRecords,
      @JsonProperty("delegationTokenInfo") DelegationTokenInfo delegationTokenInfo) throws ExecutionSetupException {
    this(userName, registry.resolve(storagePluginConfig, AccumuloStoragePlugin.class),
        scanSpec, columns, maxRecords, delegationTokenInfo);
  }

  public AccumuloSubScan(
      String userName,
      AccumuloStoragePlugin storagePlugin,
      AccumuloScanSpec scanSpec,
      List<SchemaPath> columns) {
    this(userName, storagePlugin, scanSpec, columns, -1, null);
  }

  public AccumuloSubScan(
      String userName,
      AccumuloStoragePlugin storagePlugin,
      AccumuloScanSpec scanSpec,
      List<SchemaPath> columns,
      int maxRecords) {
    this(userName, storagePlugin, scanSpec, columns, maxRecords, null);
  }

  public AccumuloSubScan(
      String userName,
      AccumuloStoragePlugin storagePlugin,
      AccumuloScanSpec scanSpec,
      List<SchemaPath> columns,
      int maxRecords,
      DelegationTokenInfo delegationTokenInfo) {
    super(userName);
    this.storagePlugin = storagePlugin;
    this.scanSpec = scanSpec;
    this.columns = columns;
    this.maxRecords = maxRecords;
    this.delegationTokenInfo = delegationTokenInfo;
  }

  @JsonProperty("storagePluginConfig")
  public AccumuloStoragePluginConfig getStoragePluginConfig() {
    return storagePlugin.getConfig();
  }

  @JsonProperty("scanSpec")
  public AccumuloScanSpec getScanSpec() {
    return scanSpec;
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

  /**
   * Returns true if this sub-scan has a delegation token for user impersonation.
   */
  @JsonIgnore
  public boolean hasDelegationToken() {
    return delegationTokenInfo != null;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new AccumuloSubScan(getUserName(), storagePlugin, scanSpec, columns, maxRecords, delegationTokenInfo);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public String getOperatorType() {
    return OPERATOR_TYPE;
  }
}
