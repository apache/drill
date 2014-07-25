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
package org.apache.drill.exec.store.mongo;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;

import parquet.org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Stopwatch;

@JsonTypeName("mongo-scan")
public class MongoGroupScan extends AbstractGroupScan implements DrillMongoConstants {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MongoGroupScan.class);
  
  private MongoStoragePlugin storagePlugin;
  
  private MongoStoragePluginConfig storagePluginConfig;
  
  private MongoScanSpec scanSpec;
  
  private List<SchemaPath> columns;
  
  private Stopwatch watch = new Stopwatch();
  
  @JsonCreator
  public MongoGroupScan(@JsonProperty("mongoScanSpec") MongoScanSpec mongoScanSpec,
                        @JsonProperty("storage") MongoStoragePluginConfig storagePluginConfig,
                        @JsonProperty("columns") List<SchemaPath> columns,
                        @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this ((MongoStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), mongoScanSpec, columns);
  }
  
  public MongoGroupScan(MongoStoragePlugin storagePlugin, MongoScanSpec scanSpec, List<SchemaPath> columns) {
    this.storagePlugin = storagePlugin;
    this.storagePluginConfig = storagePlugin.getConfig();
    this.scanSpec = scanSpec;
    this.columns = columns;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    return null;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 0;
  }

  @Override
  public String getDigest() {
    return null;
  }

  @Override
  public ScanStats getScanStats() {
    return null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return null;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return null;
  }

}
