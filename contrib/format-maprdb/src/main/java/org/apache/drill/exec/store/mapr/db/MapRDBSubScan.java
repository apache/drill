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
package org.apache.drill.exec.store.mapr.db;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

// Class containing information for reading a single HBase region
@JsonTypeName("maprdb-sub-scan")
public class MapRDBSubScan extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MapRDBSubScan.class);

  @JsonProperty
  public final StoragePluginConfig storageConfig;
  @JsonIgnore
  private final MapRDBFormatPluginConfig formatPluginConfig;
  private final FileSystemPlugin storagePlugin;
  private final List<MapRDBSubScanSpec> regionScanSpecList;
  private final List<SchemaPath> columns;
  private final String tableType;

  private final MapRDBFormatPlugin formatPlugin;

  @JsonCreator
  public MapRDBSubScan(@JacksonInject StoragePluginRegistry registry,
                       @JsonProperty("userName") String userName,
                       @JsonProperty("formatPluginConfig") MapRDBFormatPluginConfig formatPluginConfig,
                       @JsonProperty("storageConfig") StoragePluginConfig storage,
                       @JsonProperty("regionScanSpecList") List<MapRDBSubScanSpec> regionScanSpecList,
                       @JsonProperty("columns") List<SchemaPath> columns,
                       @JsonProperty("tableType") String tableType) throws ExecutionSetupException {
    this(userName, formatPluginConfig,
        (FileSystemPlugin) registry.getPlugin(storage),
        storage, regionScanSpecList, columns, tableType);
  }

  public MapRDBSubScan(String userName, MapRDBFormatPluginConfig formatPluginConfig, FileSystemPlugin storagePlugin, StoragePluginConfig storageConfig,
      List<MapRDBSubScanSpec> maprSubScanSpecs, List<SchemaPath> columns, String tableType) {
    super(userName);
    this.storageConfig = storageConfig;
    this.storagePlugin = storagePlugin;
    this.formatPluginConfig = formatPluginConfig;
    this.formatPlugin = (MapRDBFormatPlugin) storagePlugin.getFormatPlugin(formatPluginConfig);

    this.regionScanSpecList = maprSubScanSpecs;
    this.columns = columns;
    this.tableType = tableType;
  }

  public List<MapRDBSubScanSpec> getRegionScanSpecList() {
    return regionScanSpecList;
  }

  public List<SchemaPath> getColumns() {
    return columns;
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
    return new MapRDBSubScan(getUserName(), formatPluginConfig, storagePlugin, storageConfig, regionScanSpecList, columns, tableType);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return ImmutableSet.<PhysicalOperator>of().iterator();
  }

  @Override
  public int getOperatorType() {
    return 1001;
  }

  public String getTableType() {
    return tableType;
  }

  public MapRDBFormatPluginConfig getFormatPluginConfig() {
    return formatPluginConfig;
  }

  @JsonIgnore
  public MapRDBFormatPlugin getFormatPlugin() {
    return formatPlugin;
  }

}
