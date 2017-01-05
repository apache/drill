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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

@JsonTypeName("indexr-segments-scan")
public class IndexRSubScan extends AbstractBase implements SubScan {
  static final Logger logger = LoggerFactory.getLogger(IndexRSubScan.class);

  private final IndexRStoragePlugin plugin;
  private final IndexRSubScanSpec spec;
  private final List<SchemaPath> columns;

  @JsonCreator
  public IndexRSubScan(@JacksonInject StoragePluginRegistry registry,//
                       @JsonProperty("pluginConfig") IndexRStoragePluginConfig pluginConfig,//
                       @JsonProperty("spec") IndexRSubScanSpec spec,//
                       @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
    this((IndexRStoragePlugin) registry.getPlugin(pluginConfig), spec, columns);
  }

  public IndexRSubScan(IndexRStoragePlugin plugin, IndexRSubScanSpec spec, List<SchemaPath> columns) {
    super((String) null);
    this.plugin = plugin;
    this.spec = spec;
    this.columns = columns;
  }

  @JsonIgnore
  public IndexRStoragePlugin getPlugin() {
    return plugin;
  }

  @JsonProperty("pluginConfig")
  public IndexRStoragePluginConfig getPluginConfig() {
    return plugin.getConfig();
  }

  @JsonProperty("spec")
  public IndexRSubScanSpec getSpec() {
    return spec;
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new IndexRSubScan(plugin, spec, columns);
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.MOCK_SUB_SCAN_VALUE;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

}
