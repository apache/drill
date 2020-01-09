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

package org.apache.drill.exec.store.elasticsearch;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@JsonTypeName("elasticsearch-read")
public class ElasticSearchSubScan extends AbstractBase implements SubScan {

  private static final Logger logger = LoggerFactory.getLogger(ElasticSearchSubScan.class);

  @JsonIgnore
  private final ElasticSearchStoragePlugin elasticSearchStoragePlugin;

  @JsonProperty
  private final ElasticSearchPluginConfig elasticSearchPluginConfig;

  private final List<SchemaPath> columns;

  private final List<ElasticSearchScanSpec> elasticSearchScanSpecs;

  @JsonCreator
  public ElasticSearchSubScan(@JacksonInject StoragePluginRegistry registry,
                              @JsonProperty("userName") String userName,
                              @JsonProperty("elasticSearchPluginConfig") ElasticSearchPluginConfig elasticSearchPluginConfig,
                              @JsonProperty("ElasticSearchScanSpecs") List<ElasticSearchScanSpec> elasticSearchScanSpecs,
                              @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
    this(userName, (ElasticSearchStoragePlugin) registry.getPlugin(elasticSearchPluginConfig), elasticSearchPluginConfig, elasticSearchScanSpecs, columns);
  }

  public ElasticSearchSubScan(String userName,
                              ElasticSearchStoragePlugin elasticSearchStoragePlugin,
                              ElasticSearchPluginConfig elasticSearchPluginConfig,
                              List<ElasticSearchScanSpec> elasticSearchScanSpecs,
                              List<SchemaPath> columns) {
    super(userName);
    this.columns = columns;
    this.elasticSearchScanSpecs = elasticSearchScanSpecs;
    this.elasticSearchStoragePlugin = elasticSearchStoragePlugin;
    this.elasticSearchPluginConfig = elasticSearchPluginConfig;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new ElasticSearchSubScan(super.getUserName(), elasticSearchStoragePlugin, elasticSearchPluginConfig, elasticSearchScanSpecs, columns);
  }

  @Override
  public int getOperatorType() {
    //TODO: What is this number?
    return 1009;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @JsonIgnore
  public ElasticSearchStoragePlugin getElasticSearchStoragePlugin() {
    return elasticSearchStoragePlugin;
  }

  @JsonIgnore
  public ElasticSearchPluginConfig getElasticSearchPluginConfig() {
    return elasticSearchPluginConfig;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  // this should scan for many spec
  public List<ElasticSearchScanSpec> getElasticSearchScanSpecs() {
    return elasticSearchScanSpecs;
  }
}
