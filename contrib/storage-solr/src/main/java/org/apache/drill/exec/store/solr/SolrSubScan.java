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
package org.apache.drill.exec.store.solr;

import static org.apache.drill.common.graph.GraphValue.logger;

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
import com.google.common.collect.Iterators;

public class SolrSubScan extends AbstractBase implements SubScan {
  @JsonIgnore
  private SolrStoragePlugin solrPlugin;
  @JsonProperty
  private SolrStoragePluginConfig solrPluginConfig;
  private SolrScanSpec solrScanSpec;
  private List<SchemaPath> columns;
  private List<SolrScanSpec> scanList;

  private String userName;

  public SolrSubScan(SolrGroupScan that) {
    super(that);
    this.columns = that.columns;
    this.solrPlugin = that.solrPlugin;
    this.solrPluginConfig = that.solrPluginConfig;
    this.solrScanSpec = that.solrScanSpec;
    this.scanList = that.scanList;
    logger.info("SolrSubScan : constructor ::" + columns);
  }

  @JsonCreator
  public SolrSubScan(@JacksonInject StoragePluginRegistry registry,
      String userName,
      @JsonProperty("solrPluginConfig") SolrStoragePluginConfig pluginConfig,
      @JsonProperty("solrScanSpec") List<SolrScanSpec> scanList,
      @JsonProperty("columns") List<SchemaPath> columns)
      throws ExecutionSetupException {
    super(userName);
    logger.debug("SolrSubScan : constructor11 ::" + columns);
    this.solrPlugin = (SolrStoragePlugin) registry.getPlugin(pluginConfig);
    this.solrPluginConfig = pluginConfig;
    this.columns = columns;
    this.scanList = scanList;
    this.userName = userName;
  }

  public SolrSubScan(SolrStoragePlugin plugin, String userName,
      SolrStoragePluginConfig pluginConfig, List<SolrScanSpec> scanList,
      List<SchemaPath> columns) {
    super(userName);
    this.columns = columns;
    this.solrPlugin = plugin;
    this.solrPluginConfig = pluginConfig;
    this.scanList = scanList;
    logger.debug("SolrSubScan : constructor ::" + userName);
  }

  @Override
  public <T, X, E extends Throwable> T accept(
      PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    logger.debug("SolrSubScan : getNewWithChildren ::");
    return new SolrSubScan(solrPlugin, null, solrPluginConfig, scanList,
        columns);
  }

  @Override
  public int getOperatorType() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  public SolrStoragePlugin getSolrPlugin() {
    return solrPlugin;
  }

  public List<SolrScanSpec> getScanList() {
    return scanList;
  }

  public SolrScanSpec getSolrScanSpec() {
    return solrScanSpec;
  }
}
