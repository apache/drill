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

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

@JsonTypeName("solr-scan")
public class SolrGroupScan extends AbstractGroupScan {
  protected SolrStoragePlugin solrPlugin;
  protected SolrStoragePluginConfig solrPluginConfig;
  protected SolrScanSpec solrScanSpec;
  protected List<SolrScanSpec> scanList = Lists.newArrayList();
  protected List<SchemaPath> columns;

  static final Logger logger = LoggerFactory.getLogger(SolrGroupScan.class);

  public SolrGroupScan(SolrGroupScan that) {
    super(that);
    this.solrPlugin = that.solrPlugin;
    this.solrPluginConfig = that.solrPlugin.getSolrStorageConfig();
    this.solrScanSpec = that.solrScanSpec;
    this.columns = that.columns;
    this.scanList.add(this.solrScanSpec);
    logger.info("SolrGroupScan :: default constructor :: ");
  }

  public SolrGroupScan(String userName, SolrStoragePlugin solrStoragePlugin,
      SolrScanSpec scanSpec, List<SchemaPath> columns) {
    super(userName);
    this.solrPlugin = solrStoragePlugin;
    this.solrPluginConfig = solrStoragePlugin.getSolrStorageConfig();
    this.solrScanSpec = scanSpec;
    this.columns = columns;
    this.scanList.add(this.solrScanSpec);
    logger.info("SolrGroupScan :: param constructor :: " + columns);

  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    logger.debug("SolrGroupScan :: clone :: " + columns);
    SolrGroupScan clone = new SolrGroupScan(this);
    clone.columns = columns;
    return clone;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints)
      throws PhysicalOperatorSetupException {
    // TODO Auto-generated method stub
    logger.debug("SolrGroupScan :: applyAssignments");
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId)
      throws ExecutionSetupException {
    // TODO Auto-generated method stub
    logger.debug("SolrGroupScan :: getSpecificScan :: " + columns);
    return new SolrSubScan(this);

  }

  @Override
  public int getMaxParallelizationWidth() {
    // TODO Auto-generated method stub
    logger.debug("SolrGroupScan :: getMaxParallelizationWidth");
    return -1;
  }

  @Override
  public String getDigest() {
    // TODO Auto-generated method stub
    return toString();
  }

  @Override
  public ScanStats getScanStats() {
    // TODO Auto-generated method stub
    return ScanStats.TRIVIAL_TABLE;
  }

  @JsonIgnore
  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    // TODO Auto-generated method stub
    logger.debug("SolrGroupScan :: getNewWithChildren");
    Preconditions.checkArgument(children.isEmpty());
    return new SolrGroupScan(this);
  }

  @JsonProperty
  public SolrScanSpec getSolrScanSpec() {
    return solrScanSpec;
  }

  @JsonProperty
  public SolrStoragePluginConfig getSolrPluginConfig() {
    return solrPluginConfig;
  }

  @JsonIgnore
  public SolrStoragePlugin getSolrPlugin() {
    return solrPlugin;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public String toString() {
    return "SolrGroupScan [SolrScanSpec=" + solrScanSpec + ", columns="
        + columns + "]";
  }
}
