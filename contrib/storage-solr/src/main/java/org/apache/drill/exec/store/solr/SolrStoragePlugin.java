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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.solr.schema.SolrSchemaFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

public class SolrStoragePlugin extends AbstractStoragePlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(SolrStoragePlugin.class);

  private final SolrStoragePluginConfig solrStorageConfig;
  private final SolrClient solrClient;
  private final DrillbitContext context;
  private final SolrSchemaFactory schemaFactory;
  private SolrClientAPIExec solrClientApiExec;

  public SolrStoragePlugin(SolrStoragePluginConfig solrStoragePluginConfig,
      DrillbitContext context, String name) {
    logger.debug("initializing solr storage plugin....");
    this.context = context;
    this.solrStorageConfig = solrStoragePluginConfig;
    this.solrClient = new HttpSolrClient(solrStorageConfig.getSolrServer());
    solrClientApiExec = new SolrClientAPIExec(solrClient);
    this.schemaFactory = new SolrSchemaFactory(this, name);
    logger.info("solr storage plugin name :: " + name);
  }

  public SolrClientAPIExec getSolrClientApiExec() {
    return solrClientApiExec;
  }

  public SolrStoragePluginConfig getSolrStorageConfig() {
    return solrStorageConfig;
  }

  public SolrClient getSolrClient() {
    return this.solrClient;
  }

  public DrillbitContext getContext() {
    return this.context;
  }

  @Override
  public StoragePluginConfig getConfig() {
    return this.solrStorageConfig;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent)
      throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName,
      JSONOptions selection, List<SchemaPath> columns) throws IOException {
    logger.debug("SolrStoragePlugin :: getPhysicalScan" + " userName : "
        + userName + " columns ::" + columns + " selection " + selection);
    SolrScanSpec solrScanSpec = selection.getListWith(new ObjectMapper(),
        new TypeReference<SolrScanSpec>() {
        });
    return new SolrGroupScan(userName, this, solrScanSpec, columns);
  }

  @Override
  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    logger.debug("SolrStoragePlugin :: getOptimizerRules");
    return ImmutableSet.of(SolrQueryFilterRule.FILTER_ON_SCAN,
        SolrQueryFilterRule.FILTER_ON_PROJECT,
        SolrQueryFilterRule.AGG_PUSH_DOWN);
  }
}
