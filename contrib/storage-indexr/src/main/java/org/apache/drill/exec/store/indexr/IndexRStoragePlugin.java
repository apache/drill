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

import com.google.common.collect.Sets;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import io.indexr.segment.pack.IndexMemCache;
import io.indexr.segment.pack.PackMemCache;
import io.indexr.server.IndexRConfig;
import io.indexr.server.IndexRNode;

public class IndexRStoragePlugin extends AbstractStoragePlugin {
  private static final Logger log = LoggerFactory.getLogger(IndexRStoragePlugin.class);

  private final IndexRStoragePluginConfig pluginConfig;
  private final DrillbitContext context;
  private final String pluginName;
  private final IndexRSchemaFactory schemaFactory;
  private IndexRNode indexRNode;
  private IndexMemCache indexMemCache;
  private PackMemCache packMemCache;

  public IndexRStoragePlugin(IndexRStoragePluginConfig engineConfig, DrillbitContext context, String name) {
    this.pluginConfig = engineConfig;
    this.context = context;
    this.pluginName = name;

    this.schemaFactory = new IndexRSchemaFactory(this);
  }

  @Override
  public IndexRStoragePluginConfig getConfig() {
    return pluginConfig;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  public DrillbitContext context() {
    return context;
  }

  public String pluginName() {
    return pluginName;
  }

  public IndexRNode indexRNode() {
    return indexRNode;
  }

  public IndexMemCache indexMemCache() {
    return indexMemCache;
  }

  public PackMemCache packMemCache() {
    return packMemCache;
  }

  @Override
  public void start() throws IOException {
    super.start();
    try {
      this.indexRNode = new IndexRNode(context.getEndpoint().getAddress());
      IndexRConfig config = indexRNode.getConfig();
      // Indexes are always cached.
      this.indexMemCache = config.getIndexMemCache();
      this.packMemCache = config.getPackMemCache();
    } catch (Exception e) {
      throw new IOException(e);
    }
    log.info("Plugin started");
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (indexRNode != null) {
      indexRNode.close();
      indexRNode = null;
    }
    if (indexMemCache != null) {
      indexMemCache.close();
      indexMemCache = null;
    }
    if (packMemCache != null) {
      packMemCache.close();
      packMemCache = null;
    }
    log.info("Plugin closed");
  }

  @Override
  public Set<? extends RelOptRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    return Sets.newHashSet(
        IndexRPushDownRSFilter.FilterScan,
        IndexRPushDownRSFilter.FilterProjectScan);
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    return getPhysicalScan(userName, selection, GroupScan.ALL_COLUMNS);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns) throws IOException {
    IndexRScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<IndexRScanSpec>() {});
    return new IndexRGroupScan(userName, this, scanSpec, columns, Long.MAX_VALUE, UUID.randomUUID().toString());
  }
}
