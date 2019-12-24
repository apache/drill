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

import java.io.IOException;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.elasticsearch.schema.ElasticSearchSchemaFactory;
import org.elasticsearch.client.RestClient;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

/**
 * Main ElasticSearch Plugin class to configure storage instance
 */
public class ElasticSearchStoragePlugin extends AbstractStoragePlugin {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticSearchStoragePlugin.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final String name;
    private final DrillbitContext context;
    private final ElasticSearchPluginConfig config;
    private final ElasticSearchSchemaFactory schemaFactory;
    private RestClient client;

    /**
     * Constructor of instance
     * @param config configuration for this instance
     * @param context drillbit context
     * @param name name of the storagePlugin instance
     * @throws IOException may be thrown in case construction of instance fails
     */
    public ElasticSearchStoragePlugin(ElasticSearchPluginConfig config, DrillbitContext context, String name) throws IOException {
        super(context, name);
        this.context = context;
        this.name = name;
        this.config = config;

        this.schemaFactory = new ElasticSearchSchemaFactory(this, name, this.config.getCacheDuration(), this.config.getCacheTimeUnit());
    }
    
    public DrillbitContext getContext() {
        return this.context;
      }

    /**
     *
     * @return return instance configuration
     */
    @Override
    public ElasticSearchPluginConfig getConfig() {
        return this.config;
    }

    /**
     * {@inheritDoc}
     * @param schemaConfig
     * @param parent
     * @throws IOException
     */
    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        this.schemaFactory.registerSchemas(schemaConfig, parent);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportsRead() {
        return true;
    }

    @Override
    public void start() throws IOException {
        if (this.client == null) {
            this.client = this.config.createClient();
            logger.debug("Client created");
        } else {
            logger.warn("Already created");
        }
    }

    @Override
    public void close() throws IOException {
        if (this.client != null) {
            this.client.close();
            this.client = null;
            logger.debug("Client closed");
        } else {
            logger.warn("Client not started or already closed");
        }
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
        ElasticSearchScanSpec elasticSearchScanSpec = selection.getListWith(this.getObjectMapper(), new TypeReference<ElasticSearchScanSpec>() {});
        return new ElasticSearchGroupScan(userName, this, elasticSearchScanSpec, null);
    }

    @Override
    public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
  	  // 创建优化器
      return ImmutableSet.of(MongoPushDownFilterForScan.INSTANCE);
    }
    
    
    public RestClient getClient() { return this.client; }

    public ElasticSearchSchemaFactory getSchemaFactory() {
        return this.schemaFactory;
    }

    public ObjectMapper getObjectMapper() { return OBJECT_MAPPER; }
}
