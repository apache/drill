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
package org.apache.drill.exec.store.elasticsearch.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.elasticsearch.ElasticsearchStoragePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ElasticsearchDrillSchemaFactory extends AbstractSchemaFactory {

  private static final Logger logger = LoggerFactory.getLogger(ElasticsearchDrillSchemaFactory.class);
  private final ElasticsearchStoragePlugin plugin;
  private final ElasticsearchSchemaFactory delegate;

  public ElasticsearchDrillSchemaFactory(String name, ElasticsearchStoragePlugin plugin) {
    super(name);
    this.plugin = plugin;
    this.delegate = new ElasticsearchSchemaFactory();
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws JsonProcessingException {
    ElasticsearchDrillSchema schema;
    if (plugin.getConfig().getAuthMode() == AuthMode.SHARED_USER) {
      Schema elasticsearchSchema = delegate.create(parent, getName(), plugin.getConfig().toConfigMap());
      schema = new ElasticsearchDrillSchema(getName(), plugin, elasticsearchSchema);
    } else if (plugin.getConfig().getAuthMode() == AuthMode.USER_TRANSLATION) {
      // Get user's info
      Schema elasticsearchUTSchema = delegate.create(parent, getName(), plugin.getConfig().toConfigMap(schemaConfig.getUserName()));
      schema = new ElasticsearchDrillSchema(getName(), plugin, elasticsearchUTSchema);
    } else {
      throw UserException.internalError()
          .message("User Impersonation not supported as an authentication mode for ElasticSearch.  The only authentication modes supported are SHARED_USER and USER_TRANSLATION")
          .build(logger);
    }
    parent.add(getName(), schema);
  }
}
