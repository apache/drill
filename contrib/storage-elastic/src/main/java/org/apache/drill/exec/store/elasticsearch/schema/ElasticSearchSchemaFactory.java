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

import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.elasticsearch.ElasticSearchStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class ElasticSearchSchemaFactory implements SchemaFactory {

  private final String schemaName;

  private final ElasticSearchStoragePlugin plugin;

  private final LoadingCache<String, Collection<String>> indexCache;

  private final LoadingCache<String, Collection<String>> typeMappingCache;

  public ElasticSearchSchemaFactory(ElasticSearchStoragePlugin plugin, String schemaName, long cacheDuration, TimeUnit cacheTimeUnit) {
    this.schemaName = schemaName;
    this.plugin = plugin;

    // index
    indexCache = CacheBuilder.newBuilder().expireAfterAccess(cacheDuration, cacheTimeUnit).build(new ElasticSearchIndexLoader(plugin));

    // index map type
    typeMappingCache = CacheBuilder.newBuilder().expireAfterAccess(cacheDuration, cacheTimeUnit).build(new ElasticSearchTypeMappingLoader(plugin));
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    // Registered here
    ElasticSearchSchema schema = new ElasticSearchSchema(schemaName, plugin);
    SchemaPlus hPlus = parent.add(schemaName, schema);
    schema.setHolder(hPlus);
  }


  public LoadingCache<String, Collection<String>> getIndexCache() {
    return indexCache;
  }

  public LoadingCache<String, Collection<String>> getTypeMappingCache() {
    return typeMappingCache;
  }

  public String getSchemaName() {
    return schemaName;
  }
}