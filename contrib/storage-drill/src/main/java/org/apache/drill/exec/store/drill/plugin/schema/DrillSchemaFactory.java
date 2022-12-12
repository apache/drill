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
package org.apache.drill.exec.store.drill.plugin.schema;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.drill.plugin.DrillStoragePlugin;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheLoader;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class DrillSchemaFactory extends AbstractSchemaFactory {

  private final LoadingCache<Pair<String, String>, DrillPluginSchema> databases;
  private final DrillStoragePlugin plugin;

  public DrillSchemaFactory(DrillStoragePlugin plugin, String schemaName) {
    super(schemaName);
    this.plugin = plugin;

    databases = CacheBuilder
        .newBuilder()
        .expireAfterAccess(1, TimeUnit.MINUTES)
        .build(new DatabaseLoader());
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    try {
      String userName = Optional.ofNullable(schemaConfig.getUserName()).orElse(ImpersonationUtil.getProcessUserName());
      DrillPluginSchema schema = databases.get(Pair.of(getName(), userName));
      SchemaPlus hPlus = parent.add(getName(), schema);
      schema.setHolder(hPlus);
    } catch (ExecutionException e) {
      throw new DrillRuntimeException(e);
    }
  }

  private class DatabaseLoader extends CacheLoader<Pair<String, String>, DrillPluginSchema> {
    @Override
    public DrillPluginSchema load(Pair<String, String> key) {
      return new DrillPluginSchema(plugin, key.getKey(), key.getValue());
    }
  }
}
