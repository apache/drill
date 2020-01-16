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
package org.apache.drill.exec.store.cassandra;

import java.io.IOException;
import java.util.Set;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;

public class CassandraStoragePlugin extends AbstractStoragePlugin {

  private final DrillbitContext context;

  private final CassandraStoragePluginConfig cassandraConfig;

  private final CassandraSchemaFactory schemaFactory;

  public CassandraStoragePlugin(CassandraStoragePluginConfig cassandraConfig, DrillbitContext context, String name) {
    super(context, name);
    this.context = context;
    this.cassandraConfig = cassandraConfig;
    this.schemaFactory = new CassandraSchemaFactory(this, name);
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  public DrillbitContext getContext() {
    return context;
  }

  @Override
  public CassandraStoragePluginConfig getConfig() {
    return cassandraConfig;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }


  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    CassandraScanSpec cassandraScanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<CassandraScanSpec>() {});
    return new CassandraGroupScan(userName, this, cassandraScanSpec, null);
  }

  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    return ImmutableSet.of(CassandraPushDownFilterForScan.INSTANCE);
  }


}
