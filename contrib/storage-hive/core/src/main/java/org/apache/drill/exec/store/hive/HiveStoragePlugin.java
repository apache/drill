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
package org.apache.drill.exec.store.hive;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import com.google.common.collect.ImmutableSet;

import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.sql.logical.HivePushPartitionFilterIntoScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.hive.schema.HiveSchemaFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HiveStoragePlugin extends AbstractStoragePlugin {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveStoragePlugin.class);

  private final HiveStoragePluginConfig config;
  private final HiveSchemaFactory schemaFactory;
  private final DrillbitContext context;
  private final String name;

  public HiveStoragePlugin(HiveStoragePluginConfig config, DrillbitContext context, String name) throws ExecutionSetupException {
    this.config = config;
    this.context = context;
    this.schemaFactory = new HiveSchemaFactory(this, name, config.getHiveConfigOverride());
    this.name = name;
  }

  public HiveStoragePluginConfig getConfig() {
    return config;
  }

  public String getName(){
    return name;
  }

  public DrillbitContext getContext() {
    return context;
  }

  @Override
  public HiveScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns) throws IOException {
    HiveReadEntry hiveReadEntry = selection.getListWith(new ObjectMapper(), new TypeReference<HiveReadEntry>(){});
    try {
      if (hiveReadEntry.getJdbcTableType() == TableType.VIEW) {
        throw new UnsupportedOperationException(
            "Querying views created in Hive from Drill is not supported in current version.");
      }

      return new HiveScan(userName, hiveReadEntry, this, columns);
    } catch (ExecutionSetupException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }
  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    return ImmutableSet.of(HivePushPartitionFilterIntoScan.HIVE_FILTER_ON_PROJECT, HivePushPartitionFilterIntoScan.HIVE_FILTER_ON_SCAN);
  }

}
