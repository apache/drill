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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.planner.sql.logical.ConvertHiveParquetScanToDrillParquetScan;
import org.apache.drill.exec.planner.sql.logical.HivePushPartitionFilterIntoScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.hive.schema.HiveSchemaFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

public class HiveStoragePlugin extends AbstractStoragePlugin {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveStoragePlugin.class);

  private final HiveStoragePluginConfig config;
  private final HiveSchemaFactory schemaFactory;
  private final DrillbitContext context;
  private final String name;
  private final HiveConf hiveConf;

  public HiveStoragePlugin(HiveStoragePluginConfig config, DrillbitContext context, String name) throws ExecutionSetupException {
    this.config = config;
    this.context = context;
    this.name = name;
    this.hiveConf = createHiveConf(config.getHiveConfigOverride());
    this.schemaFactory = new HiveSchemaFactory(this, name, hiveConf);
  }

  public HiveConf getHiveConf() {
    return hiveConf;
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

      return new HiveScan(userName, hiveReadEntry, this, columns, null);
    } catch (ExecutionSetupException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public Set<StoragePluginOptimizerRule> getLogicalOptimizerRules(OptimizerRulesContext optimizerContext) {
    final String defaultPartitionValue = hiveConf.get(ConfVars.DEFAULTPARTITIONNAME.varname);

    ImmutableSet.Builder<StoragePluginOptimizerRule> ruleBuilder = ImmutableSet.builder();

    ruleBuilder.add(HivePushPartitionFilterIntoScan.getFilterOnProject(optimizerContext, defaultPartitionValue));
    ruleBuilder.add(HivePushPartitionFilterIntoScan.getFilterOnScan(optimizerContext, defaultPartitionValue));

    return ruleBuilder.build();
  }

  @Override
  public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    if(optimizerRulesContext.getPlannerSettings().getOptions()
        .getOption(ExecConstants.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS).bool_val) {
      return ImmutableSet.<StoragePluginOptimizerRule>of(ConvertHiveParquetScanToDrillParquetScan.INSTANCE);
    }

    return ImmutableSet.of();
  }

  private static HiveConf createHiveConf(final Map<String, String> hiveConfigOverride) {
    final HiveConf hiveConf = new HiveConf();
    for(Entry<String, String> config : hiveConfigOverride.entrySet()) {
      final String key = config.getKey();
      final String value = config.getValue();
      hiveConf.set(key, value);
      logger.trace("HiveConfig Override {}={}", key, value);
    }
    return hiveConf;
  }
}
