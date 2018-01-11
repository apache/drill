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
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class HiveStoragePlugin extends AbstractStoragePlugin {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveStoragePlugin.class);

  private final HiveStoragePluginConfig config;
  private HiveSchemaFactory schemaFactory;
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

  // Forced to synchronize this method to allow error recovery
  // in the multi-threaded case. Can remove synchronized only
  // by restructuring connections and cache to allow better
  // recovery from failed secure connections.

  @Override
  public synchronized void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    try {
      schemaFactory.registerSchemas(schemaConfig, parent);
      return;

    // Hack. We may need to retry the connection. But, we can't because
    // the retry logic is implemented in the very connection we need to
    // discard and rebuild. To work around, we discard the entire schema
    // factory, and all its invalid connections. Very crude, but the
    // easiest short-term solution until we refactor the code to do the
    // job properly. See DRILL-5510.

    } catch (Throwable e) {
      // Unwrap exception
      Throwable ex = e;
      for (;;) {
        // Case for failing on an invalid cached connection
        if (ex instanceof MetaException ||
            // Case for a timed-out impersonated connection, and
            // an invalid non-secure connection used to get security
            // tokens.
            ex instanceof TTransportException) {
          break;
        }

        // All other exceptions are not handled, just pass along up
        // the stack.

        if (ex.getCause() == null  ||  ex.getCause() == ex) {
          logger.error("Hive metastore register schemas failed", e);
          throw new DrillRuntimeException("Unknown Hive error", e);
        }
        ex = ex.getCause();
      }
    }

    // Build a new factory which will cause an all new set of
    // Hive metastore connections to be created.

    try {
      schemaFactory.close();
    } catch (Throwable t) {
      // Ignore, we're in a bad state.
      logger.warn("Schema factory forced close failed, error ignored", t);
    }
    try {
      schemaFactory = new HiveSchemaFactory(this, name, hiveConf);
    } catch (ExecutionSetupException e) {
      throw new DrillRuntimeException(e);
    }

    // Try the schemas again. If this fails, just give up.

    schemaFactory.registerSchemas(schemaConfig, parent);
    logger.debug("Successfully recovered from a Hive metastore connection failure.");
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
    // TODO: Remove implicit using of convert_fromTIMESTAMP_IMPALA function
    // once "store.parquet.reader.int96_as_timestamp" will be true by default
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
