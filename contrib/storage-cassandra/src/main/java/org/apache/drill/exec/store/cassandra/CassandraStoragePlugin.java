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

import org.apache.calcite.adapter.cassandra.CalciteUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.cassandra.schema.CassandraRootDrillSchemaFactory;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

public class CassandraStoragePlugin extends AbstractStoragePlugin {

  private static final Logger logger = LoggerFactory.getLogger(CassandraStoragePlugin.class);
  private final CassandraStorageConfig config;
  private final SchemaFactory schemaFactory;

  public CassandraStoragePlugin(
      CassandraStorageConfig config, DrillbitContext context, String name) {
    super(context, name);
    this.config = config;
    this.schemaFactory = new CassandraRootDrillSchemaFactory(name, this);
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    // Check to see if user translation is enabled.  If so, and creds are
    // not present, then do not register any schemata.  This prevents
    // info schema errors.
    if (config.getAuthMode() == AuthMode.USER_TRANSLATION) {
      Optional<UsernamePasswordCredentials> userCreds = config.getUsernamePasswordCredentials(schemaConfig.getUserName());
      if (! userCreds.isPresent()) {
        logger.debug(
            "No schemas will be registered in {} for query user {}.",
            getName(), schemaConfig.getUserName()
        );
        return;
      }
    }
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public CassandraStorageConfig getConfig() {
    return config;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext, PlannerPhase phase) {
    switch (phase) {
      case PHYSICAL:
      case LOGICAL:
        return CalciteUtils.cassandraRules();
      case LOGICAL_PRUNE_AND_JOIN:
      case LOGICAL_PRUNE:
      case PARTITION_PRUNING:
      case JOIN_PLANNING:
      default:
        return ImmutableSet.of();
    }
  }
}
