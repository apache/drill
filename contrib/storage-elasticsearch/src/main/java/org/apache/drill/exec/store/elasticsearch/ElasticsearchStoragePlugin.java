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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.calcite.adapter.elasticsearch.CalciteUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.elasticsearch.schema.ElasticsearchDrillSchemaFactory;

import java.util.Collections;
import java.util.Set;

public class ElasticsearchStoragePlugin extends AbstractStoragePlugin {
  private final ElasticsearchStorageConfig config;
  private final ElasticsearchDrillSchemaFactory schemaFactory;

  public ElasticsearchStoragePlugin(
    ElasticsearchStorageConfig config, DrillbitContext context, String name, UserSession session) {
    super(context, name, session);
    this.config = config;
    this.schemaFactory = new ElasticsearchDrillSchemaFactory(name, this);
  }

  private ElasticsearchStoragePlugin (ElasticsearchStoragePlugin that, UserSession session) {
    super(that, session);
    this.config = that.config;
    this.schemaFactory = that.schemaFactory;
  }

  @Override
  public ElasticsearchStoragePlugin clone(UserSession session) {
    return new ElasticsearchStoragePlugin(this, session);
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws JsonProcessingException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public ElasticsearchStorageConfig getConfig() {
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
        return CalciteUtils.elasticSearchRules();
      case LOGICAL_PRUNE_AND_JOIN:
      case LOGICAL_PRUNE:
      case PARTITION_PRUNING:
      case JOIN_PLANNING:
      default:
        return Collections.emptySet();
    }
  }
}
