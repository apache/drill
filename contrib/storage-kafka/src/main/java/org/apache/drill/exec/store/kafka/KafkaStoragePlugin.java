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
package org.apache.drill.exec.store.kafka;

import java.io.IOException;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.kafka.schema.KafkaSchemaFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.apache.drill.shaded.guava.com.google.common.io.Closer;

public class KafkaStoragePlugin extends AbstractStoragePlugin {

  private static final Logger logger = LoggerFactory.getLogger(KafkaStoragePlugin.class);
  private final KafkaSchemaFactory kafkaSchemaFactory;
  private final KafkaStoragePluginConfig config;
  private final Closer closer = Closer.create();

  public KafkaStoragePlugin(KafkaStoragePluginConfig config, DrillbitContext context, String name)
      throws ExecutionSetupException {
    super(context, name);
    logger.debug("Initializing {}", KafkaStoragePlugin.class.getName());
    this.config = config;
    this.kafkaSchemaFactory = new KafkaSchemaFactory(this, name);
  }

  @Override
  public KafkaStoragePluginConfig getConfig() {
    return this.config;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    this.kafkaSchemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    return ImmutableSet.of(KafkaPushDownFilterIntoScan.INSTANCE);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName,
      JSONOptions selection) throws IOException {
    KafkaScanSpec kafkaScanSpec = selection.getListWith(new ObjectMapper(),
        new TypeReference<KafkaScanSpec>() {
        });
    return new KafkaGroupScan(this, kafkaScanSpec, null);
  }

  public KafkaConsumer<byte[], byte[]> registerConsumer(KafkaConsumer<byte[], byte[]> consumer) {
    return closer.register(consumer);
  }

  @Override
  public void close() throws IOException {
    closer.close();
  }

}
