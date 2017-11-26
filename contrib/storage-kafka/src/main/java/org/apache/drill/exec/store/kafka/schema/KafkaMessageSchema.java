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
package org.apache.drill.exec.store.kafka.schema;

import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.kafka.KafkaScanSpec;
import org.apache.drill.exec.store.kafka.KafkaStoragePlugin;
import org.apache.drill.exec.store.kafka.KafkaStoragePluginConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class KafkaMessageSchema extends AbstractSchema {

  private static final Logger logger = LoggerFactory.getLogger(KafkaMessageSchema.class);
  private final KafkaStoragePlugin plugin;
  private final Map<String, DrillTable> drillTables = Maps.newHashMap();
  private Set<String> tableNames;

  public KafkaMessageSchema(final KafkaStoragePlugin plugin, final String name) {
    super(ImmutableList.<String> of(), name);
    this.plugin = plugin;
  }

  @Override
  public String getTypeName() {
    return KafkaStoragePluginConfig.NAME;
  }

  void setHolder(SchemaPlus plusOfThis) {
    for (String s : getSubSchemaNames()) {
      plusOfThis.add(s, getSubSchema(s));
    }
  }

  @Override
  public Table getTable(String tableName) {
    if (!drillTables.containsKey(tableName)) {
      KafkaScanSpec scanSpec = new KafkaScanSpec(tableName);
      DrillTable table = new DynamicDrillTable(plugin, getName(), scanSpec);
      drillTables.put(tableName, table);
    }

    return drillTables.get(tableName);
  }

  @Override
  public Set<String> getTableNames() {
    if (tableNames == null) {
      try (KafkaConsumer<?, ?> kafkaConsumer = new KafkaConsumer<>(plugin.getConfig().getKafkaConsumerProps())) {
        tableNames = kafkaConsumer.listTopics().keySet();
      } catch(KafkaException e) {
        throw UserException.dataReadError(e).message("Failed to get tables information").addContext(e.getMessage())
            .build(logger);
      }
    }
    return tableNames;
  }
}
