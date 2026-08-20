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
package org.apache.drill.exec.store.accumulo;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schema factory for Accumulo storage plugin.
 *
 * <p>Responsible for registering the Accumulo schema and discovering tables.</p>
 */
public class AccumuloSchemaFactory extends AbstractSchemaFactory {
  private static final Logger logger = LoggerFactory.getLogger(AccumuloSchemaFactory.class);

  private final AccumuloStoragePlugin plugin;

  public AccumuloSchemaFactory(AccumuloStoragePlugin plugin) {
    super(plugin.getName());
    this.plugin = plugin;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    AccumuloSchema schema = new AccumuloSchema(getName());
    SchemaPlus schemaPlus = parent.add(getName(), schema);
    schema.setHolder(schemaPlus);
  }

  /**
   * Accumulo schema implementation.
   */
  class AccumuloSchema extends AbstractSchema {

    AccumuloSchema(String name) {
      super(Collections.emptyList(), name);
    }

    public void setHolder(SchemaPlus plusOfThis) {
      // No-op for now
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return Collections.emptySet();
    }

    @Override
    public Table getTable(String name) {
      AccumuloScanSpec scanSpec = new AccumuloScanSpec(name);
      try {
        return new DrillAccumuloTable(plugin, getName(), scanSpec);
      } catch (Exception e) {
        logger.warn("Failure while loading table '{}' for schema '{}'.", name, getName(), e);
        return null;
      }
    }

    @Override
    public Set<String> getTableNames() {
      try {
        return plugin.getSchemaProvider().discoverTableNames(plugin.getClient());
      } catch (Exception e) {
        logger.warn("Failure while loading table names for schema '{}'.", getName(), e);
        return Collections.emptySet();
      }
    }

    @Override
    public String getTypeName() {
      return AccumuloStoragePluginConfig.NAME;
    }
  }
}
