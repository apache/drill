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
package org.apache.drill.exec.store.http;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSchemaFactory extends AbstractSchemaFactory {
  private static final Logger logger = LoggerFactory.getLogger(HttpSchemaFactory.class);

  private final HttpStoragePlugin plugin;

  public HttpSchemaFactory(HttpStoragePlugin plugin, String schemaName) {
    super(schemaName);
    this.plugin = plugin;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    HttpSchema schema = new HttpSchema(getName());
    logger.debug("Registering {} {}", schema.getName(), schema.toString());

    SchemaPlus schemaPlus = parent.add(getName(), schema);
    schema.setHolder(schemaPlus);
  }

  class HttpSchema extends AbstractSchema {

    public HttpSchema(String name) {
      super(Collections.emptyList(), name);
    }

    void setHolder(SchemaPlus plusOfThis) {
      for (String s : getSubSchemaNames()) {
        plusOfThis.add(s, getSubSchemaKnownExists(s));
      }
    }

    @Override
    public Set<String> getSubSchemaNames() {
      HttpStoragePluginConfig config = plugin.getConfig();
      Map<String, HttpAPIConfig> connections = config.connections();
      Set<String> subSchemaNames = new HashSet<>();

      // Get the possible subschemas.
      for (Map.Entry<String, HttpAPIConfig> entry : connections.entrySet()) {
        subSchemaNames.add(entry.getKey());
      }
      return subSchemaNames;
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      if (plugin.getConfig().connections().containsKey(name)) {
        return getSubSchemaKnownExists(name);
      } else {
        throw UserException
          .connectionError()
          .message("API '{}' does not exist in HTTP Storage plugin '{}'", name, getName())
          .build(logger);
      }
    }

    /**
     * Helper method to get subschema when we know it exists (already checked the existence)
     */
    private HttpAPIConnectionSchema getSubSchemaKnownExists(String name) {
      return new HttpAPIConnectionSchema(this, name, plugin);
    }

    @Override
    public String getTypeName() {
      return HttpStoragePluginConfig.NAME;
    }
  }
}
