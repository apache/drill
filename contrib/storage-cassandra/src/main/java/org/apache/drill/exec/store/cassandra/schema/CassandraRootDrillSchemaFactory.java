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
package org.apache.drill.exec.store.cassandra.schema;

import org.apache.calcite.adapter.cassandra.CassandraSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.cassandra.CassandraStoragePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraRootDrillSchemaFactory extends AbstractSchemaFactory {

  private static final Logger logger = LoggerFactory.getLogger(CassandraRootDrillSchemaFactory.class);
  private final CassandraStoragePlugin plugin;
  private final SchemaFactory calciteSchemaFactory;

  public CassandraRootDrillSchemaFactory(String name, CassandraStoragePlugin plugin) {
    super(name);
    this.plugin = plugin;
    this.calciteSchemaFactory = new CassandraSchemaFactory();
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    Schema schema;
    if (plugin.getConfig().getAuthMode() == AuthMode.SHARED_USER) {
      schema = new CassandraRootDrillSchema(getName(), plugin,
          calciteSchemaFactory, parent, getName(), plugin.getConfig().toConfigMap());
    } else if (plugin.getConfig().getAuthMode() == AuthMode.USER_TRANSLATION) {
      schema = new CassandraRootDrillSchema(getName(), plugin,
          calciteSchemaFactory, parent, getName(), plugin.getConfig().toConfigMap(schemaConfig.getUserName()));
    } else {
      throw UserException.internalError()
          .message("Cassandra only supports SHARED_USER and USER_TRANSLATION authentication.")
          .build(logger);
    }
    parent.add(getName(), schema);
  }
}
