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
package org.apache.drill.exec.store.jdbc;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.jdbc.clickhouse.ClickhouseCatalogSchema;

import static ru.yandex.clickhouse.ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX;

public class JdbcSchemaFactory {
  private final JdbcStoragePlugin plugin;

  public JdbcSchemaFactory (JdbcStoragePlugin plugin) {
    this.plugin = plugin;
  }

  public void registerSchemas(SchemaConfig config, SchemaPlus parent) {
    if (plugin.getConfig().getUrl().startsWith(JDBC_CLICKHOUSE_PREFIX)) {
      ClickhouseCatalogSchema schema = new ClickhouseCatalogSchema(plugin.getName(),
        plugin.getDataSource(), plugin.getDialect(), plugin.getConvention());
      SchemaPlus holder = parent.add(plugin.getName(), schema);
      schema.setHolder(holder);
    } else {
      JdbcCatalogSchema schema = new JdbcCatalogSchema(plugin.getName(),
        plugin.getDataSource(), plugin.getDialect(), plugin.getConvention(),
        !plugin.getConfig().areTableNamesCaseInsensitive());
      SchemaPlus holder = parent.add(plugin.getName(), schema);
      schema.setHolder(holder);
    }
  }
}
