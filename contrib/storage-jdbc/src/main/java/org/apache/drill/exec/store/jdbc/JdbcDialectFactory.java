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

import org.apache.calcite.sql.SqlDialect;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.jdbc.clickhouse.ClickhouseJdbcDialect;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.Cache;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class JdbcDialectFactory {
  public static final String JDBC_CLICKHOUSE_PREFIX = "jdbc:clickhouse";
  public static final int CACHE_SIZE = 100;

  private final Cache<SqlDialect, JdbcDialect> cache = CacheBuilder.newBuilder()
      .maximumSize(CACHE_SIZE)
      .build();

  public JdbcDialect getJdbcDialect(JdbcStoragePlugin plugin, SqlDialect dialect) {
    try {
      return cache.get(dialect, new Callable<JdbcDialect>() {
        @Override
        public JdbcDialect call() {
          return plugin.getConfig().getUrl().startsWith(JDBC_CLICKHOUSE_PREFIX)
              ? new ClickhouseJdbcDialect(plugin, dialect)
              : new DefaultJdbcDialect(plugin, dialect);
        }
      });
    } catch (ExecutionException ex) {
      throw new DrillRuntimeException("Cannot load the requested JdbcDialect", ex);
    }
  }
}
