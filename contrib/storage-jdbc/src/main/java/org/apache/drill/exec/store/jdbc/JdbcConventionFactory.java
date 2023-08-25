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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.time.Duration;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class JdbcConventionFactory {
  public static final int CACHE_SIZE = 100;
  public static final Duration CACHE_TTL = Duration.ofHours(1);

  private final Cache<Pair<SqlDialect, UserCredentials>, DrillJdbcConvention> cache = CacheBuilder.newBuilder()
      .maximumSize(CACHE_SIZE)
      .expireAfterAccess(CACHE_TTL)
      .build();

  public DrillJdbcConvention getJdbcConvention(
      JdbcStoragePlugin plugin,
      SqlDialect dialect,
      UserCredentials userCredentials) {
    try {
      return cache.get(Pair.of(dialect, userCredentials), new Callable<DrillJdbcConvention>() {
        @Override
        public DrillJdbcConvention call() {
          return new DrillJdbcConvention(dialect, plugin.getName(), plugin, userCredentials);
        }
      });
    } catch (ExecutionException ex) {
      throw new DrillRuntimeException("Cannot load the requested DrillJdbcConvention", ex);
    }
  }
}
