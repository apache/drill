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
package org.apache.drill.exec.cache;

import java.util.concurrent.TimeUnit;

import org.apache.calcite.rel.RelNode;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.handlers.DefaultSqlHandler.CacheKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class CustomCacheManager {
  private static final Logger logger = LoggerFactory.getLogger(CustomCacheManager.class);

  private static Cache<String, PhysicalPlan> queryCache;
  private static Cache<CacheKey, RelNode> transformCache;

  private static int queryMaxEntries;
  private static int queryTtlMinutes;
  private static int transformMaxEntries;
  private static int transformTtlMinutes;

  static {
    loadConfig();
  }

  private static void loadConfig() {
    DrillConfig config = DrillConfig.create();

    queryMaxEntries = getConfigInt(config, "planner.query.cache.max_entries_amount", 100);
    queryTtlMinutes = getConfigInt(config, "planner.query.cache.plan_cache_ttl_minutes", 300);
    transformMaxEntries = getConfigInt(config, "planner.transform.cache.max_entries_amount", 100);
    transformTtlMinutes = getConfigInt(config, "planner.transform.cache.plan_cache_ttl_minutes", 300);

    queryCache = Caffeine.newBuilder()
        .maximumSize(queryMaxEntries)
        .expireAfterWrite(queryTtlMinutes, TimeUnit.MINUTES)
        .recordStats()
        .build();

    transformCache = Caffeine.newBuilder()
        .maximumSize(transformMaxEntries)
        .expireAfterWrite(transformTtlMinutes, TimeUnit.MINUTES)
        .recordStats()
        .build();
  }

  private static int getConfigInt(DrillConfig config, String path, int defaultValue) {
    logger.info("Fetching: " + path);
    Boolean pathFound = config.hasPath(path);
    int value = pathFound ? config.getInt(path) : defaultValue;
    if (!pathFound) {
      logger.info("Using default value: " + defaultValue);
    } else {
      logger.info("Using found value: " + value);
    }
    return value;
  }

  public static PhysicalPlan getQueryPlan(String sql) {
    return queryCache.getIfPresent(sql);
  }

  public static void putQueryPlan(String sql, PhysicalPlan plan) {
    queryCache.put(sql, plan);
  }

  public static RelNode getTransformedPlan(CacheKey key) {
    return transformCache.getIfPresent(key);
  }

  public static void putTransformedPlan(CacheKey key, RelNode plan) {
    transformCache.put(key, plan);
  }

  public static void logCacheStats() {
    logger.info("Query Cache Stats: " + queryCache.stats());
    logger.info("Query Cache Size: " + queryCache.estimatedSize());

    logger.info("Transform Cache Stats: " + transformCache.stats());
    logger.info("Transform Cache Size: " + transformCache.estimatedSize());
  }
}
