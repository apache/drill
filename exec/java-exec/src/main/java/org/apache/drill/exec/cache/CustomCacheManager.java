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

/**
 * Manages caches for query plans and transform results to improve query planning performance.
 * Uses Caffeine cache with configurable TTL and maximum size.
 */
public class CustomCacheManager {
  private static final Logger logger = LoggerFactory.getLogger(CustomCacheManager.class);

  private static volatile Cache<String, PhysicalPlan> queryCache;
  private static volatile Cache<CacheKey, RelNode> transformCache;
  private static volatile boolean initialized = false;

  private static final int DEFAULT_MAX_ENTRIES = 100;
  private static final int DEFAULT_TTL_MINUTES = 300;

  private CustomCacheManager() {
    // Utility class
  }

  /**
   * Lazily initializes the caches if not already initialized.
   * Uses double-checked locking for thread safety.
   */
  private static void ensureInitialized() {
    if (!initialized) {
      synchronized (CustomCacheManager.class) {
        if (!initialized) {
          initializeCaches();
          initialized = true;
        }
      }
    }
  }

  private static void initializeCaches() {
    DrillConfig config = DrillConfig.create();

    int queryMaxEntries = getConfigInt(config, "planner.query.cache.max_entries_amount", DEFAULT_MAX_ENTRIES);
    int queryTtlMinutes = getConfigInt(config, "planner.query.cache.plan_cache_ttl_minutes", DEFAULT_TTL_MINUTES);
    int transformMaxEntries = getConfigInt(config, "planner.transform.cache.max_entries_amount", DEFAULT_MAX_ENTRIES);
    int transformTtlMinutes = getConfigInt(config, "planner.transform.cache.plan_cache_ttl_minutes", DEFAULT_TTL_MINUTES);

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

    logger.debug("Query plan cache initialized with maxEntries={}, ttlMinutes={}", queryMaxEntries, queryTtlMinutes);
    logger.debug("Transform cache initialized with maxEntries={}, ttlMinutes={}", transformMaxEntries, transformTtlMinutes);
  }

  private static int getConfigInt(DrillConfig config, String path, int defaultValue) {
    if (config.hasPath(path)) {
      int value = config.getInt(path);
      logger.debug("Config {}: {}", path, value);
      return value;
    }
    logger.debug("Config {} not found, using default: {}", path, defaultValue);
    return defaultValue;
  }

  public static PhysicalPlan getQueryPlan(String sql) {
    ensureInitialized();
    return queryCache.getIfPresent(sql);
  }

  public static void putQueryPlan(String sql, PhysicalPlan plan) {
    ensureInitialized();
    queryCache.put(sql, plan);
  }

  public static RelNode getTransformedPlan(CacheKey key) {
    ensureInitialized();
    return transformCache.getIfPresent(key);
  }

  public static void putTransformedPlan(CacheKey key, RelNode plan) {
    ensureInitialized();
    transformCache.put(key, plan);
  }

  public static void logCacheStats() {
    ensureInitialized();
    if (logger.isDebugEnabled()) {
      logger.debug("Query Cache Stats: {}", queryCache.stats());
      logger.debug("Query Cache Size: {}", queryCache.estimatedSize());
      logger.debug("Transform Cache Stats: {}", transformCache.stats());
      logger.debug("Transform Cache Size: {}", transformCache.estimatedSize());
    }
  }

  /**
   * Clears both caches. Useful for testing.
   */
  public static void clearCaches() {
    if (initialized) {
      queryCache.invalidateAll();
      transformCache.invalidateAll();
    }
  }

  /**
   * Resets the cache manager, forcing reinitialization on next use.
   * Useful for testing with different configurations.
   */
  public static synchronized void reset() {
    if (initialized) {
      queryCache.invalidateAll();
      transformCache.invalidateAll();
      queryCache = null;
      transformCache = null;
      initialized = false;
    }
  }
}
