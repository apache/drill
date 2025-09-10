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
    transformTtlMinutes = getConfigInt(config, "planner.transform.plan_cache_ttl_minutes", 300);

    queryCache = Caffeine.newBuilder()
        .maximumSize(queryMaxEntries)
        .expireAfterWrite(queryTtlMinutes, TimeUnit.MILLISECONDS)
        .recordStats()
        .build();

    transformCache = Caffeine.newBuilder()
        .maximumSize(transformMaxEntries)
        .expireAfterWrite(transformTtlMinutes, TimeUnit.MINUTES)
        .recordStats()
        .build();
  }

  private static int getConfigInt(DrillConfig config, String path, int defaultValue) {
    return config.hasPath(path) ? config.getInt(path) : defaultValue;
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
