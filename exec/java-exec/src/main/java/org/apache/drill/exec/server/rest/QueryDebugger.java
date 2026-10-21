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
package org.apache.drill.exec.server.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * Debug utility for analyzing RestQueryRunner results.
 * Provides detailed logging of what RestQueryRunner returns.
 */
public class QueryDebugger {
  private static final Logger logger = LoggerFactory.getLogger(QueryDebugger.class);

  public static void analyzeResult(String sql, RestQueryRunner.QueryResult result) {
    logger.info("========== QUERY ANALYSIS ==========");
    logger.info("SQL: {}", sql);
    logger.info("");

    // Analyze columns
    logger.info("--- COLUMNS ---");
    if (result.columns == null) {
      logger.info("columns: NULL");
    } else if (result.columns.isEmpty()) {
      logger.info("columns: EMPTY (size 0)");
    } else {
      logger.info("columns: {} items", result.columns.size());
      int index = 0;
      for (String col : result.columns) {
        logger.info("  [{}] {}", index++, col);
      }
    }
    logger.info("");

    // Analyze metadata
    logger.info("--- METADATA ---");
    if (result.metadata == null) {
      logger.info("metadata: NULL");
    } else if (result.metadata.isEmpty()) {
      logger.info("metadata: EMPTY (size 0)");
    } else {
      logger.info("metadata: {} items", result.metadata.size());
      int index = 0;
      for (String meta : result.metadata) {
        logger.info("  [{}] {}", index++, meta);
      }
    }
    logger.info("");

    // Analyze rows
    logger.info("--- ROWS ---");
    if (result.rows == null) {
      logger.info("rows: NULL");
    } else if (result.rows.isEmpty()) {
      logger.info("rows: EMPTY (size 0)");
    } else {
      logger.info("rows: {} items", result.rows.size());
      for (int i = 0; i < Math.min(3, result.rows.size()); i++) {
        Map<String, String> row = result.rows.get(i);
        logger.info("  Row {}: {} keys", i, row.size());
        for (Map.Entry<String, String> entry : row.entrySet()) {
          logger.info("    {} = {}", entry.getKey(), entry.getValue());
        }
      }
      if (result.rows.size() > 3) {
        logger.info("  ... and {} more rows", result.rows.size() - 3);
      }
    }
    logger.info("");

    // Summary
    logger.info("--- SUMMARY ---");
    logger.info("Expected columns: ? (from SQL SELECT clause)");
    logger.info("Actual columns: {}", result.columns == null ? "NULL" : result.columns.size());
    logger.info("Expected rows: > 0 (at least 1)");
    logger.info("Actual rows: {}", result.rows == null ? "NULL" : result.rows.size());
    logger.info("Query state: {}", result.queryState);
    logger.info("Query ID: {}", result.getQueryId());
    logger.info("========== END ANALYSIS ==========");
  }
}
