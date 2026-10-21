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

export interface SqlUsageMap {
  tables: Map<string, number>;
  columns: Map<string, number>;
}

/**
 * Parse SQL strings to extract referenced table and column names.
 * Uses regex-based extraction (not a full SQL parser).
 */
export function parseSqlUsage(sqlStatements: string[]): SqlUsageMap {
  const tables = new Map<string, number>();
  const columns = new Map<string, number>();

  for (const sql of sqlStatements) {
    const normalized = sql.replace(/`/g, '');

    // Extract table references from FROM and JOIN clauses
    const tablePatterns = [
      /\bFROM\s+([a-zA-Z_][\w.]*)/gi,
      /\bJOIN\s+([a-zA-Z_][\w.]*)/gi,
    ];
    for (const pattern of tablePatterns) {
      let match;
      while ((match = pattern.exec(normalized)) !== null) {
        const tableName = match[1].toLowerCase();
        if (!tableName.startsWith('information_schema') && tableName !== 'dual') {
          tables.set(tableName, (tables.get(tableName) || 0) + 1);
        }
      }
    }

    // Extract column references from SELECT, WHERE, GROUP BY, ORDER BY
    const colPatterns = [
      /\bSELECT\s+([\s\S]*?)\bFROM\b/gi,
      /\bWHERE\s+([\s\S]*?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|$)/gi,
      /\bGROUP\s+BY\s+([\s\S]*?)(?:\bORDER\b|\bLIMIT\b|\bHAVING\b|$)/gi,
      /\bORDER\s+BY\s+([\s\S]*?)(?:\bLIMIT\b|$)/gi,
    ];
    for (const pattern of colPatterns) {
      let match;
      while ((match = pattern.exec(normalized)) !== null) {
        const clause = match[1];
        // Extract identifiers (skip keywords, functions, operators)
        const identifiers = clause.match(/\b([a-zA-Z_]\w*)\b/g) || [];
        const keywords = new Set([
          'AS', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'BETWEEN', 'LIKE',
          'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'ASC', 'DESC',
          'DISTINCT', 'ALL', 'TRUE', 'FALSE', 'COUNT', 'SUM', 'AVG', 'MIN',
          'MAX', 'CAST', 'COALESCE', 'NULLIF', 'IF', 'CONCAT', 'TRIM',
          'UPPER', 'LOWER', 'LENGTH', 'SUBSTRING', 'REPLACE', 'DATE_TRUNC',
          'EXTRACT', 'CURRENT_DATE', 'CURRENT_TIMESTAMP', 'SELECT', 'FROM',
          'WHERE', 'GROUP', 'BY', 'ORDER', 'LIMIT', 'HAVING', 'JOIN', 'ON',
          'LEFT', 'RIGHT', 'INNER', 'OUTER', 'FULL', 'CROSS', 'UNION',
          'INTERSECT', 'EXCEPT',
        ]);
        for (const id of identifiers) {
          if (!keywords.has(id.toUpperCase()) && !/^\d+$/.test(id)) {
            const colName = id.toLowerCase();
            columns.set(colName, (columns.get(colName) || 0) + 1);
          }
        }
      }
    }
  }

  return { tables, columns };
}
