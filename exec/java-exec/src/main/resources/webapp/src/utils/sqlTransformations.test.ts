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
import { describe, it, expect } from 'vitest';
import {
  applyDashboardFilters,
  applySqlTransformation,
  prettifySql,
  calculateColumnStats,
  buildTimeGrainQuery,
  buildAggregationQuery,
  getEffectiveQuery,
  hasCompleteAggregationConfig,
  hasCompleteTimeGrainConfig,
  isTemporalType,
  quoteColumnName,
  type ColumnTransformation,
  type TimeGrainConfig,
  type AggregationConfig,
} from './sqlTransformations';
import type { DashboardFilter } from '../types';

// ===========================================================================
// applySqlTransformation
// ===========================================================================

describe('applySqlTransformation', () => {
  // ---- UPPERCASE ----
  describe('uppercase transformation', () => {
    const transformation: ColumnTransformation = {
      type: 'uppercase',
      columnName: 'name',
    };

    it('wraps a simple column with UPPER()', () => {
      const result = applySqlTransformation(
        'SELECT name FROM users',
        transformation
      );
      expect(result).toBe('SELECT UPPER(name) FROM users');
    });

    it('wraps a column among multiple columns', () => {
      const result = applySqlTransformation(
        'SELECT id, name, email FROM users',
        transformation
      );
      expect(result).toBe('SELECT id, UPPER(name), email FROM users');
    });

    it('preserves existing alias', () => {
      const result = applySqlTransformation(
        'SELECT name AS user_name FROM users',
        { type: 'uppercase', columnName: 'name' }
      );
      expect(result).toBe('SELECT UPPER(name) AS user_name FROM users');
    });

    it('matches column names case-insensitively', () => {
      const result = applySqlTransformation(
        'SELECT Name FROM users',
        transformation
      );
      expect(result).toBe('SELECT UPPER(Name) FROM users');
    });
  });

  // ---- LOWERCASE ----
  describe('lowercase transformation', () => {
    it('wraps a column with LOWER()', () => {
      const result = applySqlTransformation(
        'SELECT email FROM users',
        { type: 'lowercase', columnName: 'email' }
      );
      expect(result).toBe('SELECT LOWER(email) FROM users');
    });
  });

  // ---- TRIM ----
  describe('trim transformation', () => {
    it('wraps a column with TRIM()', () => {
      const result = applySqlTransformation(
        'SELECT address FROM users',
        { type: 'trim', columnName: 'address' }
      );
      expect(result).toBe('SELECT TRIM(address) FROM users');
    });
  });

  // ---- CAST ----
  describe('cast transformation', () => {
    it('wraps a column with CAST() and adds alias', () => {
      const result = applySqlTransformation(
        'SELECT price FROM products',
        { type: 'cast', columnName: 'price', targetType: 'INTEGER' }
      );
      expect(result).toBe('SELECT CAST(price AS INTEGER) AS `price` FROM products');
    });

    it('preserves existing alias over column name alias', () => {
      const result = applySqlTransformation(
        'SELECT price AS cost FROM products',
        { type: 'cast', columnName: 'price', targetType: 'DOUBLE' }
      );
      expect(result).toBe('SELECT CAST(price AS DOUBLE) AS cost FROM products');
    });

    it('returns unchanged SQL when targetType is missing', () => {
      const result = applySqlTransformation(
        'SELECT price FROM products',
        { type: 'cast', columnName: 'price' }
      );
      expect(result).toBe('SELECT price FROM products');
    });

    it('handles VARCHAR target type', () => {
      const result = applySqlTransformation(
        'SELECT id FROM products',
        { type: 'cast', columnName: 'id', targetType: 'VARCHAR' }
      );
      expect(result).toBe('SELECT CAST(id AS VARCHAR) AS `id` FROM products');
    });

    it('quotes alias for column names with special characters', () => {
      const result = applySqlTransformation(
        'SELECT `total amount` FROM orders',
        { type: 'cast', columnName: 'total amount', targetType: 'DOUBLE' }
      );
      expect(result).toBe(
        'SELECT CAST(`total amount` AS DOUBLE) AS `total amount` FROM orders'
      );
    });
  });

  // ---- TRUNCATE / SUBSTRING ----
  describe('truncate transformation', () => {
    it('wraps a column with SUBSTRING()', () => {
      const result = applySqlTransformation(
        'SELECT description FROM products',
        { type: 'truncate', columnName: 'description', length: 50 }
      );
      expect(result).toBe(
        'SELECT SUBSTRING(description, 1, 50) FROM products'
      );
    });

    it('returns unchanged SQL when length is missing', () => {
      const result = applySqlTransformation(
        'SELECT description FROM products',
        { type: 'truncate', columnName: 'description' }
      );
      expect(result).toBe('SELECT description FROM products');
    });
  });

  describe('substring transformation', () => {
    it('wraps a column with SUBSTRING()', () => {
      const result = applySqlTransformation(
        'SELECT code FROM items',
        { type: 'substring', columnName: 'code', length: 10 }
      );
      expect(result).toBe('SELECT SUBSTRING(code, 1, 10) FROM items');
    });

    it('preserves alias', () => {
      const result = applySqlTransformation(
        'SELECT code AS short_code FROM items',
        { type: 'substring', columnName: 'code', length: 5 }
      );
      expect(result).toBe(
        'SELECT SUBSTRING(code, 1, 5) AS short_code FROM items'
      );
    });
  });

  // ---- STAR QUERY EXPANSION ----
  describe('star query expansion', () => {
    it('expands * to explicit columns and transforms the target', () => {
      const result = applySqlTransformation(
        'SELECT * FROM users',
        { type: 'uppercase', columnName: 'name' },
        ['id', 'name', 'email']
      );
      expect(result).toBe('SELECT `id`, UPPER(`name`), `email` FROM users');
    });

    it('returns null if target column is not in resultColumns', () => {
      const result = applySqlTransformation(
        'SELECT * FROM users',
        { type: 'uppercase', columnName: 'missing' },
        ['id', 'name', 'email']
      );
      expect(result).toBeNull();
    });

    it('quotes columns with special characters during expansion', () => {
      const result = applySqlTransformation(
        'SELECT * FROM data',
        { type: 'uppercase', columnName: 'first name' },
        ['id', 'first name', 'last name']
      );
      expect(result).toBe(
        'SELECT `id`, UPPER(`first name`), `last name` FROM data'
      );
    });

    it('does not expand * when no resultColumns provided', () => {
      const result = applySqlTransformation(
        'SELECT * FROM users',
        { type: 'uppercase', columnName: 'name' }
      );
      // Cannot find 'name' in '*', returns null
      expect(result).toBeNull();
    });
  });

  // ---- EDGE CASES ----
  describe('edge cases', () => {
    it('returns null for non-SELECT queries', () => {
      const result = applySqlTransformation(
        'INSERT INTO users VALUES (1, "test")',
        { type: 'uppercase', columnName: 'name' }
      );
      expect(result).toBeNull();
    });

    it('returns null if target column not found', () => {
      const result = applySqlTransformation(
        'SELECT id, email FROM users',
        { type: 'uppercase', columnName: 'name' }
      );
      expect(result).toBeNull();
    });

    it('returns null for empty SQL', () => {
      const result = applySqlTransformation(
        '',
        { type: 'uppercase', columnName: 'name' }
      );
      expect(result).toBeNull();
    });

    it('handles backtick-quoted column names', () => {
      const result = applySqlTransformation(
        'SELECT `user name` FROM users',
        { type: 'uppercase', columnName: 'user name' }
      );
      expect(result).toBe('SELECT UPPER(`user name`) FROM users');
    });

    it('handles double-quoted column names', () => {
      const result = applySqlTransformation(
        'SELECT "user name" FROM users',
        { type: 'uppercase', columnName: 'user name' }
      );
      expect(result).toBe('SELECT UPPER("user name") FROM users');
    });

    it('handles table-prefixed column matching', () => {
      const result = applySqlTransformation(
        'SELECT users.name FROM users',
        { type: 'uppercase', columnName: 'name' }
      );
      expect(result).toBe('SELECT UPPER(users.name) FROM users');
    });

    it('only transforms the first matching column', () => {
      // If somehow two columns match, only the first gets transformed
      const result = applySqlTransformation(
        'SELECT name, name FROM users',
        { type: 'uppercase', columnName: 'name' }
      );
      expect(result).toBe('SELECT UPPER(name), name FROM users');
    });

    it('handles columns inside functions (no match — function wrapping is the column)', () => {
      // UPPER(name) should not match "name" directly because matchesColumn
      // checks the whole expression including function call
      const result = applySqlTransformation(
        'SELECT UPPER(name) FROM users',
        { type: 'lowercase', columnName: 'name' }
      );
      // "UPPER(name)" does not match "name" as exact match,
      // and there's no dot prefix match, so null
      expect(result).toBeNull();
    });

    it('preserves WHERE clause and other parts of the query', () => {
      const result = applySqlTransformation(
        'SELECT id, name FROM users WHERE active = true ORDER BY name',
        { type: 'uppercase', columnName: 'name' }
      );
      expect(result).toBe(
        'SELECT id, UPPER(name) FROM users WHERE active = true ORDER BY name'
      );
    });

    it('returns null for subqueries in SELECT (regex matches inner FROM)', () => {
      // The simple regex-based parser matches the first FROM, which is inside
      // the subquery — so it cannot parse this correctly. This is expected.
      const result = applySqlTransformation(
        'SELECT id, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count, name FROM users',
        { type: 'uppercase', columnName: 'name' }
      );
      expect(result).toBeNull();
    });

    it('handles SQL with different casing for SELECT and FROM', () => {
      const result = applySqlTransformation(
        'select name from users',
        { type: 'uppercase', columnName: 'name' }
      );
      expect(result).toBe('select UPPER(name) from users');
    });
  });
});

// ===========================================================================
// prettifySql
// ===========================================================================

describe('prettifySql', () => {
  it('returns empty/whitespace strings as-is', () => {
    expect(prettifySql('')).toBe('');
    expect(prettifySql('   ')).toBe('   ');
  });

  it('formats a simple SELECT query with line breaks', () => {
    const result = prettifySql('SELECT id, name FROM users WHERE active = true');
    expect(result).toContain('SELECT');
    expect(result).toContain('\nFROM');
    expect(result).toContain('\nWHERE');
  });

  it('indents column list after SELECT', () => {
    const result = prettifySql('SELECT id, name, email FROM users');
    // Columns should be on separate lines after SELECT
    expect(result).toContain('\n  id,');
    expect(result).toContain('name,');
    expect(result).toContain('email');
    // Each column should be on its own line
    const lines = result.split('\n');
    expect(lines.length).toBeGreaterThan(2);
  });

  it('formats JOIN clauses with indentation', () => {
    const result = prettifySql(
      'SELECT u.id, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id'
    );
    // JOIN keywords should appear on their own lines
    expect(result).toContain('LEFT');
    expect(result).toContain('JOIN');
    expect(result).toContain('\n  ON');
  });

  it('handles GROUP BY and ORDER BY', () => {
    const result = prettifySql(
      'SELECT department, COUNT(*) FROM employees GROUP BY department ORDER BY COUNT(*) DESC'
    );
    expect(result).toContain('\nGROUP BY');
    expect(result).toContain('\nORDER BY');
  });

  it('formats UNION queries', () => {
    const result = prettifySql(
      'SELECT id FROM users UNION ALL SELECT id FROM admins'
    );
    expect(result).toContain('\nUNION ALL');
  });

  it('handles LIMIT and OFFSET', () => {
    const result = prettifySql('SELECT * FROM users LIMIT 10 OFFSET 20');
    expect(result).toContain('\nLIMIT');
    expect(result).toContain('\nOFFSET');
  });

  it('normalizes excessive whitespace', () => {
    const result = prettifySql('SELECT   id,   name   FROM   users');
    // Should not contain multiple consecutive spaces in output
    expect(result).not.toMatch(/  {3,}/);
  });

  it('does not split commas inside function calls', () => {
    const result = prettifySql(
      'SELECT CONCAT(first_name, last_name), id FROM users'
    );
    // CONCAT(first_name, last_name) should stay on one line
    expect(result).toContain('CONCAT(first_name, last_name)');
  });

  it('handles AND/OR in WHERE clause with indentation', () => {
    const result = prettifySql(
      'SELECT * FROM users WHERE active = true AND age > 18 OR role = \'admin\''
    );
    expect(result).toContain('\n  AND');
    expect(result).toContain('\n  OR');
  });
});

// ===========================================================================
// calculateColumnStats
// ===========================================================================

describe('calculateColumnStats', () => {
  it('calculates basic stats for numeric data', () => {
    const rows = [
      { value: 10 },
      { value: 20 },
      { value: 30 },
      { value: 40 },
      { value: 50 },
    ];
    const stats = calculateColumnStats(rows, 'value');
    expect(stats.count).toBe(5);
    expect(stats.nullCount).toBe(0);
    expect(stats.distinctCount).toBe(5);
    expect(stats.min).toBe(10);
    expect(stats.max).toBe(50);
    expect(stats.avg).toBe(30);
    expect(stats.nonNullPercentage).toBe(100);
    expect(stats.uniquenessPercentage).toBe(100);
  });

  it('handles null and undefined values', () => {
    const rows = [
      { value: 10 },
      { value: null },
      { value: 20 },
      { value: undefined },
      { value: 30 },
    ];
    const stats = calculateColumnStats(rows, 'value');
    expect(stats.count).toBe(3);
    expect(stats.nullCount).toBe(2);
    expect(stats.min).toBe(10);
    expect(stats.max).toBe(30);
    expect(stats.avg).toBe(20);
  });

  it('handles all null values', () => {
    const rows = [{ value: null }, { value: null }, { value: null }];
    const stats = calculateColumnStats(rows, 'value');
    expect(stats.count).toBe(0);
    expect(stats.nullCount).toBe(3);
    expect(stats.min).toBeUndefined();
    expect(stats.max).toBeUndefined();
    expect(stats.avg).toBeUndefined();
    expect(stats.nonNullPercentage).toBe(0);
  });

  it('handles string values (no numeric stats)', () => {
    const rows = [
      { name: 'Alice' },
      { name: 'Bob' },
      { name: 'Alice' },
    ];
    const stats = calculateColumnStats(rows, 'name');
    expect(stats.count).toBe(3);
    expect(stats.nullCount).toBe(0);
    expect(stats.distinctCount).toBe(2);
    expect(stats.min).toBeUndefined();
    expect(stats.max).toBeUndefined();
    expect(stats.avg).toBeUndefined();
  });

  it('calculates uniqueness percentage correctly', () => {
    const rows = [
      { status: 'active' },
      { status: 'active' },
      { status: 'inactive' },
      { status: 'active' },
    ];
    const stats = calculateColumnStats(rows, 'status');
    expect(stats.distinctCount).toBe(2);
    expect(stats.uniquenessPercentage).toBe(50);
  });

  it('handles empty dataset', () => {
    const stats = calculateColumnStats([], 'value');
    expect(stats.count).toBe(0);
    expect(stats.nullCount).toBe(0);
    expect(stats.distinctCount).toBe(0);
    expect(stats.nonNullPercentage).toBe(0);
  });

  it('handles mixed numeric and string values', () => {
    const rows = [
      { value: '10' },
      { value: '20' },
      { value: 'not-a-number' },
      { value: '30' },
    ];
    const stats = calculateColumnStats(rows, 'value');
    expect(stats.count).toBe(4);
    // Numeric stats only from parseable values
    expect(stats.min).toBe(10);
    expect(stats.max).toBe(30);
    expect(stats.avg).toBe(20);
  });

  it('handles column that does not exist in rows', () => {
    const rows = [{ name: 'Alice' }, { name: 'Bob' }];
    const stats = calculateColumnStats(rows, 'nonexistent');
    expect(stats.count).toBe(0);
    expect(stats.nullCount).toBe(2);
  });

  it('handles single-row dataset', () => {
    const rows = [{ value: 42 }];
    const stats = calculateColumnStats(rows, 'value');
    expect(stats.count).toBe(1);
    expect(stats.min).toBe(42);
    expect(stats.max).toBe(42);
    expect(stats.avg).toBe(42);
    expect(stats.uniquenessPercentage).toBe(100);
  });

  it('handles duplicate numeric values', () => {
    const rows = [
      { value: 5 },
      { value: 5 },
      { value: 5 },
    ];
    const stats = calculateColumnStats(rows, 'value');
    expect(stats.distinctCount).toBe(1);
    expect(stats.min).toBe(5);
    expect(stats.max).toBe(5);
    expect(stats.avg).toBe(5);
  });
});

// ===========================================================================
// quoteColumnName
// ===========================================================================

describe('quoteColumnName', () => {
  it('wraps a simple name with backticks', () => {
    expect(quoteColumnName('date')).toBe('`date`');
  });

  it('escapes existing backticks', () => {
    expect(quoteColumnName('col`name')).toBe('`col``name`');
  });
});

// ===========================================================================
// isTemporalType
// ===========================================================================

describe('isTemporalType', () => {
  it('returns true for DATE type', () => {
    expect(isTemporalType('DATE')).toBe(true);
  });

  it('returns true for TIMESTAMP type', () => {
    expect(isTemporalType('TIMESTAMP')).toBe(true);
  });

  it('returns true for TIME type', () => {
    expect(isTemporalType('TIME')).toBe(true);
  });

  it('is case-insensitive', () => {
    expect(isTemporalType('timestamp')).toBe(true);
    expect(isTemporalType('Date')).toBe(true);
  });

  it('returns false for non-temporal types', () => {
    expect(isTemporalType('VARCHAR')).toBe(false);
    expect(isTemporalType('INTEGER')).toBe(false);
    expect(isTemporalType('DOUBLE')).toBe(false);
  });
});

// ===========================================================================
// hasCompleteAggregationConfig
// ===========================================================================

describe('hasCompleteAggregationConfig', () => {
  it('returns false when chartOptions is undefined', () => {
    expect(hasCompleteAggregationConfig(undefined, ['hosts'])).toBe(false);
  });

  it('returns false when metrics is empty', () => {
    expect(hasCompleteAggregationConfig({ metricAggregations: { hosts: 'SUM' } }, [])).toBe(false);
  });

  it('returns false when no metricAggregations', () => {
    expect(hasCompleteAggregationConfig({}, ['hosts'])).toBe(false);
  });

  it('returns true when at least one metric has an aggregation', () => {
    expect(hasCompleteAggregationConfig(
      { metricAggregations: { hosts: 'SUM' } },
      ['hosts', 'orgs']
    )).toBe(true);
  });

  it('returns false when no metric has an aggregation', () => {
    expect(hasCompleteAggregationConfig(
      { metricAggregations: { other: 'SUM' } },
      ['hosts']
    )).toBe(false);
  });
});

// ===========================================================================
// hasCompleteTimeGrainConfig
// ===========================================================================

describe('hasCompleteTimeGrainConfig', () => {
  it('returns false when no timeGrain is set', () => {
    expect(hasCompleteTimeGrainConfig({}, ['hosts'])).toBe(false);
  });

  it('returns false when metrics is empty', () => {
    expect(hasCompleteTimeGrainConfig(
      { timeGrain: 'MONTH', metricAggregations: { hosts: 'SUM' } },
      []
    )).toBe(false);
  });

  it('returns true when all metrics have aggregations', () => {
    expect(hasCompleteTimeGrainConfig(
      { timeGrain: 'MONTH', metricAggregations: { hosts: 'SUM', orgs: 'AVG' } },
      ['hosts', 'orgs']
    )).toBe(true);
  });

  it('returns false when not all metrics have aggregations', () => {
    expect(hasCompleteTimeGrainConfig(
      { timeGrain: 'MONTH', metricAggregations: { hosts: 'SUM' } },
      ['hosts', 'orgs']
    )).toBe(false);
  });
});

// ===========================================================================
// buildTimeGrainQuery
// ===========================================================================

describe('buildTimeGrainQuery', () => {
  const baseSql = 'SELECT date, hosts, orgs FROM metrics';

  it('builds a time grain query with DATE_TRUNC and GROUP BY', () => {
    const config: TimeGrainConfig = {
      grain: 'MONTH',
      temporalColumn: 'date',
      metricAggregations: { hosts: 'SUM' },
    };
    const result = buildTimeGrainQuery(baseSql, config);
    expect(result).toContain("DATE_TRUNC('MONTH', _t.`date`) AS `date`");
    expect(result).toContain('SUM(_t.`hosts`) AS `hosts`');
    expect(result).toContain('GROUP BY `date`');
    expect(result).toContain('ORDER BY 1');
    expect(result).toContain(`FROM (${baseSql}) AS _t`);
  });

  it('includes dimensions in SELECT and GROUP BY', () => {
    const config: TimeGrainConfig = {
      grain: 'YEAR',
      temporalColumn: 'date',
      metricAggregations: { hosts: 'SUM' },
      dimensions: ['industry'],
    };
    const result = buildTimeGrainQuery(baseSql, config);
    expect(result).toContain('_t.`industry`');
    expect(result).toContain('GROUP BY `date`, _t.`industry`');
  });

  it('handles multiple metrics with different aggregations', () => {
    const config: TimeGrainConfig = {
      grain: 'DAY',
      temporalColumn: 'date',
      metricAggregations: { hosts: 'SUM', orgs: 'AVG' },
    };
    const result = buildTimeGrainQuery(baseSql, config);
    expect(result).toContain('SUM(_t.`hosts`) AS `hosts`');
    expect(result).toContain('AVG(_t.`orgs`) AS `orgs`');
  });

  it('returns null when grain is missing', () => {
    const config: TimeGrainConfig = {
      grain: '' as never,
      temporalColumn: 'date',
      metricAggregations: { hosts: 'SUM' },
    };
    expect(buildTimeGrainQuery(baseSql, config)).toBeNull();
  });

  it('returns null when no metric aggregations', () => {
    const config: TimeGrainConfig = {
      grain: 'MONTH',
      temporalColumn: 'date',
      metricAggregations: {},
    };
    expect(buildTimeGrainQuery(baseSql, config)).toBeNull();
  });

  it('strips trailing semicolons from the inner query', () => {
    const result = buildTimeGrainQuery('SELECT date, hosts FROM t;', {
      grain: 'MONTH',
      temporalColumn: 'date',
      metricAggregations: { hosts: 'SUM' },
    });
    expect(result).toContain('FROM (SELECT date, hosts FROM t) AS _t');
    expect(result).not.toContain(';');
  });
});

// ===========================================================================
// buildAggregationQuery
// ===========================================================================

describe('buildAggregationQuery', () => {
  const baseSql = 'SELECT category, amount FROM sales';

  it('builds an aggregation query with GROUP BY', () => {
    const config: AggregationConfig = {
      metricAggregations: { amount: 'SUM' },
      groupByColumns: ['category'],
    };
    const result = buildAggregationQuery(baseSql, config);
    expect(result).toContain('_t.`category`');
    expect(result).toContain('SUM(_t.`amount`) AS `amount`');
    expect(result).toContain('GROUP BY _t.`category`');
    expect(result).toContain('ORDER BY 1');
  });

  it('returns null when no metric aggregations', () => {
    const config: AggregationConfig = {
      metricAggregations: {},
      groupByColumns: ['category'],
    };
    expect(buildAggregationQuery(baseSql, config)).toBeNull();
  });

  it('handles no GROUP BY columns (total aggregation)', () => {
    const config: AggregationConfig = {
      metricAggregations: { amount: 'SUM' },
      groupByColumns: [],
    };
    const result = buildAggregationQuery(baseSql, config);
    expect(result).toContain('SUM(_t.`amount`) AS `amount`');
    expect(result).not.toContain('GROUP BY');
    expect(result).toContain('ORDER BY 1');
  });

  it('handles multiple group-by columns', () => {
    const config: AggregationConfig = {
      metricAggregations: { amount: 'AVG' },
      groupByColumns: ['category', 'region'],
    };
    const result = buildAggregationQuery(baseSql, config);
    expect(result).toContain('GROUP BY _t.`category`, _t.`region`');
  });
});

// ===========================================================================
// getEffectiveQuery
// ===========================================================================

describe('getEffectiveQuery', () => {
  const baseSql = 'SELECT date, industry, hosts, orgs FROM metrics';

  it('returns original SQL when no config is set', async () => {
    const result = await getEffectiveQuery(baseSql, {});
    expect(result).toBe(baseSql);
  });

  it('returns original SQL when only xAxis is set (no metrics)', async () => {
    const result = await getEffectiveQuery(baseSql, { xAxis: 'date' });
    expect(result).toBe(baseSql);
  });

  it('returns original SQL when metrics are set but no aggregation and no time grain', async () => {
    const result = await getEffectiveQuery(baseSql, {
      xAxis: 'date',
      metrics: ['hosts'],
    });
    expect(result).toBe(baseSql);
  });

  it('defaults to SUM when time grain is set without explicit aggregation', async () => {
    const result = await getEffectiveQuery(baseSql, {
      xAxis: 'date',
      metrics: ['hosts'],
      chartOptions: { timeGrain: 'MONTH' },
    });
    expect(result).toContain("DATE_TRUNC('MONTH', _t.`date`)");
    expect(result).toContain('SUM(_t.`hosts`) AS `hosts`');
    expect(result).toContain('GROUP BY');
  });

  it('uses explicit aggregation when provided with time grain', async () => {
    const result = await getEffectiveQuery(baseSql, {
      xAxis: 'date',
      metrics: ['hosts'],
      chartOptions: {
        timeGrain: 'YEAR',
        metricAggregations: { hosts: 'AVG' },
      },
    });
    expect(result).toContain("DATE_TRUNC('YEAR', _t.`date`)");
    expect(result).toContain('AVG(_t.`hosts`) AS `hosts`');
  });

  it('applies aggregation-only (no time grain) when aggregation is configured', async () => {
    const result = await getEffectiveQuery(baseSql, {
      xAxis: 'date',
      metrics: ['hosts'],
      chartOptions: {
        metricAggregations: { hosts: 'SUM' },
      },
    });
    expect(result).not.toContain('DATE_TRUNC');
    expect(result).toContain('SUM(_t.`hosts`) AS `hosts`');
    expect(result).toContain('GROUP BY _t.`date`');
  });

  it('includes dimensions in GROUP BY', async () => {
    const result = await getEffectiveQuery(baseSql, {
      xAxis: 'date',
      metrics: ['hosts'],
      dimensions: ['industry'],
      chartOptions: { timeGrain: 'MONTH' },
    });
    expect(result).toContain('_t.`industry`');
    expect(result).toContain('GROUP BY `date`, _t.`industry`');
  });

  it('returns original SQL when time grain is set but metrics are empty', async () => {
    const result = await getEffectiveQuery(baseSql, {
      xAxis: 'date',
      metrics: [],
      chartOptions: { timeGrain: 'MONTH' },
    });
    expect(result).toBe(baseSql);
  });

  it('defaults SUM for multiple metrics when time grain is set', async () => {
    const result = await getEffectiveQuery(baseSql, {
      xAxis: 'date',
      metrics: ['hosts', 'orgs'],
      chartOptions: { timeGrain: 'QUARTER' },
    });
    expect(result).toContain("DATE_TRUNC('QUARTER', _t.`date`)");
    expect(result).toContain('SUM(_t.`hosts`) AS `hosts`');
    expect(result).toContain('SUM(_t.`orgs`) AS `orgs`');
  });

  it('applies DATE_TRUNC when explicit aggregation AND time grain AND dimensions are all set', async () => {
    // Exact user scenario: explicit SUM + MONTH time grain + industry dimension
    const userSql = `SELECT to_date(\`date\`) AS \`date\`, \`industry\`, \`botfam\`, CAST(hosts as INTEGER) AS hosts, CAST(orgs as INTEGER) AS orgs FROM dfs.\`demo\`.\`dailybots.csvh\` ORDER BY \`date\` ASC`;
    const result = await getEffectiveQuery(userSql, {
      xAxis: 'date',
      metrics: ['hosts'],
      dimensions: ['industry'],
      chartOptions: {
        timeGrain: 'MONTH',
        metricAggregations: { hosts: 'SUM' },
      },
    });
    expect(result).toContain("DATE_TRUNC('MONTH', _t.`date`)");
    expect(result).toContain('SUM(_t.`hosts`) AS `hosts`');
    expect(result).toContain('_t.`industry`');
    expect(result).toContain('GROUP BY');
    expect(result).not.toEqual(userSql);
  });
});

// ===========================================================================
// applyDashboardFilters — functional tests
// ===========================================================================

describe('applyDashboardFilters', () => {
  const baseSql = 'SELECT region, amount FROM sales';

  /** Helper to create a minimal text filter. */
  function textFilter(column: string, value: string, extra?: Partial<DashboardFilter>): DashboardFilter {
    return { id: '1', column, value, ...extra };
  }

  /** Helper to create a temporal filter. */
  function temporalFilter(
    column: string,
    rangeStart: string,
    rangeEnd: string,
    extra?: Partial<DashboardFilter>,
  ): DashboardFilter {
    return { id: '2', column, value: rangeStart, isTemporal: true, rangeStart, rangeEnd, ...extra };
  }

  /** Helper to create a numeric filter. */
  function numericFilter(
    column: string,
    value: string,
    numericOp: DashboardFilter['numericOp'],
    extra?: Partial<DashboardFilter>,
  ): DashboardFilter {
    return { id: '3', column, value, isNumeric: true, numericOp, ...extra };
  }

  // ---- Basic functional tests ----

  describe('basic functionality', () => {
    it('returns original SQL for empty filter array', () => {
      expect(applyDashboardFilters(baseSql, [])).toBe(baseSql);
    });

    it('returns original SQL for null/undefined filters', () => {
      expect(applyDashboardFilters(baseSql, null as never)).toBe(baseSql);
      expect(applyDashboardFilters(baseSql, undefined as never)).toBe(baseSql);
    });

    it('adds a WHERE clause for a simple text filter', () => {
      const result = applyDashboardFilters(baseSql, [textFilter('region', 'North America')]);
      expect(result).toBe(
        "SELECT region, amount FROM sales WHERE CAST(`region` AS VARCHAR) = 'North America'"
      );
    });

    it('appends AND to an existing WHERE clause', () => {
      const sql = "SELECT region, amount FROM sales WHERE amount > 100";
      const result = applyDashboardFilters(sql, [textFilter('region', 'Europe')]);
      expect(result).toContain("AND CAST(`region` AS VARCHAR) = 'Europe'");
    });

    it('inserts WHERE before GROUP BY', () => {
      const sql = 'SELECT region, SUM(amount) FROM sales GROUP BY region';
      const result = applyDashboardFilters(sql, [textFilter('region', 'Asia')]);
      expect(result).toContain("WHERE CAST(`region` AS VARCHAR) = 'Asia' GROUP BY");
    });

    it('combines multiple text filters with AND', () => {
      const result = applyDashboardFilters(baseSql, [
        textFilter('region', 'US'),
        { id: '2', column: 'status', value: 'active' },
      ]);
      expect(result).toContain("CAST(`region` AS VARCHAR) = 'US'");
      expect(result).toContain("AND");
      expect(result).toContain("CAST(`status` AS VARCHAR) = 'active'");
    });

    it('generates IS NULL for null values', () => {
      const result = applyDashboardFilters(baseSql, [textFilter('region', 'null')]);
      expect(result).toContain('`region` IS NULL');
    });

    it('escapes single quotes in text values', () => {
      const result = applyDashboardFilters(baseSql, [textFilter('region', "O'Brien")]);
      expect(result).toContain("'O''Brien'");
    });

    it('strips trailing semicolons', () => {
      const result = applyDashboardFilters('SELECT * FROM t;', [textFilter('col', 'val')]);
      expect(result).not.toContain(';');
    });
  });

  // ---- Temporal filter tests ----

  describe('temporal filters', () => {
    it('generates DATE equality for same start/end', () => {
      const result = applyDashboardFilters(baseSql, [
        temporalFilter('date', '2024-01-15', '2024-01-15'),
      ]);
      expect(result).toContain("CAST(`date` AS DATE) = DATE '2024-01-15'");
    });

    it('generates BETWEEN for different start/end', () => {
      const result = applyDashboardFilters(baseSql, [
        temporalFilter('date', '2024-01-01', '2024-12-31'),
      ]);
      expect(result).toContain("CAST(`date` AS DATE) BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'");
    });
  });

  // ---- Numeric filter tests ----

  describe('numeric filters', () => {
    it('generates equality for = operator', () => {
      const result = applyDashboardFilters(baseSql, [numericFilter('amount', '100', '=')]);
      expect(result).toContain('`amount` = 100');
    });

    it('generates <> for != operator', () => {
      const result = applyDashboardFilters(baseSql, [numericFilter('amount', '0', '!=')]);
      expect(result).toContain('`amount` <> 0');
    });

    it('generates comparison operators correctly', () => {
      for (const op of ['>', '>=', '<', '<='] as const) {
        const result = applyDashboardFilters(baseSql, [numericFilter('amount', '50', op)]);
        expect(result).toContain(`\`amount\` ${op} 50`);
      }
    });

    it('generates BETWEEN for between operator', () => {
      const result = applyDashboardFilters(baseSql, [
        numericFilter('amount', '10', 'between', { numericEnd: '100' }),
      ]);
      expect(result).toContain('`amount` BETWEEN 10 AND 100');
    });

    it('handles negative numbers', () => {
      const result = applyDashboardFilters(baseSql, [numericFilter('amount', '-50', '>=')]);
      expect(result).toContain('`amount` >= -50');
    });

    it('handles decimal numbers', () => {
      const result = applyDashboardFilters(baseSql, [numericFilter('amount', '99.99', '=')]);
      expect(result).toContain('`amount` = 99.99');
    });
  });
});

// ===========================================================================
// applyDashboardFilters — security / SQL injection tests
// ===========================================================================

describe('applyDashboardFilters — SQL injection prevention', () => {
  const baseSql = 'SELECT region, amount FROM sales';

  // ---- Column name injection ----

  describe('column name validation', () => {
    it('rejects column names containing SQL keywords and semicolons', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: "region; DROP TABLE sales; --",
        value: 'x',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
    });

    it('rejects column names containing single quotes', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: "region' OR '1'='1",
        value: 'x',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
    });

    it('rejects column names containing parentheses', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: "region) UNION SELECT * FROM passwords --",
        value: 'x',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
    });

    it('rejects column names containing backtick escape sequences', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: "col` = 1 OR `x",
        value: 'x',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
    });

    it('rejects empty column names', () => {
      const malicious: DashboardFilter = { id: '1', column: '', value: 'x' };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
    });

    it('allows column names with letters, numbers, underscores', () => {
      const safe: DashboardFilter = { id: '1', column: 'user_name_2', value: 'Alice' };
      const result = applyDashboardFilters(baseSql, [safe]);
      expect(result).toContain("`user_name_2`");
      expect(result).not.toBe(baseSql);
    });

    it('allows column names with spaces', () => {
      const safe: DashboardFilter = { id: '1', column: 'first name', value: 'Alice' };
      const result = applyDashboardFilters(baseSql, [safe]);
      expect(result).toContain("`first name`");
      expect(result).not.toBe(baseSql);
    });

    it('allows column names with dots (table.column)', () => {
      const safe: DashboardFilter = { id: '1', column: 'users.name', value: 'Alice' };
      const result = applyDashboardFilters(baseSql, [safe]);
      expect(result).toContain("`users.name`");
      expect(result).not.toBe(baseSql);
    });

    it('allows column names with hyphens', () => {
      const safe: DashboardFilter = { id: '1', column: 'first-name', value: 'Alice' };
      const result = applyDashboardFilters(baseSql, [safe]);
      expect(result).toContain("`first-name`");
      expect(result).not.toBe(baseSql);
    });
  });

  // ---- Numeric value injection ----

  describe('numeric value validation', () => {
    it('rejects SQL injection via numeric value', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'amount',
        value: '0; DROP TABLE sales --',
        isNumeric: true,
        numericOp: '=',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
      expect(result).not.toContain('DROP');
    });

    it('rejects SQL injection via numeric value with OR tautology', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'amount',
        value: '1 OR 1=1',
        isNumeric: true,
        numericOp: '=',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
    });

    it('rejects non-numeric strings in numeric filter value', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'amount',
        value: 'abc',
        isNumeric: true,
        numericOp: '>',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
    });

    it('rejects empty string in numeric filter value', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'amount',
        value: '',
        isNumeric: true,
        numericOp: '=',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
    });

    it('rejects Infinity in numeric filter value', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'amount',
        value: 'Infinity',
        isNumeric: true,
        numericOp: '=',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
    });

    it('rejects NaN in numeric filter value', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'amount',
        value: 'NaN',
        isNumeric: true,
        numericOp: '=',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
    });

    it('rejects SQL injection via numericEnd (between upper bound)', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'amount',
        value: '10',
        isNumeric: true,
        numericOp: 'between',
        numericEnd: '100; DROP TABLE sales',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
      expect(result).not.toContain('DROP');
    });

    it('accepts valid integers', () => {
      const safe: DashboardFilter = {
        id: '1', column: 'amount', value: '42', isNumeric: true, numericOp: '=',
      };
      const result = applyDashboardFilters(baseSql, [safe]);
      expect(result).toContain('`amount` = 42');
    });

    it('accepts valid negative decimals', () => {
      const safe: DashboardFilter = {
        id: '1', column: 'amount', value: '-3.14', isNumeric: true, numericOp: '>=',
      };
      const result = applyDashboardFilters(baseSql, [safe]);
      expect(result).toContain('`amount` >= -3.14');
    });

    it('accepts scientific notation', () => {
      const safe: DashboardFilter = {
        id: '1', column: 'amount', value: '1e3', isNumeric: true, numericOp: '<',
      };
      const result = applyDashboardFilters(baseSql, [safe]);
      expect(result).toContain('`amount` < 1000');
    });
  });

  // ---- Numeric operator injection ----

  describe('numeric operator validation', () => {
    it('rejects arbitrary SQL injected as operator', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'amount',
        value: '1',
        isNumeric: true,
        numericOp: '= 1 OR 1=1 --' as never,
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
      expect(result).not.toContain('OR');
    });

    it('rejects unknown operator strings', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'amount',
        value: '1',
        isNumeric: true,
        numericOp: 'LIKE' as never,
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
    });

    it('accepts all valid operators', () => {
      const ops: Array<DashboardFilter['numericOp']> = ['=', '!=', '>', '>=', '<', '<=', 'between'];
      for (const op of ops) {
        const f: DashboardFilter = {
          id: '1',
          column: 'amount',
          value: '50',
          isNumeric: true,
          numericOp: op,
          numericEnd: op === 'between' ? '100' : undefined,
        };
        const result = applyDashboardFilters(baseSql, [f]);
        expect(result).not.toBe(baseSql);
      }
    });
  });

  // ---- Temporal value injection ----

  describe('temporal value validation', () => {
    it('rejects SQL injection via rangeStart', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'date',
        value: '2024-01-01',
        isTemporal: true,
        rangeStart: "2024-01-01' OR '1'='1",
        rangeEnd: '2024-12-31',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
      expect(result).not.toContain('OR');
    });

    it('rejects SQL injection via rangeEnd', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'date',
        value: '2024-01-01',
        isTemporal: true,
        rangeStart: '2024-01-01',
        rangeEnd: "2024-12-31'; DROP TABLE sales --",
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
      expect(result).not.toContain('DROP');
    });

    it('rejects non-date strings in temporal rangeStart', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'date',
        value: 'x',
        isTemporal: true,
        rangeStart: 'not-a-date',
        rangeEnd: '2024-12-31',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      expect(result).toBe(baseSql);
    });

    it('rejects dates with extra characters appended', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'date',
        value: '2024-01-01',
        isTemporal: true,
        rangeStart: '2024-01-01 00:00:00',
        rangeEnd: '2024-12-31',
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      // The regex requires exactly YYYY-MM-DD, not datetime strings
      expect(result).toBe(baseSql);
    });

    it('accepts valid ISO dates', () => {
      const safe: DashboardFilter = {
        id: '1',
        column: 'date',
        value: '2024-06-15',
        isTemporal: true,
        rangeStart: '2024-01-01',
        rangeEnd: '2024-12-31',
      };
      const result = applyDashboardFilters(baseSql, [safe]);
      expect(result).toContain("DATE '2024-01-01'");
      expect(result).toContain("DATE '2024-12-31'");
    });
  });

  // ---- Text value injection (defense in depth) ----

  describe('text value escaping', () => {
    it('escapes single quotes to prevent breakout', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'region',
        value: "'; DROP TABLE sales; --",
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      // The leading single quote in the value must be doubled so the SQL
      // string literal is never terminated prematurely.  The "DROP TABLE"
      // text remains inside the literal — it is data, not a SQL command.
      expect(result).toBe(
        "SELECT region, amount FROM sales WHERE CAST(`region` AS VARCHAR) = '''; DROP TABLE sales; --'"
      );
      // Crucially, the value opens with a doubled quote ('') keeping the
      // literal intact.  There is no unquoted semicolon that could start
      // a second statement.
    });

    it('escapes UNION-based injection in text value', () => {
      const malicious: DashboardFilter = {
        id: '1',
        column: 'region',
        value: "x' UNION SELECT password FROM users --",
      };
      const result = applyDashboardFilters(baseSql, [malicious]);
      // Value quotes are doubled — the UNION stays inside the literal
      expect(result).toContain("x'' UNION SELECT password FROM users --'");
    });

    it('handles values with multiple single quotes', () => {
      const filter: DashboardFilter = {
        id: '1',
        column: 'name',
        value: "It's a 'test' value",
      };
      const result = applyDashboardFilters(baseSql, [filter]);
      expect(result).toContain("'It''s a ''test'' value'");
    });
  });

  // ---- Mixed valid and malicious filters ----

  describe('mixed valid and malicious filters', () => {
    it('applies only the valid filters and skips the malicious ones', () => {
      const filters: DashboardFilter[] = [
        // Valid text filter
        { id: '1', column: 'region', value: 'US' },
        // Malicious column name — should be skipped
        { id: '2', column: "x'; DROP TABLE t; --", value: 'y' },
        // Malicious numeric value — should be skipped
        { id: '3', column: 'amount', value: '0 OR 1=1', isNumeric: true, numericOp: '=' },
        // Valid numeric filter
        { id: '4', column: 'amount', value: '100', isNumeric: true, numericOp: '>=' },
      ];
      const result = applyDashboardFilters(baseSql, filters);
      expect(result).toContain("CAST(`region` AS VARCHAR) = 'US'");
      expect(result).toContain('`amount` >= 100');
      expect(result).not.toContain('DROP');
      expect(result).not.toContain('OR 1=1');
      // Should have exactly 2 conditions joined by AND
      const andCount = (result.match(/ AND /g) || []).length;
      expect(andCount).toBe(1);
    });

    it('returns original SQL when ALL filters are malicious', () => {
      const filters: DashboardFilter[] = [
        { id: '1', column: "'; DROP TABLE t; --", value: 'x' },
        { id: '2', column: 'amount', value: 'not-a-number', isNumeric: true, numericOp: '=' },
        { id: '3', column: 'date', value: 'd', isTemporal: true, rangeStart: 'bad', rangeEnd: 'bad' },
      ];
      const result = applyDashboardFilters(baseSql, filters);
      expect(result).toBe(baseSql);
    });
  });

  // ---- URL-crafted payloads (simulating tampered search params) ----

  describe('URL-crafted attack payloads', () => {
    it('blocks stacked query injection via numeric value', () => {
      const payload: DashboardFilter = {
        id: '1',
        column: 'id',
        value: '1; DELETE FROM users',
        isNumeric: true,
        numericOp: '=',
      };
      const result = applyDashboardFilters(baseSql, [payload]);
      expect(result).toBe(baseSql);
    });

    it('blocks UNION SELECT via temporal rangeEnd', () => {
      const payload: DashboardFilter = {
        id: '1',
        column: 'created_at',
        value: '2024-01-01',
        isTemporal: true,
        rangeStart: '2024-01-01',
        rangeEnd: "2024-12-31' UNION SELECT * FROM secrets --",
      };
      const result = applyDashboardFilters(baseSql, [payload]);
      expect(result).toBe(baseSql);
    });

    it('blocks operator injection via numericOp field', () => {
      const payload: DashboardFilter = {
        id: '1',
        column: 'amount',
        value: '1',
        isNumeric: true,
        numericOp: "> 0 UNION SELECT * FROM passwords --" as never,
      };
      const result = applyDashboardFilters(baseSql, [payload]);
      expect(result).toBe(baseSql);
    });

    it('blocks column injection with comment syntax', () => {
      const payload: DashboardFilter = {
        id: '1',
        column: 'col/**/OR/**/1=1--',
        value: 'x',
      };
      const result = applyDashboardFilters(baseSql, [payload]);
      expect(result).toBe(baseSql);
    });

    it('blocks column injection with double-dash comment', () => {
      const payload: DashboardFilter = {
        id: '1',
        column: 'col -- ',
        value: 'x',
      };
      const result = applyDashboardFilters(baseSql, [payload]);
      // The regex allows word chars, spaces, dots, hyphens. '--' has hyphens
      // followed by space which technically matches [\w\s.\-]+. However the
      // backtick quoting neutralizes the comment syntax inside a column name.
      // The important thing is no unquoted SQL can be injected.
      expect(result).toContain('`col -- `');
    });
  });
});
