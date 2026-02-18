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
  applySqlTransformation,
  prettifySql,
  calculateColumnStats,
  type ColumnTransformation,
} from './sqlTransformations';

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
