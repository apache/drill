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
import { describe, expect, it } from 'vitest';
import { buildViewDdl, isCreatableAsView, isValidViewName, formatSchema } from './sql';

describe('formatSchema', () => {
  it('leaves a bare plugin unquoted', () => {
    expect(formatSchema('dfs')).toBe('dfs');
  });

  it('backtick-quotes workspace parts but not the plugin', () => {
    expect(formatSchema('dfs.tmp')).toBe('dfs.`tmp`');
  });
});

/**
 * Checking only the leading keyword is sufficient, not merely heuristic: in the grammar
 * SqlInsert/SqlDelete/SqlUpdate/SqlMerge are siblings of OrderedQueryOrExpr in
 * SqlQueryOrDml (Parser.jj:2484), reachable only as the leading production, and WITH is
 * followed strictly by LeafQueryOrExpr (Parser.jj:4475) — Drill has no writable CTEs.
 */
describe('isCreatableAsView', () => {
  it('accepts a plain SELECT regardless of case', () => {
    expect(isCreatableAsView('select 1 from t')).toBe(true);
  });

  it('accepts a CTE', () => {
    expect(isCreatableAsView('WITH x AS (SELECT 1) SELECT * FROM x')).toBe(true);
  });

  it('accepts a SELECT behind a leading line comment', () => {
    expect(isCreatableAsView('-- daily rollup\nSELECT 1 FROM t')).toBe(true);
  });

  it('accepts a SELECT behind a leading block comment', () => {
    expect(isCreatableAsView('/* rollup */ SELECT 1 FROM t')).toBe(true);
  });

  it('accepts a parenthesised union', () => {
    expect(isCreatableAsView('(SELECT 1 UNION SELECT 2)')).toBe(true);
  });

  it.each(['INSERT INTO t VALUES (1)', 'UPDATE t SET a = 1', 'DELETE FROM t', 'DROP TABLE t'])(
    'rejects %s', (sql) => {
      expect(isCreatableAsView(sql)).toBe(false);
    });

  it('rejects an empty statement', () => {
    expect(isCreatableAsView('   ')).toBe(false);
  });

  /**
   * The reason the body is never scanned. A substring search for INSERT/UPDATE/DELETE
   * would reject all three of these, and every one is a perfectly valid view.
   */
  it('accepts DML keywords appearing as literals, columns and table names', () => {
    expect(isCreatableAsView("SELECT 'INSERT' AS action FROM audit_log")).toBe(true);
    expect(isCreatableAsView('SELECT update_time FROM logs')).toBe(true);
    expect(isCreatableAsView('SELECT * FROM deleted_users')).toBe(true);
  });
});

/**
 * The name is a path, not an identifier: ViewHandler.java:64 calls removeLeadingSlash on
 * it and getViewPath resolves it against the workspace location, so a slash places the
 * view in a subdirectory.
 */
describe('isValidViewName', () => {
  it('accepts a simple name', () => {
    expect(isValidViewName('sales_summary')).toBe(true);
  });

  it('accepts a slash for subdirectory placement', () => {
    expect(isValidViewName('reports/daily_sales')).toBe(true);
  });

  it('rejects a backtick, which would break out of the generated quoting', () => {
    expect(isValidViewName('a`b')).toBe(false);
  });

  it('rejects a parent-directory segment', () => {
    expect(isValidViewName('../escaped')).toBe(false);
  });

  it('rejects an empty name', () => {
    expect(isValidViewName('')).toBe(false);
  });
});

describe('buildViewDdl', () => {
  const base = { schema: 'dfs.tmp', name: 'sales', sql: 'SELECT 1 FROM t', replace: false } as const;

  it('builds a plain CREATE VIEW', () => {
    expect(buildViewDdl({ ...base, mode: 'view' }))
      .toBe('CREATE VIEW dfs.`tmp`.`sales` AS SELECT 1 FROM t');
  });

  it('builds CREATE OR REPLACE VIEW when replace is set', () => {
    expect(buildViewDdl({ ...base, mode: 'view', replace: true }))
      .toBe('CREATE OR REPLACE VIEW dfs.`tmp`.`sales` AS SELECT 1 FROM t');
  });

  it('builds a materialized view', () => {
    expect(buildViewDdl({ ...base, mode: 'materialized_view' }))
      .toBe('CREATE MATERIALIZED VIEW dfs.`tmp`.`sales` AS SELECT 1 FROM t');
  });

  it('quotes a slashed name as a single identifier', () => {
    expect(buildViewDdl({ ...base, mode: 'view', name: 'reports/daily' }))
      .toBe('CREATE VIEW dfs.`tmp`.`reports/daily` AS SELECT 1 FROM t');
  });

  /** A trailing semicolon would land mid-statement, after AS. */
  it('strips a trailing semicolon from the query', () => {
    expect(buildViewDdl({ ...base, mode: 'view', sql: 'SELECT 1 FROM t;' }))
      .toBe('CREATE VIEW dfs.`tmp`.`sales` AS SELECT 1 FROM t');
  });
});
