# Save as View / Materialized View — Implementation Plan (Phases 1-2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a user save the SQL Lab editor's query as a Drill view or materialized view, and have it appear correctly in the schema tree.

**Architecture:** Drill already has the DDL (`CREATE [OR REPLACE] [MATERIALIZED] VIEW`) and `/query.json` executes arbitrary SQL, so no new execution machinery. One new read-only REST endpoint lists the schemas where a view can legally be created; the frontend builds the DDL and sends it through the same `executeQuery` path the Run button uses.

**Tech Stack:** Java 8+ / JAX-RS / Jackson (backend, JUnit 4 + JUnit 5 assertions, OkHttp for cluster tests); React 18 / TypeScript / antd / react-query / Vitest + @testing-library/react (frontend).

**Spec:** [`docs/dev/specs/2026-07-15-save-as-view-design.md`](../specs/2026-07-15-save-as-view-design.md)

## Scope

Phases 1-2 of the spec's five. Ships a working feature: save as view/MV, correctly rendered in the tree. **Out of scope** (each gets its own plan): view descriptions store, MV refresh action, nested-view rendering in the file browser.

## Global Constraints

- Run `mvn -o checkstyle:check -pl exec/java-exec` after any change under `exec/java-exec`. Zero violations required.
- Every `if` statement needs braces. No unused imports.
- Every new source file (Java, TS, TSX) needs an Apache 2.0 license header. Copy it verbatim from any neighbouring file.
- Do **not** add Claude as a git co-author. Commit messages in the imperative.
- Frontend commands run from `exec/java-exec/src/main/resources/webapp`.
- `npm run lint` must pass with **zero warnings** (`--max-warnings 0`); CI fails otherwise.
- Never pass `autoLimitRowCount` when executing DDL — it makes Drill cancel the query once N rows arrive, which would truncate a materialized view's data.

## File Structure

| File | Responsibility |
|---|---|
| `MetadataResources.java` (modify) | New `/view-targets` endpoint + `isViewTarget` predicate |
| `TestViewTargets.java` (create) | Unit test for the predicate — no cluster needed |
| `TestMetadataResources.java` (modify) | Cluster test hitting the live endpoint |
| `src/utils/sql.ts` (create) | Pure SQL helpers: `formatSchema`, `isCreatableAsView`, `isValidViewName`, `buildViewDdl` |
| `src/utils/sql.test.ts` (create) | Tests for the above |
| `src/utils/createView.ts` (create) | Orchestration: run the DDL, then link the project; returns the partial outcome |
| `src/utils/createView.test.ts` (create) | Tests for the above |
| `src/api/metadata.ts` (modify) | `getViewTargets()`; import `formatSchema` from `utils/sql` instead of its private copy |
| `src/types/index.ts` (modify) | `TableInfo.type` gains `'MATERIALIZED VIEW'`; `DatasetRef.type` gains `'view'`/`'materialized_view'` |
| `SaveQueryDialog.tsx` (modify) | Mode radio, view fields, DDL execution, project linking |
| `TreeNodeBuilder.tsx` (modify) | Icon + tooltip branch on `TableInfo.type` |
| `ProjectDetailPage.tsx` (modify) | Real label cases for the new dataset types |
| `SchemaExplorer.tsx` (modify) | `refreshSchema` prop so a create can invalidate one schema |
| `SqlLabPage.tsx` (modify) | Wire dialog → explorer refresh |

The logic lives outside the components, so it can be tested without rendering anything:
`utils/sql.ts` is pure (quoting, guard, DDL shape) and has **no imports from `api/`**, so
its tests need no mocking at all; `utils/createView.ts` holds the orchestration and
partial-failure rule, and mocks only `api/queries` and `api/projects`. The dialog is left
with form state and messages.

---

### Task 1: `/view-targets` endpoint

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/MetadataResources.java`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestViewTargets.java` (create)
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestMetadataResources.java` (modify)

**Interfaces:**
- Produces: `GET /api/v1/metadata/view-targets` → `{"schemas":[{"name":"dfs.tmp","type":"schema","plugin":"dfs","browsable":true}]}`, reusing the existing `SchemaInfo`/`SchemasResponse` DTOs.
- Produces: `static boolean MetadataResources.isViewTarget(StoragePluginConfig config, String isMutable)` — package-private, for testing.

**Background the implementer needs:** Only `WorkspaceSchema` overrides `createView` (`WorkspaceSchemaFactory.java:329`) and `createMaterializedView` (`:384`); `AbstractSchema` throws for both (`:160`, `:185`). `IS_MUTABLE` is exactly `WorkspaceConfig.isWritable()` (`:867`), but Splunk, Kudu, JDBC and GoogleSheets also report mutable while rejecting views — hence the two-part rule. Do **not** use `PluginInfo.type == "filesystem"`: that string is a display artifact built by stripping `Config` off a class name (`MetadataResources.java:342-346`).

- [ ] **Step 1: Write the failing unit test**

Create `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestViewTargets.java` (Apache header first, copied from `TestMetadataResources.java`):

```java
package org.apache.drill.exec.server.rest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.mock.MockStorageEngineConfig;
import org.junit.Test;

/**
 * A view can only be created in a writable file-based workspace. Both halves of that
 * rule matter: plugins such as Splunk, Kudu and JDBC report IS_MUTABLE = YES yet reject
 * CREATE VIEW, because only WorkspaceSchema overrides createView.
 */
public class TestViewTargets {

  private static FileSystemConfig fileConfig() {
    return new FileSystemConfig("file:///", null, null, null, null);
  }

  @Test
  public void writableFileWorkspaceIsAViewTarget() {
    assertTrue(MetadataResources.isViewTarget(fileConfig(), "YES"));
  }

  @Test
  public void nonWritableFileWorkspaceIsNotAViewTarget() {
    assertFalse(MetadataResources.isViewTarget(fileConfig(), "NO"));
  }

  /** The case the endpoint exists for: mutable, but CREATE VIEW would throw. */
  @Test
  public void mutableNonFilePluginIsNotAViewTarget() {
    assertFalse(MetadataResources.isViewTarget(MockStorageEngineConfig.INSTANCE, "YES"));
  }

  /** A schema whose plugin is absent from the registry must not be offered. */
  @Test
  public void unknownPluginIsNotAViewTarget() {
    assertFalse(MetadataResources.isViewTarget(null, "YES"));
  }
}
```

- [ ] **Step 2: Run it to verify it fails**

```bash
mvn -o test -pl exec/java-exec -Dtest=TestViewTargets
```

Expected: compilation failure — `cannot find symbol: method isViewTarget`.

- [ ] **Step 3: Add the predicate and the endpoint**

In `MetadataResources.java`, add the import (alphabetical order among the `org.apache.drill.exec.store` imports):

```java
import org.apache.drill.exec.store.dfs.FileSystemConfig;
```

Add both members immediately after the existing `getSchemas()` method:

```java
  /**
   * Whether a schema can hold a view or materialized view.
   *
   * <p>Both halves are required. IS_MUTABLE is exactly WorkspaceConfig.isWritable(), so
   * it is the right writability source, but plugins such as Splunk, Kudu, JDBC and
   * GoogleSheets report mutable while rejecting views: only WorkspaceSchema overrides
   * createView/createMaterializedView, and AbstractSchema throws for everything else.
   *
   * @param config the schema's plugin config, or null if the plugin is not registered
   * @param isMutable the IS_MUTABLE column from INFORMATION_SCHEMA.SCHEMATA
   */
  static boolean isViewTarget(StoragePluginConfig config, String isMutable) {
    return config instanceof FileSystemConfig && "YES".equalsIgnoreCase(isMutable);
  }

  @GET
  @Path("/view-targets")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List schemas that can hold a view",
      description = "Returns writable file-based schemas — the only ones where CREATE VIEW succeeds")
  public SchemasResponse getViewTargets() {
    logger.debug("Fetching view target schemas");

    List<SchemaInfo> schemas = new ArrayList<>();

    Map<String, StoragePluginConfig> enabledPlugins;
    try {
      enabledPlugins = storageRegistry.enabledConfigs();
    } catch (Exception e) {
      logger.error("Error fetching plugins for view targets", e);
      return new SchemasResponse(schemas);
    }

    String sql = "SELECT SCHEMA_NAME, IS_MUTABLE FROM INFORMATION_SCHEMA.SCHEMATA "
        + "ORDER BY SCHEMA_NAME";
    try {
      QueryResult result = executeQuery(sql);
      for (Map<String, String> row : result.rows) {
        String schemaName = row.get("SCHEMA_NAME");
        if (schemaName == null) {
          continue;
        }
        String pluginName = schemaName.split("\\.")[0];
        if (isViewTarget(enabledPlugins.get(pluginName), row.get("IS_MUTABLE"))) {
          schemas.add(new SchemaInfo(schemaName, pluginName, true));
        }
      }
    } catch (Exception e) {
      logger.error("Error fetching view targets from INFORMATION_SCHEMA", e);
    }

    return new SchemasResponse(schemas);
  }
```

- [ ] **Step 4: Run the unit test to verify it passes**

```bash
mvn -o test -pl exec/java-exec -Dtest=TestViewTargets
```

Expected: `Tests run: 4, Failures: 0, Errors: 0` and `BUILD SUCCESS`.

- [ ] **Step 5: Add the cluster test**

Append to `TestMetadataResources.java` (the class already has `httpClient`, `mapper` and `portNumber`):

```java
  /**
   * dfs.tmp is writable and file-based, so it must be offered. cp is file-based but not
   * writable, and sys is neither — both must be absent.
   */
  @Test
  public void testGetViewTargets() throws Exception {
    String url = String.format("http://localhost:%d/api/v1/metadata/view-targets", portNumber);
    Request request = new Request.Builder().url(url).build();
    try (Response response = httpClient.newCall(request).execute()) {
      assertEquals(200, response.code());
      JsonNode json = mapper.readTree(response.body().string());
      assertTrue(json.has("schemas"));

      Set<String> names = new HashSet<>();
      for (JsonNode schema : json.get("schemas")) {
        names.add(schema.get("name").asText());
      }
      assertTrue(names.contains("dfs.tmp"), "dfs.tmp is writable and must be a view target");
      assertFalse(names.contains("sys"), "sys is not file-based and must not be a view target");
      assertFalse(names.contains("cp"), "cp is not writable and must not be a view target");
    }
  }
```

- [ ] **Step 6: Run the cluster test**

```bash
mvn -o test -pl exec/java-exec -Dtest=TestMetadataResources#testGetViewTargets
```

Expected: `Tests run: 1, Failures: 0, Errors: 0`.

If `dfs.tmp` is absent, the test fixture's workspace is not writable — check `ClusterFixtureBuilder`; do **not** weaken the assertion.

- [ ] **Step 7: Checkstyle**

```bash
mvn -o checkstyle:check -pl exec/java-exec
```

Expected: `You have 0 Checkstyle violations` and `BUILD SUCCESS`.

- [ ] **Step 8: Commit**

```bash
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/MetadataResources.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestViewTargets.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestMetadataResources.java
git commit -m "Add view-targets endpoint listing schemas that can hold a view"
```

---

### Task 2: Pure SQL helpers

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/utils/sql.ts`
- Create: `exec/java-exec/src/main/resources/webapp/src/utils/sql.test.ts`
- Modify: `exec/java-exec/src/main/resources/webapp/src/api/metadata.ts:29-35` (delete the private `formatSchema`, import it instead)

**Interfaces:**
- Produces: `formatSchema(schema: string): string`
- Produces: `isCreatableAsView(sql: string): boolean`
- Produces: `isValidViewName(name: string): boolean`
- Produces: `buildViewDdl(opts: ViewDdlOptions): string` where
  `ViewDdlOptions = { mode: 'view' | 'materialized_view'; schema: string; name: string; sql: string; replace: boolean }`

**Why this file has no `api/` imports:** so its tests need no axios mocking. `api/metadata.ts` will import *from* it, never the reverse.

- [ ] **Step 1: Write the failing tests**

Create `src/utils/sql.test.ts` (Apache header first, copied from `src/hooks/useProspector.test.tsx`):

```ts
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
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd exec/java-exec/src/main/resources/webapp && npx vitest run src/utils/sql.test.ts
```

Expected: FAIL — `Failed to resolve import "./sql"`.

- [ ] **Step 3: Write the implementation**

Create `src/utils/sql.ts` (Apache header first):

```ts
export type ViewMode = 'view' | 'materialized_view';

export interface ViewDdlOptions {
  mode: ViewMode;
  schema: string;
  name: string;
  sql: string;
  replace: boolean;
}

/**
 * Format a compound schema name for use in SQL queries.
 * Plugin name stays unquoted; workspace parts are backtick-quoted.
 * e.g. "dfs.test" → "dfs.`test`", "dfs" → "dfs"
 */
export function formatSchema(schema: string): string {
  const parts = schema.split('.');
  if (parts.length <= 1) {
    return schema;
  }
  return parts[0] + '.' + parts.slice(1).map((p) => `\`${p}\``).join('.');
}

/** Strip comments and leading whitespace/parens so the leading keyword can be read. */
function firstKeyword(sql: string): string {
  const stripped = sql
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/--[^\n]*/g, ' ')
    .replace(/^[\s(]+/, '');
  const match = stripped.match(/^[A-Za-z_]+/);
  return match ? match[0].toUpperCase() : '';
}

/**
 * Whether this SQL may become a view. Only SELECT and WITH qualify.
 *
 * Checking the leading keyword is sufficient rather than heuristic: INSERT/DELETE/
 * UPDATE/MERGE are siblings of OrderedQueryOrExpr in the grammar's SqlQueryOrDml,
 * reachable only as the leading production, and WITH is followed strictly by
 * LeafQueryOrExpr — Drill has no writable CTEs. So a statement starting with SELECT or
 * WITH cannot contain DML anywhere inside it.
 *
 * Deliberately does not scan the body: that would reject valid views such as
 * SELECT 'INSERT' AS action FROM audit_log, or SELECT update_time FROM logs.
 */
export function isCreatableAsView(sql: string): boolean {
  const keyword = firstKeyword(sql);
  return keyword === 'SELECT' || keyword === 'WITH';
}

/**
 * A view name is a path, not an identifier — ViewHandler calls removeLeadingSlash on it
 * and getViewPath resolves it against the workspace location, so slashes place the view
 * in a subdirectory. Backticks are rejected because they would break out of the quoting
 * buildViewDdl generates; ".." is rejected because it would escape the workspace.
 */
export function isValidViewName(name: string): boolean {
  if (!/^[A-Za-z0-9_\-./]+$/.test(name)) {
    return false;
  }
  return !name.split('/').includes('..');
}

export function buildViewDdl({ mode, schema, name, sql, replace }: ViewDdlOptions): string {
  const keyword = mode === 'materialized_view' ? 'MATERIALIZED VIEW' : 'VIEW';
  const orReplace = replace ? 'OR REPLACE ' : '';
  const target = `${formatSchema(schema)}.\`${name}\``;
  return `CREATE ${orReplace}${keyword} ${target} AS ${sql.replace(/;\s*$/, '')}`;
}
```

- [ ] **Step 4: Run to verify it passes**

```bash
npx vitest run src/utils/sql.test.ts
```

Expected: all tests pass.

- [ ] **Step 5: Remove the duplicate `formatSchema` from `api/metadata.ts`**

`api/metadata.ts:24-35` has a byte-identical private copy. Delete lines 24-35 (the JSDoc block and the function) and add to the imports at the top:

```ts
import { formatSchema } from '../utils/sql';
```

Leave `SchemaExplorer.tsx`'s third copy alone — deduping it is not in this plan's scope.

- [ ] **Step 6: Verify nothing broke**

```bash
npx vitest run && npm run lint
```

Expected: all tests pass; lint clean with zero warnings.

- [ ] **Step 7: Commit**

```bash
git add src/utils/sql.ts src/utils/sql.test.ts src/api/metadata.ts
git commit -m "Add pure SQL helpers for building view DDL"
```

---

### Task 3: Frontend types and `getViewTargets`

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/types/index.ts:34-38` and `:288-295`
- Modify: `exec/java-exec/src/main/resources/webapp/src/api/metadata.ts` (after `getSchemas`, ~line 86)

**Interfaces:**
- Consumes: `GET /api/v1/metadata/view-targets` from Task 1.
- Produces: `getViewTargets(): Promise<SchemaInfo[]>`
- Produces: `TableInfo.type` now includes `'MATERIALIZED VIEW'`; `DatasetRef.type` now includes `'view'` and `'materialized_view'`.

- [ ] **Step 1: Widen `TableInfo.type`**

`src/types/index.ts:34-38` currently reads:

```ts
export interface TableInfo {
  name: string;
  schema: string;
  type: 'TABLE' | 'VIEW' | 'SYSTEM TABLE';
}
```

Replace with:

```ts
export interface TableInfo {
  name: string;
  schema: string;
  // Values come straight from INFORMATION_SCHEMA.TABLES.TABLE_TYPE, which the backend
  // passes through untouched. Materialized views report 'MATERIALIZED VIEW'.
  type: 'TABLE' | 'VIEW' | 'MATERIALIZED VIEW' | 'SYSTEM TABLE';
}
```

- [ ] **Step 2: Widen `DatasetRef.type`**

`src/types/index.ts:288-295` currently reads:

```ts
export interface DatasetRef {
  id: string;
  type: 'table' | 'saved_query' | 'plugin' | 'schema';
  schema?: string;
  table?: string;
  savedQueryId?: string;
  label: string;
}
```

Replace the `type` line with:

```ts
  type: 'table' | 'saved_query' | 'plugin' | 'schema' | 'view' | 'materialized_view';
```

The Java `DatasetRef.type` is a free `String` defaulting to `"table"` (`ProjectResources.java:129`), so no backend change is needed.

- [ ] **Step 3: Add `getViewTargets`**

In `src/api/metadata.ts`, immediately after `getSchemas` (~line 86):

```ts
/**
 * Fetch the schemas a view or materialized view can be created in.
 *
 * Deliberately a dedicated endpoint rather than a flag on getSchemas: getSchemas returns
 * root plugin names only, and workspaces such as dfs.tmp come from getPluginSchemas one
 * plugin at a time, so filtering client-side would take N+1 requests.
 */
export async function getViewTargets(): Promise<SchemaInfo[]> {
  const response = await apiClient.get<SchemasResponse>(`${METADATA_BASE}/view-targets`);
  return response.data.schemas;
}
```

- [ ] **Step 4: Verify it compiles and lints**

```bash
npx tsc --noEmit && npm run lint
```

Expected: no errors, no warnings.

If `tsc` reports errors in `ProjectDetailPage.tsx` or `DatasetPickerModal.tsx`, they are exhaustiveness gaps — Task 5 fixes `ProjectDetailPage`. `DatasetPickerModal.tsx:101-105` only maps `plugin`/`schema`/`table` and needs no change, since view datasets are never pre-checked into its tree.

- [ ] **Step 5: Commit**

```bash
git add src/types/index.ts src/api/metadata.ts
git commit -m "Add view target API and widen table and dataset type unions"
```

---

### Task 4: Save dialog mode radio

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/query-editor/SaveQueryDialog.tsx`

**Interfaces:**
- Consumes: `isCreatableAsView`, `isValidViewName` (Task 2); `getViewTargets` (Task 3).
- Produces: local `mode` state of type `'query' | 'view' | 'materialized_view'`, consumed by Task 5.

This task adds the form controls only. The view modes render their fields but **do not save yet** — Task 5 wires execution. That keeps the diff reviewable.

- [ ] **Step 1: Add imports**

At the top of `SaveQueryDialog.tsx`, extend the antd import and add the new ones:

```ts
import { Modal, Form, Input, Switch, message, Radio, Select, Checkbox, Tooltip } from 'antd';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getViewTargets } from '../../api/metadata';
import { isCreatableAsView, isValidViewName } from '../../utils/sql';
```

- [ ] **Step 2: Add mode state and the view-target query**

Immediately after `const [saving, setSaving] = useState(false);` (line 46):

```ts
  const [mode, setMode] = useState<'query' | 'view' | 'materialized_view'>('query');
  const isViewMode = mode !== 'query';

  // A view can only wrap a SELECT. Disabling the radios is friendlier than letting
  // Drill reject the DDL, and saving a non-SELECT as a plain query stays valid.
  const canCreateView = isCreatableAsView(sql);

  const { data: viewTargets = [], isLoading: loadingTargets } = useQuery({
    queryKey: ['viewTargets'],
    queryFn: getViewTargets,
    enabled: open && isViewMode,
  });
```

- [ ] **Step 3: Reset the mode when the dialog closes**

`handleCancel` (line 93) currently resets only the form. Replace it:

```ts
  const handleCancel = () => {
    form.resetFields();
    setMode('query');
    onClose();
  };
```

- [ ] **Step 4: Add the radio and the view fields**

Inside `<Form>`, immediately before the existing `name` `Form.Item` (line 113):

```tsx
        <Form.Item label="Save as">
          <Radio.Group value={mode} onChange={(e) => setMode(e.target.value)}>
            <Radio value="query">Query</Radio>
            <Tooltip
              title={canCreateView ? undefined
                : 'Only SELECT queries (including CTEs) can be saved as a view'}
            >
              <Radio value="view" disabled={!canCreateView}>View</Radio>
            </Tooltip>
            <Tooltip
              title={canCreateView ? undefined
                : 'Only SELECT queries (including CTEs) can be saved as a materialized view'}
            >
              <Radio value="materialized_view" disabled={!canCreateView}>
                Materialized View
              </Radio>
            </Tooltip>
          </Radio.Group>
        </Form.Item>

        {isViewMode && (
          <>
            <Form.Item
              name="schema"
              label="Schema"
              rules={[{ required: true, message: 'Please choose a schema' }]}
              extra="Only writable file-based schemas can hold a view."
            >
              <Select
                loading={loadingTargets}
                placeholder="Choose where to save the view"
                options={viewTargets.map((s) => ({ label: s.name, value: s.name }))}
                showSearch
              />
            </Form.Item>

            <Form.Item
              name="replace"
              valuePropName="checked"
            >
              <Checkbox>Replace if it already exists</Checkbox>
            </Form.Item>
          </>
        )}
```

- [ ] **Step 5: Make the name field mode-aware**

Replace the existing `name` `Form.Item` (lines 113-119) with:

```tsx
        <Form.Item
          name="name"
          label={isViewMode ? 'View Name' : 'Query Name'}
          extra={isViewMode
            ? 'Use a slash to place the view in a subfolder, e.g. reports/daily_sales'
            : undefined}
          rules={[
            { required: true, message: `Please enter a ${isViewMode ? 'view' : 'query'} name` },
            {
              validator: (_, value) =>
                !isViewMode || !value || isValidViewName(value)
                  ? Promise.resolve()
                  : Promise.reject(new Error(
                      'Use letters, numbers, _ - . and / only')),
            },
          ]}
        >
          <Input placeholder={isViewMode ? 'e.g. sales_summary' : 'Enter a name for this query'} autoFocus />
        </Form.Item>
```

- [ ] **Step 6: Hide the query-only fields in view mode**

Replace lines 121-137 in full. `isPublic` is meaningless for a view — a view is visible to anyone who can read its schema. View descriptions need a store that does not exist yet and arrive in the descriptions plan, so the field is query-only for now:

```tsx
        {!isViewMode && (
          <>
            <Form.Item
              name="description"
              label="Description"
            >
              <TextArea
                placeholder="Optional description"
                rows={3}
              />
            </Form.Item>

            <Form.Item
              name="isPublic"
              label="Make Public"
              valuePropName="checked"
            >
              <Switch />
            </Form.Item>
          </>
        )}
```

- [ ] **Step 7: Set the modal title from the mode**

Replace `title="Save Query"` (line 100):

```tsx
      title={mode === 'query' ? 'Save Query'
        : mode === 'view' ? 'Save as View' : 'Save as Materialized View'}
```

- [ ] **Step 8: Verify**

```bash
npx tsc --noEmit && npm run lint
```

Expected: no errors, no warnings.

- [ ] **Step 9: Manual check**

Run `npm run dev`, open SQL Lab against a running Drillbit, type `SELECT 1`, press Save. Expected: three radios; View and Materialized View enabled; choosing one swaps in a Schema dropdown listing writable schemas (e.g. `dfs.tmp`), plus the replace checkbox. Now replace the SQL with `DROP TABLE x` and reopen: both view radios greyed with the tooltip.

- [ ] **Step 10: Commit**

```bash
git add src/components/query-editor/SaveQueryDialog.tsx
git commit -m "Add view and materialized view modes to the save dialog"
```

---

### Task 5: Execute the DDL and link the project

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/utils/createView.ts`
- Create: `exec/java-exec/src/main/resources/webapp/src/utils/createView.test.ts`
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/query-editor/SaveQueryDialog.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/ProjectDetailPage.tsx:203-213`

**Interfaces:**
- Consumes: `buildViewDdl`, `ViewMode` (Task 2); `mode` state (Task 4); `DatasetRef` types (Task 3).
- Produces: `createViewFromQuery(opts: CreateViewOptions): Promise<CreateViewResult>` where
  `CreateViewOptions = { mode: ViewMode; schema: string; name: string; sql: string; replace: boolean; projectId?: string }`
  and `CreateViewResult = { ddl: string; projectError?: string }`.
- Produces: new optional prop `onViewCreated?: (schema: string) => void` on `SaveQueryDialogProps`, consumed by Task 7.

**Why the orchestration is its own module:** the partial-failure rule is real logic and needs a real test — the view exists the moment `executeQuery` resolves, so a later project-link failure must not be reported as total failure. Reaching that path through the rendered dialog would mean driving antd's `Select` and `Form` from RTL, which is brittle. A plain async function is not. Unlike `utils/sql.ts`, this module *may* import from `api/`.

- [ ] **Step 1: Write the failing tests**

Create `src/utils/createView.test.ts` (Apache header first):

```ts
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { createViewFromQuery } from './createView';
import { executeQuery } from '../api/queries';
import { addDataset } from '../api/projects';

vi.mock('../api/queries', () => ({ executeQuery: vi.fn() }));
vi.mock('../api/projects', () => ({ addDataset: vi.fn() }));

const base = {
  mode: 'view' as const,
  schema: 'dfs.tmp',
  name: 'sales',
  sql: 'SELECT 1 FROM t',
  replace: false,
};

describe('createViewFromQuery', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(executeQuery).mockResolvedValue({} as never);
    vi.mocked(addDataset).mockResolvedValue({} as never);
  });

  it('sends the CREATE VIEW statement to Drill', async () => {
    await createViewFromQuery(base);
    expect(vi.mocked(executeQuery).mock.calls[0][0].query)
      .toBe('CREATE VIEW dfs.`tmp`.`sales` AS SELECT 1 FROM t');
  });

  /** Auto-limit makes Drill cancel once N rows arrive, truncating an MV's data. */
  it('never sets an auto limit, which would truncate a materialized view', async () => {
    await createViewFromQuery({ ...base, mode: 'materialized_view' });
    expect(vi.mocked(executeQuery).mock.calls[0][0].autoLimitRowCount).toBeUndefined();
  });

  it('does not touch the project when there is no active project', async () => {
    await createViewFromQuery(base);
    expect(addDataset).not.toHaveBeenCalled();
  });

  it('links a view to the active project as a view dataset', async () => {
    await createViewFromQuery({ ...base, projectId: 'p1' });
    expect(addDataset).toHaveBeenCalledWith('p1', expect.objectContaining({
      type: 'view', schema: 'dfs.tmp', table: 'sales', label: 'sales',
    }));
  });

  it('links a materialized view as a materialized_view dataset', async () => {
    await createViewFromQuery({ ...base, mode: 'materialized_view', projectId: 'p1' });
    expect(addDataset).toHaveBeenCalledWith('p1', expect.objectContaining({
      type: 'materialized_view',
    }));
  });

  /**
   * The view already exists at this point. Failing the whole call would tell the user
   * nothing was created and invite a retry, which would then fail on the name conflict.
   */
  it('reports a project-linking failure without losing the created view', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(addDataset).mockRejectedValue(new Error('project not found'));

    const result = await createViewFromQuery({ ...base, projectId: 'p1' });

    expect(result.ddl).toContain('CREATE VIEW');
    expect(result.projectError).toContain('project not found');
    expect(consoleError).toHaveBeenCalled();
    consoleError.mockRestore();
  });

  it('throws when the DDL itself fails, because nothing was created', async () => {
    vi.mocked(executeQuery).mockRejectedValue(new Error('already exists'));
    await expect(createViewFromQuery(base)).rejects.toThrow('already exists');
    expect(addDataset).not.toHaveBeenCalled();
  });
});
```

- [ ] **Step 2: Run to verify it fails**

```bash
npx vitest run src/utils/createView.test.ts
```

Expected: FAIL — `Failed to resolve import "./createView"`.

- [ ] **Step 3: Write the orchestration**

Create `src/utils/createView.ts` (Apache header first):

```ts
import { executeQuery } from '../api/queries';
import { addDataset } from '../api/projects';
import { buildViewDdl, type ViewMode } from './sql';

export interface CreateViewOptions {
  mode: ViewMode;
  schema: string;
  name: string;
  sql: string;
  replace: boolean;
  projectId?: string;
}

export interface CreateViewResult {
  /** The statement sent to Drill. */
  ddl: string;
  /** Set when the view was created but could not be linked to the project. */
  projectError?: string;
}

/**
 * Create a view or materialized view, then link it to the active project.
 *
 * Creating a view is DDL, not metadata: it executes immediately and is visible to
 * anyone who can read the schema. The view exists the moment executeQuery resolves, so
 * a later failure to link it to a project is reported as a partial outcome rather than
 * thrown — the same rule the visualization tool follows when addVisualization fails.
 *
 * Throws only if the DDL itself fails, in which case nothing was created.
 */
export async function createViewFromQuery(opts: CreateViewOptions): Promise<CreateViewResult> {
  const { mode, schema, name, sql, replace, projectId } = opts;
  const ddl = buildViewDdl({ mode, schema, name, sql, replace });

  // No autoLimitRowCount: it makes Drill cancel the query once N rows arrive, which
  // would truncate a materialized view's data.
  await executeQuery({ query: ddl, queryType: 'SQL' });

  if (!projectId) {
    return { ddl };
  }

  try {
    await addDataset(projectId, {
      id: '',
      type: mode === 'materialized_view' ? 'materialized_view' : 'view',
      schema,
      table: name,
      label: name,
    });
    return { ddl };
  } catch (err) {
    // The result goes back to a user who was told the view was created, so this must
    // not be silent.
    console.error('View created but could not be added to the project', err);
    return { ddl, projectError: err instanceof Error ? err.message : String(err) };
  }
}
```

- [ ] **Step 4: Run to verify it passes**

```bash
npx vitest run src/utils/createView.test.ts
```

Expected: 7 passing.

- [ ] **Step 5: Wire it into the dialog**

Add the imports to `SaveQueryDialog.tsx`:

```ts
import { createViewFromQuery } from '../../utils/createView';
import type { ViewMode } from '../../utils/sql';
```

Add to `SaveQueryDialogProps` (lines 27-34):

```ts
  /** Called after a view or materialized view is created, with its schema. */
  onViewCreated?: (schema: string) => void;
```

and add `onViewCreated,` to the destructured props (lines 36-43).

Insert this handler before `handleSave` (line 74):

```ts
  const createView = async (values: { schema: string; name: string; replace?: boolean }) => {
    const { projectError } = await createViewFromQuery({
      mode: mode as ViewMode,
      schema: values.schema,
      name: values.name,
      sql,
      replace: values.replace ?? false,
      projectId,
    });

    const label = mode === 'materialized_view' ? 'Materialized view' : 'View';
    if (projectError) {
      message.warning(`${label} created, but not added to the project: ${projectError}`);
    } else {
      message.success(`${label} ${values.schema}.${values.name} created`);
      if (projectId) {
        queryClient.invalidateQueries({ queryKey: ['project', projectId] });
      }
    }

    onViewCreated?.(values.schema);
    form.resetFields();
    setMode('query');
    onClose();
  };
```

- [ ] **Step 6: Branch `handleSave` on the mode**

Replace the body of `handleSave` (lines 74-91):

```ts
  const handleSave = async () => {
    let values;
    try {
      values = await form.validateFields();
    } catch {
      return; // Form validation failed; antd has already shown the messages.
    }

    setSaving(true);

    if (isViewMode) {
      try {
        await createView(values);
      } catch (err) {
        const detail = err instanceof Error ? err.message : String(err);
        message.error(`Failed to create ${mode === 'view' ? 'view' : 'materialized view'}: ${detail}`);
      } finally {
        setSaving(false);
      }
      return;
    }

    const query: SavedQueryCreate = {
      name: values.name,
      description: values.description,
      sql: sql,
      defaultSchema: defaultSchema,
      isPublic: values.isPublic || false,
    };

    mutation.mutate(query);
  };
```

- [ ] **Step 7: Give the project page real labels**

`ProjectDetailPage.tsx:203-213` is a fallthrough chain ending in `'Table'`, so an unhandled `'view'` renders as "Table" silently. Replace:

```tsx
  const typeLabel = dataset.type === 'plugin' ? 'Plugin'
    : dataset.type === 'schema' ? 'Schema'
    : dataset.type === 'saved_query' ? 'Saved Query'
    : dataset.type === 'view' ? 'View'
    : dataset.type === 'materialized_view' ? 'Materialized View'
    : 'Table';
```

Read the surrounding lines first — the existing chain's exact formatting and variable name must be preserved.

- [ ] **Step 8: Verify**

```bash
npx tsc --noEmit && npm run lint && npx vitest run
```

Expected: no errors, no warnings, all tests pass.

- [ ] **Step 9: Manual check — the real gate**

With `npm run dev` against a running Drillbit:

1. `SELECT 1 AS x` → Save → View → schema `dfs.tmp` → name `plan_test_view` → Save.
   Expected: success message.
2. In the editor: ``SELECT * FROM dfs.`tmp`.`plan_test_view` `` → Run. Expected: returns `1`.
3. Save the same query as a view with the same name, replace **unticked**. Expected: an error from Drill saying the view already exists.
4. Same again with replace **ticked**. Expected: success.
5. Save as Materialized View, name `plan_test_mv`. Expected: success; querying it returns rows.
6. Inside a project, repeat step 1. Expected: the view appears on the project page labelled "View", not "Table".

Clean up: ``DROP VIEW dfs.`tmp`.`plan_test_view` `` and ``DROP MATERIALIZED VIEW dfs.`tmp`.`plan_test_mv` ``.

- [ ] **Step 10: Commit**

```bash
git add src/utils/createView.ts src/utils/createView.test.ts src/components/query-editor/SaveQueryDialog.tsx src/pages/ProjectDetailPage.tsx
git commit -m "Create views and materialized views from the save dialog"
```

---

### Task 6: Distinguish views in the schema tree

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/schema-explorer/TreeNodeBuilder.tsx:246-344`

**Interfaces:**
- Consumes: `TableInfo.type` including `'MATERIALIZED VIEW'` (Task 3).

`TableInfo.type` is currently dead data: `buildTableNodes` destructures only `table.name`, and every table gets `<TableOutlined style={{ color: '#faad14' }} />` at lines 267 and 339. Views are already in the tree, indistinguishable from tables. Keep the list flat and alphabetical — only the icon and tooltip change.

- [ ] **Step 1: Add the icon imports**

Line 19 currently reads:

```tsx
import { FolderOutlined, TableOutlined, WarningOutlined, InfoCircleOutlined, SearchOutlined } from '@ant-design/icons';
```

Replace it with:

```tsx
import { FolderOutlined, TableOutlined, WarningOutlined, InfoCircleOutlined, SearchOutlined, EyeOutlined, ThunderboltOutlined } from '@ant-design/icons';
```

`Tooltip` (line 18) and `TableInfo` (line 22) are already imported — do not add them again.

- [ ] **Step 2: Add the icon helper**

Immediately above `export function buildTableNodes` (line 246):

```tsx
/**
 * Tables, views and materialized views are all rows in INFORMATION_SCHEMA.TABLES and
 * were previously rendered identically. The distinction matters: a view is always live,
 * whereas a materialized view serves stored data that can be stale until refreshed.
 */
function tableIcon(type: TableInfo['type']) {
  if (type === 'VIEW') {
    return <EyeOutlined style={{ color: '#722ed1' }} />;
  }
  if (type === 'MATERIALIZED VIEW') {
    return <ThunderboltOutlined style={{ color: '#13c2c2' }} />;
  }
  return <TableOutlined style={{ color: '#faad14' }} />;
}

function tableTooltip(type: TableInfo['type']): string | undefined {
  if (type === 'VIEW') {
    return 'View — runs its query each time it is read';
  }
  if (type === 'MATERIALIZED VIEW') {
    return 'Materialized view — serves stored data; may be stale until refreshed';
  }
  return undefined;
}
```

- [ ] **Step 3: Use it at both icon sites**

Replace `icon: <TableOutlined style={{ color: '#faad14' }} />,` at **line 267** (the dynamic-schema branch) and **line 339** (the normal branch) with:

```tsx
      icon: tableIcon(table.type),
```

Then wrap the title at line 339's return so the type is discoverable on hover. The current title is:

```tsx
      title: (
        <span>
          {table.name}
          {tableCount > 0 && (
```

Replace the opening of that `<span>` with a tooltip-wrapped name, leaving the `tableCount` tag exactly as it is:

```tsx
      title: (
        <span>
          <Tooltip title={tableTooltip(table.type)}>
            <span>{table.name}</span>
          </Tooltip>
          {tableCount > 0 && (
```

`Tooltip` is already imported in this file (it wraps the usage-count tag at line 330). A `Tooltip` with `title={undefined}` renders nothing, so plain tables are unaffected.

- [ ] **Step 4: Verify**

```bash
npx tsc --noEmit && npm run lint && npx vitest run
```

Expected: no errors, no warnings, all tests pass.

- [ ] **Step 3a: Write the failing icon tests FIRST**

Do this before Steps 1-3. `buildTableNodes` is a pure function returning `DataNode[]`, and `src/components/schema-explorer/TreeNodeBuilder.test.tsx` already asserts directly on `result[0].key` / `.children`. Append to that file:

```tsx
/**
 * TableInfo.type was dead data: tables, views and materialized views all rendered the
 * same amber TableOutlined. Asserting the exact component per type means these fail if
 * the branch is deleted, rather than passing on a shared default.
 */
describe('buildTableNodes icons', () => {
  const emptyNestedCache: Record<string, never[]> = {};

  const iconOf = (type: TableInfo['type']) =>
    (buildTableNodes('s', [{ name: 'x', schema: 's', type }], {}, emptyNestedCache)[0]
      .icon as ReactElement).type;

  it('gives a plain table the table icon', () => {
    expect(iconOf('TABLE')).toBe(TableOutlined);
  });

  it('gives a view its own icon, not the table icon', () => {
    expect(iconOf('VIEW')).toBe(EyeOutlined);
  });

  it('gives a materialized view its own icon, distinct from a plain view', () => {
    expect(iconOf('MATERIALIZED VIEW')).toBe(ThunderboltOutlined);
  });
});
```

Extend that file's existing imports — do not duplicate them:

```tsx
import type { ReactElement } from 'react';
import { EyeOutlined, TableOutlined, ThunderboltOutlined } from '@ant-design/icons';
```

Run `npx vitest run src/components/schema-explorer/TreeNodeBuilder.test.tsx`. Expected: the TABLE case passes; the VIEW and MATERIALIZED VIEW cases FAIL, each reporting it received `TableOutlined`. That failure is the proof the test is wired to the behaviour. Then do Steps 1-3 and re-run — all three pass.

- [ ] **Step 5: Manual check**

With `npm run dev`: create a view in `dfs.tmp` (Task 5's manual check), refresh the tree via the sidebar refresh button, expand `dfs.tmp`. Expected: the view shows a purple eye icon, a materialized view a cyan bolt, and ordinary tables the unchanged amber grid. Hovering a view shows the explanatory tooltip.

- [ ] **Step 6: Commit**

```bash
git add src/components/schema-explorer/TreeNodeBuilder.tsx
git commit -m "Give views and materialized views distinct icons in the schema tree"
```

---

### Task 7: Refresh the tree after a view is created

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/schema-explorer/SchemaExplorer.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/SqlLabPage.tsx`

**Interfaces:**
- Consumes: `onViewCreated?: (schema: string) => void` (Task 5).
- Produces: new optional prop `refreshSchema?: { schema: string; nonce: number }` on `SchemaExplorerProps`.

`SchemaExplorer` keeps a hand-rolled `useState` cache (`:322`), not react-query, so `invalidateQueries({ queryKey: ['tables', schema] })` does nothing. The existing `handleRefreshNode` (`:847`) already drops one schema's cache entry and its `loadedKeys` entry — reuse it rather than inventing a second mechanism. The `nonce` is what makes two consecutive saves to the same schema fire twice.

- [ ] **Step 1: Add the prop**

In `SchemaExplorerProps` (`:54-69`), after `savedQueryIds?: string[];`:

```ts
  /**
   * Ask the explorer to reload one schema — used after a view is created, since the
   * tables cache is local state rather than react-query and cannot be invalidated from
   * outside. The nonce makes repeat saves to the same schema distinct.
   */
  refreshSchema?: { schema: string; nonce: number };
```

Add `refreshSchema,` to the destructured props of the `SchemaExplorer` component.

- [ ] **Step 2: React to it**

`handleRefreshNode` is defined at `:847` with `useCallback`. Add this effect **after** that definition (it must be after, or the reference is a TDZ error):

```ts
  useEffect(() => {
    if (refreshSchema) {
      handleRefreshNode(`schema:${refreshSchema.schema}`);
    }
  }, [refreshSchema, handleRefreshNode]);
```

`useEffect` is already imported at `SchemaExplorer.tsx:18` — no import change needed.

- [ ] **Step 3: Hold the signal in `SqlLabPage`**

Next to the other dialog state (`const [saveDialogOpen, setSaveDialogOpen] = useState(false);`, `:148`):

```ts
  const [refreshSchema, setRefreshSchema] = useState<{ schema: string; nonce: number }>();
```

- [ ] **Step 4: Pass it to the explorer**

At the `<SchemaExplorer>` render (`:871`), add the prop:

```tsx
        refreshSchema={refreshSchema}
```

The element lives in a `useMemo` whose dependency array is at `:879-886`. Add `refreshSchema` to that array, or the explorer will never see a new value.

- [ ] **Step 5: Raise the signal from the dialog**

At the `<SaveQueryDialog>` render (`:1356`), add:

```tsx
        onViewCreated={(schema) =>
          setRefreshSchema((prev) => ({ schema, nonce: (prev?.nonce ?? 0) + 1 }))}
```

- [ ] **Step 6: Verify**

```bash
npx tsc --noEmit && npm run lint && npx vitest run
```

Expected: no errors, no warnings, all tests pass. A missing `refreshSchema` in the `useMemo` deps surfaces here as a `react-hooks/exhaustive-deps` warning, which fails the build.

- [ ] **Step 7: Manual check — the whole feature end to end**

With `npm run dev`:

1. Expand `dfs.tmp` in the schema tree.
2. In the editor: `SELECT 1 AS x` → Save → View → `dfs.tmp` → `refresh_test_view` → Save.
3. Expected: the tree reloads `dfs.tmp` and `refresh_test_view` appears with the purple eye icon, **without** pressing the sidebar refresh button.
4. Save again as `refresh_test_view_2`. Expected: it appears too — proving the nonce works for a repeat save to the same schema.

Clean up both views with `DROP VIEW`.

- [ ] **Step 8: Commit**

```bash
git add src/components/schema-explorer/SchemaExplorer.tsx src/pages/SqlLabPage.tsx
git commit -m "Reload the schema tree after creating a view"
```

---

## Done when

- `SELECT`-only queries can be saved as a view or materialized view, into any writable file-based schema.
- Non-SELECT queries grey out both view radios with an explanation, and can still be saved as a query.
- A name conflict fails unless "Replace if it already exists" is ticked.
- Views and MVs get distinct icons in the tree and appear immediately after creation.
- Inside a project, a new view is auto-added and labelled correctly.
- `mvn -o checkstyle:check -pl exec/java-exec` reports zero violations; `npm run lint` zero warnings; `npx vitest run` and `TestViewTargets` pass.

## Follow-up plans

1. **Descriptions** — `drill.sqllab.view_descriptions` store, `ViewDescriptionResources`, edit UI.
2. **MV refresh** — context-menu `REFRESH MATERIALIZED VIEW` plus staleness from `INFORMATION_SCHEMA.MATERIALIZED_VIEWS`.
3. **Nested views** — render `.view.drill` files as view nodes in the file browser.
