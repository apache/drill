# SQL Transpiler (GraalPy + sqlglot)

## Overview

The SQL Transpiler automatically converts AI-generated SQL from common dialects
(e.g. MySQL) to Apache Drill's SQL dialect. This ensures that when a user accepts
an optimized query from the Optimize modal, the SQL inserted into the editor uses
valid Drill syntax (backtick quoting, Drill function names, Drill data types, etc.).

The transpilation is invisible to the user -- they see the AI's explanation and
SQL in the modal, and when they click Accept, the SQL is silently transpiled to
Drill dialect before being placed in the editor.

## Architecture

```
User clicks Optimize
  -> Frontend streams AI response (existing flow)
  -> User clicks Accept
  -> Frontend extracts SQL from markdown code block
  -> Frontend calls POST /api/v1/transpile
  -> Backend transpiles via GraalPy + sqlglot
  -> Returns transpiled SQL
  -> Editor receives valid Drill SQL
```

The backend embeds a Python runtime (GraalPy) inside the JVM and uses
[sqlglot](https://github.com/tobymao/sqlglot) for SQL transpilation. The Python
environment and sqlglot package are embedded at build time via the
`graalpy-maven-plugin`.

## REST API

### POST /api/v1/transpile

Transpile SQL from one dialect to another.

**Request:**
```json
{
  "sql": "SELECT CAST(x AS TEXT) FROM t",
  "sourceDialect": "mysql",
  "targetDialect": "drill"
}
```

**Response:**
```json
{
  "sql": "SELECT CAST(x AS VARCHAR) FROM t",
  "success": true
}
```

Both `sourceDialect` and `targetDialect` are optional and default to `"mysql"`
and `"drill"` respectively.

### GET /api/v1/transpile/status

Check whether the transpiler is available.

**Response:**
```json
{
  "available": true
}
```

Returns `false` if GraalPy or sqlglot could not be initialized. In this case,
the transpile endpoint will pass SQL through unchanged.

## Supported Dialects

sqlglot supports a wide range of SQL dialects including:

- `drill` (Apache Drill)
- `mysql` (MySQL)
- `postgres` (PostgreSQL)
- `hive` (Apache Hive)
- `spark` (Apache Spark SQL)
- `bigquery` (Google BigQuery)
- `snowflake` (Snowflake)
- `presto` / `trino`
- `sqlite`
- `tsql` (SQL Server / T-SQL)
- `oracle`
- `duckdb`

See [sqlglot dialects](https://github.com/tobymao/sqlglot/tree/main/sqlglot/dialects)
for the full list.

## Graceful Fallback

If GraalPy or sqlglot cannot be initialized (e.g. due to missing native
libraries or classpath issues), the transpiler falls back gracefully:

- `GET /api/v1/transpile/status` returns `{ "available": false }`
- `POST /api/v1/transpile` returns the original SQL unchanged
- The frontend `transpileSql()` function also catches errors and returns the
  original SQL

This means the Optimize feature continues to work even without transpilation --
users will just get the AI's raw SQL without dialect conversion.

## License

The GraalPy artifacts (`org.graalvm.polyglot:polyglot`, `org.graalvm.polyglot:python`)
are licensed under MIT + UPL (Universal Permissive License) + PSF (Python Software
Foundation License). All three are ASF Category A compatible licenses.

Apache Hive adopted GraalVM under the same rationale (HIVE-28984).

## Key Files

| File | Purpose |
|------|---------|
| `SqlTranspiler.java` | Singleton service wrapping GraalPy + sqlglot |
| `TranspileResources.java` | REST endpoint at `/api/v1/transpile` |
| `DrillRestServer.java` | Registers `TranspileResources` |
| `api/ai.ts` | Frontend `transpileSql()` function |
| `pages/SqlLabPage.tsx` | Calls transpile on Optimize accept |
| `exec/java-exec/pom.xml` | GraalPy Maven dependencies + plugin |
| `exec/jdbc-all/pom.xml` | GraalPy exclusions from JDBC jar |
