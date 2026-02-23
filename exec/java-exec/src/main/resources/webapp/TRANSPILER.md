# SQL Transpiler (Java sqlglot)

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
  -> Backend transpiles via Java sqlglot
  -> Returns transpiled SQL
  -> Editor receives valid Drill SQL
```

The backend uses [Java sqlglot](https://github.com/gtkcyber/sqlglot_java), a
pure Java port of the sqlglot library, for SQL parsing, transpilation, and AST
manipulation. No external runtime (Python, GraalPy, etc.) is required.

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

The transpiler is always available with Java sqlglot.

## Supported Dialects

Java sqlglot supports a wide range of SQL dialects including:

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

## License

The Java sqlglot artifacts (`com.gtkcyber.sqlglot:sqlglot-core`,
`com.gtkcyber.sqlglot:sqlglot-dialects`) are licensed under the MIT License,
which is ASF Category A compatible.

## Key Files

| File | Purpose |
|------|---------|
| `SqlTranspiler.java` | Singleton service using Java sqlglot for transpilation |
| `TranspileResources.java` | REST endpoint at `/api/v1/transpile` |
| `DrillRestServer.java` | Registers `TranspileResources` |
| `api/ai.ts` | Frontend `transpileSql()` function |
| `pages/SqlLabPage.tsx` | Calls transpile on Optimize accept |
| `exec/java-exec/pom.xml` | Java sqlglot Maven dependencies |
| `exec/jdbc-all/pom.xml` | Java sqlglot exclusions from JDBC jar |
