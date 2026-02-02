# Materialized Views

Materialized views in Apache Drill provide a mechanism to store pre-computed query results for improved query performance. Unlike regular views which are virtual and execute the underlying query each time they are accessed, materialized views persist the query results as physical data that can be queried directly.

## Overview

Materialized views are useful for:
- Accelerating frequently executed queries with complex aggregations or joins
- Reducing compute resources for repetitive analytical workloads
- Providing consistent snapshots of data at a point in time

Drill's materialized view implementation includes:
- SQL syntax for creating, dropping, and refreshing materialized views
- Automatic query rewriting using Calcite's SubstitutionVisitor
- Integration with Drill Metastore for centralized metadata management
- Parquet-based data storage for efficient columnar access

## SQL Syntax

### CREATE MATERIALIZED VIEW

```sql
CREATE MATERIALIZED VIEW [schema.]view_name AS select_statement
CREATE OR REPLACE MATERIALIZED VIEW [schema.]view_name AS select_statement
CREATE MATERIALIZED VIEW IF NOT EXISTS [schema.]view_name AS select_statement
```

Examples:

```sql
-- Create a materialized view with aggregations
CREATE MATERIALIZED VIEW dfs.tmp.sales_summary AS
SELECT region, product_category, SUM(amount) as total_sales, COUNT(*) as num_transactions
FROM dfs.`/data/sales`
GROUP BY region, product_category;

-- Create or replace an existing materialized view
CREATE OR REPLACE MATERIALIZED VIEW dfs.tmp.customer_stats AS
SELECT customer_id, COUNT(*) as order_count, AVG(order_total) as avg_order
FROM dfs.`/data/orders`
GROUP BY customer_id;

-- Create only if it doesn't exist
CREATE MATERIALIZED VIEW IF NOT EXISTS dfs.tmp.daily_metrics AS
SELECT date_col, SUM(value) as daily_total
FROM dfs.`/data/metrics`
GROUP BY date_col;
```

### DROP MATERIALIZED VIEW

```sql
DROP MATERIALIZED VIEW [schema.]view_name
DROP MATERIALIZED VIEW IF EXISTS [schema.]view_name
```

Examples:

```sql
-- Drop a materialized view (error if not exists)
DROP MATERIALIZED VIEW dfs.tmp.sales_summary;

-- Drop only if it exists (no error if not exists)
DROP MATERIALIZED VIEW IF EXISTS dfs.tmp.old_view;
```

### REFRESH MATERIALIZED VIEW

```sql
REFRESH MATERIALIZED VIEW [schema.]view_name
```

The REFRESH command re-executes the underlying query and replaces the stored data with fresh results.

Example:

```sql
-- Refresh the materialized view with current data
REFRESH MATERIALIZED VIEW dfs.tmp.sales_summary;
```

## Query Rewriting

Drill supports automatic query rewriting where queries against base tables can be transparently rewritten to use materialized views when appropriate. This feature leverages Apache Calcite's SubstitutionVisitor for structural query matching.

### Enabling Query Rewriting

Query rewriting is controlled by the `planner.enable_materialized_view_rewrite` option:

```sql
-- Enable materialized view rewriting (enabled by default)
SET `planner.enable_materialized_view_rewrite` = true;

-- Disable materialized view rewriting
SET `planner.enable_materialized_view_rewrite` = false;
```

### How Rewriting Works

When query rewriting is enabled, Drill's query planner:

1. Discovers all available materialized views in accessible schemas
2. Filters candidates to those with COMPLETE refresh status
3. For each candidate, parses the MV's defining SQL and converts it to a relational expression
4. Uses Calcite's SubstitutionVisitor to check if the MV's query structure matches part or all of the user's query
5. If a match is found, substitutes the matching portion with a scan of the materialized view data
6. Selects the rewritten plan if it offers better performance characteristics

### Rewriting Scenarios

Query rewriting can apply in several scenarios:

**Exact Match**: The user's query exactly matches the MV definition.

```sql
-- MV definition
CREATE MATERIALIZED VIEW dfs.tmp.region_totals AS
SELECT r_regionkey, COUNT(*) as cnt FROM cp.`region.json` GROUP BY r_regionkey;

-- This query will use the MV
SELECT r_regionkey, COUNT(*) as cnt FROM cp.`region.json` GROUP BY r_regionkey;
```

**Partial Match with Additional Filters**: The user's query adds filters on top of the MV.

```sql
-- This query may use the MV and apply the filter
SELECT r_regionkey, cnt FROM dfs.tmp.region_totals WHERE cnt > 10;
```

**Aggregate Rollup**: Higher-level aggregations computed from MV aggregates.

### Viewing the Execution Plan

Use EXPLAIN to see if a materialized view is being used:

```sql
EXPLAIN PLAN FOR
SELECT r_regionkey, COUNT(*) FROM cp.`region.json` GROUP BY r_regionkey;
```

If the MV is used, the plan will show a scan of the materialized view data location rather than the original table.

## Storage Architecture

### Definition Storage

Materialized view definitions are stored as JSON files with the `.materialized_view.drill` extension in the workspace directory. This follows the same pattern as regular Drill views (`.view.drill` files).

The definition file contains:
- View name
- Defining SQL statement
- Field names and types
- Workspace schema path
- Data storage path
- Last refresh timestamp
- Refresh status (PENDING or COMPLETE)

Example definition file structure:

```json
{
  "name": "sales_summary",
  "sql": "SELECT region, SUM(amount) as total FROM sales GROUP BY region",
  "fields": [
    {"name": "region", "type": "VARCHAR"},
    {"name": "total", "type": "DOUBLE"}
  ],
  "workspaceSchemaPath": ["dfs", "tmp"],
  "dataStoragePath": "sales_summary",
  "lastRefreshTime": 1706900000000,
  "refreshStatus": "COMPLETE"
}
```

### Data Storage

Materialized view data is stored as Parquet files in a directory named `{view_name}_mv_data` within the workspace. Parquet format provides:
- Efficient columnar storage
- Compression
- Predicate pushdown support
- Schema evolution capabilities

For a materialized view named `sales_summary` in `dfs.tmp`, the storage structure would be:

```
/tmp/
  sales_summary.materialized_view.drill    # Definition file
  sales_summary_mv_data/                   # Data directory
    0_0_0.parquet                          # Data files
    0_0_1.parquet
    ...
```

## Metastore Integration

When Drill Metastore is enabled, materialized view metadata is automatically synchronized to the central metastore. This provides:
- Centralized metadata management across the cluster
- Better discoverability of materialized views
- Integration with metadata-driven query optimization

### Enabling Metastore Integration

Set the `metastore.enabled` option to enable metastore integration:

```sql
SET `metastore.enabled` = true;
```

When enabled, the following operations automatically sync to the metastore:
- CREATE MATERIALIZED VIEW: Stores MV metadata in metastore
- DROP MATERIALIZED VIEW: Removes MV metadata from metastore
- REFRESH MATERIALIZED VIEW: Updates MV metadata in metastore

### Metastore Schema

The MaterializedViewMetadataUnit stored in the metastore contains:

| Field | Type | Description |
|-------|------|-------------|
| storagePlugin | String | Storage plugin name (e.g., "dfs") |
| workspace | String | Workspace name (e.g., "tmp") |
| name | String | Materialized view name |
| owner | String | Owner username |
| sql | String | Defining SQL statement |
| workspaceSchemaPath | List | Schema path components |
| dataLocation | String | Path to data directory |
| refreshStatus | String | PENDING or COMPLETE |
| lastRefreshTime | Long | Timestamp of last refresh |
| lastModifiedTime | Long | Timestamp of last modification |

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `planner.enable_materialized_view_rewrite` | true | Enables automatic query rewriting to use materialized views |
| `metastore.enabled` | false | Enables Drill Metastore for centralized metadata storage |

## Lifecycle Management

### Creating a Materialized View

1. Parse and validate the SQL statement
2. Create the data directory in the workspace
3. Execute the defining query and write results as Parquet
4. Write the definition file with COMPLETE status
5. Sync metadata to metastore (if enabled)

### Refreshing a Materialized View

1. Read the existing definition file
2. Delete the existing data directory
3. Re-execute the defining query and write new results
4. Update the definition file with new refresh timestamp
5. Sync updated metadata to metastore (if enabled)

### Dropping a Materialized View

1. Delete the definition file
2. Delete the data directory and all contents
3. Remove metadata from metastore (if enabled)

## Limitations

Current limitations of the materialized view implementation:

1. **Full Refresh Only**: Incremental refresh is not yet supported. Each refresh completely replaces the stored data.

2. **No Automatic Refresh**: Materialized views must be manually refreshed. There is no automatic refresh mechanism based on source data changes.

3. **Single Workspace**: Materialized views can only be created in file-system based workspaces that support write operations.

4. **No Partitioning**: Materialized view data is not partitioned. All data is stored in a single directory.

5. **Query Rewriting Scope**: Query rewriting works best for exact or near-exact matches. Complex transformations may not be recognized.

## Implementation Details

### Key Classes

| Class | Package | Description |
|-------|---------|-------------|
| MaterializedView | org.apache.drill.exec.dotdrill | Data model for MV definition |
| DrillMaterializedViewTable | org.apache.drill.exec.planner.logical | TranslatableTable implementation |
| MaterializedViewHandler | org.apache.drill.exec.planner.sql.handlers | SQL handler for CREATE/DROP/REFRESH |
| MaterializedViewRewriter | org.apache.drill.exec.planner.logical | Query rewriting using Calcite |
| SqlCreateMaterializedView | org.apache.drill.exec.planner.sql.parser | SQL parser for CREATE |
| SqlDropMaterializedView | org.apache.drill.exec.planner.sql.parser | SQL parser for DROP |
| SqlRefreshMaterializedView | org.apache.drill.exec.planner.sql.parser | SQL parser for REFRESH |

### Parser Grammar

The SQL parser grammar for materialized views is defined in `parserImpls.ftl`. The grammar supports:
- CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS]
- DROP MATERIALIZED VIEW [IF EXISTS]
- REFRESH MATERIALIZED VIEW

### Query Rewriting Process

The MaterializedViewRewriter class implements query rewriting:

1. **Discovery**: Scans all accessible schemas for materialized views
2. **Filtering**: Selects candidates with COMPLETE refresh status
3. **Matching**: Uses Calcite's SubstitutionVisitor to match query structures
4. **Substitution**: Replaces matched portions with MV scans
5. **Selection**: Returns the first successful substitution

The SubstitutionVisitor performs structural matching by:
- Comparing relational expression trees
- Identifying equivalent subexpressions
- Handling column renaming and reordering
- Supporting partial matches with residual predicates

## Testing

### Unit Tests

- `TestMaterializedViewSqlParser`: Parser syntax validation
- `TestMaterializedView`: Data model serialization tests

### Integration Tests

- `TestMaterializedViewSupport`: End-to-end CREATE/DROP/REFRESH tests
- `TestMaterializedViewRewriting`: Query rewriting scenarios

Run the tests with:

```bash
mvn test -pl exec/java-exec -Dtest='TestMaterializedView*'
```

## Future Enhancements

Planned improvements for future releases:

1. **Incremental Refresh**: Support for refreshing only changed data based on source table modifications.

2. **Automatic Refresh**: Scheduled or trigger-based automatic refresh mechanisms.

3. **Partitioned Storage**: Partition materialized view data for better query performance.

4. **Cost-Based Selection**: When multiple MVs match, select based on estimated query cost.

5. **Staleness Tracking**: Track source table changes to identify stale materialized views.

6. **INFORMATION_SCHEMA Integration**: Expose materialized views in INFORMATION_SCHEMA tables.
