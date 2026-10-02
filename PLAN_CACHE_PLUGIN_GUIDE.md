<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Adding plan-cache support to a plugin

Plan caching is disabled by default. A plugin is eligible only when both its
metadata interface and every participating `GroupScan` opt in. The engine binds
SQL literals in serialized Drill expressions; the plugin remains responsible for
rebuilding scan state derived from those expressions.

## Storage-plugin API

Implement these methods in [StoragePlugin](exec/java-exec/src/main/java/org/apache/drill/exec/store/StoragePlugin.java):

```java
boolean supportPlanCache();
PlanCacheTable planCacheTable(DrillTableSelection selection) throws IOException;
String planCacheTableVersion(String identifier) throws IOException;
```

`planCacheTable` is called during a miss. Return a stable, opaque physical table
identifier and a compatibility version. Return `null` for unsupported selections.
`planCacheTableVersion` is called on a hit with that same identifier; it should
read the current version without rebuilding the SQL schema. Return `null` if the
table is missing or unsupported. Throwing also causes a fallback to normal
planning. The engine separately compares the storage-plugin configuration.

The version must change whenever the cached plan's table definition becomes
incompatible. Do not use a data snapshot number if compatible data updates should
keep hitting. A path alone is insufficient when replacement can change the
schema or table identity.

For a DFS format, implement `supportPlanCache()` and both
`planCacheTableVersion(FileSelection)` and `planCacheTableVersion(Path)` in
[FormatPlugin](exec/java-exec/src/main/java/org/apache/drill/exec/store/dfs/FormatPlugin.java).
[FileSystemPlugin](exec/java-exec/src/main/java/org/apache/drill/exec/store/dfs/FileSystemPlugin.java)
provides the storage-plugin bridge. The selection overload validates the original
selection; the path overload validates a hit without rediscovering the schema.

## Scan API and JSON reconstruction

Override `GroupScan.supportPlanCache()`; its default is `false`. The engine rejects
a plan containing even one unsupported scan.

Keep the bound `LogicalExpression` in the scan's JSON. Converting a predicate to
a native filter or file list and dropping the source expression loses the
information needed to bind the next query. On deserialization:

1. Resolve the plugin and reload current table metadata.
2. Translate the newly bound expression into the plugin's native predicate.
3. Recompute value-dependent row bounds, filters, files/splits, region locations
   and assignments. Preserve residual filtering for unsupported predicates.
4. Reject reconstruction if the original pushdown guarantees cannot be met.

Serialize stable inputs rather than live connections, execution resources or old
endpoint assignments. Validate the round trip with Drill's `PhysicalPlanReader`.
If a parameter changes projection, schema, limit or another derived field that
cannot be rebuilt, keep that argument structural or leave the scan ineligible.

For explain output containing expressions, use
`ExpressionStringBuilder.toExplainString()` via `getExplainDigest()` to display
`?index`. Continue using the regular expression serialization for JSON so slot
numbers survive the round trip.

## Reference implementations

| Plugin | Compatibility version | Reconstruction |
| --- | --- | --- |
| [HBaseStoragePlugin](contrib/storage-hbase/src/main/java/org/apache/drill/exec/store/hbase/HBaseStoragePlugin.java) / [HBaseGroupScan](contrib/storage-hbase/src/main/java/org/apache/drill/exec/store/hbase/HBaseGroupScan.java) | Table descriptor fingerprint | Retains the selection before pushdown and the original predicate; rebuilds native row-key bounds/filters and discovers current regions. |
| [IcebergFormatPlugin](contrib/format-iceberg/src/main/java/org/apache/drill/exec/store/iceberg/format/IcebergFormatPlugin.java) / [IcebergGroupScan](contrib/format-iceberg/src/main/java/org/apache/drill/exec/store/iceberg/IcebergGroupScan.java) | Table UUID and schema ID | Reloads the table and runs `planTasks()` for the current predicate and snapshot. Metadata tables and explicit snapshot selection return no support. |

HBase descriptor equality means definition compatibility; a replacement with an
identical descriptor can still be read safely because regions and filters are
rebuilt. Iceberg uses a UUID to distinguish tables recreated at the same path.

## Required correctness checks

Compare complete results with caching disabled, after the first planning pass,
and after a hit with different literals. Exercise cross-connection reuse,
pushdown and residual filtering, and an empty scan after rebinding. Verify that
compatible data updates are visible on hits, while incompatible schema changes,
deletions, plugin configuration changes and failed metadata reads reject the old
entry. Include joins or subqueries so every table dependency is checked, and
confirm that a mixed plan containing an unsupported scan is rejected.

Check `planCacheHit` in the query profile. In isolated tests, the Drillbit's hit
counter can also establish reuse:

```java
PlanCache cache = cluster.drillbit().getContext().getPlanCache();
first.queryBuilder().sql(firstSql).run();
cache.awaitWrites(); // Test synchronization for asynchronous publication.
long before = cache.getHitCount();
second.queryBuilder().sql(sameTemplateWithNewValues).run();
assertEquals(before + 1, cache.getHitCount());
```

The counter is shared across the Drillbit; use per-query profiles for concurrent
workloads. `awaitWrites()` is a test helper, not a SQL operation.

## Scope of the contract

The cache validates saved physical identifiers. Sessions with temporary tables
or mutable user/public aliases bypass caching, because those mappings can redirect
a SQL name without changing the old physical table. Statistics changes alone do
not trigger re-optimization.
