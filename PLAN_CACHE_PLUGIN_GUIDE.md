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

Plan caching is disabled by default. A plugin opts in through its metadata interface and guarantees that every scan of an eligible selection can be safely rebuilt. The engine binds SQL literals in serialized Drill expressions; the plugin remains responsible for rebuilding scan state derived from those expressions.

## Storage-plugin API

Implement these methods in [StoragePlugin](exec/java-exec/src/main/java/org/apache/drill/exec/store/StoragePlugin.java):

```java
boolean supportPlanCache(DrillTableSelection selection);
PlanCacheTable planCacheTable(DrillTableSelection selection) throws IOException;
```

`planCacheTable` is called before each lookup to capture the current query's table dependencies. Return a stable, opaque physical table identifier and a compatibility version. Return `null` for missing tables or unsupported selections. Failed metadata reads also cause a fallback to normal planning. The engine compares this complete context snapshot, including effective options and storage-plugin configurations, with the cached entry's snapshot. A miss retains the same snapshot for publication after successful execution.

The engine first calls `supportPlanCache(selection)` for every participating plugin and returns immediately if any selection is unsupported. Its default returns `false`. A plugin can cast `DrillTableSelection` to its own selection type to decide eligibility. Keep this check free of table-version reads. Only after all checks pass does the engine call `planCacheTable` and compute configuration and option fingerprints.

The version must change whenever the cached plan's table definition becomes
incompatible. Do not use a data snapshot number if compatible data updates should
keep hitting. A path alone is insufficient when replacement can change the
schema or table identity.

DFS formats use the same two-method contract in [FormatPlugin](exec/java-exec/src/main/java/org/apache/drill/exec/store/dfs/FormatPlugin.java), with `FileSelection` as the input:

```java
boolean supportPlanCache(FileSelection selection);
PlanCacheTable planCacheTable(FileSelection selection) throws IOException;
```

[FileSystemPlugin](exec/java-exec/src/main/java/org/apache/drill/exec/store/dfs/FileSystemPlugin.java) delegates eligibility and metadata reads to the selected format plugin. It prefixes the returned table identifier with the format name to distinguish formats within the storage plugin.

## Scan API and JSON reconstruction

Declaring plugin support covers every `GroupScan` produced for an eligible selection. Ensure that all planning, pushdown and cloning paths preserve the inputs needed for reconstruction. Unsupported selections must be rejected during snapshot construction.

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
cannot be rebuilt, keep that argument structural or leave the selection ineligible.

Keep the regular expression serialization for JSON so parameter slot numbers survive the round trip.

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
confirm that queries involving an unsupported plugin or selection bypass caching.

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

The cache compares the current query's physical identifiers and versions with those captured for the cached plan. Sessions with temporary tables or mutable user/public aliases bypass caching. Statistics changes alone do not trigger re-optimization.
