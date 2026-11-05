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

Implement these methods in [StoragePlugin](../../exec/java-exec/src/main/java/org/apache/drill/exec/store/StoragePlugin.java):

```java
boolean supportPlanCache(DrillTableSelection selection);
PlanCacheTable planCacheTable(DrillTableSelection selection) throws IOException;
```

`planCacheTable` is called before each lookup to capture the current query's table dependencies. Return a stable, opaque physical table identifier and a compatibility version. Return `null` for missing tables or unsupported selections. Failed metadata reads also cause a fallback to normal planning. The engine compares this complete context snapshot, including effective options and storage-plugin configurations, with the cached entry's snapshot. A miss retains the same snapshot for publication after successful execution.

The engine first calls `supportPlanCache(selection)` for every participating plugin and returns immediately if any selection is unsupported. Its default returns `false`. A plugin can cast `DrillTableSelection` to its own selection type to decide eligibility. Keep this check free of table-version reads. Only after all checks pass does the engine call `planCacheTable` and compute configuration and option fingerprints.

The plugin version must change whenever the cached plan's native table definition becomes
incompatible. Do not use a data snapshot number if compatible data updates should
keep hitting. A path alone is insufficient when replacement can change the
schema or table identity.

The core combines this native version with a fingerprint of the provided schema
obtained from the resolved `DrillTable`'s `SchemaProvider`. Format plugins do not
need to include `.drill.schema` in their own version. The combined version lives
in the existing table-version context, so changes to either definition invalidate
the entry without changing its key. A disabled or absent provided schema uses an
explicit no-schema state; unreadable schemas bypass caching. Hashing uses parsed
schema contents, including defaults and properties, rather than file timestamps
or the schema-container format version.

DFS formats use the same two-method contract in [FormatPlugin](../../exec/java-exec/src/main/java/org/apache/drill/exec/store/dfs/FormatPlugin.java), with `FileSelection` as the input:

```java
boolean supportPlanCache(FileSelection selection);
PlanCacheTable planCacheTable(FileSelection selection) throws IOException;
```

[FileSystemPlugin](../../exec/java-exec/src/main/java/org/apache/drill/exec/store/dfs/FileSystemPlugin.java) delegates eligibility and metadata reads to the selected format plugin. It prefixes the returned table identifier with the format name to distinguish formats within the storage plugin.

## Scan API and JSON reconstruction

Declaring plugin support covers every `GroupScan` produced for an eligible selection. Ensure that all planning, pushdown and cloning paths preserve the inputs needed for reconstruction. Unsupported selections must be rejected during snapshot construction.

For the initial text integration, predicates stay in Drill's Filter operator.
The cache binds those expressions and uses `PhysicalPlanReader.readPhysicalPlan`
to construct a fresh plan. `EasyGroupScan` already rebuilds file blocks and
assignments when read from JSON, so no cache-specific scan constructor is needed.

A future plugin with predicate pushdown must preserve the source
`LogicalExpression` and rebuild every value-dependent native filter, row bound,
file list and assignment after binding. If reconstruction cannot preserve its
pushdown guarantees, leave the selection ineligible. A failed reconstruction
invalidates the entry and replans the SQL; it must never reuse native state
containing the previous bindings.

Serialize stable inputs rather than live connections, execution resources or old
endpoint assignments. Validate the round trip with Drill's `PhysicalPlanReader`.
The cache performs a trial bind with the first execution's literal values before
publishing an entry. Failed expression binding or scan reconstruction prevents
publication; successful trial binding does not replace compatibility checks on
later hits or prove reconstruction is safe for every possible new value.
If a parameter changes projection, schema, limit or another derived field that
cannot be rebuilt, keep that argument structural or leave the selection ineligible.

Keep the regular expression serialization for JSON so parameter slot numbers survive the round trip.

## Reference implementation

[TextFormatPlugin](../../exec/java-exec/src/main/java/org/apache/drill/exec/store/easy/text/TextFormatPlugin.java)
opts in explicit single-file selections. Its table identifier is the qualified
file path; its compatibility version is the current file length and modification
time. It reads fresh status rather than the status retained in `FileSelection`.
Any change to either version component conservatively forces a replan, including
appends and header changes. Writers must update modification times on content
changes; same-length edits preserving modification time are not detected.
Directories and globs are excluded, including directories containing only one
file, because their cached file lists could otherwise retain old partition pruning.

[EasyGroupScan](../../exec/java-exec/src/main/java/org/apache/drill/exec/store/dfs/easy/EasyGroupScan.java)
uses its existing JSON constructor to rebuild blocks and endpoint assignments.
Predicates stay in Drill, with no native filter serialization or rebuilding.
[Single-file integration tests](../../exec/java-exec/src/test/java/org/apache/drill/exec/planner/sql/TestTextPlanCache.java)
exercise misses, cross-connection hits, changed bindings, invalidation and bypass.

## Required correctness checks

Compare complete results with caching disabled, after the first planning pass,
and after a hit with different literals. Exercise cross-connection reuse,
filter rebinding and an empty result after rebinding. For plugins with native
pushdown, also check native and residual filtering. Verify that data updates are
visible after a hit or conservative invalidation, while incompatible schema changes,
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
