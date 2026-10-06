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

# Physical plan cache

Drill normally validates and optimizes every SQL query, even when the same query
shape has just been executed. The plan cache reuses that work for repeated reads.
It caches a physical plan, not query results: every execution reads the current
source data.

The feature is opt-in and disabled by default:

```sql
ALTER SESSION SET planner.enable_plan_cache = true;
```

Use `ALTER SYSTEM` instead of `ALTER SESSION` to set the default for new queries.

## The basic idea

These queries can share a plan when the scan plugin supports it:

```sql
SELECT row_key FROM hbase.usertable WHERE row_key = 'user00000101';
SELECT row_key FROM hbase.usertable WHERE row_key = 'user00000250';
```

Drill replaces eligible data literals with typed slots when building the cache
key. Each slot carries its current value during the first planning pass. The
resulting physical expressions retain the slot number, so a later query can
replace the value without running the optimizer again. Literal type, numeric
precision and scale, and string length are part of the template.

```mermaid
flowchart LR
  SQL[Parse SQL and build template] --> Lookup{Compatible cached plan?}
  Lookup -->|Yes| Bind[Bind current values and rebuild scans]
  Lookup -->|No| Plan[Validate and optimize normally]
  Bind --> Execute[Execute against current data]
  Plan --> Execute
  Execute -->|Successful first execution| Save[Publish immutable plan snapshot]
```

The cache belongs to one Drillbit and can be shared across connections. Its key
includes the query user, default schema and SQL template. Each entry also records
effective planner options, plugin configurations and the compatibility version
of every referenced table, including tables inside joins, CTEs and subqueries.
A mismatch or failure falls back to ordinary planning.

The cached representation is immutable physical-plan JSON. A hit creates a new
plan object graph and binds the new values throughout its expressions. Each
participating scan must also rebuild any state derived from those values.
HBase rebuilds row-key bounds and remote filters, then discovers current regions.
Iceberg reloads table metadata and plans tasks for the current snapshot and
predicate. Reusing the first execution's filters or file list would be incorrect.

## What can be reused

Read queries can include joins, aggregates, subqueries, CTEs, ordering and limits.
Every source plugin must explicitly support caching and guarantee safe reconstruction of its scans. This change opts in HBase and Iceberg; other plugins retain the default of no support.

Constants that determine SQL structure stay in the key: group/order expressions,
window definitions, limit/offset, field selectors, and function configuration
arguments such as `DATE_PART`'s unit or `CONVERT_FROM`'s encoding. DATE, TIME,
TIMESTAMP and INTERVAL literals also stay in the key because their parameter
binding is not supported. Other eligible literals in the same query can still
be rebound. Queries with no slots can reuse their complete template.

Writes, DDL, existing dynamic parameters, volatile/query-context functions and
sessions containing temporary tables or mutable aliases bypass the cache.
Unsupported scans, views without a compatible table identity, Iceberg metadata tables and explicit
snapshot selections use normal planning.

## Lifetime and observation

An entry is published only after the first query succeeds. A bounded background
queue performs serialization and read-back validation; a full queue simply skips
publication. The cache is bounded by serialized-plan weight and entries expire
after ten minutes. Cached row-count estimates come from the first optimization;
new values do not trigger a new cost-based plan choice.

The query profile's **Plan Cache Hit** field, or JSON `planCacheHit`, identifies a
successful hit. Explain shows slot names and this execution's parameter values;
those alone do not prove a hit.

See the [plugin developer guide](PLAN_CACHE_PLUGIN_GUIDE.md) for the integration
contract. The accompanying pull request description contains validation results
and benchmark methodology.
