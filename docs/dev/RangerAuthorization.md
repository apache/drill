# Drill Ranger Authorization Quick Start Guide

This document describes the architecture, configuration, and development
conventions of the Apache Ranger authorization integration for Drill. It is
intended for contributors who want to extend or debug the Ranger integration,
and for operators who want to understand the column-level authorization
behavior end-to-end.

## 1. Architecture Overview

The Ranger integration spans three layers:

```
+--------------------------------------------------------------+
|  exec/java-exec (Drillbit, JDK 11)                          |
|  +-------------------------+      +------------------------+ |
|  | SqlConverter (toRel)    | ---> | ColumnAccessChecker    | |
|  | DrillCalciteCatalogReader|     | (RelShuttle, column)   | |
|  | Drillbit (startup)      |      +------------------------+ |
|  +-------------------------+             |                    |
|                                          v                    |
|  +-------------------------------+  +-----------------------+ |
|  | AccessAuthorizerFactory       |  | DrillAccessControl    | |
|  | (singleton, config-driven)    |  | (SPI implementation)  | |
|  +-------------------------------+  +-----------------------+ |
|                                          |                    |
|  +----------------------------------------------------------------+
|  | drill-ranger-plugin (JDK 11, deployed to jars/3rdparty/)      |
|  |  DrillAuthorizer  DrillAccessResource  DrillRangerAccessRequest|
|  |  RangerDrillPlugin  RangerBaseAuthorizer                       |
|  +----------------------------------------------------------------+
```

The Ranger Admin-side service plugin (`validateConfig` / `lookupResource`,
which talks to the Drill REST API `POST /query.json`) is implemented directly
in the Ranger codebase, not in this repository.

### 1.1 Modules

| Module                | JDK | Deployed To               | Responsibility                                                                                    |
| --------------------- | --- | ------------------------- | ------------------------------------------------------------------------------------------------- |
| `drill-ranger-plugin` | 11  | Drillbit `jars/3rdparty/` | Drillbit-side authorization: wraps `RangerBasePlugin`, exposes `DrillAccessControl` facade        |
| `exec/security-spi`   | 11  | Drillbit `jars/`          | JDK-only authorization SPI (`AccessAuthorizer`, `UserIdentity`, ...); shared by engine and plugin |
| `exec/java-exec`      | 11  | Drillbit                  | Integration hooks: `AccessAuthorizerFactory`, `ColumnAccessChecker`, `DrillCalciteCatalogReader`  |

## 2. Resource Model

Ranger policies for Drill use a **four-level resource hierarchy**:

```
datasource  →  schema  →  table  →  column
```

| Level      | Ranger resource key | Example             | Notes                                 |
| ---------- | ------------------- | ------------------- | ------------------------------------- |
| datasource | `datasource`        | `mysql`             | Drill storage plugin name             |
| schema     | `schema`            | `shf`               | Schema path WITHOUT datasource prefix |
| table      | `table`             | `orders`            | Table name                            |
| column     | `column`            | `id`, `amount`, `*` | `*` matches all columns               |

**Critical conventions**:

* Resource keys must be **lowercase** (`datasource`, not `DATASOURCE`). Ranger
  validates names against `[a-z_-]` only (error code 2022).

* The `schema` value must NOT include the datasource prefix. Use `shf`, not
  `mysql.shf`.

* Access type name in the service-def must exactly match what the code sends —
  both uppercase `SELECT`.

### 2.1 DDL Authorization (CREATE / DROP)

Besides the table- and column-level `SELECT` checks issued during SQL
validation (in `DrillCalciteCatalogReader` / `ColumnAccessChecker`), the
engine checks CREATE/DROP privileges on the **target object** of DDL
statements, in the DDL handlers themselves (`DdlAccessChecker`):

| Statement                           | AccessType checked | Resource checked  |
| ----------------------------------- | ------------------ | ----------------- |
| `CREATE TABLE ... AS SELECT` (CTAS) | `CREATE`           | the new table     |
| `CREATE VIEW ... AS SELECT`         | `CREATE`           | the new view      |
| `DROP TABLE`                        | `DROP`             | the dropped table |
| `DROP VIEW`                         | `DROP`             | the dropped view  |

Semantics:

* **Two-layer checks for CTAS / CREATE VIEW**: the query part is still subject
  to the `SELECT` checks above (source tables and columns), and the target
  object additionally requires `CREATE`. A statement is executed only when
  both checks pass.

* **`CREATE VIEW OR REPLACE`** is treated as a single CREATE operation — no
  extra DROP privilege is required.

* **Temporary tables** (`CREATE TEMPORARY TABLE`, dropping a temporary table)
  are session-scoped objects (UUID names, invisible to other users) and
  bypass authorization.

* **Authorization before existence checks**: the permission check runs before
  "table not found" style errors, so an unauthorized user cannot probe object
  existence through differing error messages.

* **Denial surfaces as a** **`PERMISSION ERROR`** (`UserException.permissionError`),
  exactly like a denied SELECT.

* Resource mapping reuses `TableAccessResource.resolve` — the same
  datasource/schema/table triple the SELECT checks address. For objects in
  schema-less locations (e.g. `dfs` root) the default schema `default` is
  synthesized (`datasource=dfs, schema=default`).

A sample policy allowing all DDL creates in a schema (table-level
`SELF_OR_DESCENDANTS` matching, so `table=*` covers every table):

| Field       | Value    |
| ----------- | -------- |
| datasource  | `dfs`    |
| schema      | `tmp`    |
| table       | `*`      |
| access type | `CREATE` |
| user/group  | (user)   |

> **Upgrade note (behavior change)**: once authorization is enabled, users
> without a `CREATE` policy are now denied CTAS / CREATE VIEW (previously
> allowed when only SELECT was checked). Same for `DROP`. Ranger denies by
> default — plan policies accordingly when enabling.

## 3. Configuration

### 3.1 Drillbit side (`drill-module.conf`)

```hocon
drill.exec.security.ranger: {
  enabled: true,
  service.name: "drill",
  impl: "org.apache.drill.exec.security.ranger.RangerAccessAuthorizer"
}
```

| Key                                       | Default                                                        | Description                                                     |
| ----------------------------------------- | -------------------------------------------------------------- | --------------------------------------------------------------- |
| `drill.exec.security.ranger.enabled`      | `false`                                                        | Master switch. `false` → `AllowAllAccessAuthorizer` (fail-open) |
| `drill.exec.security.ranger.service.name` | `"drill"`                                                      | Ranger service name registered in Ranger Admin                  |
| `drill.exec.security.ranger.impl`         | `org.apache.drill.exec.security.ranger.RangerAccessAuthorizer` | `AccessAuthorizer` implementation class                         |

### 3.2 Ranger Admin side

* `drill.connection.url` — Drill REST API URL, e.g. `http://drillbit-host:8047`.
  Bare `host:port` is normalized to `http://host:port`.

* `username` / `password` — Drill user for `validateConfig` and
  `lookupResource` REST calls (HTTP Basic auth).

### 3.3 Deployment Steps

After building the distribution, two deployment actions are required to make
Ranger Admin recognize Drill as an authorization provider.

#### Step 1: Update Ranger config files in Drill

Copy the Ranger configuration files into Drill's `conf/` directory and edit
them to match your environment.

```bash
DRILL_HOME=/opt/drill

cp distribution/src/main/resources/ranger/ranger-drill-security.xml  $DRILL_HOME/conf/
cp distribution/src/main/resources/ranger/ranger-drill-audit.xml     $DRILL_HOME/conf/
```

Then edit `$DRILL_HOME/conf/ranger-drill-security.xml`:

| Property                              | Value to set                                                                                            |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `ranger.plugin.drill.policy.rest.url` | `http://<ranger-admin-host>:6080`                                                                       |
| `ranger.plugin.drill.service.name`    | The Ranger service name (must match `drill.exec.security.ranger.service.name` in `drill-override.conf`) |


### 3.4 Ranger policy files

| File                           | Location                                  | Purpose                                                        |
| ------------------------------ | ----------------------------------------- | -------------------------------------------------------------- |
| `ranger-drill-security.xml`    | `distribution/src/main/resources/ranger/` | Ranger plugin config (policy cache dir, polling interval)      |
| `ranger-drill-audit.xml`       | `distribution/src/main/resources/ranger/` | Audit sink config (HDFS, Solr, etc.)                           |

### 3.5 Audit Log Configuration

By default, Ranger audit records are written to the **Drillbit log** via log4j.
This is the simplest setup and requires no external dependencies. The default
values in `ranger-drill-audit.xml` are:

| Property                               | Default                                            | Description                                                                 |
| -------------------------------------- | -------------------------------------------------- | --------------------------------------------------------------------------- |
| `xasecure.audit.is.enabled`            | `true`                                             | **Master switch.** Must be `true` for any audit destination to work.        |
| `xasecure.audit.log4j.is.enabled`      | `true`                                             | Audit to log4j (Drillbit log). **Enabled by default.**                      |
| `xasecure.audit.solr.is.enabled`       | `false`                                            | Audit to a Solr collection. Disabled by default.                            |
| `xasecure.audit.solr.url`              | `http://ranger-admin-host:6083/solr/ranger_audits` | Solr endpoint (used only when `solr.is.enabled=true`).                      |
| `xasecure.audit.hdfs.is.enabled`       | `false`                                            | Audit to HDFS. Disabled by default.                                         |
| `xasecure.audit.hdfs.config.directory` | `hdfs://namenode:8020/ranger/audit`                | HDFS audit directory (used only when `hdfs.is.enabled=true`).               |
| `xasecure.audit.hdfs.config.file`      | `/etc/hadoop/conf/core-site.xml`                   | Hadoop config file for HDFS client (used only when `hdfs.is.enabled=true`). |

> **Property name caveat:** The property names above are verified from
> `AuditProviderFactory` bytecode in `ranger-audit-core-2.8.0.jar`. The older
> names `xasecure.audit.is.audit.to.{log4j,solr,hdfs}` are **not** read by
> `AuditProviderFactory` and have no effect.

When `xasecure.audit.log4j.is.enabled=true`, `AuditProviderFactory` loads
`org.apache.ranger.audit.provider.Log4jAuditProvider` (from
`ranger-audit-dest-log4j` jar) via `Class.forName()`. That class logs audit
events through an SLF4J logger named
`xaaudit.org.apache.ranger.audit.provider.Log4jAuditProvider`
(the prefix `xaaudit.` is prepended to the class name in the static
initializer).

**Important:** For audit records to reach `drillbit.log`, a logger entry for
this logger name must be present in `logback.xml`. The shipped
`distribution/src/main/resources/logback.xml` already includes this entry:

```xml
<logger name="xaaudit.org.apache.ranger.audit.provider.Log4jAuditProvider"
        additivity="false" level="info">
  <appender-ref ref="FILE" />
</logger>
```

Without this entry, audit events (logged at INFO) fall through to the root
logger (`error` level, STDOUT only) and are silently dropped.

#### Verifying audit output in drillbit.log

1. **Ranger Admin side** — create a policy that either allows or denies the
   test user access to a table (e.g. `mysql.shf.orders`). Make sure the policy
   is saved and the Drillbit has pulled it (default poll interval is 30 s).

2. **Drill side** — run a query that triggers an authorization decision:

   ```sql
   SELECT id FROM mysql.shf.orders;
   ```

3. **Check drillbit.log** — look for audit entries from the
   `xaaudit.org.apache.ranger.audit.provider.Log4jAuditProvider` logger:

   ```bash
   grep -i "xaaudit\|ranger\|audit\|access" $DRILL_HOME/log/drillbit.log | tail -20
   ```

   A typical audit log line looks like:

   ```
   2026-08-11 10:30:45,123 [...] INFO  xaaudit.org.apache.ranger.audit.provider.Log4jAuditProvider -
     accessType=SELECT resource=mysql.shf.orders reqUser=alice ...
     action=accessAllowed result=1
   ```

   For a denied query, `action=accessDenied` / `result=0` is logged instead.
   If nothing appears, verify:

   * `ranger-audit-dest-log4j-2.8.0.jar` is present in `$DRILL_HOME/jars/3rdparty/`

   * `xasecure.audit.is.enabled=true` and `xasecure.audit.log4j.is.enabled=true`
     in `$DRILL_HOME/conf/ranger/ranger-drill-audit.xml`

   * The `logback.xml` entry above is present

   * The Drillbit was restarted after editing configuration

#### Switching the audit destination

To send audit records to **Solr** instead of (or in addition to) the Drillbit
log, edit `$DRILL_HOME/conf/ranger/ranger-drill-audit.xml` after deployment:

```xml
<property>
  <name>xasecure.audit.solr.is.enabled</name>
  <value>true</value>
</property>
<property>
  <name>xasecure.audit.solr.url</name>
  <value>http://your-ranger-admin-host:6083/solr/ranger_audits</value>
</property>
```

To send audit records to **HDFS**:

```xml
<property>
  <name>xasecure.audit.hdfs.is.enabled</name>
  <value>true</value>
</property>
<property>
  <name>xasecure.audit.hdfs.config.directory</name>
  <value>hdfs://your-namenode:8020/ranger/audit</value>
</property>
<property>
  <name>xasecure.audit.hdfs.config.file</name>
  <value>/etc/hadoop/conf/core-site.xml</value>
</property>
```

Multiple sinks can be enabled simultaneously — for example, keep `log4j=true`
as a local fallback while also forwarding to Solr for centralized search. After
changing the file, restart the Drillbit for the new settings to take effect.

## 4. Authorization Policy Test Cases

The following test cases document the expected authorization behavior with the
sample policies below. All SQL runs against tables `mysql.shf.orders` and
`mysql.shf.users`.

### 4.1 Sample Ranger Policies

**Policy A — users table, all columns**

| Field       | Value             |
| ----------- | ----------------- |
| datasource  | `mysql`           |
| schema      | `shf`             |
| table       | `users`           |
| column      | `*`               |
| access type | `SELECT`          |
| user/group  | (authorized user) |

**Policy B — orders table, specific columns only**

| Field       | Value             |
| ----------- | ----------------- |
| datasource  | `mysql`           |
| schema      | `shf`             |
| table       | `orders`          |
| column      | `id`, `amount`    |
| access type | `SELECT`          |
| user/group  | (authorized user) |

**Policy C — DDL creates in the shf schema**

| Field       | Value             |
| ----------- |-------------------|
| datasource  | `mysql`,`dfs`     |
| schema      | `shf` ,`test`     |
| table       | `*`               |
| access type | `CREATE`          |
| user/group  | (authorized user) |

Under these policies, the `orders.user_id` and `orders.order_date` columns are
NOT authorized. The `users` table allows all columns via `*`. Policy C grants
CTAS / CREATE VIEW (but not DROP) anywhere in `dfs.test`.

### 4.2 Test Cases

| #  | SQL                                                                                                                   | Expected        | Why                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| -- |-----------------------------------------------------------------------------------------------------------------------| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1  | `SELECT id, amount FROM orders`                                                                                       | **PASS**        | `id`, `amount` both in Policy B                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 2  | `SELECT * FROM orders`                                                                                                | **DENY**        | `*` expands to all columns including `user_id`, which is not in Policy B                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| 3  | `SELECT sum(id) FROM orders`                                                                                          | **PASS**        | Aggregate on authorized column `id`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 4  | `SELECT id FROM orders WHERE user_id > 1`                                                                             | **DENY**        | `user_id` in WHERE clause is checked (LogicalFilter condition traced)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 5  | `SELECT sum(user_id) FROM orders`                                                                                     | **DENY**        | `user_id` not in Policy B                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| 6  | `SELECT o.id, u.name, o.amount, o.order_date FROM orders o INNER JOIN users u ON o.user_id = u.id`                    | **DENY**        | `o.user_id` appears in JOIN condition; `o.order_date` not in Policy B                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 7  | `SELECT o.id, u.name, o.amount FROM orders o INNER JOIN users u ON o.id = u.id`                                       | **PASS**        | All referenced columns authorized: `o.id`, `u.name` (via `*`), `o.amount`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| 8  | `SELECT u.*, o.amount FROM orders o JOIN users u ON o.id = u.id WHERE o.amount > (SELECT AVG(amount) FROM orders)`    | **PASS**        | `u.*` authorized via Policy A; `o.amount` and subquery `amount` in Policy B                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 9  | `SELECT o.*, u.name FROM orders o JOIN users u ON o.user_id = u.id WHERE o.amount > (SELECT AVG(amount) FROM orders)` | **DENY**        | `o.*` expands to `user_id` (not authorized); `o.user_id` in JOIN condition                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 10 | `SELECT o.id, u.name FROM orders o JOIN users u ON o.id = u.id WHERE o.amount > (SELECT AVG(amount) FROM orders)`     | **PASS**        | All columns authorized; subquery only references `amount`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| 11 | `SELECT o.id, u.name FROM orders o JOIN users u ON o.id = u.id WHERE o.amount > (SELECT sum(user_id) FROM orders)`    | **DENY**        | Scalar subquery references `user_id` (not authorized). **Requires RexSubQuery handling.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| 12 | `SELECT name FROM users WHERE id IN (SELECT DISTINCT user_id FROM orders)`                                            | **DENY**        | IN-subquery references `user_id` (not authorized). **Requires RexSubQuery handling.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 13 | `SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)`                              | **DENY**        | EXISTS-subquery references `o.user_id` (not authorized). **Requires RexSubQuery handling.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 14 | `WITH t AS (SELECT * FROM mysql.shf.orders) SELECT * FROM t`                                                          | **DENY**        | CTE body `SELECT *` expands to all columns including `order_date` (not in Policy B). CTE is inlined before `ColumnAccessChecker` runs, so the underlying `orders` TableScan is checked.                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 15 | `WITH t AS (SELECT id FROM mysql.shf.orders) SELECT * FROM t`                                                         | **PASS**        | CTE body only selects `id` (in Policy B); outer `SELECT *` from CTE resolves to the same authorized column.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 16 | `WITH t AS (SELECT id, order_date FROM mysql.shf.orders) SELECT order_date FROM t`                                    | **DENY**        | CTE body selects `order_date` (not in Policy B). Column check traces back to the underlying `orders` TableScan after CTE inlining.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 17 | `WITH t AS (SELECT id, order_date FROM mysql.shf.orders) SELECT id FROM t`                                            | **DENY**        | Even though the outer query only projects `id`, the CTE body references `order_date` (not in Policy B). After inlining, the `orders` TableScan has both columns referenced, so the check fails on `order_date`.                                                                                                                                                                                                                                                                                                                                                                                                    |
| 18 | `WITH a AS (SELECT id FROM mysql.shf.orders), b AS (SELECT id FROM mysql.shf.users) SELECT * FROM b`                  | **PASS**        | Multiple CTEs: `a` selects `id` from `orders` (Policy B); `b` selects `id` from `users` (Policy A via `*`). Outer query selects from `b` only. All referenced columns authorized.                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 19 | `SELECT o.order_date FROM mysql.shf.orders o`                                                                         | **DENY**        | Table alias is transparent: `o.order_date` resolves to `orders.order_date` (not in Policy B). `RexInputRef` indexes point to row-type positions, and `getColumnOrigins` traces through to the underlying `TableScan` regardless of alias.                                                                                                                                                                                                                                                                                                                                                                          |
| 20 | `SELECT a.id FROM mysql.shf.users a JOIN mysql.shf.orders b ON a.id = b.id`                                           | **PASS**        | Join with aliases: `a.id` resolves to `users.id` (Policy A via `*`); `b.id` resolves to `orders.id` (Policy B). Join condition columns also authorized.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 21 | `SELECT o.id FROM mysql.shf.orders o`                                                                                 | **PASS**        | Simple table alias on an authorized column; `o.id` resolves to `orders.id` (Policy B).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 22 | `SELECT o.id FROM mysql.shf.orders o WHERE o.amount = '150.0'`                                                        | **PASS**        | Alias in WHERE clause on an authorized column; `o.amount` resolves to `orders.amount` (Policy B).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 23 | `SELECT o.id FROM mysql.shf.orders o WHERE EXISTS (SELECT 1 FROM mysql.shf.users u WHERE u.id = o.user_id)`           | **DENY**        | Correlated subquery: the outer-column reference `o.user_id` (represented as a `RexFieldAccess` over a `RexCorrelVariable`, not a `RexInputRef`) resolves to `orders.user_id`, which is not in Policy B. The outer query is NOT `SELECT *`, so nothing else covers `orders.user_id` — the denial comes solely from the correlated reference. **Requires** **`RexFieldAccess`/`RexCorrelVariable`** **handling**; without it this column is silently bypassed. (Case 13 does not catch this because its outer `SELECT *` over `users` masks the bypass and its correlated column `u.id` is authorized via Policy A.) |
| 24 | `SELECT o.id FROM mysql.shf.orders o WHERE EXISTS (SELECT 1 FROM mysql.shf.users u WHERE u.id = o.id)`                | **PASS**        | Same shape as case 23, but the correlated reference `o.id` resolves to `orders.id` (Policy B). Confirms that correlated references to authorized outer columns are still allowed.                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 25 | `SELECT SUM(o.amount) FILTER (WHERE o.user_id > 0) FROM mysql.shf.orders o`                                           | **DENY**        | Aggregate `FILTER (WHERE ...)` clause: `o.user_id` is not in Policy B. In the Rel tree, Calcite lowers the FILTER predicate into a boolean column beneath the `LogicalAggregate`, and the `AggregateCall.filterArg` ordinal points at that column. `visit(LogicalAggregate)` traces `filterArg` explicitly so the unauthorized `orders.user_id` is caught even without relying on an implicit Project beneath. **Without the** **`filterArg`** **trace this column is silently bypassed.**                                                                                                                         |
| 26 | `SELECT SUM(o.amount) FILTER (WHERE o.id > 0) FROM mysql.shf.orders o`                                                | **PASS**        | Same shape as case 25, but the FILTER clause references `o.id` (Policy B). Confirms that aggregate call arguments and FILTER columns that are authorized are still allowed. **Note:** The `FILTER (WHERE ...)` clause is SQL:2008 standard but not supported by MySQL; the permission check passes but query execution will fail on MySQL storage. Use PostgreSQL or a Drill-native table for end-to-end execution, or use `SELECT SUM(CASE WHEN id > 0 THEN amount END) FROM ...` as a MySQL-compatible alternative (though `CASE` goes through `argList`, not `filterArg`).                                      |
| 27 | `CREATE TABLE mysql.shf.new_orders AS SELECT id, amount FROM mysql.shf.orders`                                        | **PASS**        | Target table `new_orders` covered by Policy C (`CREATE` on `mysql.shf.*`); query part references only `id`, `amount` (Policy B). Both layers pass.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 28 | `CREATE TABLE mysql.shf.new_orders AS SELECT * FROM mysql.shf.orders`                                                 | **DENY**        | `CREATE` on the target is granted (Policy C), but the query part's `SELECT *` expands to unauthorized columns (`user_id`, `order_date` — not in Policy B). The source SELECT check rejects the statement before the target CREATE check matters.                                                                                                                                                                                                                                                                                                                                                                   |
| 29 | `DROP TABLE mysql.shf.orders`                                                                                         | **DENY**        | No policy grants `DROP` on `mysql.shf.*` (Policy C is CREATE-only). The DDL check in `DropTableHandler` denies with a PERMISSION ERROR before the existence check.                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 30 | `use dfs.test; CREATE VIEW v_orders AS SELECT id FROM mysql.shf.orders; DROP VIEW v_orders`                 | **PASS / DENY** | `CREATE VIEW` passes (Policy C, query part only references `id` in Policy B). `DROP VIEW` is denied: no `DROP` policy. `OR REPLACE` on the view would still be a single CREATE check (PASS). Temporary tables bypass both checks entirely.                                                                                                                                                                                                                                                                                                                                                                         |

