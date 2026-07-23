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
|  | (singleton, config-driven)    |  | (static facade)       | |
|  +-------------------------------+  +-----------------------+ |
|                                          |                    |
|  +----------------------------------------------------------------+
|  | drill-ranger-plugin (JDK 11, deployed to jars/3rdparty/)      |
|  |  DrillAuthorizer  DrillAccessResource  DrillRangerAccessRequest|
|  |  RangerDrillPlugin  RangerBaseAuthorizer                       |
|  +-----------------------------+--------------------------------+
|                                |
|                                v
|  +----------------------------------------------------------------+
|  | drill-ranger-service (JDK 8, deployed to Ranger Admin)         |
|  |  RangerServiceDrill  (validateConfig, lookupResource)          |
|  |  Uses Drill REST API (POST /query.json) — NOT JDBC             |
|  +----------------------------------------------------------------+
```

### 1.1 Modules

| Module | JDK | Deployed To | Responsibility |
|--------|-----|------------|----------------|
| `drill-ranger-plugin` | 11 | Drillbit `jars/3rdparty/` | Drillbit-side authorization: wraps `RangerBasePlugin`, exposes `DrillAccessControl` facade |
| `drill-ranger-service` | 8 | Ranger Admin `WEB-INF/classes/lib/` | Ranger Admin-side service plugin: `validateConfig` and `lookupResource` via Drill REST API |
| `exec/java-exec` | 11 | Drillbit | Integration hooks: `AccessAuthorizerFactory`, `ColumnAccessChecker`, `DrillCalciteCatalogReader` |

### 1.2 Why Two Submodules with Different JDK?

Ranger Admin runs on JDK 8. If `drill-ranger-service` were compiled with JDK 11
bytecode (class major version 55), Ranger Admin would throw
`UnsupportedClassVersionError`. Conversely, `drill-ranger-plugin` runs inside
Drillbit which requires JDK 11. The split ensures each jar matches its host
runtime.

`drill-ranger-service` uses the Drill REST API (`POST /query.json`) instead of
JDBC precisely to avoid pulling in `drill-jdbc` (JDK 11 bytecode) into the
Ranger Admin classpath.

## 2. Resource Model

Ranger policies for Drill use a **four-level resource hierarchy**:

```
datasource  →  schema  →  table  →  column
```

| Level | Ranger resource key | Example | Notes |
|-------|--------------------|---------|-------|
| datasource | `datasource` | `mysql` | Drill storage plugin name |
| schema | `schema` | `shf` | Schema path WITHOUT datasource prefix |
| table | `table` | `orders` | Table name |
| column | `column` | `id`, `amount`, `*` | `*` matches all columns |

**Critical conventions**:
- Resource keys must be **lowercase** (`datasource`, not `DATASOURCE`). Ranger
  validates names against `[a-z_-]` only (error code 2022).
- The `schema` value must NOT include the datasource prefix. Use `shf`, not
  `mysql.shf`.
- Access type name in the service-def must exactly match what the code sends —
  both uppercase `SELECT`.

## 3. Configuration

### 3.1 Drillbit side (`drill-module.conf`)

```hocon
drill.exec.security.ranger: {
  enabled: true,
  service.name: "drill",
  impl: "org.apache.drill.exec.security.ranger.RangerAccessAuthorizer"
}
```

| Key | Default | Description |
|-----|---------|-------------|
| `drill.exec.security.ranger.enabled` | `false` | Master switch. `false` → `NoOpAccessAuthorizer` (fail-open) |
| `drill.exec.security.ranger.service.name` | `"drill"` | Ranger service name registered in Ranger Admin |
| `drill.exec.security.ranger.impl` | `org.apache.drill.exec.security.ranger.RangerAccessAuthorizer` | `AccessAuthorizer` implementation class |

### 3.2 Ranger Admin side

Register the Drill service using `ranger-servicedef-drill.json` (located in
`distribution/src/main/resources/ranger/`). Configure:

- `drill.connection.url` — Drill REST API URL, e.g. `http://drillbit-host:8047`.
  Bare `host:port` is normalized to `http://host:port`.
- `username` / `password` — Drill user for `validateConfig` and
  `lookupResource` REST calls (HTTP Basic auth).

### 3.3 Deployment Steps

After building the distribution, three deployment actions are required to make
Ranger Admin recognize Drill as an authorization provider.

#### Step 1: Upload `drill-ranger-service` jar to Ranger Admin

Copy the `drill-ranger-service` jar (the thin jar, NOT the
`jar-with-dependencies` classifier) into Ranger Admin's per-service plugin
directory. Create the `drill` subdirectory if it does not exist.

```bash
# On the Ranger Admin host
RANGER_ADMIN_HOME=/data/ranger-2.8.1-SNAPSHOT-admin
TARGET_DIR=$RANGER_ADMIN_HOME/ews/webapp/WEB-INF/classes/ranger-plugins/drill

mkdir -p "$TARGET_DIR"
cp drill-ranger-service-X.XX.X-SNAPSHOT.jar "$TARGET_DIR/"
```

#### Step 2: Update Ranger config files in Drill

Copy the Ranger configuration files into Drill's `conf/` directory and edit
them to match your environment.

```bash
DRILL_HOME=/opt/drill

cp distribution/src/main/resources/ranger/ranger-drill-security.xml  $DRILL_HOME/conf/
cp distribution/src/main/resources/ranger/ranger-drill-audit.xml     $DRILL_HOME/conf/
```

Then edit `$DRILL_HOME/conf/ranger-drill-security.xml`:

| Property | Value to set |
|----------|--------------|
| `ranger.plugin.drill.policy.rest.url` | `http://<ranger-admin-host>:6080` |
| `ranger.plugin.drill.service.name` | The Ranger service name (must match `drill.exec.security.ranger.service.name` in `drill-override.conf`) |

#### Step 3: Register the Drill service definition in Ranger Admin

Upload `ranger-servicedef-drill.json` to Ranger Admin's REST API. After this
call succeeds, the "drill" service type appears in Ranger Admin's "Service
Manager" → "+" dropdown, and you can create a Drill service instance and
author policies.

```bash
curl -u user:password -X POST \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  http://ranger-admin-host:port/service/plugins/definitions \
  -d@distribution/src/main/resources/ranger/ranger-servicedef-drill.json
```
### 3.4 Ranger policy files

| File | Location | Purpose |
|------|----------|---------|
| `ranger-drill-security.xml` | `distribution/src/main/resources/ranger/` | Ranger plugin config (policy cache dir, polling interval) |
| `ranger-drill-audit.xml` | `distribution/src/main/resources/ranger/` | Audit sink config (HDFS, Solr, etc.) |
| `ranger-servicedef-drill.json` | `distribution/src/main/resources/ranger/` | Service definition: resources, access types, config validation |

## 4. Authorization Policy Test Cases

The following test cases document the expected authorization behavior with the
sample policies below. All SQL runs against tables `mysql.shf.orders` and
`mysql.shf.users`.

### 4.1 Sample Ranger Policies

**Policy A — users table, all columns**

| Field | Value |
|-------|-------|
| datasource | `mysql` |
| schema | `shf` |
| table | `users` |
| column | `*` |
| access type | `SELECT` |
| user/group | (authorized user) |

**Policy B — orders table, specific columns only**

| Field | Value |
|-------|-------|
| datasource | `mysql` |
| schema | `shf` |
| table | `orders` |
| column | `id`, `amount` |
| access type | `SELECT` |
| user/group | (authorized user) |

Under these policies, the `orders.user_id` column is NOT authorized. The
`users` table allows all columns via `*`.

### 4.2 Test Cases

| # | SQL | Expected | Why |
|---|-----|----------|-----|
| 1 | `SELECT id, amount FROM orders` | **PASS** | `id`, `amount` both in Policy B |
| 2 | `SELECT * FROM orders` | **DENY** | `*` expands to all columns including `user_id`, which is not in Policy B |
| 3 | `SELECT sum(id) FROM orders` | **PASS** | Aggregate on authorized column `id` |
| 4 | `SELECT id FROM orders WHERE user_id > 1` | **DENY** | `user_id` in WHERE clause is checked (LogicalFilter condition traced) |
| 5 | `SELECT sum(user_id) FROM orders` | **DENY** | `user_id` not in Policy B |
| 6 | `SELECT o.id, u.name, o.amount, o.order_date FROM orders o INNER JOIN users u ON o.user_id = u.id` | **DENY** | `o.user_id` appears in JOIN condition; `o.order_date` not in Policy B |
| 7 | `SELECT o.id, u.name, o.amount FROM orders o INNER JOIN users u ON o.id = u.id` | **PASS** | All referenced columns authorized: `o.id`, `u.name` (via `*`), `o.amount` |
| 8 | `SELECT u.*, o.amount FROM orders o JOIN users u ON o.id = u.id WHERE o.amount > (SELECT AVG(amount) FROM orders)` | **PASS** | `u.*` authorized via Policy A; `o.amount` and subquery `amount` in Policy B |
| 9 | `SELECT o.*, u.name FROM orders o JOIN users u ON o.user_id = u.id WHERE o.amount > (SELECT AVG(amount) FROM orders)` | **DENY** | `o.*` expands to `user_id` (not authorized); `o.user_id` in JOIN condition |
| 10 | `SELECT o.id, u.name FROM orders o JOIN users u ON o.id = u.id WHERE o.amount > (SELECT AVG(amount) FROM orders)` | **PASS** | All columns authorized; subquery only references `amount` |
| 11 | `SELECT o.id, u.name FROM orders o JOIN users u ON o.id = u.id WHERE o.amount > (SELECT sum(user_id) FROM orders)` | **DENY** | Scalar subquery references `user_id` (not authorized). **Requires RexSubQuery handling.** |
| 12 | `SELECT name FROM users WHERE id IN (SELECT DISTINCT user_id FROM orders)` | **DENY** | IN-subquery references `user_id` (not authorized). **Requires RexSubQuery handling.** |
| 13 | `SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)` | **DENY** | EXISTS-subquery references `o.user_id` (not authorized). **Requires RexSubQuery handling.** |
