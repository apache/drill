<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# Apache Drill — Threat Model (v0 draft)

## §1 Header

- **Project:** Apache Drill (`apache/drill`), `master` @ HEAD (2026-06). Scope: `apache/drill` only.
- **Date:** 2026-06-18. **Author:** ASF Security team, drafted via the threat-model-producer (Scovetta) rubric at the Drill PMC's request (path 3 — chosen by Charles Givre, 2026-06-17).
- **Status:** DRAFT — under Drill PMC review (Charles Givre, 2026-06). Most load-bearing claims remain *(inferred)* pending further PMC confirmation (see §14).
- **Version binding:** versioned with the project; a report against version *N* is triaged against the model as it stood at *N*.
- **Reporting cross-reference:** §8-property violations → report privately per the ASF process (`security@apache.org` → `private@drill.apache.org`); §3/§9 findings are closed citing this document.
- **Provenance legend:** *(documented)* = Drill's own docs/repo; *(maintainer)* = confirmed by a Drill PMC member through this process; *(inferred)* = reasoned from architecture/docs, not yet PMC-ratified — each has a matching §14 open question.
- **Draft confidence:** ~10 documented / 2 maintainer / ~26 inferred (cgivre's PR #3052 review folded: `contrib/` plugins in scope + the storage-enumeration non-finding).

**What Drill is.** Apache Drill is a **schema-free, distributed SQL query engine** for large-scale datasets. A cluster of **Drillbit** daemons (coordinated via Apache ZooKeeper) accepts ANSI-SQL queries over JDBC, ODBC, a REST API, and a Web UI (default port **8047**), and executes them directly against data in configurable **storage plugins** (local/HDFS/S3 files, Hive, HBase, Kafka, MongoDB, RDBMS via JDBC, HTTP, …) — discovering schema at query time rather than requiring a pre-defined schema. *(documented — drill.apache.org)*

## §2 Scope and intended use

Intended deployment: a **clustered set of Drillbits** inside an operator-controlled, network-isolated cluster, queried by analytics clients. Drill also runs in an **embedded** single-JVM mode (developer/laptop) where the caller controls everything. *(documented — install docs; inferred that clustered behind a perimeter is the security-relevant shape)*

**Caller roles** (Drill is a network service — the role splits):

- **SQL client** — submits queries over JDBC/ODBC/REST. May be authenticated or, if auth is disabled, anonymous. The primary untrusted boundary. *(inferred — §14 Q1)*
- **Operator / admin** — configures storage plugins, system/session options, security settings, and uploads dynamic UDFs via the Web UI/REST. **Trusted.** *(documented — storage-plugin + option config is an admin function)*
- **Embedding / submitting user under impersonation** — when impersonation is enabled, the Drillbit accesses the underlying data source *as* the submitting user, delegating authorization to the data source. *(documented — impersonation docs)*
- **Peer Drillbit** — another node in the cluster, authenticated over the inter-Drillbit RPC. Trusted once authenticated (§7 Byzantine note). *(inferred — §14 Q3)*

**Component families.**

| Family | Entry point | Touches OS/network | In model? |
| --- | --- | --- | --- |
| Client RPC / SQL front door | Drillbit user RPC (JDBC/ODBC), the "foreman" | network (listens) | **In — primary boundary** *(inferred — §14 Q1)* |
| Web UI + REST API | `:8047` — query submit, storage-plugin config, profiles, options | network (listens) | **In — high value (admin surface)** *(documented — Web UI)* |
| SQL engine | parse → plan (Calcite) → distributed execute | depends on plugins | **In** *(inferred)* |
| Storage plugins | file/HDFS/S3, Hive, HBase, Kafka, Mongo, **JDBC (RDBMS)**, **HTTP** | filesystem + network out | **In — file-read / SSRF / connector surface** *(documented — storage plugins)* |
| UDF layer | built-in functions + **dynamic UDFs** (runtime jar upload) | in-JVM code execution | **In as code-execution-by-design** *(documented — dynamic UDFs)* |
| Inter-Drillbit RPC | control + data RPC between Drillbits, SASL | network (intra-cluster) | **In (boundary is authenticated)** *(inferred — §14 Q3)* |
| Client connectors | JDBC driver, ODBC driver, C++ client | client trust domain | client-side — out (§3) |
| **`contrib/` storage + format plugins** | connectors/formats bundled + maintained with Drill (`contrib/storage-*`, `contrib/format-*`) | filesystem + network out | **In — same connector surface as the storage-plugin row** *(maintainer — cgivre)* |
| Tests / examples / sample data | `exec/.../test`, demo code, sample datasets | n/a | **Out** *(see §3)* |

## §3 Out of scope (explicit non-goals)

- **The security of the underlying data sources.** HDFS/S3 permissions, the RDBMS behind the JDBC plugin, Hive authorization, the Kafka cluster — each enforces its own access control. Drill assumes them; it is not responsible for their misconfiguration. *(inferred — §14 Q2)*
- **Dynamic UDFs and storage-plugin configuration as code/connectivity execution.** Uploading a UDF jar or configuring a storage plugin is an **authorized admin operation**; the code/connection runs with the Drillbit's privileges. This is a feature, not a sandbox escape — `BY-DESIGN`. A finding that requires admin rights to register a UDF or add a plugin is out of model. *(documented — admin function; §14 Q4 confirms the trust line)*
- **An operator with cluster/root access, the Drillbit service account, or direct ZooKeeper write access.** Anyone who controls the cluster processes or the coordination store has already won. `OUT-OF-MODEL: adversary-not-in-scope`. *(inferred — §14 Q5)*
- **Embedded / single-user mode**, where the caller owns the JVM, the filesystem, and all input — equivalent to an in-process library call. *(inferred — §14 Q1)*
- **SQL injection in an embedding application** that string-concatenates untrusted input into Drill SQL — that is the embedding app's bug, not Drill's. *(inferred — §11)*
- **Tests, examples, and sample data** (`exec/.../test`, demo code, sample datasets) — not a shipped runtime surface. *(inferred)* **NB:** the `contrib/` storage/format **plugins are in scope** — they ship with Drill and are PMC-maintained, carrying the same connector trust surface as the core storage plugins (§2); only tests/examples/samples are out. *(maintainer — cgivre, PR #3052)*

## §4 Trust boundaries and data flow

- **Client → Drillbit (the foreman)** is the primary boundary, over both the user RPC (JDBC/ODBC) and the Web UI/REST (`:8047`). The session is authenticated when `drill.exec.security.auth` is enabled (PLAIN via PAM/LDAP, Kerberos, or custom); every statement is then subject to the configured authorization (impersonation + storage permissions + views). Whether SQL text, connection properties, and storage-plugin references are treated as untrusted at this boundary is the load-bearing triage question. *(documented — auth mechanisms; inferred — §14 Q6 on what is treated untrusted)*
- **Drillbit → data source.** With **impersonation enabled**, the Drillbit accesses the source *as the submitting user*, so the source's own ACLs bound the query; with impersonation disabled, all access runs as the Drillbit service principal and authorization collapses to whatever Drill itself enforces. The two modes have materially different blast radii. *(documented — impersonation; §14 Q7)*
- **Drillbit ↔ Drillbit** (control/data RPC) and **Drillbit ↔ ZooKeeper** are intra-cluster boundaries assumed to run inside an operator-controlled, network-isolated perimeter, optionally SASL/encryption-protected. *(inferred — §14 Q3)*

**Reachability precondition (triager's test).** A finding is in-model only if it is reachable by an **authenticated low-privilege SQL/REST client** (or an anonymous one when auth is a supported-disabled posture, §5a) — i.e. without admin rights to configure plugins or upload UDFs, and without control of the data source or the cluster host. A finding that requires any of those is `OUT-OF-MODEL`. *(inferred — §14 Q1/Q4/Q5)*

## §5 Assumptions about the environment

- A JVM on each Drillbit; Apache ZooKeeper for cluster coordination; the cluster network is operator-controlled and not directly exposed to untrusted clients except through the intended client RPC / `:8047` endpoints. *(inferred — §14 Q5)*
- Authorization correctness depends on the data sources (and/or Drill views + impersonation) enforcing access; Drill does not re-implement source-side ACLs. *(inferred — §14 Q2)*
- **Negative side-effects inventory** *(inferred — §14 Q8; high-value to confirm)*: beyond listening on its RPC/Web ports, reading the storage plugins it is configured for, writing query profiles + spill files to its configured areas, and coordinating via ZooKeeper, a Drillbit is assumed to make **no other outbound connections** except those a query explicitly drives (JDBC/HTTP/Kafka/Mongo plugins, `SERVICE`-like federation). Confirm there is no ambient phone-home / auto-fetch.

## §5a Build-time and configuration variants

The security envelope is set primarily by runtime configuration, not compile flags. The load-bearing knobs *(all defaults to be confirmed — §14 Q9; the insecure-default question reshapes §8/§10/§11a/§13)*:

| Knob | Effect | Default (to confirm) |
| --- | --- | --- |
| `drill.exec.security.auth.enabled` (+ mechanism: PLAIN/Kerberos/…) | Whether the client front door authenticates at all. | **disabled** by default *(inferred — §14 Q9)* |
| `drill.exec.impersonation.enabled` | Whether queries run as the submitting user against the source (delegating authz) vs. as the Drill service principal. | **disabled** by default *(inferred — §14 Q9)* |
| Wire encryption (SASL / SSL-TLS, client↔Drillbit and inter-Drillbit) | Whether sessions + data are in clear on the wire. | **off** unless configured *(inferred — §14 Q9)* |
| Web UI / REST auth (form / SPNEGO) | Whether `:8047` — which can configure plugins + upload UDFs — is open. | tied to `auth.enabled` *(inferred — §14 Q9)* |
| Dynamic UDF support (`drill.exec.udf.*`) | Whether authorized users can upload UDF jars at runtime. | enabled *(inferred — §14 Q9/Q4)* |

**Insecure-default question (wave 1).** If auth and impersonation ship **disabled**, is a multi-user deployment left in that state a `VALID` finding, or `OUT-OF-MODEL: non-default-build` (operator is documented as required to enable them before exposing Drill)? — §14 Q9.

## §6 Assumptions about inputs

| Surface | Input | Attacker-controllable? | Concern |
| --- | --- | --- | --- |
| Client RPC / REST | SQL text | **yes** (authenticated client) | query-driven file/URL access, resource cost, planner bugs |
| Client RPC | JDBC/ODBC connection properties (incl. impersonation target) | **yes** | inbound-impersonation / identity spoofing if not gated |
| Web UI / REST | storage-plugin config JSON, option sets, UDF jar upload | **admin only** (trusted) | code/connection execution — by design (§3) |
| Query execution | file paths / URIs named in queries (`dfs`, `http`, `jdbc` plugins) | **yes** | local-file read / SSRF / arbitrary-RDBMS reach, bounded by impersonation + plugin config |
| Storage scan | data files (CSV/JSON/Parquet/Avro…) from configured sources | **maybe** (whoever can write the source) | parser robustness / resource use on malformed data |
| Inter-Drillbit RPC | control/data frames | no — authenticated peer | §7 Byzantine-peer only |

## §7 Adversary model

- **In scope:** an **authenticated low-privilege SQL/REST client** (and, if auth-disabled is a supported posture, an anonymous network client reaching the client RPC or `:8047`). Capabilities: submit arbitrary SQL, set connection properties, drive query-time file/URL access through configured plugins, submit pathological queries. Goals: read data outside their authorization, make a Drillbit issue outbound requests (SSRF via HTTP/JDBC plugins), read local files via the `dfs` plugin, exhaust cluster resources. *(inferred — §14 Q1/Q6)*
- **Network MITM** between client and Drillbit, or between Drillbits, where encryption is not configured. *(inferred — §14 Q9)*
- **Out of scope:** the operator/admin (configures plugins, uploads UDFs — trusted, §3); anyone with cluster-host / service-account / ZooKeeper-write access (§3); the data sources themselves (§3).
- **Authenticated-but-Byzantine peer.** A compromised Drillbit holding a valid cluster identity could behave arbitrarily over the inter-Drillbit RPC. Drill is assumed to make **no cross-node integrity claim** against a malicious authenticated peer — full peer trust once authenticated. *(inferred — §14 Q3)*

## §8 Security properties the project provides

*(All (inferred) pending §14; each lists violation symptom + severity.)*

1. **Client authentication (when enabled).** With `auth.enabled`, no SQL/REST statement runs before the session authenticates via the configured mechanism. *Violation:* unauthenticated query execution against an auth-enabled cluster. *Severity:* security-critical. *(inferred — §14 Q9/Q10)*
2. **Authorization scoping via impersonation + views + storage permissions.** A query reads/writes only what the effective identity is permitted at the data source (impersonation on) and/or what Drill views/permissions allow. *Violation:* a low-priv client reading data it is not authorized for, or bypassing a view. *Severity:* security-critical (CVE-class). *(inferred — §14 Q7/Q10)*
3. **No arbitrary code execution or plugin reconfiguration for a non-admin client.** Registering UDFs and configuring storage plugins are admin-gated; a plain SQL client cannot reach them. *Violation:* a non-admin causing code exec / plugin change / arbitrary file or URL access beyond their authorization. *Severity:* security-critical. *(inferred — §14 Q4)*
4. **Inbound-impersonation is gated.** A client cannot assume an arbitrary user identity unless the operator configured a proxy/impersonation policy permitting it. *Violation:* identity spoof via connection properties. *Severity:* critical. *(inferred — §14 Q6)*
5. **Memory safety on the JVM** for well-formed input, to the extent the JVM provides it. *(inferred)*
6. **Bounded/fair resource use** — *deferred.* Whether a single expensive query can exhaust a Drillbit (heap/direct memory/CPU) or whether per-query memory limits + queues bound it is a §14 question; stated as a property only if the PMC confirms a guarantee. *(inferred — §14 Q11)*

## §9 Security properties the project does *not* provide

- **No sandbox around dynamic UDFs or storage-plugin configuration.** Code/connections an authorized admin registers run with the Drillbit's privileges — by design. *(inferred — §14 Q4)*
- **No protection when auth/impersonation are left disabled** (if that is a non-default posture, §5a) — an open, impersonation-off Drillbit exposes its service-principal access to any client. *(inferred — §14 Q9)*
- **No defense against query-driven SSRF / local-file read beyond what plugin config + impersonation bound.** The `http`/`jdbc`/`dfs` plugins exist to reach those resources; restricting them is operator config, not an engine guarantee. *(inferred — §14 Q2)*
- **No hard resource-fairness / anti-DoS guarantee** unless §14 Q11 says otherwise — a sufficiently expensive query may degrade a Drillbit.
- **Not a security boundary over its data sources** — Drill does not add confidentiality/integrity on top of a source that lacks it.
- **Well-known classes (SQL-engine / connector):** SSRF via outbound-connector plugins, local-file disclosure via filesystem plugins, decompression/parser resource attacks on scanned files, and SQL injection in *embedding* apps — the standard risks of a federated query engine that reaches arbitrary configured sources. *(inferred)*

## §10 Downstream (operator) responsibilities

- **Enable authentication and impersonation** before exposing Drill to more than one trusted user; do not run a multi-user cluster with the shipped auth-off / impersonation-off defaults. *(inferred — §14 Q9)*
- **Lock down the Web UI / REST (`:8047`)** — it can configure storage plugins and upload UDFs; restrict it to admins and protect it with auth + TLS.
- **Restrict who can configure storage plugins and upload dynamic UDFs** to trusted admins (these are code/connectivity-execution surfaces).
- **Configure wire encryption** (SASL/SSL) on client↔Drillbit and inter-Drillbit links on untrusted networks.
- **Network-isolate** the Drillbit cluster + ZooKeeper from untrusted networks; give data-source credentials least privilege.

## §11 Known misuse patterns

- Exposing the client RPC or `:8047` to an untrusted network **with auth/impersonation disabled**.
- Granting storage-plugin-config or UDF-upload rights to untrusted users (= handing them code execution + arbitrary connectivity).
- Configuring a `dfs`/`http`/`jdbc` plugin broadly (e.g. filesystem root, open egress) and exposing it to low-priv query clients without impersonation.
- Treating Drill as an authorization layer over a data source that has none.
- String-concatenating untrusted input into Drill SQL in an embedding application.

## §11a Known non-findings (recurring false positives)

*(v0 seed — the PMC will own the authoritative list; §14 Q12.)*

- **"A dynamic UDF / storage plugin can run arbitrary code or reach any URL/file."** By design — both are admin-authorized operations. `BY-DESIGN` unless a non-admin can reach them. *(inferred)*
- **"The REST API / Web UI accepts requests without authentication."** Only when auth is disabled — a non-default/operator-chosen posture (pending §14 Q9), not a Drill defect. `OUT-OF-MODEL: non-default-build`.
- **"A query can read an arbitrary local file via the `dfs` plugin."** In-model only if it crosses the impersonated identity's authorization or the configured workspace; otherwise it is the operator's plugin-scope choice. *(inferred — §14 Q2/Q7)*
- **"Drill can enumerate / read the files, schemas, or storage systems it is pointed at."** That is the **function** of a federated query engine — exposing configured storage to SQL is the point of Drill, not a vulnerability. `KNOWN-NON-FINDING` / `BY-DESIGN`. In-model **only** if the access crosses the authenticated identity's authorization (reading data an impersonated user shouldn't, or bypassing a Drill view — §8.2). *(maintainer — cgivre, PR #3052: "yes… that's the point of Drill".)*
- **"SQL injection in Drill"** where the untrusted string was concatenated by an *embedding app* — that app's bug (§11), not Drill's.
- **Dependency-tail CVEs** (Calcite, Hadoop, Jackson, Netty, a transitive jar) from an SCA scan — triage upstream unless Drill's own code reaches the vulnerable path with untrusted input.

## §12 Conditions that would change this model

- A new client-facing surface or a change to the auth/impersonation defaults (§5a).
- A new storage plugin or connector with a new outbound/trust surface.
- Sandboxing UDFs, or moving plugin config out of the admin-only trust tier.
- A report that cannot be routed to a §13 disposition → revise §8/§9.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | A §8 property breaks via an in-scope (authenticated low-priv / anonymous-if-supported) client on a realistic config. | §8, §6, §7 |
| `VALID-HARDENING` | A §11 misuse is too easy (e.g. an over-broad default plugin scope). | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires admin-tier input (plugin config / UDF jar) or data the operator already trusts. | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires operator / cluster-host / ZooKeeper / data-source control. | §7 |
| `OUT-OF-MODEL: unsupported-component` | Lands in tests, examples/sample data, or embedded single-user mode. (NB: `contrib/` storage/format plugins are **in** scope — §2.) | §3 |
| `OUT-OF-MODEL: non-default-build` | Only manifests with auth/impersonation/encryption left at a discouraged setting. | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a property §9 disclaims (UDF/plugin code exec, SSRF-by-config, data-source security). | §9 |
| `KNOWN-NON-FINDING` | Matches §11a. | §11a |
| `MODEL-GAP` | Unroutable. | triggers §12 |

## §14 Open questions for the maintainers

**Wave 1 — scope & intended use**

- **Q1.** Confirm the in-scope surface is "an authenticated SQL/REST client at the Drillbit front door (JDBC/ODBC/REST/Web UI), in a clustered multi-user deployment", with embedded single-user mode out of scope. Anything to add?
- **Q2.** Confirm Drill is *not* responsible for the underlying data sources' own access control — that HDFS/S3/RDBMS/Hive/Kafka security is theirs, and Drill's job is to honor it (via impersonation/views), not replace it.
- **Q4.** Confirm dynamic-UDF upload and storage-plugin configuration are **admin-only, code/connectivity-execution-by-design** — i.e. a finding that requires those rights is out of model.
- **Q5.** Confirm operators with cluster-host / Drillbit-service-account / ZooKeeper-write access (and the data sources themselves) are out of model.

**Wave 2 — auth, impersonation, defaults**

- **Q9.** *(reshapes §5a/§8/§10/§11a/§13)* What are the shipped **defaults** for `security.auth.enabled`, `impersonation.enabled`, wire encryption, and Web-UI auth — and is "left at the default in a multi-user deployment" a `VALID` finding or `OUT-OF-MODEL: non-default-build`?
- **Q6.** At the client→Drillbit boundary, are SQL text, connection properties, and inbound-impersonation targets all treated as untrusted (i.e. is identity-spoof via connection properties prevented unless an explicit proxy policy permits it)?
- **Q7.** Which impersonation posture is the supported/recommended one, and does authorization fully delegate to the data source when it's on?
- **Q10.** What does Drill claim to uphold given a valid authenticated session (auth, authz scoping, others) — confirm the §8 list.

**Wave 3 — boundaries, resources, false-friends**

- **Q3.** Inter-Drillbit + ZooKeeper trust: confirm these are assumed network-isolated, the RPC is authenticated (SASL), and Drill makes no integrity claim against a Byzantine authenticated peer.
- **Q8.** Confirm the §5 "no ambient side effects" inventory — beyond its ports, configured plugins, profile/spill writes, and ZooKeeper, a Drillbit makes no other outbound connection except those a query drives.
- **Q11.** Is super-linear CPU/memory in query/data size a bug, or is bounding it the operator's job (per-query memory limits, queues)? Is a hang on a pathological query a security issue? Or is no resource guarantee made?
- **Q12.** *(Partially answered — cgivre, PR #3052: storage/filesystem enumeration via Drill is by-design, not a finding — folded into §11a. More welcome.)* What do scanners/researchers most often report against Drill that you consider a non-finding? (Feeds §11a.)
