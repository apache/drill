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

# Login / Authentication Refactor — Design Notes

**Status:** Exploration / design discussion. Nothing here is committed to yet.
**Goal:** Make Drill's login experience feel like a modern data tool (Superset,
Metabase, Grafana) — a real username/password front door — while preserving the
security guarantees that secure Hadoop/Kerberos clusters depend on.

This document captures the design discussion so we can pick it up later. It is a
notes/options doc, **not** an implementation plan.

---

## 1. How Drill authenticates today

Drill has **three independent configuration toggles** that combine into a
confusing matrix:

| Config key | What it controls |
|---|---|
| `drill.exec.security.user.auth.enabled` | Whether users must prove identity at all. Pluggable impl (`drill.exec.security.user.auth.impl`): **PAM**, **htpasswd**, **Kerberos/SASL**, **Vault**, or custom. |
| `drill.exec.http.auth.mechanisms` | How the *web UI* authenticates: **FORM** (the username/password page), **SPNEGO** (Kerberos SSO), **BASIC**. |
| `drill.exec.impersonation.enabled` | Whether the query engine *proxies as the logged-in user* when touching HDFS / filesystem / storage. |

These produce four practical modes (see
`WebServer.isOnlyImpersonationEnabled` and friends):

1. **Wide open** — no auth, no impersonation. Everyone is anonymous; queries run
   as the Drillbit OS process user.
2. **Impersonation-only** — no login, but the client *asserts* a username
   (trusted blindly), and Drill proxies as it for storage access.
3. **Auth-only** — FORM / PAM / htpasswd login, but queries still run as the
   Drillbit process user.
4. **Auth + impersonation** — login *and* the query engine proxies as the
   authenticated user. This is the "secure cluster" configuration.

### The key architectural fact

**Drill has no user store of its own.** It never owns usernames/passwords — every
credential check is delegated to PAM (OS users), an htpasswd file, or Kerberos.
The web login page (Jetty's `FormAuthenticator` + `/j_security_check`, which
`LogInLogOutResources` wires into the React SPA) is a thin front-end over
whichever SASL authenticator is configured.

### Relevant code

- `exec/.../server/rest/LogInLogOutResources.java` — login/logout endpoints; now
  serves the React SPA for the login page.
- `exec/.../server/rest/WebServer.java` — `isOnlyImpersonationEnabled`, security
  handler wiring, session handler.
- `exec/.../server/rest/auth/` — `DrillHttpSecurityHandlerProvider`,
  `FormSecurityHandler`, `SpnegoSecurityHandler`, `HttpBasicAuthSecurityHandler`,
  `DrillRestLoginService`, `DrillUserPrincipal`, `AuthDynamicFeature`.
- `exec/.../rpc/security/` — SASL authenticators: `plain/PlainFactory` (the
  `UserAuthenticator` SPI path used by JDBC/ODBC/native clients),
  `kerberos/KerberosFactory`.
- `exec/.../server/rest/CredentialResources.java`, `OAuthRequests.java` — per-plugin
  stored credentials / OAuth (the "execution identity" column — see §3).

---

## 2. The gap we're trying to close

Tools like Superset, Metabase, and Grafana ship with a **built-in user table, a
real login page on by default, sessions, and roles**. Login is the front door,
not an optional security mode bolted onto an engine that defaults to wide-open.

Drill inverts that: the default is anonymous, and "real login" requires an admin
to correctly assemble PAM/htpasswd + FORM + impersonation.

---

## 3. The mental model to adopt: three separable layers

Drill conflates two questions that modern tools keep separate:

1. **Authentication** — *who are you?* (login)
2. **Execution identity** — *what does the query run as when it touches storage?*
   (impersonation)
3. **Authorization** — *what are you allowed to do?* (roles/permissions)

Today layers #1 and #2 are welded together: the only way to get a "real login" is
to enable an authenticator *and* (usually) impersonation, because the logged-in
identity **is** the HDFS proxy identity. Other tools decouple these — you log into
the *app*, and the app reaches storage with its *own* service credentials (or
per-connection stored creds), not by proxying as you at the OS level.

```
┌─ Who are you? ────────┐   ┌─ Runs queries as? ──────────┐
│ • Built-in user store │   │ • Drillbit process user     │
│ • PAM / htpasswd      │ → │ • Impersonate(you) [HDFS]   │
│ • Kerberos / SPNEGO   │   │ • Per-plugin stored creds   │
│ • OIDC / SAML (future)│   │ • Per-user OAuth tokens      │
└───────────────────────┘   └─────────────────────────────┘
   authentication              execution identity
        (independent choices)
```

The existing `CredentialResources` / `OAuthRequests` machinery already implements
the **right-hand column** — "this plugin has its own credentials," separate from
impersonation. That is the seam to lean on.

---

## 4. What "first-class login" concretely requires

| Requirement | Drill today | Gap |
|---|---|---|
| **A user identity store** | None — delegates to PAM/htpasswd/Kerberos | No "create user / set password" without editing OS or files |
| **Login is the default front door** | Default is anonymous; login is opt-in | Inverted default |
| **Sessions are first-class** | Jetty `FormAuthenticator` + `HttpSession`, server-side only | Tied to one Drillbit's heap (see §5A) |
| **Roles / permissions** | Only admin-vs-user (`security.admin.users`) | No real RBAC |

---

## 5. Hard problems specific to Drill

These are the interesting constraints — the parts that make Drill different from
a single-process web app.

**A. Distributed sessions.** A Drill cluster is N Drillbits behind (often) a load
balancer. Jetty's `HttpSession` lives in *one* Drillbit's heap. Superset uses a
shared DB/Redis for sessions; Drill has no equivalent. Today this is handled by
sticky sessions or single-node. First-class cluster login needs either
**stateless sessions (signed JWT cookies)** or a **shared session store**. JWT is
the natural fit — no new infra, dovetails with a built-in user store.

**B. The two front doors.** Drill authenticates the **native/JDBC/ODBC client**
(SASL/RPC) *and* the **web UI** (HTTP) through related-but-separate paths. A
built-in user store should serve **both**, or you get users who can log into the
web UI but cannot connect via JDBC. This is why a built-in *authenticator* (a
`UserAuthenticator` impl backed by a Drill-owned store) is more powerful than a
web-only login — it slots into the existing SASL `PlainFactory` path *and* the
FORM path.

**C. Impersonation cannot just be dropped.** On a secure Hadoop cluster,
impersonation **is** the security boundary — it stops user A from reading user B's
HDFS files. A built-in user store with app-level login but `drillbit-process-user`
execution would be a **security regression** there. The design must let the two
columns vary independently: built-in login *with* impersonation for clusters,
built-in login *with* stored/service creds for single-node.

**D. Where does the user store physically live?** Drill has a `PersistentStore`
abstraction (already used for `drill.sqllab.ai_config`, etc.). A users table could
live there — ZooKeeper-backed or local. Path of least resistance, but ZK is not a
great secret store: passwords must be hashed (**argon2 / bcrypt**), never stored
in plaintext, and we should think about who can read the store.

---

## 6. Recommended direction (layered, each layer degrades gracefully)

Ordered so each step is shippable on its own and nothing forces a security
regression.

1. **Unify the front-of-house (low risk, do first).**
   One coherent React `LoginPage` that adapts to whatever auth is configured
   (FORM password / SPNEGO button / "anonymous — click to enter"). Backend stays
   as-is; `AuthConfigResource` drives the UI. Improves every existing mode. *This
   is the refactor already in progress on `feature/sqllab-react-ui`.*

2. **Add a built-in user store as a new `UserAuthenticator` impl (medium).**
   A Drill-owned users table (PersistentStore, argon2-hashed passwords; admin can
   create/disable users from the UI). Because it plugs in at the
   `UserAuthenticator` SPI level, it serves **both** JDBC and web. Becomes the
   default for single-node "just works" deployments. PAM/htpasswd/Kerberos remain
   selectable.

3. **Stateless sessions via signed cookies (medium — enables clustering).**
   Decouples login from a single Drillbit's heap so multi-node works behind a
   plain load balancer.

4. **Make execution-identity an explicit, separate setting (larger).**
   Today it is implied by `impersonation.enabled`. Make it a first-class choice:
   *process user* / *impersonate* / *stored plugin creds* — so single-node can
   have rich login without HDFS proxying, and clusters keep impersonation as the
   boundary.

5. **RBAC later (largest, optional).**
   Real roles on top of the built-in store. Only worth it once step 2 exists.

### Design guardrail

**Do not build a web-only login that bypasses the `UserAuthenticator` SPI.** It
would split the JDBC and web identity worlds and recreate the "logged into the UI
but can't connect a BI tool" confusion. The built-in store should be an
*authenticator*, not a *web feature*.

---

## 7. Open questions to resolve before building

- **Governance:** Is a built-in user store acceptable to the Drill community/PMC,
  or is the expectation that identity always comes from an external system
  (LDAP/Kerberos/OIDC)? Partly a project-governance question, not just technical.
- **Built-in store vs. OIDC/SAML:** OIDC (delegate to Okta/Keycloak/etc.) is
  arguably *more* "in line with modern tools" than a homegrown user table, and
  sidesteps password-storage risk entirely — at the cost of requiring an IdP.
  Which do we invest in first?
- **JDBC/ODBC scope:** How much do we care about the native client path vs. just
  the web UI? This single answer decides whether this is an *authenticator*
  project or a *web login* project.
- **Default posture:** Do we flip the default from "anonymous" to "login
  required" (with a generated/printed admin password on first boot, like many
  tools), or keep anonymous as the default for embedded use?

---

## 8. Decisions captured so far

- **Mode:** Exploration only — no implementation committed.
- **Target deployment:** Must degrade gracefully across **both** single-node /
  embedded **and** secure multi-user cluster, with sensible defaults.
