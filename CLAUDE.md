# Apache Drill — Claude / AI assistant guide

Apache Drill is a schema-free SQL query engine for Hadoop, NoSQL, and cloud storage. This file is the entry point for AI coding assistants working in this repo: it points you to where the docs live, calls out hard constraints, and lists must-run commands.

## Where the docs live

All developer documentation is under [`docs/dev/`](docs/dev/). Start with [`docs/dev/DevDocs.md`](docs/dev/DevDocs.md) for the broader index.

| Topic | Doc |
|---|---|
| **Web UI (React SPA)** — architecture, routing, sidebar, contexts, page-by-page reference | [`docs/dev/ui/README.md`](docs/dev/ui/README.md) |
| **Login / authentication refactor** — design notes, current auth model, options | [`docs/dev/LoginRefactor.md`](docs/dev/LoginRefactor.md) |
| Prospector AI assistant — backend | [`docs/dev/PROSPECTOR.md`](docs/dev/PROSPECTOR.md) |
| Enterprise AI provider configuration — proxy, SSL/TLS, custom headers, static parameters | [`docs/dev/ai/ENTERPRISE_PROVIDERS.md`](docs/dev/ai/ENTERPRISE_PROVIDERS.md) |
| SQL transpiler (GraalPy + sqlglot) | [`docs/dev/TRANSPILER.md`](docs/dev/TRANSPILER.md) |
| AI features overview | [`docs/dev/AI_FEATURES.md`](docs/dev/AI_FEATURES.md) |
| Dev environment setup | [`docs/dev/Environment.md`](docs/dev/Environment.md) |
| Testing | [`docs/dev/Testing.md`](docs/dev/Testing.md) |
| Maven build | [`docs/dev/Maven.md`](docs/dev/Maven.md) |
| Jetty 12 migration notes | [`docs/dev/Jetty12Migration.md`](docs/dev/Jetty12Migration.md) |
| License headers | [`docs/dev/LicenseHeadersAndNotices.md`](docs/dev/LicenseHeadersAndNotices.md) |

If you write new documentation, place it in [`docs/dev/`](docs/dev/), not in source directories. Update this table and the relevant index file.

## Hard constraints

These are easy to get wrong and break the build.

### Checkstyle

After modifying anything in `exec/java-exec`, run:

```bash
mvn checkstyle:check -pl exec/java-exec
```

Common failures: `if` statements without braces, unused imports, missing Apache license headers on new files.

### `jdbc-all` jar size limit

The shaded JDBC driver (`exec/jdbc-all`) has a size limit enforced by `maven-enforcer-plugin` (~62 MB).

If you add large resources to `exec/java-exec` (anything in `src/main/resources/webapp/`, large fonts, model files, etc.), update the exclusion filters in [`exec/jdbc-all/pom.xml`](exec/jdbc-all/pom.xml) under the `maven-shade-plugin` configuration. The `webapp/**` filter is already present.

### License headers

Every source file (Java, TypeScript, TSX, JavaScript, CSS, HTML) must have an Apache 2.0 license header. See [`docs/dev/LicenseHeadersAndNotices.md`](docs/dev/LicenseHeadersAndNotices.md).

## UI workflow

The web UI is a React SPA at `exec/java-exec/src/main/resources/webapp/`. Build, dev server, conventions, and per-page reference are all in [`docs/dev/ui/`](docs/dev/ui/README.md).

Quick reference:

```bash
# Production build (run from repo root via Maven, or directly):
cd exec/java-exec/src/main/resources/webapp && npm install && npm run build

# Dev server (with HMR; proxies API calls to a running Drillbit on :8047):
cd exec/java-exec/src/main/resources/webapp && npm run dev
```

When adding or substantially modifying a UI page, update its doc under [`docs/dev/ui/pages/`](docs/dev/ui/pages/) in the same PR. See [`docs/dev/ui/README.md`](docs/dev/ui/README.md) for conventions.

## Git conventions

- Do **not** add Claude as a co-author on commits.
- Commits should be descriptive in the imperative ("Add cluster page", not "Added cluster page").
