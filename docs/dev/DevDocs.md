# Drill Developer Docs

## Web UI

For documentation of the React single-page app served by the Drillbit — architecture, routing, sidebar/shell, and per-page references — see [`ui/README.md`](ui/README.md).

## Authentication / Login

- [LoginRefactor.md](LoginRefactor.md) — design notes for the login/auth refactor: current auth model, the three separable layers (authentication / execution identity / authorization), and a layered direction. Exploration stage.

## AI features

- [PROSPECTOR.md](PROSPECTOR.md) — Prospector AI assistant (backend, REST endpoints, providers)
- [AI_FEATURES.md](AI_FEATURES.md) — overview of all AI features
- [ai/ENTERPRISE_PROVIDERS.md](ai/ENTERPRISE_PROVIDERS.md) — enterprise AI provider configuration (proxy, SSL/TLS, custom headers, static parameters)
- [TRANSPILER.md](TRANSPILER.md) — GraalPy + sqlglot SQL transpiler

## Environment

For information about configuring your development enviornment see [Environment.md](Environment.md).

## Testing

For information about how to do integration and unit testing in Drill see [Testing.md](Testing.md).

## License Headers

For more information about working with license headers see [LicenseHeaders.md](LicenseHeaders.md)

## Javadocs

For more info about generating and using javadocs see [Javadocs.md](Javadocs.md)

## Building with Maven

For more info about the use of maven see [Maven.md](Maven.md)

## Jetty 12 Migration

For information about the Jetty 12 upgrade, known limitations, and developer guidelines see [Jetty12Migration.md](Jetty12Migration.md)

## Materialized Views

For information about materialized view support, including SQL syntax, query rewriting, and metastore integration, see [MaterializedViews.md](MaterializedViews.md)
