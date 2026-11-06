# Drill Developer Docs

## Credentials and OAuth

- [OAuthTokenStorage.md](OAuthTokenStorage.md) — **where plugin credentials live and who may change them**: shared vs `USER_TRANSLATION` auth modes, the per-identity OAuth token registry layout, the admin gate on shared tokens, how identity resolves when user authentication is disabled, and the orphaned-token-table limitation.
- [PluginCredentialsProvider.md](PluginCredentialsProvider.md) — credential provider implementations (plain, env, Vault, Hadoop).

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
