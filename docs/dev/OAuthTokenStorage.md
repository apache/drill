# OAuth token storage and per-user credentials

How Drill stores the credentials a storage plugin uses to authenticate at its
data source, who is allowed to change them over REST, and how both behave when
Drill's own user authentication is switched off.

Companion doc: [`PluginCredentialsProvider.md`](PluginCredentialsProvider.md)
for the credential provider implementations.

## Two kinds of credential, two places they live

A `StoragePluginConfig` has an `AuthMode`:

| Auth mode | Whose credentials | Where they live |
|---|---|---|
| `SHARED_USER` (default) | one service identity for the whole cluster | the plugin config's `credentialsProvider` |
| `USER_TRANSLATION` | each end user's own | per-user, outside the shared config |

Under `USER_TRANSLATION` the storage differs again by credential type:

- **Username/password** — in the config's credentials provider, under a per-user
  map. `PlainCredentialsProvider` keeps `userCredentials: {user -> {username,
  password}}`; `VaultCredentialsProvider` resolves a path containing `$user`.
- **OAuth tokens** — *not* in the plugin config. Access and refresh tokens live
  in a `TokenRegistry` backed by the persistent store, one registry per
  identity.

That split matters: the shared `credentials` map on an OAuth plugin still holds
the admin-owned `clientID`, `clientSecret` and `tokenURI`, while the per-user
tokens obtained with them are held separately.

## Token registry layout

`OAuthTokenProvider.getOauthTokenRegistry(username)` returns the registry for
one identity, creating it on first use:

```
oauth_tokens                 <- shared registry (username null or empty)
oauth_tokens/<username>      <- one registry per user translation user
```

Each registry is a separate persistent store, keyed inside by plugin name.

**A registry must be resolved per call and never cached across identities.**
Returning a registry belonging to another user would let that user's queries run
with someone else's tokens at the data source, which is exactly what user
translation exists to prevent. `OAuthTokenProvider` therefore keeps a map of
registries keyed by identity rather than a single instance.

The same rule applies on the plugin side. `HttpStoragePlugin`,
`FileSystemPlugin` and `GoogleSheetsStoragePlugin` each previously cached the
registry in a mutable field, re-pointed by whichever user planned a query most
recently. Because a plugin instance is shared by every query, that let a
concurrent query by another user read it and authenticate at the data source as
the wrong user. They now resolve per call:

```java
plugin.getTokenTable(queryUserName)   // not plugin.getTokenTable()
```

Call sites pass the user they already hold — `HttpSchemaFactory` and
`HttpAPIConnectionSchema` their `queryUserName`, `FileSystemSchemaFactory` its
`schemaConfig.getUserName()`.

GoogleSheets additionally cached the authenticated `Sheets` and `Drive` services
and the Google `DataStore` in plugin fields, which was worse than a race: once
set, the first caller's service was returned to every later caller whoever asked.
Those are now keyed per identity. Its cache key is prefixed (`user:<name>` or
`shared`) rather than being the bare user name, so that a user actually called
`anonymous` — which is every web user when authentication is disabled — cannot
share an entry with a shared-credential plugin.

### Why not the aliases layout

[`Aliases.md`](Aliases.md)' `PersistentAliasRegistry` solves the same per-user
problem the other way round: **one** store, keyed by user name inside it, with a
reserved `$public_aliases` key for the shared case. That makes
`store.getAll()` an enumeration of every user.

OAuth tokens put the user in the store *path* instead, so there is no way to ask
"which users have tokens?" — see [Known limitation](#known-limitation-orphaned-token-tables)
below. The aliases layout is the better design; changing to it is a persistent
store format migration and has not been done.

## Who may change what, over REST

The token and credential endpoints live on two resource classes:

| Resource | Role required | Purpose |
|---|---|---|
| `StorageResources` (`/storage/*`) | `ADMIN_ROLE` | full plugin configuration |
| `CredentialResources` (`/credentials/*`) | `AUTHENTICATED_ROLE` | a user managing their own credentials |

`CredentialResources` is open to any authenticated user on purpose: supplying
your own data source credentials is not an admin action. Two consequences:

1. **It must not serialize plugin configs.** A config carries every user's
   stored credentials plus shared secrets, so `/credentials.json` returns
   `CredentialPluginInfo` — name, enabled, OAuth flag, authorize URL, and the
   caller's own credential state — never the config.
2. **Shared tokens need an in-method admin check.** The `/storage/*` OAuth
   endpoints are deprecated in favour of the `/credentials/*` ones, so admins and
   non-admins reach the same code. `OAuthRequests.getQueryUser` decides the
   target table and refuses the write if the caller may not make it:

| Plugin auth mode | Caller | Target |
|---|---|---|
| `USER_TRANSLATION` | anyone authenticated | that caller's own registry |
| anything else | admin | the shared registry |
| anything else | non-admin | **refused**, HTTP 403 |

Overwriting shared tokens changes the identity *all* users' queries run under at
the data source, which is why it is admin-only.

`updateAuthToken` resolves the target before exchanging the authorization code,
so a refused caller cannot make Drill spend the client secret on an outbound
request or consume a single-use code.

## With user authentication disabled

This is easy to get wrong, because the usual way of asking "who is calling?"
stops working.

`WebServer` installs the Jetty security handler only when authentication is
enabled. With it off there is no security handler, so `SecurityContext`
carries **no principal at all**: `getUserPrincipal()` returns null and
`isUserInRole()` returns false. `@RolesAllowed` is not enforced either, because
`RolesAllowedDynamicFeature` is only registered when auth is on.

Meanwhile queries still run as *somebody*: `DrillRestServer` builds the
anonymous session's user credentials from `AnonDrillUserPrincipal`, whose name is
`anonymous` and which reports `isAdminUser() == true` ("in anonymous mode all
users are admins").

So **`anonymous` is the identity, not "no identity"**. Resolve it by injecting
`DrillUserPrincipal`, which Drill binds to `AnonDrillUserPrincipalProvider` when
auth is off and `DrillUserPrincipalProvider` when it is on:

```java
@Inject
DrillUserPrincipal principal;   // never read identity from SecurityContext here
```

`UserResources.getCurrentUser` is the reference use. `CredentialResources`,
`StorageResources` and `OAuthRequests` all use it, which is what keeps a user
translation plugin's REST writes pointed at `oauth_tokens/anonymous` — the same
registry the query path reads — rather than at the shared registry.

Getting this wrong is silent: before registries were resolved per identity, a
single cached registry served every lookup, so a write to the "wrong" registry
still read back correctly and the mismatch was invisible until the caching was
fixed.

### Upgrade note

An auth-off deployment with a `USER_TRANSLATION` OAuth plugin previously wrote
tokens to the shared `oauth_tokens` registry. Those tokens are now read from
`oauth_tokens/anonymous`, so the old ones are ignored and the user authorizes
once more. No data is lost; the stale entries can be removed by hand.

### The shared identity must be spelled the same way everywhere

`null` means "the shared registry". GoogleSheets also has a Google-side data
store id, which must be a non-null string and is a placeholder name
(`anonymous`) for a shared plugin. Passing that placeholder where a registry
identity is expected sends a shared plugin's lookups to
`oauth_tokens/anonymous`, which the REST credential endpoints never write to —
so tokens appear to vanish. `DrillDataStoreFactory` therefore carries an explicit
`registryIdentity` alongside the Google store id, and the two are not
interchangeable.

This was invisible while one cached registry served every lookup, and only
surfaced once registries were resolved per identity.

## Known limitation: orphaned token tables

Deleting a storage plugin (`DELETE /storage/{name}.json`) deletes its token
table from the registries Drill can reach. Because the user is part of the store
*path*, there is no way to enumerate every user who ever held tokens for a
plugin, so per-user token tables belonging to users that this Drillbit has not
seen since it started cannot be discovered, and are left behind in the
persistent store.

What that means in practice:

- Tokens for users who have queried or authorized since the last restart *are*
  removed, because those registries are live in `OAuthTokenProvider`.
- Tokens written before a restart, by a user who has not been active since, are
  not reachable and remain until removed by hand.
- Nothing retroactively finds tables already orphaned by earlier versions.

A complete fix needs one of:

1. **Adopt the aliases layout** — one store keyed by user, making enumeration a
   `store.getAll()`. Correct, and a store format migration. Note that existing
   per-user stores still cannot be enumerated, so a migration cannot find them
   either; users would re-authorize.
2. **Keep an index** of users holding token registries, maintained by
   `OAuthTokenProvider`. Complete for anything written after the change, adds
   persistent state.
3. **Add listing to `PersistentStoreProvider`** so a path's child stores can be
   enumerated per backend (local filesystem, ZooKeeper). The most general, and it
   touches an SPI shared by every Drill store.

Until one is chosen, treat per-user OAuth token tables as data that outlives the
plugin, and remove them from the persistent store directly when
decommissioning a plugin that used user translation.
