# Credentials

**File:** `src/pages/CredentialsPage.tsx`
**Route:** `/credentials`

## Purpose

Lets each user manage their own credentials for storage plugins that run with `USER_TRANSLATION` auth mode — i.e. plugins where the end user supplies the credentials Drill uses to query the underlying source. Two flavors:

- **Username/password plugins** — modal form to set/replace credentials.
- **OAuth 2.0 plugins** — popup to the provider's authorize URL; closes once the provider redirects back through Drill's OAuth callback.

Replaces the legacy `credentials/list.ftl` page.

## Data sources

| API | Module | Used for |
|---|---|---|
| `GET /credentials.json` (`getCredentialPlugins`) | `api/credentials.ts` | Plugins list (USER_TRANSLATION-only) |
| `POST /credentials/{name}/update_credentials.json` (`updateCredentials`) | `api/credentials.ts` | Save user/pass |
| `GET /credentials/{name}/update_oauth2_authtoken?code=…` | (browser, popup) | OAuth provider redirects here; backend exchanges code for tokens and serves `success.html` (popup closes itself) |

`/credentials.json` returns a list of `CredentialPlugin` rows. The new `isOauth` and `authorizationUrl` fields (added in Phase 2) come from `PluginConfigWrapper.isOauth()` and `getAuthorizationURIWithParams()` — exposed via `@JsonProperty` instead of `@JsonIgnore` so the React app can branch on auth type without a second request.

The page does not need an admin role; `CredentialResources` is annotated `@RolesAllowed(AUTHENTICATED_ROLE)` and operations are scoped to the calling user via `sc.getUserPrincipal()`.

## Child components

No external child components. The page composes:

- AntD `Table` for the plugin list (name + auth type + status + per-row Action button).
- AntD `Modal` + `Form` for the username/password editor.
- Native `window.open` for the OAuth popup; the page polls `popup.closed` and refetches.

## Key state

- `editing: { plugin, username, password } | null` — drives the credentials modal.
- `pollersRef` — `Map<string, number>` tracking active OAuth-popup pollers per plugin so the page can clean them up.
- `updateMutation` (react-query) — fires the JSON POST for username/password updates.

## Chrome

Breadcrumb: `Administration > Credentials`. Toolbar: Refresh.

## OAuth flow details

1. User clicks **Authorize**.
2. Page opens `plugin.authorizationUrl` in a 450×600 popup. URL is pre-built server-side (via `PluginConfigWrapper.getAuthorizationURIWithParams()`) so the React app never has to know the OAuth secrets.
3. Provider authenticates the user and redirects to `/credentials/{name}/update_oauth2_authtoken?code=…`.
4. Backend (`CredentialResources.updateAuthToken` → `OAuthRequests.updateAuthToken`) exchanges the code for an access/refresh token pair, stores them in `TokenRegistry`, and serves `rest/storage/success.html` — a static page that shows "Success" and a Close Window button.
5. User closes the popup. The page's 1-second poller notices `popup.closed === true`, clears the interval, and invalidates the `['credentials']` query so the row reflects the new auth state.

If the user blocks popups the page surfaces an AntD message asking them to allow them and retry.

## Routing

`/credentials` is served by the React SPA via `SpaServletFilter`. In Phase 2 the path was removed from the filter's `JERSEY_EXACT_PATHS` so the SPA gets the GET. The sub-path prefix `/credentials/` (in `JERSEY_PREFIXES`) keeps everything else — `/credentials.json`, `/credentials/{name}/update_credentials.json`, `/credentials/{name}/update_refresh_token`, `/credentials/{name}/update_access_token`, `/credentials/{name}/update_oauth_tokens`, `/credentials/{name}/update_oauth2_authtoken`, `/credentials{group}-plugins.json` — routed to `CredentialResources`.

## Quirks

- `/credentials.json` lists ONLY plugins with `USER_TRANSLATION` auth mode. Plugins configured with shared credentials don't appear here — those are managed via the Data Sources page (their config is just regular plugin config).
- The page intentionally doesn't pre-fill the username/password fields, even though the legacy UI did so via Freemarker. The JSON endpoint doesn't expose stored credentials, and starting with empty fields avoids accidentally re-submitting cached passwords.
- The OAuth popup uses 450×600 pixels to match the legacy size — some providers' login pages render poorly at narrower widths.
- The Authorize button is disabled (with a tooltip) when `authorizationUrl` is empty — typically a misconfigured plugin missing `oAuthConfig` fields.
