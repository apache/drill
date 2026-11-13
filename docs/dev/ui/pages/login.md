# Login

Covers both `LoginPage` (the form-auth screen) and `MainLoginPage` (the auth-mechanism chooser). Both render outside `AppShell` — the login UI has its own minimal centered-card chrome and bypasses the sidebar / toolbar.

## Routes

| Route | Page | Purpose |
|---|---|---|
| `/login` | `pages/LoginPage.tsx` | Username / password form. POSTs to `/j_security_check`. |
| `/mainLogin` | `pages/MainLoginPage.tsx` | "Sign in with FORM or SPNEGO" chooser. |
| `/spnegoLogin` | (no React page) | Triggers SPNEGO Negotiate via the Jetty security handler. After auth the backend redirects to `/`. |

## Auth flow

Drill's web UI uses Jetty's `FormAuthenticator`. The session-redirect dance:

1. Browser hits a protected URL, e.g. `/projects`.
2. Jetty stores `/projects` in the session under `FormAuthenticator.__J_URI` and redirects to `/login` (or `/mainLogin` if SPNEGO is also configured).
3. Backend `LogInLogOutResources.getLoginPage` (still a JAX-RS handler) updates the session redirect if a `?redirect=` query param is present, then streams the SPA `index.html` body via `SpaResponseUtil.serveSpaIndex()`.
4. React Router sees pathname `/login` and renders `LoginPage`.
5. User submits the form. The form's `action` is `/j_security_check` — Jetty's hardcoded form-auth submission URL. The browser POSTs directly; the React API client / CSRF flow is *not* involved.
6. Jetty validates credentials.
   - **Success.** Jetty marks the session authenticated and redirects to the URL it stored in step 2 (or `/` if none was stored).
   - **Failure.** Jetty internally dispatches `POST /login`. The backend (`getLoginPageAfterValidationError`) returns a tiny HTML stub that bounces the browser to `/login?error=1`. The React `LoginPage` reads `?error=1` from the URL and renders an "Invalid username or password" alert.

## SPNEGO

1. Browser hits `/spnegoLogin`. The Drill security handler routes the request to the SPNEGO authenticator.
2. Authenticator returns `401 WWW-Authenticate: Negotiate`. The browser supplies a Kerberos ticket and retries.
3. On success Jetty marks the session authenticated and dispatches to the JAX-RS `getSpnegoLogin` handler, which redirects to `/`.
4. On failure (defensive fallback only — usually the browser handles SPNEGO failures itself) the handler redirects to `/mainLogin?error=spnego` and `MainLoginPage` surfaces the message.

## Logout

`GET /logout` invalidates the session and 303-redirects to `/`. There's no React page for this; the browser just follows the redirect back to the SPA, which then re-runs the auth check on its next protected request.

## Data sources

| API | Module | Used by |
|---|---|---|
| `GET /api/v1/auth-config` (`getAuthConfig`) | `api/auth.ts` | `MainLoginPage` to decide which buttons to show |
| `POST /j_security_check` | Jetty (not modeled in any TypeScript module) | `LoginPage` form action |
| `GET /login`, `GET /mainLogin` | `LogInLogOutResources` (serves SPA index.html via `SpaResponseUtil` + session side effects) | The browser arriving at these URLs |
| `GET /spnegoLogin`, `GET /logout` | `LogInLogOutResources` (redirects only) | The browser |

`getAuthConfig` uses raw `axios` rather than the `apiClient` interceptor: a pre-auth `401` redirect loop is impossible because the response is read regardless of status.

## Chrome

Neither page renders `AppShell`. The route table in `App.tsx` uses a `ShellSwitch` wrapper that checks the pathname against `SHELL_FREE_ROUTES = {'/login', '/mainLogin'}` and skips the shell for those. Replicate the pattern if you add another pre-auth screen.

## Styling

Login styles live in the bottom of `src/index.css` under the `.login-shell` / `.login-card` / `.login-brand` / `.login-logo` selectors:

- `.login-shell` — full-viewport flex centerer with `var(--color-bg-window)` background.
- `.login-card` — 380px max, surface background, large radius, mid elevation.
- `.login-brand` — vertical logo + title + subtitle stack.
- `.login-logo` — 48px Drill logo from `/static/img/apache-drill-logo.png` (served by the existing static servlet).

## Quirks

- The form is a **native browser form submit** to `/j_security_check`, not a fetch through the API client. Jetty's FormAuthenticator intercepts the request before any application code runs, so the CSRF interceptor on `apiClient` is intentionally not involved.
- After login Jetty redirects via a `302` to the stored URL. React Router does a fresh route match against the new pathname; no special handling is needed.
- `getSpnegoLogin` is rarely reached in practice: when SPNEGO fails the browser typically shows its native auth dialog and never dispatches to the JAX-RS handler. The redirect to `/mainLogin?error=spnego` covers defensive edge cases.
- `prefetchCsrfToken()` from `main.tsx` runs on every app boot, including on the login page. The CSRF endpoint is `@PermitAll`, so this works pre-auth. The token is associated with the (anonymous) session and rotates on login.
