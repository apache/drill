# System Options

**File:** `src/pages/OptionsPage.tsx`
**Route:** `/options`

## Purpose

Administer Drill's system options. Category sidebar + searchable list. Edit values inline, reset to default, toggle internal (developer) options.

## Data sources

| API | Module |
|---|---|
| `GET /options.json` (`getOptions`) | `api/options.ts` |
| `GET /internal_options.json` (`getInternalOptions`) | `api/options.ts` |
| `GET /api/v1/options/descriptions` (`getOptionDescriptions`) | `api/options.ts` |
| `POST /option/{name}` (`updateOption`) | `api/options.ts` |

## Layout

- **Left sidebar** — category list (planner, exec, store, security, etc.) plus pseudo-categories "All" and "Modified".
- **Main pane** — one row per option:
  - Name + description
  - Type / scope / default badges
  - Control: `Switch` (BOOLEAN), `InputNumber` (LONG / DOUBLE), `Input` (STRING)
  - Reset button (only if the value differs from default)

## Key state

- `activeCategory` — `ALL_KEY | MODIFIED_KEY | <category name>`
- `search` — substring filter
- `showInternal` — toggles whether internal options are fetched and shown
- Per-option edit state: `value`, `saving`, plus a `Modified` badge if it has unsaved changes

## Behavior

- **Categories** are derived from option names by prefix (`planner.*`, `exec.*`, …). `CATEGORY_LABELS` maps prefixes to friendly names.
- **Control selection** is driven by the option's `kind` from the description payload.
- **Saving.** Calls `updateOption` and `invalidateQueries(['options'])`. Optimistic UI is intentionally not used — the server enforces validation and rejection should surface immediately.
- **Reset** sends the default value via the same endpoint.

## Chrome

Breadcrumb: `Administration > System Options`. Toolbar: search input, "Show internal" switch.

## Quirks

- The descriptions endpoint is decoupled from the values endpoint so descriptions can be cached longer than values (they rarely change).
- Some options have side effects (e.g. queue thresholds) — the UI does not warn about them; the user is expected to know what they're changing.
