# Styling

The UI styling stack:

- **Ant Design 5** for components, themed via `ConfigProvider` in `src/hooks/useTheme.tsx`.
- **`src/index.css`** for global tokens, the Sonoma/Tahoe surface treatment, the shell layout, page-specific helpers, and dark-mode overrides.
- **Inline `style` props** for one-off layout (flex, padding) — used liberally; CSS Modules / styled-components are not in the stack.

There is no Tailwind, no Emotion, no SCSS. CSS custom properties are the design-token mechanism.

## Design tokens

Defined as CSS custom properties at the top of `src/index.css` (~lines 31-171).

### Color

Light and dark have separate token sets, scoped via `:root` and `:root.dark` respectively. The dark class is toggled by `ThemeProvider` (see [`contexts.md`](contexts.md)).

| Token | Purpose |
|---|---|
| `--color-bg-window`, `--color-bg-surface`, `--color-bg-overlay` | Background layers from deepest to most-elevated |
| `--color-text-primary`, `--color-text-secondary`, `--color-text-tertiary` | Text hierarchy |
| `--color-separator`, `--color-separator-strong` | Hairline rules |
| `--color-accent` | System Blue (`#007aff` light, `#0a84ff` dark) |
| `--color-success`, `--color-warning`, `--color-error`, `--color-info` | Status colors |
| `--color-glass-*` | Translucent vibrancy materials used by Sidebar, Toolbar, modals |

When picking colors in new components, use these tokens — do not hardcode hex values, except for chart palettes (defined separately).

### Spacing, radii, motion

| Token | Value |
|---|---|
| `--radius-xs` … `--radius-xl`, `--radius-pill` | 4, 8, 12, 16, 20px, 9999px |
| `--motion-fast`, `--motion-base`, `--motion-slow` | 140ms, 220ms, 360ms |
| `--ease-apple`, `--ease-spring` | Curve definitions matching Apple HIG |
| `--shadow-hairline`, `--shadow-1`, `--shadow-2`, `--shadow-3` | Elevation |

Animations should use these motion tokens — consistency across the app matters more than per-component perfection.

## Dark mode

`ThemeProvider`:

- Reads `localStorage` (`drill-theme-mode`) → `'light' | 'dark' | 'system'`.
- For `system`, watches `prefers-color-scheme: dark` and reacts to changes.
- Sets `html.dark` class for dark, removes it for light.
- Passes the matching AntD algorithm (`theme.darkAlgorithm` / `theme.defaultAlgorithm`) to `ConfigProvider`.

Component CSS pattern for dark-only overrides:

```css
.my-component { background: var(--color-bg-surface); }
:root.dark .my-component { backdrop-filter: blur(28px); }
```

Always derive from tokens first; only add a `:root.dark` override when the dark variant needs a structurally different treatment (vibrancy, different border, etc.).

When checking dark in JS (e.g. picking a Monaco theme):

```ts
const { isDark } = useTheme();
<MonacoEditor theme={isDark ? 'vs-dark' : 'vs-light'} />
```

## Ant Design theming

`useTheme.tsx` (~line 39-292) configures AntD's `ConfigProvider` with custom `token` and `components` blocks:

- **`token`** overrides global tokens (color primary, border radius, font, motion).
- **`components`** overrides per-component tokens (Button, Input, Select, Tabs, Modal, Table, etc.) for tighter alignment with the Sonoma look — softer shadows, continuous radii, glass surfaces on modals.

When using AntD components, prefer the customizations there over local CSS — they apply consistently across the app.

If you find yourself wanting to restyle an AntD component significantly, add the token override in `useTheme.tsx` rather than scattering CSS overrides across pages.

## Shell layout CSS

`index.css` lines ~1277-2099 cover the three-pane shell layout:

- `.shell` — the outer flex container.
- `.shell-sidebar`, `.shell-toolbar`, `.shell-left-rail`, `.shell-right-inspector`, `.shell-content` — the columns.
- `.shell-sidebar-section`, `.shell-sidebar-item`, `.shell-sidebar-item-icon`, `.shell-sidebar-item-label`, `.shell-sidebar-item-trailing`, `.shell-sidebar-caret` — sidebar internals.
- `.shell-sidebar-project-glyph`, `.shell-sidebar-project-row` — project list internals.
- `.shell-toolbar-breadcrumb`, `.shell-toolbar-search`, `.shell-toolbar-actions` — toolbar slots.
- `.shell-inspector-tabs`, `.shell-inspector-content` — inspector internals.

These class names are stable — don't rename without grepping for usages. When adding a new shell element, follow the `shell-*` prefix convention.

Vibrancy / glass:

```css
.shell-sidebar {
  background: var(--color-glass-sidebar);
  backdrop-filter: blur(28px) saturate(180%);
}
```

This is what gives the sidebar and toolbar their translucent material.

## Mobile / responsive

Breakpoints (defined in `index.css`):

| Width | Behavior |
|---|---|
| ≤ 768px | Sidebar and inspector become overlay drawers; some toolbar buttons hide |
| ≤ 1100px | Left rail auto-collapses (`AppChromeContext.tsx`) |

`Grid.useBreakpoint()` from AntD is used in places (e.g. SQL Lab) to swap layouts at runtime, but most responsive behavior is CSS-driven so it works during SSR-style hydration without flicker.

## Per-page styling

Pages with substantial layout (SQL Lab, Dashboards) own their own CSS section in `index.css`:

- `.sqllab-container`, `.sqllab-editor`, `.sqllab-results-grid`, `.sqllab-schema-explorer`, `.sqllab-tabs`, etc.
- `.dashboard-grid`, `.dashboard-panel`, `.dashboard-filter-bar`, `.dashboard-settings-drawer`, etc.

When adding a substantial page, follow the prefix convention: `.<page>-<element>`. Keep selectors single-class (no descendant chains) so they're easy to override.

### AG Grid

The SQL Lab and Profiles results tables use AG Grid (Alpine theme) with overrides under `.ag-theme-drill-*` (see `index.css`). Dark mode swaps to `.ag-theme-alpine-dark` plus the same drill overrides.

### Monaco

Editor theme is picked from `useTheme().isDark`. Monaco's CSS is bundled by the `monaco` chunk in `vite.config.ts`. Do not import Monaco CSS yourself — `@monaco-editor/react` handles it.

### react-grid-layout (Dashboards)

Imports `react-grid-layout/css/styles.css` and `react-resizable/css/styles.css` in `DashboardViewPage.tsx`. Custom dashboard styles (panel chrome, edit-mode handles) live under `.dashboard-*` selectors in `index.css`.

## Conventions

- **Tokens first, hex last.** If you need a color, find or add a token. Hex literals only for chart palettes.
- **AntD before custom.** If AntD has a component for it, use it. Override via `theme` in `useTheme.tsx`, not via per-page CSS.
- **Single-class selectors.** Avoid `.foo .bar .baz` chains in `index.css` — they make refactors painful. Use compound classes (`.foo-bar`) instead.
- **Prefix consistently.** `shell-*` for the shell, `<page>-*` for page-owned styles, `dashboard-*` / `sqllab-*` / `prospector-*` for feature areas.
- **Dark mode by override, not by branching.** Set the base in the unscoped rule; only add `:root.dark .x` for structurally different dark styles.
- **Animations from tokens.** Use `var(--motion-base) var(--ease-apple)` rather than inventing your own timing.
- **No inline styles for theming.** Inline `style` is fine for one-off layout (`{ padding: 12 }`) but not for colors — pull from tokens via CSS class.
