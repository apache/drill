# Notebooks

Apache Drill's SQL Lab has a notebook: a Jupyter-style cell surface that runs
**real Python in the browser**, with the active tab's query results already
loaded as a pandas DataFrame.

The Python runtime is [Pyodide](https://pyodide.org) — CPython compiled to
WebAssembly. It runs inside the user's browser tab, not on the Drillbit. Drill
executes no Python anywhere.

## Where it is

SQL Lab → the **Notebook** tab in the lower result pane
(`SqlLabPage.tsx:1288`), next to Results, Map and the rest.

| Piece | Path |
|---|---|
| UI | `webapp/src/components/notebook/NotebookPanel.tsx` |
| Runtime wrapper | `webapp/src/hooks/usePyodide.ts` |
| Backend (export only) | `exec/java-exec/.../server/rest/NotebookResources.java` |
| System option | `ExecConstants.NOTEBOOK_WRITE_POLICY` |

## The runtime

`usePyodide` is a module-level singleton: one Pyodide instance is shared by
every consumer of the hook, so the Python namespace survives tab switches
within a page load and is lost on reload.

**First use downloads roughly 30 MB** from
`https://cdn.jsdelivr.net/pyodide/v0.26.4/full/`, then preloads pandas, numpy,
matplotlib, scipy, scikit-learn and micropip. The browser caches it; later
loads are fast. A deployment with no outbound internet access has no notebook —
the CDN URL is a constant in `usePyodide.ts`, not a setting.

After load, a prelude defines the helpers the panel relies on:

- `_drill_capture_plot()` — saves the current matplotlib figure as a base64 PNG.
  matplotlib is forced to the `AGG` backend.
- `_drill_try_html(obj)` — renders a DataFrame or Series as an HTML table
  (`max_rows=100`, `max_cols=50`).
- `_drill_list_variables()` — powers the Variables panel.
- `await install('seaborn')` — micropip install of any pure-Python PyPI package.
- `await save_to_drill(df, ...)` — see [Writing back to Drill](#writing-back-to-drill).

### Getting data in

`injectDataFrame(name, columns, rows, maxRows)` binds the result set to a Python
variable. The variable is named after the **query tab**, sanitised to a legal
identifier (`My Sales 2024` → `My_Sales_2024`), defaulting to `df`.

Transfer is Arrow IPC: `tableFromJSON` → `tableToIPC` in JS, `pyarrow.ipc`
→ `to_pandas()` in Python. pyarrow is loaded on first injection only. If
anything in that path throws, it silently falls back to base64-encoded JSON and
`pd.DataFrame(...)`.

Rows are capped at `maxNotebookRows`, default 50 000, adjustable from the
panel's settings. Everything above the cap is dropped without a warning.

Injection happens when Python becomes ready and results exist, and again
lazily before any cell runs (`ensureDataInjected`) — so a cell run right after
a query does not race the transfer.

### Running cells

`runPython(code)` swaps `sys.stdout`/`sys.stderr` for `StringIO`, runs the code
with `runPythonAsync` (so top-level `await` works), then collects, in order:
the repr of the last expression, its HTML table if it was a DataFrame, any
matplotlib figure as a PNG, stdout and stderr. Errors are caught and written
into the captured stderr rather than thrown.

Execution counts are a single counter shared across the notebook, incremented
per run — they record run order, not cell order.

## The panel

Cells are `code` or `markdown`, with the usual run / move / delete / collapse
controls. The code editor is Monaco, served from the bundle (never a CDN),
with Shift+Enter bound to run.

**Toolbar:**

- **Run All** — runs every code cell in order, sequentially.
- **Snippets** — a menu of ready-made analyses that are written against the
  injected DataFrame by name: Describe Data, Data Info, Correlation Matrix,
  Histograms, Scatter Plot, Linear Regression, K-Means Clustering, Decision
  Tree Classifier, PCA Visualization, Save to Drill.
- **Packages** — a modal listing everything installed, with micropip
  install/uninstall.
- **Variables** — name, type, shape and memory for every user variable.
- **Reset** — clears the namespace and re-injects the results.
- **Save / Open Notebook**, **Export** (see below).

### Persistence

All browser-side, all `localStorage`, nothing on the server:

| Key | Holds |
|---|---|
| `drill-tab-notebooks` | autosaved cells per query tab (code and type only) |
| `drill-notebooks` | notebooks explicitly saved by name |

Outputs are never persisted — reopening a notebook gives you the code, not the
results. Writes are wrapped in try/catch, so a full or blocked `localStorage`
loses the autosave silently.

### Export

- **Export as `.py`** — cells concatenated, markdown as `#` comments.
- **Export as `.ipynb`** — nbformat 4.5, kernelspec `Python 3 (Pyodide)`,
  **outputs always empty**.
- Any matplotlib plot can be downloaded as a PNG from its output.

All three are browser downloads. Nothing is written to a project or a
workspace.

## Writing back to Drill

`await save_to_drill(df, plugin='dfs', workspace='tmp', table_name='x', format='json')`
serialises the DataFrame (`json` or `csv`) and POSTs it to
`/api/v1/notebook/export`. On success it prints and returns the Drill path, so
the data is immediately queryable with SQL.

This is the only part of the notebook that touches the server.

### `NotebookResources`

`@Path("/api/v1/notebook")`, `@RolesAllowed(AUTHENTICATED_ROLE)`.

| Endpoint | Does |
|---|---|
| `GET /workspaces` | lists writable `FileSystemConfig` workspaces, plus the current write policy |
| `POST /export` | writes the posted body into one of them |

`POST /export` enforces, in order: CSRF token, write policy, required fields,
a 100 MB body cap, format in {`json`, `csv`}, plugin exists and is a
`FileSystemPlugin`, workspace exists and `isWritable()`, and a normalised path
that still starts inside the workspace directory. The table name is sanitised
to `[a-zA-Z0-9_-]`. Every export is logged with the user name and policy.

### `web.notebook.write_policy`

System option, default `filesystem`:

| Value | Effect |
|---|---|
| `filesystem` | any authenticated user may export, subject to OS file permissions |
| `admin_only` | only Drill admins; `GET /workspaces` returns an empty list to everyone else |
| `disabled` | export refused with 403; the panel hides the option |

## Known limits

These are properties of the current design, not bugs to be surprised by:

- **Browser-only execution.** Close the tab and nothing runs. There is no
  scheduled or server-side notebook execution.
- **Browser memory is the ceiling.** The whole result set is copied into the
  WASM heap; the 50 000-row cap exists for that reason.
- **No project storage.** Notebooks live in `localStorage` on one machine, in
  one browser. They are not shared, not versioned, and not visible to anyone
  else.
- **Export writes with `java.nio.file`** (`NotebookResources`), so it only
  works for `file://` connections — not S3 or HDFS. Compare
  [FileUploads.md](FileUploads.md), which uses the Hadoop `FileSystem` API for
  exactly this reason.
- **Pyodide packages only.** micropip installs pure-Python wheels and whatever
  Pyodide has built; packages with uncompiled native extensions will not load.
- **Requires CDN access** on first use per browser.
