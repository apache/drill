# Prospector Scripts and Models — Design

**Date:** 2026-11-12
**Status:** draft for review

## Intent

A user had sales data and wanted revenue predictions. SQL is the wrong tool for
that, so they gave up. That is the whole motivation.

Prospector can already write SQL, charts, dashboards, wiki pages and workflows.
It cannot do the class of work that needs real computation: forecasting,
classification, clustering, anomaly detection. This adds that, in a way that
keeps the result inspectable, editable, shared with the project, and —
when the user asks for it — running on a schedule.

## The shape

Two execution paths, chosen by what the work is, not by user preference.

**Scoring is SQL.** A trained model is a PMML file in a Drill workspace, and
`predict(<model>, <features…>)` is a Drill UDF that scores rows inside the
query, distributed, in the JVM, with no Python anywhere. Scheduling a scoring
job is therefore an ordinary Workflow on an ordinary query, with the existing
`save_to_table`, `email` and `record_alert` actions. Nothing new executes.

**Fitting is Python.** Training a model, and any analysis that re-fits on each
run (forecasting), is a Python script stored in the project. It runs in the
browser notebook interactively, and on the Drillbit when scheduled.

Most of the value lands in the first path. The second exists because the
motivating example — a revenue forecast that refreshes every Monday — needs it.

## Goals

- Ask Prospector a question SQL cannot answer; get a working, readable,
  documented script and a result, without writing Python.
- The script is project content: named, viewable, editable, shared with
  everyone who has access to the project, and subject to the project's
  existing role model.
- A model trained once can be applied in SQL by anyone, forever, with no Python.
- Any script can be put on a schedule, and the user chooses where its output
  goes.
- Prospector explains what it built and why — the narrative is part of the
  artifact, not a chat message that scrolls away.

## Non-goals

- Not a general notebook server. The browser notebook already exists for
  scratch work; this is for scripts that are worth keeping.
- No model registry, no experiment tracking, no versioned model lineage. A
  model is a file in a workspace.
- No GPU, no distributed training, no deep learning.
- No automatic retraining triggered by data drift. A schedule is a schedule.
- Not a replacement for the Workflows feature. Scripts plug into it.

## Anti-goals

Things that would make this a failure even if it worked:

- **A script that produces different numbers depending on where it ran.** The
  interactive run and the scheduled run must agree, or the feature is worse
  than useless — it is quietly wrong.
- A second scheduler. Workflows already schedules, logs, retries and reports.
- Generated Python that the user cannot read, cannot edit, or cannot run again
  by hand to see what it did.
- An ML feature only a data scientist can operate. The user in the motivating
  story does not write Python.

## Constraints

**The PMML scoring function already exists** at
`github.com/gtkcyber/drill-ml-functions` — the user wrote it, built on
`pmml4s`, and has not used it in production. It is built against
`drill-java-exec` 2.0.0-SNAPSHOT and needs updating. It comes into this repo as
a contrib module, because requiring users to install a jar defeats the story.
`scala-library` must be excluded from `jdbc-all` to stay under the jar size
limit (see `CLAUDE.md`).

**One Python per cluster, not one per surface.** The user's decision: when an
admin has configured server-side Python, it runs **both** interactive and
scheduled runs. The browser becomes a UI, not a second interpreter. Pyodide is
the runtime only on clusters with no server Python configured — and those
clusters cannot schedule scripts anyway, so the divergence never arises. This
is what keeps the anti-goal reachable: there is one interpreter, one package
set, one row cap and one serialisation path per cluster, so a forecast cannot
disagree with itself between the browser and Monday morning.

The cost, accepted knowingly: on a cluster with server Python, running a stored
script costs a server round-trip rather than executing locally, so it loses the
notebook's instant feel. The existing SQL Lab notebook is unaffected — it stays
browser-only scratch space, and is not what this feature stores.

**Server-side Python is a subprocess, not an embedded runtime.** A spike on
2026-11-12 tried to host Pyodide inside the JVM via GraalJS + GraalWasm,
specifically to make the two runtimes identical. It failed. Eight distinct
blockers, each individually solvable, none of them the last: runtime-environment
detection, missing `URL`, missing `TextDecoder`/`TextEncoder`, missing `crypto`
and `performance`, a WASM memory maximum of 32768 pages against GraalWasm's
32767, a 2 GB allocation that needs `wasm.UseUnsafeMemory`, no timers or event
loop, and a missing `WebAssembly.instantiateStreaming`. After the timer queue it
ran 241 seconds and was still pending. GraalJS also runs interpreted on stock
JDK 17 (no JVMCI), so performance is bad before any of this is fixed. Drill
would be maintaining a bespoke browser emulation, alone. Do not revisit without
new evidence.

**Python on the server is admin-installed and off by default.** No Python
configured means no scheduled scripts — scoring workflows still work, since
they are pure SQL. The admin sets an interpreter path or venv.

**Generated code executing on the server is a real surface.** Mitigations are
defense in depth, not a sandbox: a wall-clock timeout, a memory cap, no Drill
credentials in the subprocess environment, a configurable run-as OS user, and
the subprocess's working directory confined to a scratch area. This contains
runaway and accidental code. It does not contain hostile code, and the
documentation must say so plainly rather than imply a sandbox.

**Project roles already exist** (owner, editor, viewer — see
`docs/dev/FileUploads.md`). Viewers read and run a script; editors create and
edit one.

**Scheduling locks the script.** Workflows refuses to schedule another user's
saved query (422) because its author could change the SQL under the workflow
owner; a project-editable Python script running on the Drillbit as the owner is
a stronger form of the same hole. The user's decision: any editor may edit a
script freely until it is attached to an enabled schedule, after which editing
requires disabling the schedule, and only the owner can re-enable it. That
re-enable is the human checkpoint, the same shape as a Prospector-created
workflow arriving disabled. Collaborative editing survives; the escalation does
not.

**Model files are readable by anyone who can query the workspace.** A PMML file
is a file: the project role model governs the script and the script object, not
reads through the query engine, exactly as for uploaded files. Say so in the
docs rather than implying otherwise.

**Scripts get data from Drill, not from a connection string.** A script
declares the SQL it needs; the server runs it and hands over the rows. The
script never holds credentials and never opens its own connection.

## The parts the user decided

- A viewable, editable, server-side script stored in the project — not a
  browser-local notebook, and not an opaque artifact.
- Prospector uses Drill to get the data, writes the Python, and documents it.
- Scripts can be scheduled to run at regular intervals.
- **Setting a schedule requires choosing where the output goes.** It is not
  optional, and it is the user's choice: write to writable storage, raise an
  alert, send an AI-generated summary, or any combination.
- An AI-generated summary of a run is a new action type alongside the existing
  ones.
- Both execution backends get built, and which one is used is configurable.
- The PMML path and the scheduled-refit path are both in scope.
- Where server Python exists, it runs every script — interactive and scheduled
  alike. One interpreter per cluster.
- A scheduled script is locked: editing it means disabling the schedule, and
  the owner re-enables.
- Build order is A, then B, then C, with C gated on evidence (see Delivery).

## The parts left to the builder

These are my calls, made in response to a builder's review of the draft. They
are the places to push back while reading.

**The script object**

- **A script is a first-class project object**, stored server-side like a saved
  query (`SavedQueryResources`): name, description, the SQL it consumes, the
  Python body, Prospector's markdown narrative, declared package requirements,
  ownership, timestamps, and the id of the schedule holding it locked, if any.
  Not a file in a workspace — it needs listing, permissions, and references
  from a workflow.
- **Editing keeps the previous body.** One step of history, not a version
  control system: enough to recover from a bad edit to a script someone relies
  on. The model files it produced are not versioned (stated non-goal).
- **Scripts get a page of their own** under the project, listed beside saved
  queries, with a Monaco editor (bundled, not CDN) and the narrative rendered
  above the code. A Conversations canvas card links to it rather than embedding
  a second editor.

**Running a script**

- **Rows reach Python as Arrow IPC**, matching what the notebook does today
  (`tableToIPC` → `pyarrow.ipc` → `to_pandas()`). Not JSON, not CSV: pandas
  infers dtypes differently from each, and a dtype difference is a
  numbers-differ bug.
- **One row cap, one constant**, applied by whichever runtime is in use, and
  surfaced in the run record. The notebook's silent 50 000-row truncation is
  the bug to avoid: exceeding the cap fails the run with a clear message rather
  than quietly analysing part of the data.
- **Requirements are verified, never installed at run time.** A run against an
  environment missing a declared package fails immediately, naming the package,
  rather than failing deep inside user code with an `ImportError`. Installing
  into a shared, possibly read-only venv from a scheduled job is not something
  this feature does.
- **The subprocess gets no Drill credentials**, a wall-clock timeout, a memory
  cap, a scratch working directory it cannot escape, and an optional run-as OS
  user. Data is fetched by Drill as the workflow owner — through
  `ImpersonationUtil` where impersonation is enabled — and handed to the
  subprocess, which never opens a connection of its own.
- **The run record carries truncated output.** Run records live in
  `drill.sqllab.snapshots`, ZooKeeper-backed with a 1 MB ceiling, so stdout and
  stderr are capped (first and last N KB kept, middle elided, truncation
  stated). A chatty training script must not break the run log.

**Scheduling**

- **The DataFrame hand-off is a file, not an action output.** `ActionType`
  outputs are scalars (`Map<String, String>`) consumed by templates and
  conditions, and a DataFrame has no channel there. The script runner writes
  its result to a scratch file and publishes the *path* plus `rowCount` as
  ordinary scalar outputs; downstream actions read the file. This is the
  smallest change that leaves the existing action API intact.
- **A new `save_result` action, not a second mode on `save_to_table`.**
  `SaveToTableAction` re-runs the workflow's SQL as CTAS/INSERT and never looks
  at upstream outputs; teaching it to do both would make a working action
  conditional on something it was not built for. `save_result` takes the
  upstream result file and writes it to a workspace, reusing the same location,
  format and mode options so the two read alike.
- **Script workflows have no saved query.** A workflow today requires one and
  runs it first; a script declares its own SQL, so making it a saved query too
  would run the query twice and put the script's data source somewhere the
  script does not control. The workflow document gains a script reference as an
  alternative to a saved-query reference, and the validator and runner accept
  exactly one of the two.
- **A new `run_script` action type** and a new **`ai_summary`** action type,
  both implementing `ActionType` and registered in
  `ActionTypeRegistry.defaults()`. `ai_summary` works on any workflow, script or
  plain query, so it ships independently of everything else. It uses the stored
  `drill.sqllab.ai_config`, caps its own output length so `email` and
  `record_alert` can template against it, and degrades to a stated failure when
  the provider is unreachable rather than failing the whole run.

**The PMML path**

- **`drill-ml-functions` comes in as a contrib module**, relicensed under
  Apache 2.0 with headers, package `com.datadistillr.ml` renamed. The existing
  `predict()` is sound: var-arg `FieldReader[]`, a constant model path, output
  through `ComplexWriter`, the parsed model cached in `@Workspace` so it is
  parsed once per fragment, and model loading through `DrillFileSystem` and a
  Hadoop `Path` — so S3 model files already work. It needs updating, not
  redesigning: `pmml4s` moves 0.9.15 → 1.5.8 (current, published August 2025),
  the pom targets a nonexistent `drill-java-exec` 2.0.0-SNAPSHOT, and the tests
  are thin (98 lines for the functions).
- **Restrict model loading to storage plugins.** `MLUtils.getModel` accepts
  `http://` and `https://` URLs and fetches them from the Drillbit, which is a
  server-side request forgery surface reachable from any query. Remove the URL
  path, or gate it behind an admin option that is off by default.
- **The half-built PMML format plugin is out of scope.** Reading a `.pmml` file
  as a table is a separate idea; the UDF is what this feature needs.
- **Verify, do not assume, the `jdbc-all` impact.** Check whether a new contrib
  module is shaded in at all before adding `scala-library` exclusions.
- **Models land in the existing upload target.** `UploadConfigResources`
  already names a cluster-wide writable plugin and workspace with a per-project
  folder; a model is another file in it. No second setting.
- **Export to PMML uses `nyoka`.** Verified on 2026-11-12 under Pyodide 0.26.4:
  `micropip` installs it, and a scikit-learn 1.4.2 `RandomForestClassifier`
  pipeline exported to ~16 KB of valid PMML. `sklearn2pmml` is unsuitable — it
  shells out to a bundled Java jar at run time.

**Prospector**

- **One new tool, `create_script`.** Prospector's tools are defined and
  dispatched client-side (`useProspector.ts`); the server cannot police what the
  model emits, so `create_script` posts to a server endpoint that forces the
  script to arrive unscheduled, exactly as `create_workflow` forces `paused`.
  That is the enforcement point.
- **Prospector chooses the path and says which it chose.** Fit-once work
  (classifier, clusterer) yields a model plus a scoring query; re-fit work
  (forecast) yields a script. The user is told which they got, because it
  determines whether scheduling needs server Python. Before promising a scoring
  query, the exportability of the chosen model type must be checked.
- **Running a script from a conversation is a Group B question.** The
  Conversations page has no Python runtime today, and on a cluster without
  server Python it would need the notebook's Pyodide singleton — a ~30 MB CDN
  download on first use, from a hardcoded URL that air-gapped deployments cannot
  reach. On a cluster with server Python the run is a server call and none of
  that applies.

## Delivery

Three groups, shipped in order, each useful on its own.

**Group A — no Python anywhere.** The `ai_summary` action type, then the PMML
contrib module and `predict()` UDF. Delivers the scoring loop: train by hand or
in the existing notebook, score in SQL, schedule with Workflows as they stand.

**Group B — scripts as project content.** The script object and its REST
resource, the editor UI, model export to a workspace, and the `create_script`
tool. Interactive only. The motivating story becomes half true: a readable,
shared, re-runnable script.

**Group C — scheduled server Python.** The subprocess runner, `run_script`,
`save_result`, the query-less workflow mode, and the locking rule.

**Group C is gated.** It does not start until a measured test shows that a real
forecast produces the same numbers under the server runtime as under Pyodide,
and until the hand-off file format and the workflow-document change are written
down. If that measurement fails, Group C is not worth building and the honest
product is A and B plus a Run button.

## Version control — written up, not scheduled

Raised by the user after the design was otherwise settled, and deliberately not
part of Groups A–C. The one step of edit history specified for the script
object stands as the minimum; everything here supersedes it if and when this is
picked up.

"Version control" is three features with very different costs, and they should
not be decided as one.

**Tier 1 — revision history, server-side.** Every save is a revision carrying
author, timestamp, the body, and a restore action; the UI shows a diff between
any two. No authentication, no network, no new dependency — the same store the
rest of the feature uses. This covers the need that actually bites: a scheduled
forecast moved and someone has to find out what changed. If only one tier is
ever built, it is this one, and it belongs in Group B.

**Tier 2 — one-way push to a Git remote.** A button, and optionally a push on
every save, with Drill remaining the source of truth and the repository acting
as a mirror. Costs: a credential per user or per project through the existing
`credentialsProvider` plumbing, a Git client (JGit, or shelling out to `git`,
either of which needs a `jdbc-all` size check), and the usual remote failure
handling. It fails harmlessly by construction — a rejected push leaves the
script exactly as it was, and nothing that executes depends on the remote being
reachable.

**Tier 3 — two-way sync, repository as source of truth.** Pull before run,
merge conflicts, branch selection, and a story for what a scheduled run does
when the remote is unreachable or the pulled code no longer runs.

### Tier 3 is an anti-goal unless its permission story is designed first

A scheduled script is locked: editing it requires disabling the schedule, and
only the owner can re-enable. The point is that nobody changes code running as
the workflow owner without that owner's checkpoint.

If the repository is the source of truth and a run pulls before executing, then
anybody with push access to that repository changes what the Drillbit executes
on Monday, as the owner, with no Drill-side checkpoint anywhere. Repository
access is not Drill access, is not audited by Drill, and is frequently broader
than the project's editor list. That is precisely the escalation the locking
rule closes, reopened through a side door.

Tier 2 has no such problem, which is the whole reason to prefer it.

If Tier 3 is ever wanted, it needs its own design round answering at least: who
the run executes as when the code came from a remote, what pins a scheduled run
to a reviewed commit rather than a moving branch head, and what a run does when
the remote is unreachable — fail, or proceed on the last known good commit.

## Open risks

- **Runtime divergence is reduced, not eliminated.** One interpreter per
  cluster removes the common case, but a script authored on a Pyodide-only
  cluster and later run on a server-Python cluster still crosses runtimes.
  Pin the server venv to Pyodide's versions (numpy 1.26.4, pandas 2.2.0,
  scikit-learn 1.4.2, statsmodels 0.14.2 for Pyodide 0.26.4) and record the
  versions actually used in every run record, so a disagreement is diagnosable
  rather than mysterious. This is the measurement that gates Group C.
- **PMML coverage is not total.** Strong for trees, ensembles, regression and
  clustering; thin for time series — which is why forecasting takes the script
  path rather than the scoring path.
- **`drill-ml-functions` is unproven in production**, by its author's account.
  It needs current-Drill updating and real tests before it carries this feature.
- **A scheduled script holds a whole result set in memory**, on the Drillbit, in
  Python. The row cap is the defence, and exceeding it must fail cleanly.
- **Generated code on the server is contained, not sandboxed.** The timeout,
  memory cap, credential-free environment and run-as user stop runaway and
  accidental code. They do not stop hostile code. The documentation says this
  plainly rather than implying a sandbox.

## What success looks like

The user from the motivating story uploads a sales spreadsheet, asks Prospector
for a revenue forecast, and gets a chart, a number, and a readable script
explaining how it got there. They press Schedule, choose "write to `dfs.reports`
and email me a summary", and every Monday it is there. They never type Python,
and the script is there to read on the day they want to know why the number
moved.
