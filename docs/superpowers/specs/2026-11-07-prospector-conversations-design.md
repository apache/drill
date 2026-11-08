# Prospector Conversations — design

**Date:** 2026-11-07
**Status:** approved for planning
**Author:** brainstormed with cgivre

## Intent

Today Prospector is a chat panel docked onto other pages. The analytics tool is the
product and the assistant is an accessory bolted to its edge. That framing is backwards
for the workflow we actually want: a user creates a project, links data, and from then on
*talks* their way through the analysis while Prospector does the work.

This design makes a **conversation** a first-class artifact inside a project, alongside
saved queries, visualizations, dashboards, workflows and wiki pages. The conversation
becomes the primary surface of a project. The existing tabs stay exactly as they are and
become the place you go to *verify and hand-edit* what Prospector built.

The guiding sentence from the brainstorm: **the only thing the user has to do is create
the project and link data.**

## Why now

The capability is already largely built and badly placed. Prospector can already
`execute_sql`, `create_visualization`, `create_dashboard`, `save_query` and `save_report`.
Conversations already persist (`TabConversationStore`, mirrored from `localStorage`). What
is missing is the *placement*: the chat is scoped to a SQL Lab tab, squeezed into a dock,
and has nowhere to show its work. This is a re-placement of existing capability, not a new
subsystem.

## Goals

1. A conversation is a durable, named, project-scoped artifact. A project can hold many,
   browsed from a list, resumable weeks later.
2. The conversation is the project's landing surface.
3. Prospector can create every project artifact kind from inside a conversation.
4. Everything it builds is verifiable without leaving the conversation, and independently
   verifiable from the normal tabs.
5. Manual creation keeps working everywhere it works today.

## Non-goals

- Replacing or deprecating the manual tabs. They stay, fully functional, as the
  verification and hand-editing path.
- Multi-user / real-time collaborative conversations. Single-owner, same sharing model as
  the rest of the project.
- Changing the LLM provider registry, streaming protocol, tool-call loop, or
  `ProspectorResources` chat endpoint.
- Agentic background execution. Prospector acts within a turn the user initiated.

## Anti-goals

Things that would make this a failure even if it shipped and worked:

- **A second chat surface.** If the dock survives anywhere, we have two doors to the same
  room and the "bolted on" feeling stays. The dock is deleted, not hidden behind a flag.
- **A chat that narrates instead of builds.** If Prospector's answer to "make me a
  dashboard" is SQL the user pastes somewhere, nothing has improved.
- **Verification by trust.** The user must be able to see the real artifact, its SQL and
  its rows, not the model's description of them.
- **A second navigation system.** No tab strip, MRU stack, or artifact tray competing with
  the conversation thread for "where did that chart go".

## Decisions made during the brainstorm

These were settled by the user and are not open:

### The layout: chat left, canvas right

Decided against two alternatives:

- *Thread stays light, artifacts are links* — verification costs a click each; the chat
  stops feeling like a workspace.
- *Artifacts render inline in the thread* — threads get long and heavy, and a workflow
  editor or wiki page inside a chat bubble is absurd.

The canvas is **one slot** that renders by artifact kind: chart, dashboard, saved query,
workflow, wiki page, data source. The canvas carries secondary tabs for the artifact's
substrate — for a chart: `Chart | SQL | Data | History`.

### Canvas follow behavior: latest wins, with cards and a pin

- The canvas **auto-follows the newest artifact**, settling **once at the end of a turn**
  on the last artifact created. It does not animate through each artifact mid-turn.
- **Every** artifact created drops a **card in the thread** (kind, name, click to open on
  canvas). The one currently on canvas is marked. The thread is the artifact history, in
  chronological order, with the reasoning attached.
- A **pin toggle** in the canvas corner freezes the canvas so auto-follow cannot pull a
  view out from under a user mid-read. Corner affordance, not a mode.

Explicitly rejected: a canvas tab strip. It duplicates ordering the thread already
provides and adds overflow, close, restore-on-reload and tab-vs-thread sync — the project
already paid that cost once for SQL Lab tab persistence.

### Full screen

The canvas expands to the full window; the chat collapses to a rail that flicks back open.
The expanded canvas must be **the same editor component** the corresponding tab renders,
not a chat-specific viewer. One implementation, two framings.

### Placement in the existing shell

A **Conversations** tab is added to `ProjectNavBar`, placed **first**, and becomes the
landing tab for a project. Route: `/projects/:id/chat/:conversationId`, with
`/projects/:id/chat` resolving to the most recent conversation or an empty state. The
existing six tabs are unchanged in behavior; a Workflows tab is added alongside them (see
below), so the final nav is Conversations, Query, Queries, Visualizations, Dashboards,
Workflows, Data Sources, Wiki.

### Query results are a canvas kind; ephemeral queries do not card

*(My call, from the builder check.)* `execute_sql` is the most-called tool and has no
artifact to become. The canvas gets a **results kind** — SQL plus the rows — and a bare
`execute_sql` puts its results on the canvas. This follows directly from the "verification
by trust" anti-goal: if the canvas is empty through most of a turn, the user is verifying
numbers by reading the model's prose.

Cards are for **durable artifacts only**. Exploratory query results are not artifacts and
do not drop cards; carding every intermediate query would bury the real ones. A results
canvas is transient — it is replaced by the next thing and is recoverable only by
re-running, which is correct, because it was never saved.

No existing page component provides an embeddable results grid, so this is a seventh
canvas kind that has to be built rather than mounted.

### The canvas `History` tab means conversation turns, not artifact revisions

*(My call.)* Nothing in the codebase versions an artifact, and building a revision
subsystem would be the largest item in this design by a wide margin — for a tab I
introduced in a mockup without pricing it. `History` shows **the conversation turns that
touched this artifact**: when it was created, when it was modified, and by which message.
That data already exists once conversations are project-scoped. Artifact revisions are a
separate feature and out of scope here.

### The server-side system prompt is in scope

*(My call.)* The non-goals fence off the provider registry, streaming protocol, tool-call
loop and the chat endpoint's *mechanism*. They do not fence off the prompt text assembled
inside it, and the builder check is right that "a chat that narrates instead of builds" is
a prompt property more than a UI one. Shipping a surface built on the premise that
Prospector builds things, while the prompt still coaches it to hand over SQL, would miss
the headline anti-goal.

In scope: a conversation/canvas mode in the prompt, tool-first instructions, and removal
of the notebook-tab branch. The other surface branches (dashboard panels, logs) must not
regress — see the Logs question under *Open*.

### The server is the source of truth for a conversation

*(My call, and it inverts a deliberate existing decision — worth a second look.)* Today
`localStorage` is primary and the server is a debounced mirror, chosen so "a failed server
write costs sync, never the conversation." A named, listable artifact that resumes weeks
later on another machine cannot work that way: the list pane's names and timestamps have
to come from the server.

So the server becomes primary and `localStorage` becomes an offline buffer for the active
conversation. This makes the 512 KB cap a **data-loss** risk rather than a sync risk,
which raises the stakes on the overflow decision left to the builder.

Listing must not deserialize every conversation to render the list pane, so conversation
**metadata** (id, name, project, owner, timestamps, message count) lives in its own small
store, separate from the message bodies.

### Workflows gets its tab

*(My call.)* `ProjectNavBar` has six items and no Workflows, even though
`/projects/:id/workflows` already routes. Goal 4 requires every Prospector-built artifact
to be independently verifiable from a normal tab, and a workflow is the riskiest kind to
leave verifiable only on the canvas. This PR adds the Workflows tab. It is a nav entry
pointing at routes that already exist.

### One wiki tool, not two

*(My call.)* `save_report` already writes a project wiki page, but into a fixed
`REPORTS_FOLDER` and always appending the conversation transcript, and it cannot update an
existing page. Two tools writing the same store under different rules is a
prompt-confusion and duplicate-page problem.

`write_wiki_page` generalizes it: arbitrary folder, optional transcript, and able to
update an existing page. `save_report` is removed and its behavior becomes a set of
arguments. Existing report pages are untouched.

### Data sources are read-only on the canvas

*(My call.)* The data-source canvas mounts the existing editor for inspection. Prospector
gets no data-source tool. The premise of the design is that linking data is the user's
job, and attaching storage plugins is a permissions question this feature should not open.

### Every user gets an implicit "Scratch" project

Global SQL Lab (`/query`) and Logs are not inside a project, and the dock is deleted from
both. Rather than lose AI assistance there, each user gets an auto-created personal
project. Those surfaces open a conversation inside it. This keeps one chat surface and
costs a project row plus a redirect.

### The split canvas is read-only; editing happens full screen

The split-view canvas is a viewer per artifact kind. Editing means going full screen,
which hands off to the artifact's own existing route and reuses the editors wholesale.
The cost is a back button.

This deliberately avoids extracting embeddable editors out of `DashboardViewPage` (1162
lines), `WorkflowEditorPage` and the rest, each of which is route-driven and owns its own
save, dirty and navigation behavior. If in-place editing is wanted later it is an additive
change, not a rework.

### `create_workflow` builds a live workflow, but turning it on needs confirmation

Prospector may create a fully configured, scheduled workflow with its actions set. It may
**not** switch it on by itself. Activation requires an explicit user confirmation — the
thread card carries the affordance, and the workflow does not run until the user accepts.

This is a hard gate, not a toast. A model must not be able to put a recurring email or
webhook pipeline into production without a human saying yes. Any later tool that activates,
reschedules, or re-enables a workflow inherits the same gate.

### Manual creation stays

A user can still open Visualizations and build a chart from scratch. The bet is that a
good conversational interface means they rarely want to — not that the manual path is
removed.

## Constraints

- **512 KB per conversation.** `TabConversationStore.MAX_CONVERSATION_BYTES`; the
  `PersistentStore` default provider is ZooKeeper and that is the znode limit. Over-cap
  writes return 413 today and are surfaced to the user rather than swallowed. A
  project-scoped conversation is longer-lived than a tab-scoped one, so this ceiling will
  be hit in real use and the design must say what happens. See *Left to the builder*.
- Conversations must be stored **per conversation**, not inside the `Project` record. The
  `Project` object already carries id arrays and wiki pages; embedding message bodies
  would blow the same znode limit.
- Checkstyle on `exec/java-exec` (braces on every `if`, no unused imports, Apache license
  headers on all new files).
- `jdbc-all` is already excluded via `webapp/**`; no new exclusions expected.
- Existing UI stack: React SPA, AntD, react-query, React Router. Build on it.

## What exists and carries over

| Piece | Where | Reuse |
|---|---|---|
| Chat UI, streaming, tool-call rendering | `components/prospector/*` | As-is, re-hosted in the new page |
| Tool-execution state machine | `hooks/useProspector.ts` | As-is; `storageKey` re-pointed from tab to conversation |
| SSE chat endpoint, provider registry | `ProspectorResources.java` | Unchanged |
| Conversation persistence | `TabConversationStore.java` | Re-key from tab id to conversation id |
| Artifact editors | Visualizations / Dashboards / Wiki / Query pages | Mounted by the canvas |
| Existing tools | `execute_sql`, `list_schemas`, `get_schema_info`, `create_visualization`, `create_dashboard`, `save_query`, `save_report`, `get_available_functions`, `get_project_docs` | Unchanged |

## What has to be built

1. **Conversation as a project artifact** — id, name, project id, owner, created/updated,
   message list. A store keyed by conversation id, derived from `TabConversationStore`.
   REST: list / create / rename / delete / read / write, project-scoped. A conversation
   needs an auto-derived name from its first user message, renameable.
2. **The Conversations page** — list pane, chat pane, canvas pane; full-screen toggle; pin
   toggle; artifact cards in the thread.
3. **Canvas host** — maps artifact kind to the existing editor component, with the
   secondary tab strip for SQL / Data / History. Seven kinds: results, chart, dashboard,
   saved query, workflow, wiki page, data source. **Results has no existing component and
   must be built** as an embeddable SQL-plus-rows grid.
4. **Conversation metadata store** — small, separate from message bodies, so the list pane
   does not deserialize every conversation.
5. **Missing tools** — `create_workflow` (creates live but inactive; activation is gated
   behind an explicit user confirmation), and `write_wiki_page` generalizing and replacing
   `save_report`.
5b. **Scratch project** — auto-created per user, plus the redirect that sends global SQL
   Lab and Logs into a conversation in it.
6. **System prompt work** — conversation/canvas mode, tool-first instructions, drop the
   notebook-tab branch, keep the remaining surface branches from regressing.
7. **Workflows nav tab** — a seventh entry in `ProjectNavBar` pointing at existing routes.
8. **Deletions** — the Prospector dock on Project Query, SQL Lab, Logs, and the global
   right inspector, plus `save_report` and the tab-scoped conversation path once migration
   is settled.

## Left to the builder

- **Conversation overflow at 512 KB.** Now a data-loss risk, not a sync risk, because the
  server is primary. Options: roll older turns into a summary message,
  spill to a second znode with a continuation pointer, or hard-stop with a "start a new
  conversation" prompt. Pick one and say why. The user-visible behavior matters more than
  the mechanism.
- **Migration of existing tab conversations.** Whether to lift them into project
  conversations, leave them to expire, or drop them. Low stakes — they are per-tab chat
  scratch — but it must be a deliberate call.
- Exact split ratio and resize behavior of chat vs canvas.
- Whether the conversation list lives in a pane inside the page (as mocked) or in the
  existing project sidebar.
- Empty state for a project with no conversations.
- Tool-call card design in the thread (collapsed by default vs. expanded).
- Test strategy per `docs/dev/Testing.md`.

## Docs to update in the same PR

- `docs/dev/ui/pages/` — new page doc for Conversations.
- `docs/dev/ui/pages/project-layout.md` — new tab, new landing route.
- `docs/dev/ui/components/prospector.md` — the dock is gone; the panel is re-hosted.
- `docs/dev/PROSPECTOR.md` — conversation store, new tools.
- `CLAUDE.md` doc table if a new top-level doc is added.
