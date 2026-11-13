# Tab Persistence and the Project Tree

Design notes for moving SQL Lab query tabs from browser-local scratch state to
per-user project content. This document is the spec; the implementation plan is
[`plans/2026-08-30-server-side-tabs.md`](plans/2026-08-30-server-side-tabs.md).

Status: **implemented** (13 tasks, see
[`plans/2026-08-30-server-side-tabs.md`](plans/2026-08-30-server-side-tabs.md)).
Not yet exercised against a running drillbit outside the test suite.

## Why

Today a tab exists only in `localStorage`, and closing one destroys it. There is no
way to recover a closed tab, no way to see a project's tabs from anywhere but the tab
strip, and tabs do not follow a user between machines.

## The model

A **tab** is a per-user scratch document belonging to a project. It is not shared with
other members of the project.

A tab has two content streams: **SQL** and **notebook cells**. (Notebooks are
deliberately out of scope for the first implementation — see
[Out of scope](#out-of-scope).) A tab used to carry a third, a Prospector
conversation; see [Prospector](#prospector).

Tabs are the scratch tier. **Saving promotes a tab into a saved query**, which is the
curated tier. That boundary is what keeps the two concepts distinct: a tab is where
work happens, a saved query is work someone decided to keep.

Tabs **do not expire.**

### Two-tier persistence

| Tier | Holds | Survives |
|---|---|---|
| `localStorage` | Every tab, from the first keystroke | Tab switching, navigation, refresh — on that device only |
| Server (`drill.sqllab.tabs`) | Tabs that have been promoted | Everything; visible in the project tree, follows the user across devices |

A tab is written locally immediately. It is **promoted** to the server when either:

1. **A query is executed from it** — including a query that fails. A failed query is
   still work the user cares about and will want back in order to fix it.
2. **It is closed while holding content** — non-empty SQL.

The second trigger exists to preserve this invariant:

> Every tab is either currently open (visible in the tab strip) or in the project tree
> (recoverable).

Without it, a user could type SQL, never run it, close the tab, and have content that
still exists in `localStorage` but has no path back to it — the tree lists only
server-side tabs.

Nothing is promoted for a tab closed with no content at all. That keeps a stray
**New Query** click from leaving a permanent `Query 7` in the tree.

### Hiding versus deleting

- **Closing a tab hides it.** The tab keeps existing and stays listed in the project
  tree. This replaces today's destructive close.
- **Deleting is explicit**, from a right-click in the tree or the tab's three-dot menu.
  Withheld for the last remaining tab, so a project always keeps one; both menus show a
  disabled item saying why rather than an action that would be refused.

Rules:

- **Locked tabs** (`isLocked`) can be hidden but never deleted. The check is enforced
  server-side; a client-side-only guard on a durable object is not a guard.
- **Tabs with dependants warn before deletion.** The warning does not block —
  blocking creates undeletable tabs. Two cases, which say different things:

  | Dependant | What actually happens | What the warning says |
  |---|---|---|
  | Visualizations (`vizIds`) | They lose their source tab | Names them; they will break |
  | Published API | **Nothing — the endpoint keeps serving** | Names the endpoint and says it stays live |

  The published-API case is not a broken dependency. `SharedQueryApi`
  (`SharedQueryApiResources.java:102`) holds its own `sql`, and `GET /{id}/data`
  re-executes that copy, so the endpoint is unaffected by the tab's deletion. The
  warning exists because a user can be wrong in either direction: believing they broke
  production, or believing they revoked an endpoint they meant to take down.

### Prospector

Tabs no longer hold a Prospector conversation. Conversations are durable,
project-scoped artifacts with their own page, and the tab-scoped model (one conversation
per tab, mirrored to `drill.sqllab.tab_conversations`, with a dock beside the editor)
was removed along with the dock.

**Existing tab conversations are dropped, not migrated.** They were per-tab chat
scratch, superseded by project conversations, and keeping a dead read path to serve them
would preserve exactly the model this work removes. What that means in practice:

- `GET`/`PUT /api/v1/tabs/{id}/conversation` and `TabConversationStore` are gone.
- Records already written to `drill.sqllab.tab_conversations` stay in the persistent
  store as unreachable orphans, and deleting a tab no longer cleans one up. Nothing
  reads them; an operator can drop the store.
- Chat histories in browser `localStorage` (`prospector_chat_<projectId>_<tabId>`) are
  never read again.
- A tab holding only a conversation and no SQL is no longer promoted on close.

### Reconciliation

On load, the client holds a local set of tabs and fetches the server set.

- Union by `id`.
- Where a tab is in both, the one with the newer `updatedAt` wins.
- Local-only tabs stay local until promoted.

If the drillbit is unreachable, the local tier keeps working and promotion is retried
later. `localStorage` is the write-through buffer; the server is the durable tier.
This is also the answer to save failures: moving SQL autosave to the network
introduces a way to lose work that does not exist today, and keeping the local write
unconditional removes it.

## Constraints discovered in the codebase

- **Tab ids must become UUIDs.** Today they are `tab-${tabCounter}` from a module-level
  counter restored per project (`querySlice.ts:58`). Unique within a project's
  `localStorage` key, not globally. Durable server-side records need real ids.
- **`PersistentStore` has no prefix scan** — `get`, `put`, `delete`, `putIfAbsent`,
  `getAll()`, `getRange(skip, take)`. Listing a user's tabs means `getAll()` plus a
  filter, which is the pattern `ProjectResources` already uses for projects.
- **Global (non-project) tabs exist** at `/query`, persisted under
  `tabsKey(undefined)`. They need a server-side home too: same store, null
  `projectId`, keyed by user.
- **The tab-to-published-API link is not persisted.** `sharedQueryApiIds` is
  `useState` in `SqlLabPage.tsx:156`, so after a reload nothing knows which tabs were
  published. `SharedQueryApi` has no `tabId` field. Today this only loses the lock's
  provenance; once tabs are deletable it means the delete path cannot warn at all.
  `SharedQueryApi` needs a `tabId`, set at creation.
- **Publishing auto-locks a tab** (`SqlLabPage.tsx:989`, `lockType: 'api'`), so
  published tabs are undeletable by default. But `handleUnlockTab`
  (`SqlLabPage.tsx:1013`) lets a user unlock one, after which it can be deleted while
  the endpoint is still live. Since the endpoint survives, this is a messaging
  problem rather than a data-loss one.

## Out of scope

**Notebooks.** They already follow this exact pattern —
`drill-tab-notebooks` is a per-`tabId` map in `localStorage`, and `drill-notebooks` is
an explicit save-to-keep list — so they will need the same treatment. Deferred by
decision. When picked up, note that:

- The promotion rule gains a third clause (non-empty notebook cells; running a cell
  counts as execution).
- `drill-tab-notebooks` is keyed by the old `tab-N` ids and will orphan silently
  unless migrated with them.
- `drill-notebooks` is not project-scoped, unlike saved queries.
- Notebook *runtime* state (the Python namespace, `notebookDfName`) cannot be
  persisted. Cells come back; variables do not.

## Open questions

None open. The Prospector-conversation questions that lived here went with the tab-scoped
conversation model; see [Prospector](#prospector).
