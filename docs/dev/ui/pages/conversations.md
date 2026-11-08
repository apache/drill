# Conversations

**Files:** `src/pages/ConversationsPage.tsx`, `src/components/conversation/ConversationList.tsx`
**Routes:** `/projects/:id/chat` and `/projects/:id/chat/:conversationId`. The project index route redirects to `chat`, so this is where a project opens.

## Purpose

A project's Prospector conversations as durable, project-scoped records. They are private to the user who made them: other members of the project do not see them (the server returns only the caller's conversations, and 403 on anyone else's). A conversation started outside any project (SQL Lab with no project, Logs) lives in the user's personal Scratch project, found through `GET /api/v1/projects/scratch`. The server owns them (`/api/v1/projects/:id/conversations`, client in `src/api/conversations.ts`); `useProspector` runs in server-primary mode (see the `conversationId` and `projectId` parameters in `src/hooks/useProspector.ts`).

## Layout

Three panes: `ConversationList` (left, inside the page, not in the project sidebar), `ProspectorPanel` (middle), and `CanvasHost` (right, an aside that shows what Prospector is working on).

Widths come from `index.css` (the `conversations-*` classes), not inline styles. The list is 200 px, the chat keeps a 300 px minimum, and the canvas is `clamp(280px, 30%, 480px)`. At 1100 px and below the canvas is hidden, since with the main sidebar open that leaves too little room for all three panes. The page reads the same breakpoint through `useCanvasHidden` (`CANVAS_HIDDEN_QUERY` in `components/conversation/canvasLayout.ts`; the CSS media query repeats the number, so change both) and adapts: clicking an artifact card opens that artifact's own page (the hand-off `useOpenArtifact` does for full screen) instead of setting a canvas target nobody can see, no card shows the Live badge, and an Open canvas button appears above the chat once something is on the canvas, opening it full screen. Results arrive mid-turn, so the button is user-initiated rather than the canvas opening itself. At 768 px and below the list stacks above the chat as a strip 30% of the viewport height. Full screen is unaffected: it drops the canvas class and fills the window.

## Behavior

- `/chat` with no id redirects to the most recent conversation (by `updatedAt`). A project with none shows an empty state with a "New conversation" action. If the list fails to load, the page shows an error with Retry and never the empty state.
- Each open conversation is mounted with `key={conversationId}`, so the hook starts clean on every switch.
- Naming: a conversation still called "New conversation" (the server default) is renamed from its first user message, whitespace collapsed and cut to 60 characters with an ellipsis. One attempt per mount; a failure leaves the default name.
- The panel's header button reads "New conversation" and creates one. It never empties the current conversation: the hook refuses `clearChat` in server mode because that would overwrite the durable record.
- `saveStatus` banners above the thread:
  - `too-large`: the conversation hit its size cap (`ConversationStore.MAX_CONVERSATION_BYTES`, 512 KB of serialized messages) and takes no more messages. This is a hard stop: the server refuses the write (`ConversationResources`), `sendMessage` refuses further turns with a warning, and the turns already sent stay on screen and in the offline buffer. The banner offers "Start a new conversation".
  - `conflict`: another device wrote first. The banner offers "Reload", which calls `reloadConversation()` and discards local changes. The hook never does this on its own.
  - `error`: informational, saving is retried.
- The list rows have Rename (a dialog; blank names are refused) and Delete. Delete asks for confirmation naming the conversation. Deleting the open conversation moves to the most recent one left, or the empty state.
- The list is only trusted once it is not being refreshed. `/chat` waits for the refresh before redirecting, and a conversation id that is absent from the fresh list (deleted on another device) falls back to `/chat`, so a stale cache never dead-ends on a 404.
- Artifact cards: when a tool call creates a durable artifact (`create_visualization`, `create_dashboard`, `save_query`, `create_workflow`, `write_wiki_page`) a card with its kind and name appears under that message. Cards are derived from the thread (the tool result's `id` and `name`, or `title` for a wiki page), not from separate state, so a conversation loaded from the server shows them too. A bare `execute_sql` puts its results on the canvas and drops no card. The card whose artifact is on the canvas renders as live. A card keeps the name the artifact was created with; if the artifact was deleted since, opening the card shows the canvas viewer's "This artifact was deleted". Code: `components/conversation/ArtifactCard.tsx`, `autoFollow.ts`.
- Auto-follow: the canvas settles once, when a turn ends (`isStreaming` going true to false), on the last artifact that turn created (`nextCanvasTarget`). It does not step through a multi-artifact turn, and a conversation that was only loaded does not move it. A pin holds the view while Prospector works (`CanvasHost` freezes what it shows), and unpinning catches up to the latest target, whether a query result or a finished turn's last artifact. Clicking a card is an explicit request, so it opens the artifact and releases the pin.
- Analytics feature slug: `project_chat`.

## Canvas

`CanvasHost` (`components/conversation/CanvasHost.tsx`) shows one target at a time. A target is either `results` (the rows of a bare `execute_sql`, via `ResultsCanvas`, tabs Results and SQL) or a project artifact by kind and id (`ArtifactCanvas`: `chart`, `dashboard`, `query`, `workflow`, `wiki`, `datasource`).

- Tabs are per kind: chart has Chart, SQL, History; dashboard, Dashboard, History; query, SQL, History; workflow and data source, Summary, History; wiki, Page, History. A kind with one tab shows no strip. The chosen tab is remembered per kind.
- History lists the turns of this conversation whose tool calls mention the artifact's id (`ArtifactHistory.turnsTouching`). Nothing versions artifacts, so it says what Prospector did, not what changed. The retired `save_report` tool name still labels as "Saved report" for old conversations.
- Pin: a pinned canvas keeps what it shows while the target keeps moving underneath; unpinning catches up. The pin and the target live in `ConversationView`, so they reset on each conversation switch.
- Full screen: for a project artifact the control hands off to that artifact's own page (`useOpenArtifact`; a saved query opens in the query editor with the query loaded, falling back to the saved queries list if it cannot be fetched). Results have no page, so for them it toggles an in-page full screen that is a modal dialog: Escape leaves it, focus moves to the exit control, and the rest of the page is made `inert`.
- Viewers fetch the artifact fresh (`staleTime: 0`); a 404 shows "This artifact was deleted" with a link to the list. The data source viewer shows only type, enabled state and workspace names, never the config, because plugin configs hold credentials.
- A workflow's card in the thread (`WorkflowActivationCard`) carries the Activate button. `create_workflow` only ever creates the workflow paused; Activate opens a confirm dialog built from the workflow as the server holds it, with Cancel focused, and only its OK calls `setWorkflowEnabled`. See [WORKFLOWS.md](../../WORKFLOWS.md#workflows-created-by-prospector).

## Not yet here

Search of conversations in the list. Deleting a conversation does not clear its offline buffer in localStorage.

## Seeded conversations

Other pages (SQL Lab, Logs) can open a conversation with a first turn already written. They navigate to `/projects/<id>/chat` with router state `{ seed: { prompt, context } }` (`ConversationSeed`, `useAskProspector`). `ConversationsPage` then always creates a new conversation, never joining the most recent one, navigates to it replacing the entry, and `ConversationView` sends the prompt once `useProspector` reports `isLoaded`, with `context` merged over the project context and `conversationMode` kept on. The router state is cleared in the same step as the send, because history state survives a reload and a back-navigation. If creating the conversation fails the seed is dropped with an error message.
