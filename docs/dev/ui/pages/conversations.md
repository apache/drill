# Conversations

**Files:** `src/pages/ConversationsPage.tsx`, `src/components/conversation/ConversationList.tsx`
**Routes:** `/projects/:id/chat` and `/projects/:id/chat/:conversationId`. The project index route redirects to `chat`, so this is where a project opens.

## Purpose

A project's Prospector conversations as durable, project-scoped records. The server owns them (`/api/v1/projects/:id/conversations`, client in `src/api/conversations.ts`); `useProspector` runs in server-primary mode (see the `conversationId` and `projectId` parameters in `src/hooks/useProspector.ts`).

## Layout

Three panes: `ConversationList` (left, inside the page, not in the project sidebar), `ProspectorPanel` (middle), and a canvas placeholder (right, filled by a later task).

## Behavior

- `/chat` with no id redirects to the most recent conversation (by `updatedAt`). A project with none shows an empty state with a "New conversation" action. If the list fails to load, the page shows an error with Retry and never the empty state.
- Each open conversation is mounted with `key={conversationId}`, so the hook starts clean on every switch.
- Naming: a conversation still called "New conversation" (the server default) is renamed from its first user message, whitespace collapsed and cut to 60 characters with an ellipsis. One attempt per mount; a failure leaves the default name.
- The panel's header button reads "New conversation" and creates one. It never empties the current conversation: the hook refuses `clearChat` in server mode because that would overwrite the durable record.
- `saveStatus` banners above the thread:
  - `too-large`: the conversation hit its size cap and takes no more messages. The banner offers "Start a new conversation".
  - `conflict`: another device wrote first. The banner offers "Reload", which calls `reloadConversation()` and discards local changes. The hook never does this on its own.
  - `error`: informational, saving is retried.
- The list rows have Rename (a dialog; blank names are refused) and Delete. Delete asks for confirmation naming the conversation. Deleting the open conversation moves to the most recent one left, or the empty state.
- The list is only trusted once it is not being refreshed. `/chat` waits for the refresh before redirecting, and a conversation id that is absent from the fresh list (deleted on another device) falls back to `/chat`, so a stale cache never dead-ends on a 404.
- Artifact cards: when a tool call creates a durable artifact (`create_visualization`, `create_dashboard`, `save_query`, `write_wiki_page`) a card with its kind and name appears under that message. Cards are derived from the thread (the tool result's `id` and `name`, or `title` for a wiki page), not from separate state, so a conversation loaded from the server shows them too. A bare `execute_sql` puts its results on the canvas and drops no card. The card whose artifact is on the canvas renders as live. A card keeps the name the artifact was created with; if the artifact was deleted since, opening the card shows the canvas viewer's "This artifact was deleted". Code: `components/conversation/ArtifactCard.tsx`, `autoFollow.ts`.
- Auto-follow: the canvas settles once, when a turn ends (`isStreaming` going true to false), on the last artifact that turn created (`nextCanvasTarget`). It does not step through a multi-artifact turn, and a conversation that was only loaded does not move it. A pin holds the view while Prospector works (`CanvasHost` freezes what it shows), and unpinning catches up to the latest target, whether a query result or a finished turn's last artifact. Clicking a card is an explicit request, so it opens the artifact and releases the pin.
- Analytics feature slug: `project_chat`.

## Not yet here

Search of conversations in the list. Deleting a conversation does not clear its offline buffer in localStorage.
