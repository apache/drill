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
- Analytics feature slug: `project_chat`.

## Not yet here

Rename, delete and search of conversations in the list. The sidebar and the Projects page still open a project at `/query`.
