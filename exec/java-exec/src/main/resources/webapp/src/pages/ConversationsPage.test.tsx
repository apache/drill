/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { useEffect, useReducer, useState } from 'react';
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import { render, screen, waitFor, fireEvent, within, act } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import App from '../App';
import { useProspector } from '../hooks/useProspector';
import type { UseProspectorReturn, SaveStatus } from '../hooks/useProspector';
import {
  listConversations,
  createConversation,
  renameConversation,
  deleteConversation,
} from '../api/conversations';
import type { ChatMessage, ConversationRecord } from '../types/ai';
import type { QueryResult } from '../types';
import type { ResultsGridProps } from '../components/results/ResultsGrid';
import { getSavedQuery } from '../api/savedQueries';
import type { SavedQuery } from '../types';
import { AxiosError } from 'axios';
import type { AxiosResponse } from 'axios';

vi.mock('../hooks/useProspector', () => ({ useProspector: vi.fn() }));
vi.mock('../api/conversations', () => ({
  listConversations: vi.fn(),
  createConversation: vi.fn(),
  renameConversation: vi.fn(),
  deleteConversation: vi.fn(),
}));
// The canvas viewers fetch the artifact they show. Only the saved query is faked here, which
// is enough to drive a real artifact onto the canvas; the rest of the module stays real.
vi.mock('../api/savedQueries', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../api/savedQueries')>()),
  getSavedQuery: vi.fn(),
}));
vi.mock('../api/projects', () => ({
  getProject: vi.fn(async () => ({ id: 'p1', name: 'Project One' })),
  getProjects: vi.fn(async () => []),
}));
// The shell (sidebar, toolbar, inspector) is not under test and pulls in its own data.
vi.mock('../components/shell/AppShell', () => ({
  default: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

// The real grid is covered by ResultsCanvas.test.tsx; here it only needs to say what it got.
vi.mock('../components/results/ResultsGrid', () => ({
  default: ({ results }: ResultsGridProps) => (
    <div data-testid="grid">{results?.rows.map((r) => JSON.stringify(r)).join(';')}</div>
  ),
}));

function record(over: Partial<ConversationRecord> & { id: string }): ConversationRecord {
  return {
    projectId: 'p1',
    owner: 'me',
    name: 'Named conversation',
    createdAt: 1,
    updatedAt: 1,
    messageCount: 0,
    version: 1,
    ...over,
  };
}

const reloadConversation = vi.fn();
const clearChat = vi.fn();
const sendMessage = vi.fn();
let hookState: { messages: ChatMessage[]; saveStatus: SaveStatus; isStreaming: boolean; isLoaded: boolean };
// Pages that render the mocked hook, so a test can change what it returns mid-test.
const hookSubscribers = new Set<() => void>();

/** Change what the mocked hook returns and re-render the page, as a real stream would. */
function setHook(patch: Partial<typeof hookState>) {
  act(() => {
    Object.assign(hookState, patch);
    hookSubscribers.forEach((rerender) => rerender());
  });
}

/** Where the router is now, so a test can see the state a page left behind. */
let currentLocation: ReturnType<typeof useLocation>;
function LocationProbe() {
  currentLocation = useLocation();
  return null;
}

function renderAt(
  path: string | { pathname: string; state: unknown },
  client = new QueryClient({ defaultOptions: { queries: { retry: false } } }),
) {
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter
        initialEntries={[path]}
        future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
      >
        <LocationProbe />
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

/** The conversation the page last bound the Prospector hook to. */
function boundConversation() {
  const calls = vi.mocked(useProspector).mock.calls;
  const last = calls[calls.length - 1];
  return last ? { conversationId: last[3], projectId: last[4] } : undefined;
}

/** What the page gave the hook to be told about query results, for one conversation. */
function queryResultEmitter(conversationId: string) {
  const calls = vi.mocked(useProspector).mock.calls.filter((c) => c[3] === conversationId);
  const emit = calls[calls.length - 1]?.[5];
  if (!emit) {
    throw new Error(`The page passed no onQueryResult for ${conversationId}`);
  }
  return (sql: string, rows: Record<string, unknown>[]) =>
    act(() => emit(sql, { columns: Object.keys(rows[0] ?? {}), metadata: [], rows, queryId: 'q' } as QueryResult));
}

describe('ConversationsPage', () => {
  // App lazy-loads its routes. Pay the first dynamic-import cost here, not inside a
  // test's findBy timeout, where a loaded machine makes the first test flaky.
  beforeAll(async () => {
    await Promise.all([import('./ConversationsPage'), import('../components/project')]);
  }, 30_000);

  beforeEach(() => {
    vi.clearAllMocks();
    // jsdom has no layout; the message list scrolls itself into view on every render.
    Element.prototype.scrollIntoView = vi.fn();
    hookState = { messages: [], saveStatus: 'idle', isStreaming: false, isLoaded: true };
    hookSubscribers.clear();
    vi.mocked(useProspector).mockImplementation(
      (_a, _b, _c, conversationId) => {
        // A stand-in for the real hook, so it may use hooks: it is called once per render.
        const [, rerender] = useReducer((n: number) => n + 1, 0);
        useEffect(() => {
          hookSubscribers.add(rerender);
          return () => {
            hookSubscribers.delete(rerender);
          };
        }, []);
        return {
          messages: hookState.messages,
          isStreaming: hookState.isStreaming,
          streamingContent: '',
          usage: null,
          sendMessage,
          stopStreaming: vi.fn(),
          clearChat,
          executeToolCall: vi.fn(),
          conversationId: conversationId ?? null,
          isLoaded: hookState.isLoaded,
          saveStatus: hookState.saveStatus,
          reloadConversation,
        } as UseProspectorReturn;
      },
    );
    vi.mocked(renameConversation).mockResolvedValue(record({ id: 'c1' }));
    vi.mocked(deleteConversation).mockResolvedValue(undefined);
  });

  it('lands on Conversations when opening a project', async () => {
    vi.mocked(listConversations).mockResolvedValue([record({ id: 'c1', name: 'Landing one' })]);
    renderAt('/projects/p1');
    expect(await screen.findByText('Landing one')).toBeInTheDocument();
    expect(boundConversation()).toEqual({ conversationId: 'c1', projectId: 'p1' });
  });

  it('redirects /chat to the most recent conversation', async () => {
    vi.mocked(listConversations).mockResolvedValue([
      record({ id: 'old', name: 'Older', updatedAt: 100 }),
      record({ id: 'newest', name: 'Newest', updatedAt: 900 }),
      record({ id: 'mid', name: 'Middle', updatedAt: 500 }),
    ]);
    renderAt('/projects/p1/chat');
    await waitFor(() => expect(boundConversation()?.conversationId).toBe('newest'));
  });

  it('opens the conversation named in the URL instead of the most recent one', async () => {
    vi.mocked(listConversations).mockResolvedValue([
      record({ id: 'old', updatedAt: 100 }),
      record({ id: 'newest', updatedAt: 900 }),
    ]);
    renderAt('/projects/p1/chat/old');
    await waitFor(() => expect(boundConversation()?.conversationId).toBe('old'));
    expect(vi.mocked(useProspector).mock.calls.every((c) => c[3] === 'old')).toBe(true);
  });

  it('renders an empty state with a New conversation action when the project has none', async () => {
    vi.mocked(listConversations).mockResolvedValue([]);
    vi.mocked(createConversation).mockResolvedValue(record({ id: 'fresh', name: 'New conversation' }));
    renderAt('/projects/p1/chat');

    expect(await screen.findByText('No conversations yet')).toBeInTheDocument();
    expect(boundConversation()).toBeUndefined();

    fireEvent.click(screen.getAllByRole('button', { name: /new conversation/i })[0]);
    await waitFor(() => expect(boundConversation()).toEqual({ conversationId: 'fresh', projectId: 'p1' }));
    expect(createConversation).toHaveBeenCalledWith('p1');
  });

  it('does not bind a conversation or claim the project is empty when the list fails to load', async () => {
    vi.mocked(listConversations).mockRejectedValue(new Error('boom'));
    renderAt('/projects/p1/chat');
    expect(await screen.findByText(/could not load conversations/i)).toBeInTheDocument();
    expect(screen.queryByText('No conversations yet')).not.toBeInTheDocument();
    expect(boundConversation()).toBeUndefined();
  });

  describe('a conversation seeded by another page', () => {
    const seed = {
      prompt: 'Fix the error in my SQL query.',
      context: { feature: 'sql_lab_chat', currentSql: 'select * from nope', error: 'table not found' },
    };
    const SEEDED = { pathname: '/projects/p1/chat', state: { seed } };

    beforeEach(() => {
      // Like the server: once created, the new conversation shows up in the list.
      const existing = [record({ id: 'recent', name: 'Unrelated thread', updatedAt: 900 })];
      const fresh = record({ id: 'fresh', name: 'New conversation', updatedAt: 1000 });
      let created = false;
      vi.mocked(listConversations).mockImplementation(async () => (created ? [fresh, ...existing] : existing));
      vi.mocked(createConversation).mockImplementation(async () => {
        created = true;
        return fresh;
      });
    });

    it('starts a new conversation instead of joining the most recent one, and sends the seed there', async () => {
      renderAt(SEEDED);
      await waitFor(() => expect(sendMessage).toHaveBeenCalledTimes(1));
      expect(createConversation).toHaveBeenCalledWith('p1');
      expect(boundConversation()).toEqual({ conversationId: 'fresh', projectId: 'p1' });
      expect(vi.mocked(useProspector).mock.calls.some((c) => c[3] === 'recent')).toBe(false);
      const [text, context] = sendMessage.mock.calls[0];
      expect(text).toBe(seed.prompt);
      expect(context).toMatchObject({
        feature: 'sql_lab_chat',
        currentSql: 'select * from nope',
        error: 'table not found',
        projectId: 'p1',
        conversationMode: true,
      });
    });

    it('waits for the conversation to load before sending', async () => {
      hookState.isLoaded = false;
      renderAt(SEEDED);
      await waitFor(() => expect(boundConversation()?.conversationId).toBe('fresh'));
      expect(sendMessage).not.toHaveBeenCalled();
      setHook({ isLoaded: true });
      await waitFor(() => expect(sendMessage).toHaveBeenCalledTimes(1));
    });

    it('consumes the seed: nothing is left in history to re-send on a reload or a back-navigation', async () => {
      renderAt(SEEDED);
      await waitFor(() => expect(sendMessage).toHaveBeenCalledTimes(1));
      await waitFor(() => expect(currentLocation.state).toBeNull());
      expect(currentLocation.pathname).toBe('/projects/p1/chat/fresh');
      // Later renders of the same page do not send it again.
      setHook({ isStreaming: true });
      setHook({ isStreaming: false });
      expect(sendMessage).toHaveBeenCalledTimes(1);
    });

    it('sends nothing when the same conversation is opened without a seed', async () => {
      vi.mocked(listConversations).mockResolvedValue([record({ id: 'fresh' })]);
      renderAt('/projects/p1/chat/fresh');
      await waitFor(() => expect(boundConversation()?.conversationId).toBe('fresh'));
      expect(sendMessage).not.toHaveBeenCalled();
    });

    it('drops the seed and says so when the conversation cannot be created', async () => {
      vi.mocked(createConversation).mockRejectedValue(new Error('boom'));
      renderAt(SEEDED);
      await waitFor(() => expect(currentLocation.state).toBeNull());
      expect(sendMessage).not.toHaveBeenCalled();
    });
  });

  describe('naming', () => {
    const firstMessage = (content: string): ChatMessage[] => [{ role: 'user', content }];

    it('names an unnamed conversation from the first user message', async () => {
      vi.mocked(listConversations).mockResolvedValue([record({ id: 'c1', name: 'New conversation' })]);
      hookState.messages = firstMessage('Show me the top ten talkers');
      renderAt('/projects/p1/chat/c1');
      await waitFor(() =>
        expect(renameConversation).toHaveBeenCalledWith('p1', 'c1', 'Show me the top ten talkers'),
      );
    });

    it('collapses whitespace and truncates a long first message with an ellipsis', async () => {
      vi.mocked(listConversations).mockResolvedValue([record({ id: 'c1', name: 'New conversation' })]);
      hookState.messages = firstMessage(`  Summarise\n  the   ${'very long '.repeat(20)}request`);
      renderAt('/projects/p1/chat/c1');
      await waitFor(() => expect(renameConversation).toHaveBeenCalledTimes(1));
      const name = vi.mocked(renameConversation).mock.calls[0][2];
      expect(name.startsWith('Summarise the very long very long')).toBe(true);
      expect(name.length).toBeLessThanOrEqual(60);
      expect(name.endsWith('…')).toBe(true);
    });

    it('never renames a conversation that already has a name', async () => {
      vi.mocked(listConversations).mockResolvedValue([record({ id: 'c1', name: 'Quarterly review' })]);
      hookState.messages = firstMessage('something else entirely');
      renderAt('/projects/p1/chat/c1');
      await screen.findAllByText('Quarterly review');
      expect(renameConversation).not.toHaveBeenCalled();
    });

    it('leaves the default name until a user message exists', async () => {
      vi.mocked(listConversations).mockResolvedValue([record({ id: 'c1', name: 'New conversation' })]);
      hookState.messages = [{ role: 'assistant', content: 'Hello, how can I help?' }];
      renderAt('/projects/p1/chat/c1');
      await screen.findAllByText('New conversation');
      expect(renameConversation).not.toHaveBeenCalled();
    });

    it('does not name a conversation after the previous thread when switching to it', async () => {
      // Like the real hook: messages start empty for a conversation and arrive after load.
      const serverMessages: Record<string, ChatMessage[]> = {
        c1: [{ role: 'user', content: 'a question from the first thread' }],
        c2: [],
      };
      vi.mocked(useProspector).mockImplementation(((_a, _b, _c, cid) => {
        const [messages, setMessages] = useState<ChatMessage[]>([]);
        useEffect(() => {
          setMessages(serverMessages[cid as string]);
        }, [cid]);
        return {
          messages, isStreaming: false, streamingContent: '', usage: null, sendMessage,
          stopStreaming: vi.fn(), clearChat, executeToolCall: vi.fn(),
          conversationId: cid ?? null, saveStatus: 'idle', reloadConversation,
        } as UseProspectorReturn;
      }) as typeof useProspector);
      vi.mocked(listConversations).mockResolvedValue([
        record({ id: 'c1', name: 'First thread', updatedAt: 200 }),
        record({ id: 'c2', name: 'New conversation', updatedAt: 100 }),
      ]);
      renderAt('/projects/p1/chat/c1');
      await screen.findByText('First thread');

      fireEvent.click(screen.getByRole('link', { name: /new conversation/i }));
      await waitFor(() => expect(boundConversation()?.conversationId).toBe('c2'));
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(renameConversation).not.toHaveBeenCalled();
    });

    it('shows the new name in the list once it is saved', async () => {
      vi.mocked(listConversations)
        .mockResolvedValueOnce([record({ id: 'c1', name: 'New conversation' })])
        .mockResolvedValue([record({ id: 'c1', name: 'Show me the top ten talkers' })]);
      hookState.messages = firstMessage('Show me the top ten talkers');
      renderAt('/projects/p1/chat/c1');
      expect(await screen.findByText('Show me the top ten talkers')).toBeInTheDocument();
    });
  });

  describe('save status banners', () => {
    beforeEach(() => {
      vi.mocked(listConversations).mockResolvedValue([record({ id: 'c1' })]);
    });

    it('shows no banner while idle or saving', async () => {
      hookState.saveStatus = 'saving';
      renderAt('/projects/p1/chat/c1');
      await screen.findAllByText('Named conversation');
      expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    });

    it('shows a banner when saveStatus is too-large and lets the user start a new conversation', async () => {
      hookState.saveStatus = 'too-large';
      vi.mocked(createConversation).mockResolvedValue(record({ id: 'c2', name: 'New conversation' }));
      renderAt('/projects/p1/chat/c1');

      const banner = await screen.findByRole('alert');
      expect(banner).toHaveTextContent(/size limit/i);
      fireEvent.click(screen.getByRole('button', { name: /start a new conversation/i }));

      await waitFor(() => expect(boundConversation()?.conversationId).toBe('c2'));
      expect(createConversation).toHaveBeenCalledWith('p1');
    });

    it('shows a banner when saveStatus is conflict, explains the discard, and reloads on request', async () => {
      hookState.saveStatus = 'conflict';
      renderAt('/projects/p1/chat/c1');

      const banner = await screen.findByRole('alert');
      expect(banner).toHaveTextContent(/another device/i);
      expect(banner).toHaveTextContent(/discard/i);
      expect(reloadConversation).not.toHaveBeenCalled();

      fireEvent.click(screen.getByRole('button', { name: /reload/i }));
      expect(reloadConversation).toHaveBeenCalledTimes(1);
    });

    it('tells the user when a save failed so a turn is not silently unsaved', async () => {
      hookState.saveStatus = 'error';
      renderAt('/projects/p1/chat/c1');
      expect(await screen.findByRole('alert')).toHaveTextContent(/not saved/i);
    });
  });

  it('turns the panel clear button into starting a new conversation, never emptying the old one', async () => {
    vi.mocked(listConversations).mockResolvedValue([record({ id: 'c1' })]);
    vi.mocked(createConversation).mockResolvedValue(record({ id: 'c2', name: 'New conversation' }));
    hookState.messages = [{ role: 'user', content: 'hello' }, { role: 'assistant', content: 'hi' }];
    renderAt('/projects/p1/chat/c1');

    await screen.findAllByText('Named conversation');
    const header = document.querySelector('.prospector-panel-header') as HTMLElement;
    fireEvent.click(within(header).getByRole('button', { name: /new conversation/i }));

    await waitFor(() => expect(boundConversation()?.conversationId).toBe('c2'));
    expect(clearChat).not.toHaveBeenCalled();
  });

  it('binds the Prospector hook to the open conversation and project', async () => {
    vi.mocked(listConversations).mockResolvedValue([record({ id: 'c1' })]);
    renderAt('/projects/p1/chat/c1');
    await waitFor(() => expect(boundConversation()).toBeDefined());
    const args = vi.mocked(useProspector).mock.calls[0];
    expect(args[3]).toBe('c1');
    expect(args[4]).toBe('p1');
  });

  it('tells the server a chat from this page is a conversation, so the prompt builds rather than narrates', async () => {
    vi.mocked(listConversations).mockResolvedValue([record({ id: 'c1' })]);
    renderAt('/projects/p1/chat/c1');
    const box = await screen.findByPlaceholderText(/ask about your data/i);
    fireEvent.change(box, { target: { value: 'top talkers by bytes' } });
    fireEvent.keyDown(box, { key: 'Enter' });

    expect(sendMessage).toHaveBeenCalledWith(
      'top talkers by bytes',
      expect.objectContaining({ feature: 'project_chat', projectId: 'p1', conversationMode: true }),
    );
  });

  describe('renaming from the list', () => {
    it('renames a conversation and shows the new name', async () => {
      vi.mocked(listConversations)
        .mockResolvedValueOnce([record({ id: 'c1', name: 'Quarterly review' })])
        .mockResolvedValue([record({ id: 'c1', name: 'Q3 review' })]);
      renderAt('/projects/p1/chat/c1');

      fireEvent.click(await screen.findByRole('button', { name: 'Rename Quarterly review' }));
      const dialog = await screen.findByRole('dialog');
      const input = within(dialog).getByRole('textbox');
      expect(input).toHaveValue('Quarterly review');
      fireEvent.change(input, { target: { value: '  Q3 review ' } });
      fireEvent.click(within(dialog).getByRole('button', { name: /^(ok|save|rename)$/i }));

      await waitFor(() => expect(renameConversation).toHaveBeenCalledWith('p1', 'c1', 'Q3 review'));
      expect(await screen.findByText('Q3 review')).toBeInTheDocument();
    });

    it('does not send a blank name', async () => {
      vi.mocked(listConversations).mockResolvedValue([record({ id: 'c1', name: 'Quarterly review' })]);
      renderAt('/projects/p1/chat/c1');

      fireEvent.click(await screen.findByRole('button', { name: 'Rename Quarterly review' }));
      const dialog = await screen.findByRole('dialog');
      fireEvent.change(within(dialog).getByRole('textbox'), { target: { value: '   ' } });
      fireEvent.click(within(dialog).getByRole('button', { name: /^(ok|save|rename)$/i }));

      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(renameConversation).not.toHaveBeenCalled();
    });
  });

  describe('deleting from the list', () => {
    const twoConversations = () => [
      record({ id: 'a', name: 'Alpha thread', updatedAt: 900 }),
      record({ id: 'b', name: 'Beta thread', updatedAt: 500 }),
    ];

    async function confirmDelete(name: string) {
      fireEvent.click(await screen.findByRole('button', { name: `Delete ${name}` }));
      expect(await screen.findByText(`Delete "${name}"?`)).toBeInTheDocument();
      fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
    }

    it('asks for confirmation naming the conversation and deletes nothing until confirmed', async () => {
      vi.mocked(listConversations).mockResolvedValue(twoConversations());
      renderAt('/projects/p1/chat/a');

      fireEvent.click(await screen.findByRole('button', { name: 'Delete Beta thread' }));
      expect(await screen.findByText('Delete "Beta thread"?')).toBeInTheDocument();
      expect(deleteConversation).not.toHaveBeenCalled();

      fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(deleteConversation).not.toHaveBeenCalled();
      expect(screen.getByText('Beta thread')).toBeInTheDocument();
    });

    it('removes a conversation that is not open and keeps the open one', async () => {
      vi.mocked(listConversations).mockResolvedValue(twoConversations());
      renderAt('/projects/p1/chat/a');

      await confirmDelete('Beta thread');

      await waitFor(() => expect(deleteConversation).toHaveBeenCalledWith('p1', 'b'));
      await waitFor(() => expect(screen.queryByText('Beta thread')).not.toBeInTheDocument());
      expect(boundConversation()?.conversationId).toBe('a');
    });

    it('moves to the next most recent conversation when the open one is deleted', async () => {
      vi.mocked(listConversations).mockResolvedValue(twoConversations());
      renderAt('/projects/p1/chat/a');

      await confirmDelete('Alpha thread');

      await waitFor(() => expect(boundConversation()?.conversationId).toBe('b'));
      expect(deleteConversation).toHaveBeenCalledWith('p1', 'a');
    });

    it('shows the empty state when the last conversation is deleted', async () => {
      vi.mocked(listConversations).mockResolvedValue([record({ id: 'a', name: 'Alpha thread' })]);
      renderAt('/projects/p1/chat/a');

      await confirmDelete('Alpha thread');

      expect(await screen.findByText('No conversations yet')).toBeInTheDocument();
    });

    it('keeps the conversation and says so when the delete fails', async () => {
      vi.mocked(listConversations).mockResolvedValue(twoConversations());
      vi.mocked(deleteConversation).mockRejectedValue(new Error('boom'));
      const logged = vi.spyOn(console, 'error').mockImplementation(() => undefined);
      renderAt('/projects/p1/chat/a');

      await confirmDelete('Beta thread');

      expect(await screen.findByText(/could not delete/i)).toBeInTheDocument();
      expect(screen.getByText('Beta thread')).toBeInTheDocument();
      logged.mockRestore();
    });
  });

  describe('conversations deleted elsewhere', () => {
    it('does not redirect /chat to a conversation that only a stale cache still lists', async () => {
      const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
      client.setQueryData(['conversations', 'p1'], [
        record({ id: 'gone', name: 'Deleted elsewhere', updatedAt: 900 }),
        record({ id: 'kept', name: 'Still here', updatedAt: 100 }),
      ]);
      // The refresh is held open: until it answers, the cached list must not be trusted.
      let answer: (list: ConversationRecord[]) => void = () => undefined;
      vi.mocked(listConversations).mockReturnValue(new Promise((resolve) => { answer = resolve; }));
      renderAt('/projects/p1/chat', client);

      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(vi.mocked(useProspector).mock.calls.some((c) => c[3] === 'gone')).toBe(false);

      answer([record({ id: 'kept', name: 'Still here', updatedAt: 100 })]);
      await waitFor(() => expect(boundConversation()?.conversationId).toBe('kept'));
      expect(vi.mocked(useProspector).mock.calls.some((c) => c[3] === 'gone')).toBe(false);
    });

    it('falls back to the most recent conversation when the URL names one that no longer exists', async () => {
      vi.mocked(listConversations).mockResolvedValue([record({ id: 'kept', name: 'Still here' })]);
      renderAt('/projects/p1/chat/ghost');

      await waitFor(() => expect(boundConversation()?.conversationId).toBe('kept'));
    });

    it('falls back to the empty state when nothing is left', async () => {
      vi.mocked(listConversations).mockResolvedValue([]);
      renderAt('/projects/p1/chat/ghost');

      expect(await screen.findByText('No conversations yet')).toBeInTheDocument();
    });
  });

  describe('canvas', () => {
    beforeEach(() => {
      vi.mocked(listConversations).mockResolvedValue([
        record({ id: 'c1', name: 'First thread', updatedAt: 200 }),
        record({ id: 'c2', name: 'Second thread', updatedAt: 100 }),
      ]);
    });

    it('starts empty and shows a query result the hook reports', async () => {
      renderAt('/projects/p1/chat/c1');
      expect(await screen.findByText(/nothing on the canvas yet/i)).toBeInTheDocument();

      queryResultEmitter('c1')('SELECT region FROM sales', [{ region: 'West' }]);
      expect(await screen.findByTestId('grid')).toHaveTextContent('{"region":"West"}');
      fireEvent.click(screen.getByRole('tab', { name: 'SQL' }));
      expect(screen.getByText('SELECT region FROM sales')).toBeInTheDocument();
    });

    it('replaces the result with the next one', async () => {
      renderAt('/projects/p1/chat/c1');
      await screen.findByText(/nothing on the canvas yet/i);
      const emit = queryResultEmitter('c1');
      emit('SELECT 1', [{ n: 1 }]);
      emit('SELECT 2', [{ n: 2 }]);
      expect(await screen.findByTestId('grid')).toHaveTextContent('{"n":2}');
    });

    it('holds the pinned result while a newer one arrives', async () => {
      renderAt('/projects/p1/chat/c1');
      await screen.findByText(/nothing on the canvas yet/i);
      const emit = queryResultEmitter('c1');
      emit('SELECT 1', [{ n: 1 }]);
      fireEvent.click(await screen.findByRole('button', { name: 'Pin canvas' }));
      emit('SELECT 2', [{ n: 2 }]);
      expect(screen.getByTestId('grid')).toHaveTextContent('{"n":1}');

      fireEvent.click(screen.getByRole('button', { name: 'Unpin canvas' }));
      expect(screen.getByTestId('grid')).toHaveTextContent('{"n":2}');
    });

    it('does not carry a result, or its pin, into another conversation', async () => {
      renderAt('/projects/p1/chat/c1');
      await screen.findByText(/nothing on the canvas yet/i);
      queryResultEmitter('c1')('SELECT 1', [{ n: 1 }]);
      fireEvent.click(await screen.findByRole('button', { name: 'Pin canvas' }));
      await screen.findByTestId('grid');

      fireEvent.click(screen.getByRole('link', { name: /second thread/i }));
      await waitFor(() => expect(boundConversation()?.conversationId).toBe('c2'));
      expect(await screen.findByText(/nothing on the canvas yet/i)).toBeInTheDocument();
      expect(screen.queryByTestId('grid')).not.toBeInTheDocument();

      // The pin belonged to the first conversation: the second follows its own results.
      queryResultEmitter('c2')('SELECT 2', [{ n: 2 }]);
      expect(await screen.findByTestId('grid')).toHaveTextContent('{"n":2}');
      expect(screen.getByRole('button', { name: 'Pin canvas' })).toBeInTheDocument();
    });

    it('ignores a result that finishes after the user has moved to another conversation', async () => {
      renderAt('/projects/p1/chat/c1');
      await screen.findByText(/nothing on the canvas yet/i);
      const lateEmit = queryResultEmitter('c1');

      fireEvent.click(await screen.findByRole('link', { name: /second thread/i }));
      await waitFor(() => expect(boundConversation()?.conversationId).toBe('c2'));
      lateEmit('SELECT late', [{ n: 'late' }]);

      expect(await screen.findByText(/nothing on the canvas yet/i)).toBeInTheDocument();
      expect(screen.queryByTestId('grid')).not.toBeInTheDocument();
    });

    it('toggles full screen from the canvas control', async () => {
      renderAt('/projects/p1/chat/c1');
      await screen.findByText(/nothing on the canvas yet/i);
      queryResultEmitter('c1')('SELECT 1', [{ n: 1 }]);
      fireEvent.click(await screen.findByRole('button', { name: 'Full screen' }));
      fireEvent.click(screen.getByRole('button', { name: 'Exit full screen' }));
      expect(screen.getByRole('button', { name: 'Full screen' })).toBeInTheDocument();
    });

    describe('artifact cards and following', () => {
      const asked = 'Save the churn queries for me';
      const saved = (callId: string, id: string, name: string): ChatMessage[] => [
        { role: 'assistant', content: null, toolCalls: [{ id: callId, name: 'save_query', arguments: '{}' }] },
        { role: 'tool', toolCallId: callId, name: 'save_query', content: JSON.stringify({ id, name }) },
      ];
      const ranSql = (callId: string): ChatMessage[] => [
        { role: 'assistant', content: null, toolCalls: [{ id: callId, name: 'execute_sql', arguments: '{"sql":"SELECT 1"}' }] },
        { role: 'tool', toolCallId: callId, name: 'execute_sql', content: JSON.stringify({ columns: ['n'], rowCount: 1 }) },
      ];
      const user: ChatMessage = { role: 'user', content: asked };
      const done: ChatMessage = { role: 'assistant', content: 'Done.' };

      const savedQuery = (id: string): SavedQuery => ({
        id, name: `Name of ${id}`, sql: `SELECT '${id}' AS which`, owner: 'me',
        createdAt: '', updatedAt: '', isPublic: false,
      });

      beforeEach(() => {
        vi.mocked(getSavedQuery).mockImplementation(async (id) => savedQuery(id));
      });

      const openConversation = async () => {
        renderAt('/projects/p1/chat/c1');
        await screen.findByText(/nothing on the canvas yet/i);
      };
      const cards = () => screen.queryAllByRole('button', { name: /^saved query/i });

      it('follows the artifact a finished turn created, and its History tab names the turn', async () => {
        await openConversation();
        setHook({ isStreaming: true, messages: [user] });
        setHook({ isStreaming: false, messages: [user, ...saved('t1', 'q1', 'Churn by month'), done] });

        expect(await screen.findByText("SELECT 'q1' AS which")).toBeInTheDocument();
        expect(screen.getByRole('button', { name: /^saved query.*churn by month/i })).toHaveAttribute('aria-current', 'true');

        fireEvent.click(screen.getByRole('tab', { name: 'History' }));
        // The prompt is also in the thread, so look in the canvas for it.
        const canvas = within(screen.getByRole('complementary', { name: 'Canvas' }));
        expect(await canvas.findByText(asked)).toBeInTheDocument();
        expect(canvas.getByText('Saved query')).toBeInTheDocument();
      });

      it('settles once at the end of a turn on the last artifact, never stepping through the others', async () => {
        await openConversation();
        const first = saved('t1', 'q1', 'First');
        setHook({ isStreaming: true, messages: [user, ...first] });
        // The first is already built, but the turn is not over: the canvas stays where it was.
        expect(cards()).toHaveLength(1);
        expect(screen.getByText(/nothing on the canvas yet/i)).toBeInTheDocument();

        setHook({ isStreaming: false, messages: [user, ...first, ...saved('t2', 'q2', 'Second'), done] });
        expect(await screen.findByText("SELECT 'q2' AS which")).toBeInTheDocument();
        expect(vi.mocked(getSavedQuery).mock.calls.map((c) => c[0])).not.toContain('q1');
        expect(cards()).toHaveLength(2);
      });

      it('does not follow a conversation that was only loaded, but still shows its cards', async () => {
        hookState.messages = [user, ...saved('t1', 'q1', 'Churn by month'), done];
        await openConversation();
        expect(cards()).toHaveLength(1);
        expect(screen.getByText(/nothing on the canvas yet/i)).toBeInTheDocument();
        expect(getSavedQuery).not.toHaveBeenCalled();

        fireEvent.click(cards()[0]);
        expect(await screen.findByText("SELECT 'q1' AS which")).toBeInTheDocument();
        expect(cards()[0]).toHaveAttribute('aria-current', 'true');
      });

      it('holds a pinned canvas through a turn that creates something, and catches up on unpin', async () => {
        await openConversation();
        queryResultEmitter('c1')('SELECT 1', [{ n: 1 }]);
        fireEvent.click(await screen.findByRole('button', { name: 'Pin canvas' }));

        setHook({ isStreaming: true, messages: [user] });
        setHook({ isStreaming: false, messages: [user, ...saved('t1', 'q1', 'Churn by month'), done] });

        expect(screen.getByTestId('grid')).toHaveTextContent('{"n":1}');
        expect(getSavedQuery).not.toHaveBeenCalled();
        // The card is in the thread all the same, and is not live: it is not what is shown.
        expect(cards()).toHaveLength(1);
        expect(cards()[0]).not.toHaveAttribute('aria-current');

        // The pin only held the view. Unpinning catches up to what the turn built, as it does
        // for a query result that arrived while pinned.
        fireEvent.click(screen.getByRole('button', { name: 'Unpin canvas' }));
        expect(await screen.findByText("SELECT 'q1' AS which")).toBeInTheDocument();
        expect(screen.queryByTestId('grid')).not.toBeInTheDocument();
        expect(cards()[0]).toHaveAttribute('aria-current', 'true');
      });

      it('keeps the card of the pinned artifact live while a query result arrives behind it', async () => {
        hookState.messages = [user, ...saved('t1', 'q1', 'Churn by month'), done];
        await openConversation();
        fireEvent.click(cards()[0]);
        fireEvent.click(await screen.findByRole('button', { name: 'Pin canvas' }));

        queryResultEmitter('c1')('SELECT 2', [{ n: 2 }]);
        expect(screen.getByText("SELECT 'q1' AS which")).toBeInTheDocument();
        expect(cards()[0]).toHaveAttribute('aria-current', 'true');
      });

      it('lets a card the user clicks take over a pinned canvas', async () => {
        hookState.messages = [user, ...saved('t1', 'q1', 'Churn by month'), done];
        await openConversation();
        queryResultEmitter('c1')('SELECT 1', [{ n: 1 }]);
        fireEvent.click(await screen.findByRole('button', { name: 'Pin canvas' }));

        fireEvent.click(cards()[0]);
        expect(await screen.findByText("SELECT 'q1' AS which")).toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Pin canvas' })).toBeInTheDocument();
      });

      it('puts a bare execute_sql on the canvas and drops no card for it', async () => {
        await openConversation();
        setHook({ isStreaming: true, messages: [user] });
        queryResultEmitter('c1')('SELECT 1', [{ n: 1 }]);
        setHook({ isStreaming: false, messages: [user, ...ranSql('t1'), done] });

        expect(screen.getByTestId('grid')).toHaveTextContent('{"n":1}');
        expect(cards()).toHaveLength(0);
      });

      it('carries only the saved query into the thread when a turn also ran SQL', async () => {
        await openConversation();
        setHook({ isStreaming: true, messages: [user] });
        queryResultEmitter('c1')('SELECT 1', [{ n: 1 }]);
        setHook({
          isStreaming: false,
          messages: [user, ...ranSql('t1'), ...saved('t2', 'q1', 'Churn by month'), done],
        });
        expect(await screen.findByText("SELECT 'q1' AS which")).toBeInTheDocument();
        expect(cards()).toHaveLength(1);
      });

      it('keeps the card of an artifact deleted since, and the canvas says so', async () => {
        vi.mocked(getSavedQuery).mockRejectedValue(
          new AxiosError('gone', '404', undefined, undefined, { status: 404 } as AxiosResponse),
        );
        hookState.messages = [user, ...saved('t1', 'q1', 'Churn by month'), done];
        await openConversation();

        fireEvent.click(cards()[0]);
        expect(await screen.findByText('This artifact was deleted')).toBeInTheDocument();
        expect(cards()[0]).toHaveTextContent('Churn by month');
      });
    });
  });
});
