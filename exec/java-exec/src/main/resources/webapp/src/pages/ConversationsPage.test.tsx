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
import { useEffect, useState } from 'react';
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import { render, screen, waitFor, fireEvent, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import App from '../App';
import { useProspector } from '../hooks/useProspector';
import type { UseProspectorReturn, SaveStatus } from '../hooks/useProspector';
import {
  listConversations,
  createConversation,
  renameConversation,
} from '../api/conversations';
import type { ChatMessage, ConversationRecord } from '../types/ai';

vi.mock('../hooks/useProspector', () => ({ useProspector: vi.fn() }));
vi.mock('../api/conversations', () => ({
  listConversations: vi.fn(),
  createConversation: vi.fn(),
  renameConversation: vi.fn(),
}));
vi.mock('../api/projects', () => ({
  getProject: vi.fn(async () => ({ id: 'p1', name: 'Project One' })),
  getProjects: vi.fn(async () => []),
}));
// The shell (sidebar, toolbar, inspector) is not under test and pulls in its own data.
vi.mock('../components/shell/AppShell', () => ({
  default: ({ children }: { children: React.ReactNode }) => <>{children}</>,
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
let hookState: { messages: ChatMessage[]; saveStatus: SaveStatus };

function renderAt(path: string) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter
        initialEntries={[path]}
        future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
      >
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

/** The conversation the page last bound the Prospector hook to. */
function boundConversation() {
  const calls = vi.mocked(useProspector).mock.calls;
  const last = calls[calls.length - 1];
  return last ? { conversationId: last[5], projectId: last[6] } : undefined;
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
    hookState = { messages: [], saveStatus: 'idle' };
    vi.mocked(useProspector).mockImplementation(
      (_a, _b, _c, _d, _e, conversationId) =>
        ({
          messages: hookState.messages,
          isStreaming: false,
          streamingContent: '',
          usage: null,
          sendMessage,
          stopStreaming: vi.fn(),
          clearChat,
          executeToolCall: vi.fn(),
          storageKey: null,
          tabId: null,
          conversationId: conversationId ?? null,
          saveStatus: hookState.saveStatus,
          reloadConversation,
        }) as UseProspectorReturn,
    );
    vi.mocked(renameConversation).mockResolvedValue(record({ id: 'c1' }));
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
    expect(vi.mocked(useProspector).mock.calls.every((c) => c[5] === 'old')).toBe(true);
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
      vi.mocked(useProspector).mockImplementation(((_a, _b, _c, _d, _e, cid) => {
        const [messages, setMessages] = useState<ChatMessage[]>([]);
        useEffect(() => {
          setMessages(serverMessages[cid as string]);
        }, [cid]);
        return {
          messages, isStreaming: false, streamingContent: '', usage: null, sendMessage,
          stopStreaming: vi.fn(), clearChat, executeToolCall: vi.fn(), storageKey: null,
          tabId: null, conversationId: cid ?? null, saveStatus: 'idle', reloadConversation,
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

  it('binds the Prospector hook in server mode, not the legacy storage-key mode', async () => {
    vi.mocked(listConversations).mockResolvedValue([record({ id: 'c1' })]);
    renderAt('/projects/p1/chat/c1');
    await waitFor(() => expect(boundConversation()).toBeDefined());
    const args = vi.mocked(useProspector).mock.calls[0];
    expect(args[3]).toBeNull();
    expect(args[4]).toBeNull();
    expect(args[5]).toBe('c1');
    expect(args[6]).toBe('p1');
  });
});
