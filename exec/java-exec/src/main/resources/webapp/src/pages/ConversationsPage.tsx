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
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Navigate, useLocation, useNavigate, useParams } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Alert, Button, Empty, Spin, message } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { useProjectContext } from '../contexts/ProjectContext';
import { useProspector } from '../hooks/useProspector';
import {
  createConversation,
  deleteConversation,
  listConversations,
  renameConversation,
} from '../api/conversations';
import { ProspectorPanel } from '../components/prospector';
import ConversationList from '../components/conversation/ConversationList';
import CanvasHost from '../components/conversation/CanvasHost';
import { useOpenArtifact } from '../components/conversation/ArtifactCanvas';
import type { ArtifactKind } from '../components/conversation/ArtifactCanvas';
import { artifactsOfLastTurn, nextCanvasTarget } from '../components/conversation/autoFollow';
import type { CanvasTarget } from '../components/conversation/CanvasHost';
import type { ChatContext, ConversationRecord, ConversationSeed } from '../types/ai';
import type { QueryResult } from '../types';

/** The name the server gives a conversation created without one. */
const DEFAULT_CONVERSATION_NAME = 'New conversation';
const MAX_NAME_LENGTH = 60;

function isUnnamed(name: string): boolean {
  return name.trim() === '' || name === DEFAULT_CONVERSATION_NAME;
}

/** First user message as a one-line name: whitespace collapsed, cut to 60 characters. */
function nameFromMessage(text: string): string {
  const oneLine = text.replace(/\s+/g, ' ').trim();
  return oneLine.length <= MAX_NAME_LENGTH
    ? oneLine
    : `${oneLine.slice(0, MAX_NAME_LENGTH - 1).trimEnd()}…`;
}

const listKey = (projectId: string) => ['conversations', projectId];

/** The seed another page navigated here with, if any. Router state outlives a reload. */
function seedFromState(state: unknown): ConversationSeed | undefined {
  const seed = (state as { seed?: ConversationSeed } | null)?.seed;
  return seed && typeof seed.prompt === 'string' ? seed : undefined;
}

interface ConversationViewProps {
  projectId: string;
  conversationId: string;
  record: ConversationRecord | undefined;
  onNew: () => void;
}

/**
 * One open conversation. Mounted with key={conversationId}: the Prospector hook starts
 * clean for each conversation, so the previous thread's messages can never be mistaken
 * for the new one's (which would name it after the wrong first message).
 */
function ConversationView({ projectId, conversationId, record, onNew }: ConversationViewProps) {
  const queryClient = useQueryClient();
  // The canvas lives here, not in the page: this component is keyed by conversation, so the
  // target and its pin start clean for each one, and a query that finishes after the user
  // has moved on reports to an unmounted component and lands nowhere.
  const [canvasTarget, setCanvasTarget] = useState<CanvasTarget | null>(null);
  const [pinned, setPinned] = useState(false);
  // What the canvas was showing when it was pinned. A pinned canvas holds this while
  // canvasTarget goes on moving (query results and finished turns still land there, so
  // unpinning catches up).
  const [frozenTarget, setFrozenTarget] = useState<CanvasTarget | null>(null);
  const [fullScreen, setFullScreen] = useState(false);
  const handleQueryResult = useCallback(
    (sql: string, result: QueryResult) => setCanvasTarget({ kind: 'results', sql, result }),
    [],
  );
  const prospector = useProspector(
    undefined, undefined, undefined, conversationId, projectId, handleQueryResult,
  );
  const { messages, isStreaming, isLoaded, saveStatus, reloadConversation } = prospector;
  const openArtifact = useOpenArtifact(projectId);

  // At the end of a turn the canvas settles on the last artifact the turn created. Only on
  // streaming going from true to false: a conversation loaded from the server was not just
  // built, so it must not pull the canvas to its last artifact, and nothing steps through
  // the artifacts of a turn while it runs. isStreaming stays true across the whole tool
  // phase of a turn, so this is once per turn however many rounds it took. That invariant
  // lives in the hook and is guarded by useProspector.test.tsx ("has to stay true while the
  // tool runs"); the page tests mock the hook, so they cannot catch it being reverted.
  //
  // Not conditional on the pin: a pin holds the view in CanvasHost, which catches up to
  // canvasTarget when released, so the target keeps following either way.
  const wasStreaming = useRef(false);
  useEffect(() => {
    if (wasStreaming.current && !isStreaming) {
      const turn = artifactsOfLastTurn(messages).map(({ kind, id }): CanvasTarget => ({ kind, id }));
      setCanvasTarget((current) => nextCanvasTarget(turn, current));
    }
    wasStreaming.current = isStreaming;
  }, [isStreaming, messages]);

  const handleTogglePin = () => {
    if (!pinned) {
      setFrozenTarget(canvasTarget);
    }
    setPinned(!pinned);
  };

  // A card the user clicks is an explicit request, so it takes over a pinned canvas: the pin
  // guards against Prospector moving the view, not against the user.
  const handleOpenCard = useCallback((kind: ArtifactKind, id: string) => {
    setCanvasTarget({ kind, id });
    setPinned(false);
  }, []);

  // The artifact on screen now, for the card that shows it as live.
  const onCanvas = pinned ? frozenTarget : canvasTarget;
  const liveArtifact = onCanvas && onCanvas.kind !== 'results' ? { kind: onCanvas.kind, id: onCanvas.id } : null;

  const context: ChatContext = useMemo(
    () => ({ feature: 'project_chat', projectId, conversationMode: true }),
    [projectId],
  );

  // A first turn handed over by another page (SQL Lab's Fix Error, a log Analyze). Sent once
  // the conversation has loaded, and the router state is cleared in the same step: history
  // state survives a reload and a back-navigation, so leaving it would re-send the turn.
  const location = useLocation();
  const navigate = useNavigate();
  const seed = seedFromState(location.state);
  const seedSent = useRef(false);
  const { sendMessage } = prospector;
  useEffect(() => {
    if (!seed || !isLoaded || seedSent.current) {
      return;
    }
    seedSent.current = true;
    navigate(location.pathname, { replace: true, state: null });
    sendMessage(seed.prompt, { ...context, ...seed.context, projectId, conversationMode: true });
  }, [seed, isLoaded, context, projectId, sendMessage, navigate, location.pathname]);

  // In this page "clear" means "start over somewhere else". Emptying a saved
  // conversation is something the hook refuses to do.
  const panelProspector = useMemo(
    () => ({ ...prospector, clearChat: onNew }),
    [prospector, onNew],
  );

  // Name an unnamed conversation after its first user message. One attempt per mount: a
  // failed rename leaves the default name rather than retrying in a loop.
  const naming = useRef(false);
  const firstUserText = messages.find((m) => m.role === 'user')?.content ?? null;
  const unnamed = record !== undefined && isUnnamed(record.name);
  useEffect(() => {
    if (naming.current || !unnamed || !firstUserText || !nameFromMessage(firstUserText)) {
      return;
    }
    naming.current = true;
    renameConversation(projectId, conversationId, nameFromMessage(firstUserText))
      .then(() => queryClient.invalidateQueries({ queryKey: listKey(projectId) }))
      .catch((err) => console.error('Naming conversation failed', err));
  }, [unnamed, firstUserText, projectId, conversationId, queryClient]);

  return (
    <>
      <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        {saveStatus === 'too-large' && (
          <Alert
            type="warning"
            showIcon
            banner
            message="This conversation has reached its size limit and cannot take more messages."
            action={<Button size="small" onClick={onNew}>Start a new conversation</Button>}
          />
        )}
        {saveStatus === 'conflict' && (
          <Alert
            type="warning"
            showIcon
            banner
            message="Another device wrote to this conversation first."
            description="Reloading brings in that version and discards the changes you made here since."
            action={<Button size="small" danger onClick={reloadConversation}>Reload</Button>}
          />
        )}
        {saveStatus === 'error' && (
          <Alert
            type="error"
            showIcon
            banner
            message="The latest messages are not saved to the server yet. Saving will be retried."
          />
        )}
        <div style={{ flex: 1, minHeight: 0 }}>
          <ProspectorPanel
            prospector={panelProspector}
            context={context}
            clearLabel="New conversation"
            clearIcon={<PlusOutlined />}
            onOpenArtifact={handleOpenCard}
            liveArtifact={liveArtifact}
          />
        </div>
      </div>
      <CanvasHost
        target={canvasTarget}
        pinned={pinned}
        onTogglePin={handleTogglePin}
        onFullScreen={() => setFullScreen((f) => !f)}
        onOpenArtifact={openArtifact}
        messages={messages}
        isFullScreen={fullScreen}
      />
    </>
  );
}

export default function ConversationsPage() {
  const { projectId } = useProjectContext();
  const { conversationId } = useParams<{ conversationId: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const location = useLocation();
  const [isCreating, setIsCreating] = useState(false);
  const pageSeed = conversationId ? undefined : seedFromState(location.state);
  const seeding = useRef(false);

  const { data, isLoading, isFetching, error, refetch } = useQuery({
    queryKey: listKey(projectId ?? ''),
    queryFn: () => listConversations(projectId!),
    enabled: !!projectId,
  });

  const conversations = useMemo(
    () => [...(data ?? [])].sort((a, b) => b.updatedAt - a.updatedAt),
    [data],
  );

  const startConversation = useCallback(async (seed?: ConversationSeed) => {
    if (!projectId) {
      return;
    }
    setIsCreating(true);
    try {
      const created = await createConversation(projectId);
      // A list fetch still in flight predates this conversation. Left to land, it would
      // replace the entry below and the unknown-id fallback would bounce the user away.
      await queryClient.cancelQueries({ queryKey: listKey(projectId) });
      queryClient.setQueryData<ConversationRecord[]>(listKey(projectId), (old) => [
        created,
        ...(old ?? []),
      ]);
      // The seed moves into the new conversation's own navigation, replacing this entry, so
      // it is never left behind in history under /chat.
      navigate(
        `/projects/${projectId}/chat/${created.id}`,
        seed ? { replace: true, state: { seed } } : undefined,
      );
    } catch (err) {
      console.error('Creating conversation failed', err);
      message.error('Could not create a conversation. Try again.');
      if (seed) {
        navigate(`/projects/${projectId}/chat`, { replace: true, state: null });
      }
    } finally {
      setIsCreating(false);
    }
  }, [projectId, queryClient, navigate]);

  const handleNew = useCallback(async () => {
    if (!isCreating) {
      await startConversation();
    }
  }, [isCreating, startConversation]);

  // Arriving with a seed always starts a new conversation. Appending it to whichever one is
  // most recent would put an unrelated question into somebody else's thread.
  useEffect(() => {
    if (!pageSeed || !projectId || seeding.current) {
      return;
    }
    seeding.current = true;
    void startConversation(pageSeed);
  }, [pageSeed, projectId, startConversation]);

  const handleRename = useCallback(async (id: string, name: string) => {
    try {
      await renameConversation(projectId!, id, name);
    } catch (err) {
      console.error('Renaming conversation failed', err);
      message.error('Could not rename the conversation. Try again.');
      throw err;
    }
    await queryClient.invalidateQueries({ queryKey: listKey(projectId!) });
  }, [projectId, queryClient]);

  const handleDelete = useCallback(async (id: string) => {
    try {
      await deleteConversation(projectId!, id);
    } catch (err) {
      console.error('Deleting conversation failed', err);
      message.error('Could not delete the conversation. Try again.');
      await queryClient.invalidateQueries({ queryKey: listKey(projectId!) });
      return;
    }
    // If this was the open conversation it is now missing from the list, and the unknown-id
    // fallback below sends the user to the most recent one left, or the empty state.
    queryClient.setQueryData<ConversationRecord[]>(listKey(projectId!), (old) =>
      (old ?? []).filter((c) => c.id !== id));
  }, [projectId, queryClient]);

  if (!projectId) {
    return null;
  }

  // The list is only trusted once it is not being refreshed. A cached copy can still name
  // a conversation deleted elsewhere, and a redirect to it would dead-end on a 404.
  const settled = data !== undefined && !isFetching && !error;
  if (conversationId && settled && !conversations.some((c) => c.id === conversationId)) {
    return <Navigate to={`/projects/${projectId}/chat`} replace />;
  }

  let main;
  if (pageSeed) {
    main = (
      <div style={{ flex: 1, padding: 24, textAlign: 'center' }}>
        <Spin size="large" />
      </div>
    );
  } else if (conversationId) {
    main = (
      <ConversationView
        key={conversationId}
        projectId={projectId}
        conversationId={conversationId}
        record={conversations.find((c) => c.id === conversationId)}
        onNew={handleNew}
      />
    );
  } else if (error) {
    // Never fall through to the empty state here: it would invite a duplicate "New
    // conversation" for a project whose list simply failed to load.
    main = (
      <div style={{ flex: 1, padding: 24 }}>
        <Alert
          type="error"
          showIcon
          message="Could not load conversations."
          action={<Button size="small" onClick={() => refetch()}>Retry</Button>}
        />
      </div>
    );
  } else if (isLoading || isFetching) {
    main = (
      <div style={{ flex: 1, padding: 24, textAlign: 'center' }}>
        <Spin size="large" />
      </div>
    );
  } else if (conversations.length > 0) {
    return <Navigate to={`/projects/${projectId}/chat/${conversations[0].id}`} replace />;
  } else {
    main = (
      <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Empty description="No conversations yet">
          <Button type="primary" icon={<PlusOutlined />} loading={isCreating} onClick={handleNew}>
            New conversation
          </Button>
        </Empty>
      </div>
    );
  }

  return (
    <div style={{ flex: 1, display: 'flex', minHeight: 0, height: '100%' }}>
      <ConversationList
        projectId={projectId}
        conversations={conversations}
        activeId={conversationId}
        isLoading={isLoading}
        isCreating={isCreating}
        onNew={handleNew}
        onRename={handleRename}
        onDelete={handleDelete}
      />
      {main}
    </div>
  );
}
