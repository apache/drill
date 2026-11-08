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
import { Navigate, useNavigate, useParams } from 'react-router-dom';
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
import type { ChatContext, ConversationRecord } from '../types/ai';

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
  const prospector = useProspector(
    undefined, undefined, undefined, null, null, conversationId, projectId,
  );
  const { messages, saveStatus, reloadConversation } = prospector;

  const context: ChatContext = useMemo(
    () => ({ feature: 'project_chat', projectId }),
    [projectId],
  );

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
        />
      </div>
    </div>
  );
}

/** Placeholder for the canvas pane. */
function CanvasPane() {
  return (
    <aside
      aria-label="Canvas"
      style={{
        width: 320,
        flexShrink: 0,
        borderLeft: '1px solid var(--color-border-secondary)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        background: 'var(--color-bg-container)',
      }}
    >
      <Empty description="Charts and tables from this conversation will appear here" />
    </aside>
  );
}

export default function ConversationsPage() {
  const { projectId } = useProjectContext();
  const { conversationId } = useParams<{ conversationId: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [isCreating, setIsCreating] = useState(false);

  const { data, isLoading, isFetching, error, refetch } = useQuery({
    queryKey: listKey(projectId ?? ''),
    queryFn: () => listConversations(projectId!),
    enabled: !!projectId,
  });

  const conversations = useMemo(
    () => [...(data ?? [])].sort((a, b) => b.updatedAt - a.updatedAt),
    [data],
  );

  const handleNew = useCallback(async () => {
    if (!projectId || isCreating) {
      return;
    }
    setIsCreating(true);
    try {
      const created = await createConversation(projectId);
      queryClient.setQueryData<ConversationRecord[]>(listKey(projectId), (old) => [
        created,
        ...(old ?? []),
      ]);
      navigate(`/projects/${projectId}/chat/${created.id}`);
    } catch (err) {
      console.error('Creating conversation failed', err);
      message.error('Could not create a conversation. Try again.');
    } finally {
      setIsCreating(false);
    }
  }, [projectId, isCreating, queryClient, navigate]);

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
  if (conversationId) {
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
      <CanvasPane />
    </div>
  );
}
