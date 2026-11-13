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
import { useState } from 'react';
import { Link } from 'react-router-dom';
import { Button, Input, Modal, Popconfirm, Spin } from 'antd';
import { DeleteOutlined, EditOutlined, PlusOutlined } from '@ant-design/icons';
import type { ConversationRecord } from '../../types/ai';

interface ConversationListProps {
  projectId: string;
  conversations: ConversationRecord[];
  activeId: string | undefined;
  isLoading: boolean;
  isCreating: boolean;
  onNew: () => void;
  /** Resolves when saved; rejects (after telling the user why) so the dialog stays open. */
  onRename: (id: string, name: string) => Promise<void>;
  onDelete: (id: string) => Promise<void>;
}

/** Left pane of the Conversations page: the project's conversations, most recent first. */
export default function ConversationList({
  projectId,
  conversations,
  activeId,
  isLoading,
  isCreating,
  onNew,
  onRename,
  onDelete,
}: ConversationListProps) {
  const [renaming, setRenaming] = useState<ConversationRecord | null>(null);
  const [draft, setDraft] = useState('');
  const [saving, setSaving] = useState(false);

  const openRename = (c: ConversationRecord) => {
    setRenaming(c);
    setDraft(c.name);
  };

  const submitRename = async () => {
    const name = draft.trim();
    if (!renaming || !name) {
      return;
    }
    setSaving(true);
    try {
      await onRename(renaming.id, name);
      setRenaming(null);
    } catch {
      // The caller has already told the user; keep the dialog so the edit is not lost.
    } finally {
      setSaving(false);
    }
  };

  return (
    <nav
      aria-label="Conversations"
      className="conversations-list"
      style={{
        display: 'flex',
        flexDirection: 'column',
        background: 'var(--color-bg-elevated)',
        minHeight: 0,
      }}
    >
      <div style={{ padding: 12, borderBottom: '1px solid var(--color-border-secondary)' }}>
        <Button block icon={<PlusOutlined />} loading={isCreating} onClick={onNew}>
          New conversation
        </Button>
      </div>
      <div style={{ flex: 1, overflow: 'auto', padding: 8 }}>
        {isLoading && (
          <div style={{ textAlign: 'center', padding: 16 }}>
            <Spin size="small" />
          </div>
        )}
        {conversations.map((c) => {
          const active = c.id === activeId;
          return (
            <div
              key={c.id}
              style={{
                display: 'flex',
                alignItems: 'center',
                borderRadius: 6,
                marginBottom: 2,
                background: active ? 'var(--color-bg-selected)' : 'transparent',
              }}
            >
              <Link
                to={`/projects/${projectId}/chat/${c.id}`}
                aria-current={active ? 'page' : undefined}
                style={{
                  flex: 1,
                  minWidth: 0,
                  padding: '8px 10px',
                  color: 'var(--color-text)',
                  fontWeight: active ? 600 : 400,
                }}
              >
                <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {c.name}
                </div>
                <div style={{ fontSize: 11, color: 'var(--color-text-secondary)' }}>
                  {new Date(c.updatedAt).toLocaleString()}
                </div>
              </Link>
              <Button
                type="text"
                size="small"
                aria-label={`Rename ${c.name}`}
                icon={<EditOutlined />}
                onClick={() => openRename(c)}
              />
              <Popconfirm
                title={`Delete "${c.name}"?`}
                description="This permanently removes the conversation and its messages."
                okText="Delete"
                okButtonProps={{ danger: true }}
                onConfirm={() => onDelete(c.id)}
              >
                <Button
                  type="text"
                  size="small"
                  aria-label={`Delete ${c.name}`}
                  icon={<DeleteOutlined />}
                />
              </Popconfirm>
            </div>
          );
        })}
      </div>
      <Modal
        title="Rename conversation"
        open={renaming !== null}
        okText="Rename"
        okButtonProps={{ disabled: draft.trim() === '' }}
        confirmLoading={saving}
        onOk={submitRename}
        onCancel={() => setRenaming(null)}
      >
        <Input
          value={draft}
          maxLength={200}
          onChange={(e) => setDraft(e.target.value)}
          onPressEnter={submitRename}
          aria-label="Conversation name"
        />
      </Modal>
    </nav>
  );
}
