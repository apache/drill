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
import { Link } from 'react-router-dom';
import { Button, Spin } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import type { ConversationRecord } from '../../types/ai';

interface ConversationListProps {
  projectId: string;
  conversations: ConversationRecord[];
  activeId: string | undefined;
  isLoading: boolean;
  isCreating: boolean;
  onNew: () => void;
}

/** Left pane of the Conversations page: the project's conversations, most recent first. */
export default function ConversationList({
  projectId,
  conversations,
  activeId,
  isLoading,
  isCreating,
  onNew,
}: ConversationListProps) {
  return (
    <nav
      aria-label="Conversations"
      style={{
        width: 260,
        flexShrink: 0,
        display: 'flex',
        flexDirection: 'column',
        borderRight: '1px solid var(--color-border-secondary)',
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
            <Link
              key={c.id}
              to={`/projects/${projectId}/chat/${c.id}`}
              aria-current={active ? 'page' : undefined}
              style={{
                display: 'block',
                padding: '8px 10px',
                borderRadius: 6,
                marginBottom: 2,
                color: 'var(--color-text)',
                background: active ? 'var(--color-bg-selected)' : 'transparent',
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
          );
        })}
      </div>
    </nav>
  );
}
