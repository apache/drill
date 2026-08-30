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
import { Modal, Typography } from 'antd';
import { BarChartOutlined, ApiOutlined, MessageOutlined } from '@ant-design/icons';

const { Text } = Typography;

export interface DeleteTabTarget {
  id: string;
  name: string;
  vizIds?: string[];
  /** Prospector messages belonging to this tab. */
  conversationLength?: number;
}

export interface PublishedApiRef {
  id: string;
  name: string;
}

interface DeleteTabModalProps {
  open: boolean;
  tab: DeleteTabTarget;
  /** Visualization id to display name, for whatever has loaded. */
  vizNames: Record<string, string>;
  publishedApis: PublishedApiRef[];
  onConfirm: () => void;
  onCancel: () => void;
}

/**
 * Confirms permanent deletion of a tab, listing what depends on it.
 *
 * Warns and proceeds rather than blocking: refusing to delete a tab with dependants
 * would leave tabs that can never be removed. Locked tabs are the one hard stop, and
 * that is enforced server-side with a 409 rather than here.
 *
 * The three dependants say deliberately different things. Visualizations lose their
 * source and the conversation is destroyed, but a published API is **not** broken by
 * this — it holds its own copy of the SQL and keeps serving. That line exists because
 * a user can be wrong in either direction: believing they have broken production, or
 * believing they have revoked an endpoint they meant to take down.
 *
 * See docs/dev/TabPersistence.md.
 */
export default function DeleteTabModal({
  open,
  tab,
  vizNames,
  publishedApis,
  onConfirm,
  onCancel,
}: DeleteTabModalProps) {
  const vizIds = tab.vizIds ?? [];
  const hasConversation = (tab.conversationLength ?? 0) > 0;
  const hasDependants = vizIds.length > 0 || hasConversation || publishedApis.length > 0;

  return (
    <Modal
      open={open}
      title={`Delete "${tab.name}"?`}
      okText="Delete"
      okType="danger"
      cancelText="Cancel"
      onOk={onConfirm}
      onCancel={onCancel}
    >
      <p>This permanently removes the tab and its SQL. It cannot be undone.</p>

      {hasDependants && (
        <ul className="delete-tab-dependants">
          {vizIds.length > 0 && (
            <li>
              <BarChartOutlined />{' '}
              {vizIds.length === 1 ? 'This visualization' : 'These visualizations'} will
              break:{' '}
              <Text strong>
                {vizIds.map((id) => vizNames[id] ?? id).join(', ')}
              </Text>
            </li>
          )}

          {hasConversation && (
            <li>
              <MessageOutlined />{' '}
              The Prospector conversation for this tab ({tab.conversationLength} messages)
              is deleted with it.
            </li>
          )}

          {publishedApis.length > 0 && (
            <li>
              <ApiOutlined />{' '}
              Published as{' '}
              <Text strong>{publishedApis.map((api) => api.name).join(', ')}</Text>.
              That endpoint keeps serving after this tab is gone — delete it separately
              if you meant to take it down.
            </li>
          )}
        </ul>
      )}
    </Modal>
  );
}
