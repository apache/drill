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
import { useCallback } from 'react';
import { Button, Tooltip, Tag } from 'antd';
import { DeleteOutlined, RobotOutlined, ExperimentOutlined } from '@ant-design/icons';
import ChatMessageList from './ChatMessageList';
import ChatInput from './ChatInput';
import QuickActionBar from './QuickActionBar';
import type { UseProspectorReturn } from '../../hooks/useProspector';
import type { ChatContext } from '../../types/ai';

interface ProspectorPanelProps {
  prospector: UseProspectorReturn;
  context: ChatContext;
  /** Callback to insert Python code as a new notebook cell */
  onInsertCell?: (code: string) => void;
}

export default function ProspectorPanel({
  prospector,
  context,
  onInsertCell,
}: ProspectorPanelProps) {
  const { messages, isStreaming, streamingContent, sendMessage, stopStreaming, clearChat } = prospector;

  const handleSend = useCallback(
    (text: string) => {
      sendMessage(text, context);
    },
    [sendMessage, context],
  );

  const handleQuickAction = useCallback(
    (prompt: string) => {
      sendMessage(prompt, context);
    },
    [sendMessage, context],
  );

  const isNotebook = !!context.notebookMode;

  return (
    <div className="prospector-panel">
      <div className="prospector-panel-header">
        <span className="prospector-panel-title">
          <RobotOutlined style={{ marginRight: 8 }} />
          Prospector
          {isNotebook && (
            <Tag
              color="purple"
              icon={<ExperimentOutlined />}
              style={{ marginLeft: 8, fontSize: 10 }}
            >
              Notebook
            </Tag>
          )}
        </span>
        <Tooltip title="Clear chat">
          <Button
            size="small"
            icon={<DeleteOutlined />}
            onClick={clearChat}
            disabled={messages.length === 0 && !isStreaming}
          />
        </Tooltip>
      </div>
      <ChatMessageList
        messages={messages}
        streamingContent={streamingContent}
        isStreaming={isStreaming}
        onInsertCell={isNotebook ? onInsertCell : undefined}
      />
      <div className="prospector-panel-footer">
        <QuickActionBar
          onAction={handleQuickAction}
          hasError={!!context.error}
          hasResults={!!context.resultSummary && context.resultSummary.rowCount > 0}
          hasSql={!!context.currentSql && context.currentSql.trim().length > 0}
          disabled={isStreaming}
          notebookMode={isNotebook}
          notebookCellError={!!context.notebookCellError}
          notebookDfName={context.notebookDfName}
        />
        <ChatInput
          onSend={handleSend}
          onStop={stopStreaming}
          isStreaming={isStreaming}
          placeholder={isNotebook
            ? 'Ask about your data, request analysis code, or get help with Python...'
            : undefined
          }
        />
      </div>
    </div>
  );
}
