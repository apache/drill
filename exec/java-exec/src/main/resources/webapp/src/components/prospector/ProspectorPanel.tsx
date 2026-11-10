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
import { useCallback, useState, type ReactNode } from 'react';
import { Button, Tooltip } from 'antd';
import { DeleteOutlined, RobotOutlined } from '@ant-design/icons';
import ChatMessageList from './ChatMessageList';
import ChatInput from './ChatInput';
import QuickActionBar from './QuickActionBar';
import SaveReportModal from './SaveReportModal';
import type { UseProspectorReturn } from '../../hooks/useProspector';
import type { ChatContext } from '../../types/ai';
import type { AiFeature } from '../../constants/aiFeatures';
import type { ArtifactKind } from '../conversation/ArtifactCanvas';

interface ProspectorPanelProps {
  prospector: UseProspectorReturn;
  context: ChatContext;
  /** Tooltip and accessible name of the header's clear button. Defaults to "Clear chat". */
  clearLabel?: string;
  /** Icon of the header's clear button. Defaults to a trash can. */
  clearIcon?: ReactNode;
  /** Show a card for each artifact Prospector creates, opened through this. Omitted: no cards. */
  onOpenArtifact?: (kind: ArtifactKind, id: string) => void;
  /** The artifact on the canvas now, so its card can show as live. */
  liveArtifact?: { kind: ArtifactKind; id: string } | null;
  /** Make the composer take dropped files, handing each to this. Omitted: it takes none. */
  onFileDropped?: (file: File) => void;
}

export default function ProspectorPanel({
  prospector,
  context,
  clearLabel = 'Clear chat',
  clearIcon = <DeleteOutlined />,
  onOpenArtifact,
  liveArtifact,
  onFileDropped,
}: ProspectorPanelProps) {
  const {
    messages,
    isStreaming,
    streamingContent,
    usage,
    sendMessage,
    stopStreaming,
    clearChat,
    conversationId,
  } = prospector;

  const [reportContent, setReportContent] = useState<string | null>(null);
  // Index (within the full messages array) of the message the report suggestion was
  // opened from, so the modal's query appendix does not pick up SQL run afterward.
  const [reportMessageIndex, setReportMessageIndex] = useState<number>(-1);

  const handleSaveReport = useCallback((content: string, messageIndex: number) => {
    setReportContent(content);
    setReportMessageIndex(messageIndex);
  }, []);

  const closeReportModal = useCallback(() => {
    setReportContent(null);
    setReportMessageIndex(-1);
  }, []);

  const handleSend = useCallback(
    (text: string) => {
      sendMessage(text, context);
    },
    [sendMessage, context],
  );

  const handleQuickAction = useCallback(
    (prompt: string, feature?: AiFeature) => {
      sendMessage(prompt, feature ? { ...context, feature } : context);
    },
    [sendMessage, context],
  );

  return (
    <div className="prospector-panel">
      <div className="prospector-panel-header">
        <span className="prospector-panel-title">
          <RobotOutlined style={{ marginRight: 8 }} />
          Prospector
        </span>
        <Tooltip title={clearLabel}>
          <Button
            size="small"
            aria-label={clearLabel}
            icon={clearIcon}
            onClick={clearChat}
            disabled={messages.length === 0 && !isStreaming}
          />
        </Tooltip>
      </div>
      <ChatMessageList
        messages={messages}
        streamingContent={streamingContent}
        isStreaming={isStreaming}
        onSaveReport={handleSaveReport}
        onOpenArtifact={onOpenArtifact}
        liveArtifact={liveArtifact}
      />
      <div className="prospector-panel-footer">
        {usage && (usage.totalTokens || usage.costUsd !== undefined) && (
          <div className="prospector-usage-pill" aria-live="polite">
            {usage.totalTokens !== undefined && (
              <span className="prospector-usage-tokens">
                {usage.totalTokens.toLocaleString()} tok
              </span>
            )}
            {usage.promptTokens !== undefined && usage.responseTokens !== undefined && (
              <span className="prospector-usage-breakdown">
                {usage.promptTokens.toLocaleString()} in · {usage.responseTokens.toLocaleString()} out
              </span>
            )}
            {usage.costUsd !== undefined && (
              <span className="prospector-usage-cost">
                {(usage.currency === 'USD' || !usage.currency) ? '$' : ''}
                {usage.costUsd.toFixed(usage.costUsd < 0.01 ? 4 : 3)}
                {usage.currency && usage.currency !== 'USD' ? ` ${usage.currency}` : ''}
              </span>
            )}
          </div>
        )}
        <QuickActionBar
          onAction={handleQuickAction}
          hasError={!!context.error}
          hasResults={!!context.resultSummary && context.resultSummary.rowCount > 0}
          hasSql={!!context.currentSql && context.currentSql.trim().length > 0}
          disabled={isStreaming}
        />
        <ChatInput
          onSend={handleSend}
          onStop={stopStreaming}
          isStreaming={isStreaming}
          onFileDropped={onFileDropped}
        />
      </div>
      <SaveReportModal
        open={reportContent !== null}
        content={reportContent ?? ''}
        messages={reportMessageIndex >= 0 ? messages.slice(0, reportMessageIndex + 1) : messages}
        defaultProjectId={context.projectId}
        conversationId={conversationId ?? undefined}
        onClose={closeReportModal}
      />
    </div>
  );
}
