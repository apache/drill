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
import MarkdownView from '../MarkdownView';
import { Button } from 'antd';
import { UserOutlined, RobotOutlined } from '@ant-design/icons';
import type { ChatMessage } from '../../types/ai';
import ToolCallDisplay from './ToolCallDisplay';
import { looksLikeReport } from '../../utils/report';
import ArtifactCard from '../conversation/ArtifactCard';
import WorkflowActivationCard from '../conversation/WorkflowActivationCard';
import { createdArtifact } from '../conversation/autoFollow';
import type { ArtifactKind } from '../conversation/ArtifactCanvas';

interface ChatMessageBubbleProps {
  message: ChatMessage;
  toolResults: ChatMessage[];
  isStreaming?: boolean;
  /**
   * Offer to save this message as a report. Called with the message's content and its
   * index within the full (unfiltered) messages array, so the caller can slice the
   * query appendix at this point rather than including later, unrelated queries.
   * Omitted when saving is unavailable.
   */
  onSaveReport?: (content: string, messageIndex: number) => void;
  /** This message's index within the full (unfiltered) messages array. */
  messageIndex?: number;
  /** Whether the report-save suggestion for this message was already dismissed. */
  dismissed?: boolean;
  /** Called when the user dismisses the report-save suggestion for this message. */
  onDismissReport?: () => void;
  /**
   * Drop a card under the message for each durable artifact its tool calls created, and open
   * one on click. Omitted where there is no canvas to open it on, which shows no cards.
   */
  onOpenArtifact?: (kind: ArtifactKind, id: string) => void;
  /** The artifact on the canvas now; its card renders as live. */
  liveArtifact?: { kind: ArtifactKind; id: string } | null;
}

export default function ChatMessageBubble({
  message,
  toolResults,
  isStreaming,
  onSaveReport,
  messageIndex,
  dismissed,
  onDismissReport,
  onOpenArtifact,
  liveArtifact,
}: ChatMessageBubbleProps) {
  const isUser = message.role === 'user';
  const isError = message.content?.startsWith('Error:');

  return (
    <div className={`prospector-message ${isUser ? 'prospector-message-user' : 'prospector-message-assistant'}`}>
      <div className="prospector-message-avatar">
        {isUser ? <UserOutlined /> : <RobotOutlined />}
      </div>
      <div
        className={`prospector-message-bubble ${isUser ? 'prospector-bubble-user' : 'prospector-bubble-assistant'} ${isError ? 'prospector-bubble-error' : ''}`}
      >
        {message.content && (
          <div className="prospector-message-content">
            {isUser ? (
              <span>{message.content}</span>
            ) : (
              <MarkdownView>
                {message.content}
              </MarkdownView>
            )}
          </div>
        )}
        {isStreaming && <span className="prospector-cursor" />}
        {!isUser && !isStreaming && !isError && onSaveReport && message.content
          && looksLikeReport(message.content) && !dismissed && (
          <div className="prospector-report-suggestion">
            <span>This looks like a report.</span>
            <Button
              type="link"
              size="small"
              onClick={() => onSaveReport(message.content as string, messageIndex ?? -1)}
            >
              Save to project
            </Button>
            <Button type="text" size="small" onClick={onDismissReport}>
              Dismiss
            </Button>
          </div>
        )}
        {onOpenArtifact && message.toolCalls?.map((tc) => {
          const artifact = createdArtifact(tc, toolResults.find((r) => r.toolCallId === tc.id));
          return artifact && (
            <div key={tc.id}>
              <ArtifactCard
                kind={artifact.kind}
                id={artifact.id}
                name={artifact.name}
                live={liveArtifact?.kind === artifact.kind && liveArtifact.id === artifact.id}
                onOpen={() => onOpenArtifact(artifact.kind, artifact.id)}
              />
              {/* A workflow is created paused. This is where the user turns it on, so the
                  card and its Activate control read as one thing rather than two. */}
              {artifact.kind === 'workflow' && (
                <WorkflowActivationCard workflowId={artifact.id} name={artifact.name} />
              )}
            </div>
          );
        })}
        {message.toolCalls && message.toolCalls.length > 0 && (
          <ToolCallDisplay
            toolCalls={message.toolCalls}
            toolResults={toolResults}
          />
        )}
      </div>
    </div>
  );
}
