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
import { Button, Typography } from 'antd';
import { RobotOutlined } from '@ant-design/icons';
import { useAskProspector } from '../../hooks/useAskProspector';
import type { ConversationSeed } from '../../types/ai';

interface AskProspectorButtonProps {
  /**
   * Talk in this project's conversations. Without it the button opens a conversation in the
   * user's personal Scratch project, which is where Prospector chats started outside a project live.
   */
  projectId?: string;
  /** Start a new conversation whose first turn is this prompt, with the sender's context. */
  seed?: ConversationSeed;
  label?: string;
}

/**
 * The one way into Prospector from pages that are not themselves the Conversations page. It
 * navigates to the chat surface rather than hosting a chat of its own.
 */
export default function AskProspectorButton({ projectId, seed, label = 'Ask Prospector' }: AskProspectorButtonProps) {
  const { ask, isLoading, isError } = useAskProspector(projectId);

  return (
    <>
      <Button
        icon={<RobotOutlined />}
        size="small"
        loading={isLoading}
        disabled={!ask}
        onClick={() => ask?.(seed)}
      >
        {label}
      </Button>
      {isError && (
        <Typography.Text type="secondary" style={{ fontSize: 12, marginLeft: 8 }}>
          Prospector is unavailable right now. Reload the page to try again.
        </Typography.Text>
      )}
    </>
  );
}
