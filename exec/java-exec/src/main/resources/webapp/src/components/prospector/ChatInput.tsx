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
import { useState, useCallback } from 'react';
import type { DragEvent } from 'react';
import { Input, Button, Space } from 'antd';
import { SendOutlined, StopOutlined } from '@ant-design/icons';

const { TextArea } = Input;

/** True only for drags that carry files from outside the browser, not in-app drags. */
function carriesFiles(e: DragEvent): boolean {
  return Array.from(e.dataTransfer?.types ?? []).includes('Files');
}

interface ChatInputProps {
  onSend: (message: string) => void;
  onStop: () => void;
  isStreaming: boolean;
  disabled?: boolean;
  placeholder?: string;
  /** Called with each file dropped on the composer. Omitted: the composer takes no drops. */
  onFileDropped?: (file: File) => void;
}

export default function ChatInput({ onSend, onStop, isStreaming, disabled, placeholder, onFileDropped }: ChatInputProps) {
  const [value, setValue] = useState('');

  const handleSend = useCallback(() => {
    const trimmed = value.trim();
    if (trimmed && !isStreaming && !disabled) {
      onSend(trimmed);
      setValue('');
    }
  }, [value, isStreaming, disabled, onSend]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSend();
      }
    },
    [handleSend],
  );

  const handleDragOver = (e: DragEvent) => {
    if (onFileDropped && carriesFiles(e)) {
      e.preventDefault(); // required for the browser to allow a drop
    }
  };
  const handleDrop = (e: DragEvent) => {
    if (!onFileDropped || !carriesFiles(e)) {
      return;
    }
    e.preventDefault();
    // Handled here; stop the project-wide drop zone from uploading the same file again.
    e.stopPropagation();
    Array.from(e.dataTransfer.files).forEach((f) => onFileDropped(f));
  };

  return (
    <div className="prospector-chat-input" onDragOver={handleDragOver} onDrop={handleDrop}>
      <Space.Compact style={{ width: '100%' }}>
        <TextArea
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder || "Ask about your data..."}
          autoSize={{ minRows: 1, maxRows: 4 }}
          disabled={disabled || isStreaming}
          style={{ resize: 'none' }}
        />
        {isStreaming ? (
          <Button
            icon={<StopOutlined />}
            onClick={onStop}
            danger
            type="primary"
          />
        ) : (
          <Button
            icon={<SendOutlined />}
            onClick={handleSend}
            type="primary"
            disabled={!value.trim() || disabled}
          />
        )}
      </Space.Compact>
    </div>
  );
}
