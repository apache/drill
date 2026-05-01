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
import { Input } from 'antd';
import Markdown from 'react-markdown';

const { TextArea } = Input;

interface MarkdownPanelProps {
  content: string;
  editMode: boolean;
  onContentChange: (content: string) => void;
}

export default function MarkdownPanel({ content, editMode, onContentChange }: MarkdownPanelProps) {
  if (editMode) {
    return (
      <div className="markdown-panel">
        <TextArea
          value={content}
          onChange={(e) => onContentChange(e.target.value)}
          placeholder="Enter markdown content..."
          style={{ height: '100%', resize: 'none', fontFamily: 'monospace' }}
        />
      </div>
    );
  }

  return (
    <div className="markdown-panel">
      <Markdown>{content || '*No content*'}</Markdown>
    </div>
  );
}
