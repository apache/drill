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
import { useState, useEffect } from 'react';
import { Modal, Input, Tabs, message, Typography } from 'antd';
import { createWikiPage, updateWikiPage } from '../../api/projects';
import type { WikiPage } from '../../types';

const { TextArea } = Input;
const { Text } = Typography;

interface WikiEditorProps {
  open: boolean;
  projectId: string;
  page: WikiPage | null;
  onClose: () => void;
  onSuccess: () => void;
}

export default function WikiEditor({ open, projectId, page, onClose, onSuccess }: WikiEditorProps) {
  const [title, setTitle] = useState('');
  const [content, setContent] = useState('');
  const [saving, setSaving] = useState(false);
  const [previewTab, setPreviewTab] = useState('edit');

  useEffect(() => {
    if (page) {
      setTitle(page.title);
      setContent(page.content);
    } else {
      setTitle('');
      setContent('');
    }
    setPreviewTab('edit');
  }, [page, open]);

  const handleSave = async () => {
    if (!title.trim()) {
      message.error('Page title is required');
      return;
    }

    setSaving(true);
    try {
      if (page) {
        await updateWikiPage(projectId, page.id, { title: title.trim(), content });
        message.success('Wiki page updated');
      } else {
        await createWikiPage(projectId, { title: title.trim(), content });
        message.success('Wiki page created');
      }
      onSuccess();
    } catch (err) {
      message.error(`Failed to save wiki page: ${(err as Error).message}`);
    } finally {
      setSaving(false);
    }
  };

  // Simple markdown-to-HTML renderer for preview
  const renderMarkdown = (md: string) => {
    let html = md
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');

    // Headers
    html = html.replace(/^### (.+)$/gm, '<h3>$1</h3>');
    html = html.replace(/^## (.+)$/gm, '<h2>$1</h2>');
    html = html.replace(/^# (.+)$/gm, '<h1>$1</h1>');

    // Bold and italic
    html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');

    // Code blocks
    html = html.replace(/```([\s\S]*?)```/g, '<pre><code>$1</code></pre>');
    html = html.replace(/`(.+?)`/g, '<code>$1</code>');

    // Links
    html = html.replace(/\[(.+?)\]\((.+?)\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');

    // Unordered lists
    html = html.replace(/^- (.+)$/gm, '<li>$1</li>');
    html = html.replace(/(<li>.*<\/li>\n?)+/g, '<ul>$&</ul>');

    // Paragraphs (double newline)
    html = html.replace(/\n\n/g, '</p><p>');
    html = '<p>' + html + '</p>';

    // Single newlines to <br>
    html = html.replace(/\n/g, '<br/>');

    return html;
  };

  return (
    <Modal
      title={page ? 'Edit Wiki Page' : 'New Wiki Page'}
      open={open}
      onOk={handleSave}
      onCancel={onClose}
      confirmLoading={saving}
      okText={page ? 'Update' : 'Create'}
      width={800}
    >
      <Input
        placeholder="Page title"
        value={title}
        onChange={(e) => setTitle(e.target.value)}
        style={{ marginBottom: 16 }}
      />
      <Tabs
        activeKey={previewTab}
        onChange={setPreviewTab}
        items={[
          {
            key: 'edit',
            label: 'Edit',
            children: (
              <TextArea
                placeholder="Write your content in Markdown..."
                value={content}
                onChange={(e) => setContent(e.target.value)}
                rows={16}
                style={{ fontFamily: 'monospace' }}
              />
            ),
          },
          {
            key: 'preview',
            label: 'Preview',
            children: content ? (
              <div
                style={{
                  minHeight: 300,
                  padding: 16,
                  border: '1px solid #d9d9d9',
                  borderRadius: 6,
                  overflow: 'auto',
                }}
                dangerouslySetInnerHTML={{ __html: renderMarkdown(content) }}
              />
            ) : (
              <div style={{ minHeight: 300, padding: 16, textAlign: 'center' }}>
                <Text type="secondary">Nothing to preview</Text>
              </div>
            ),
          },
        ]}
      />
    </Modal>
  );
}
