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
import { useMemo } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Card,
  Button,
  List,
  Empty,
  Typography,
  Space,
  Popconfirm,
  Tooltip,
  message,
} from 'antd';
import {
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  FileTextOutlined,
} from '@ant-design/icons';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { deleteWikiPage } from '../api/projects';
import { useProjectContext } from '../contexts/ProjectContext';
import { WikiEditor } from '../components/project';
import { useState } from 'react';

const { Title, Text, Paragraph } = Typography;

export default function ProjectWikiPage() {
  const { pageId } = useParams<{ pageId: string }>();
  const navigate = useNavigate();
  const { project, projectId } = useProjectContext();
  const queryClient = useQueryClient();
  const [wikiEditorOpen, setWikiEditorOpen] = useState(false);
  const [editingPageId, setEditingPageId] = useState<string | null>(null);

  const sortedPages = useMemo(
    () => [...(project?.wikiPages || [])].sort((a, b) => a.order - b.order),
    [project?.wikiPages]
  );

  const selectedPage = useMemo(() => {
    if (pageId) {
      return sortedPages.find((p) => p.id === pageId) || null;
    }
    return sortedPages.length > 0 ? sortedPages[0] : null;
  }, [pageId, sortedPages]);

  const deleteWikiMutation = useMutation({
    mutationFn: (pid: string) => deleteWikiPage(projectId!, pid),
    onSuccess: () => {
      message.success('Wiki page deleted');
      queryClient.invalidateQueries({ queryKey: ['project', projectId] });
      // If we deleted the selected page, navigate to wiki root
      if (pageId) {
        navigate(`/projects/${projectId}/wiki`);
      }
    },
  });

  const formatDate = (timestamp: number) => {
    return new Date(timestamp).toLocaleString();
  };

  return (
    <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
      {/* Left sidebar: page list */}
      <div
        style={{
          width: 260,
          borderRight: '1px solid var(--color-border-secondary)',
          display: 'flex',
          flexDirection: 'column',
          background: 'var(--color-bg-elevated)',
          flexShrink: 0,
        }}
      >
        <div
          style={{
            padding: '12px 16px',
            borderBottom: '1px solid var(--color-border-secondary)',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}
        >
          <Title level={5} style={{ margin: 0 }}>Wiki Pages</Title>
          <Tooltip title="New page">
            <Button
              type="primary"
              size="small"
              icon={<PlusOutlined />}
              onClick={() => {
                setEditingPageId(null);
                setWikiEditorOpen(true);
              }}
            />
          </Tooltip>
        </div>
        <div style={{ flex: 1, overflow: 'auto' }}>
          {sortedPages.length === 0 ? (
            <div style={{ padding: 16, textAlign: 'center' }}>
              <Text type="secondary">No wiki pages yet</Text>
            </div>
          ) : (
            <List
              size="small"
              dataSource={sortedPages}
              renderItem={(page) => (
                <List.Item
                  style={{
                    cursor: 'pointer',
                    padding: '8px 16px',
                    background: selectedPage?.id === page.id
                      ? 'var(--color-bg-container)'
                      : 'transparent',
                    borderLeft: selectedPage?.id === page.id
                      ? '3px solid var(--color-primary)'
                      : '3px solid transparent',
                  }}
                  onClick={() => navigate(`/projects/${projectId}/wiki/${page.id}`)}
                >
                  <Space>
                    <FileTextOutlined />
                    <Text
                      strong={selectedPage?.id === page.id}
                      ellipsis
                      style={{ maxWidth: 180 }}
                    >
                      {page.title}
                    </Text>
                  </Space>
                </List.Item>
              )}
            />
          )}
        </div>
      </div>

      {/* Main area: selected page content */}
      <div style={{ flex: 1, overflow: 'auto', padding: 24 }}>
        {selectedPage ? (
          <Card>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 16 }}>
              <div>
                <Title level={3} style={{ margin: 0 }}>{selectedPage.title}</Title>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  Last updated: {formatDate(selectedPage.updatedAt)}
                </Text>
              </div>
              <Space>
                <Tooltip title="Edit">
                  <Button
                    icon={<EditOutlined />}
                    onClick={() => {
                      setEditingPageId(selectedPage.id);
                      setWikiEditorOpen(true);
                    }}
                  >
                    Edit
                  </Button>
                </Tooltip>
                <Popconfirm
                  title="Delete this wiki page?"
                  onConfirm={() => deleteWikiMutation.mutate(selectedPage.id)}
                  okText="Delete"
                  cancelText="Cancel"
                  okButtonProps={{ danger: true }}
                >
                  <Button danger icon={<DeleteOutlined />}>Delete</Button>
                </Popconfirm>
              </Space>
            </div>
            <div
              style={{
                padding: '16px 0',
                borderTop: '1px solid var(--color-border-secondary)',
              }}
            >
              <Paragraph style={{ whiteSpace: 'pre-wrap' }}>
                {selectedPage.content}
              </Paragraph>
            </div>
          </Card>
        ) : (
          <Empty description="No wiki pages yet. Create one to get started!">
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => {
                setEditingPageId(null);
                setWikiEditorOpen(true);
              }}
            >
              Create First Page
            </Button>
          </Empty>
        )}
      </div>

      <WikiEditor
        open={wikiEditorOpen}
        projectId={projectId!}
        page={editingPageId
          ? project?.wikiPages?.find((p) => p.id === editingPageId) || null
          : null}
        onClose={() => {
          setWikiEditorOpen(false);
          setEditingPageId(null);
        }}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
          setWikiEditorOpen(false);
          setEditingPageId(null);
        }}
      />
    </div>
  );
}
