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
import { useMemo, useRef } from 'react';
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
  Spin,
} from 'antd';
import {
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  FileTextOutlined,
  RobotOutlined,
} from '@ant-design/icons';
import Markdown from 'react-markdown';
import rehypeRaw from 'rehype-raw';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { deleteWikiPage, createWikiPage } from '../api/projects';
import { streamChat, getAiStatus } from '../api/ai';
import { getSavedQueries } from '../api/savedQueries';
import { getVisualizations } from '../api/visualizations';
import { getDashboards } from '../api/dashboards';
import { useProjectContext } from '../contexts/ProjectContext';
import { WikiEditor } from '../components/project';
import { useState } from 'react';
import type { ChatMessage, DeltaEvent } from '../types/ai';

const { Title, Text } = Typography;

export default function ProjectWikiPage() {
  const { pageId } = useParams<{ pageId: string }>();
  const navigate = useNavigate();
  const { project, projectId } = useProjectContext();
  const queryClient = useQueryClient();
  const [wikiEditorOpen, setWikiEditorOpen] = useState(false);
  const [editingPageId, setEditingPageId] = useState<string | null>(null);
  const [aiGenerating, setAiGenerating] = useState(false);
  const [aiContent, setAiContent] = useState('');
  const abortRef = useRef<AbortController | null>(null);

  // AI status check
  const { data: aiStatus } = useQuery({
    queryKey: ['ai-status'],
    queryFn: getAiStatus,
  });
  const aiEnabled = aiStatus?.enabled && aiStatus?.configured;

  // Fetch items for AI context
  const { data: allQueries } = useQuery({ queryKey: ['savedQueries'], queryFn: getSavedQueries });
  const { data: allVizs } = useQuery({ queryKey: ['visualizations'], queryFn: getVisualizations });
  const { data: allDashboards } = useQuery({ queryKey: ['dashboards'], queryFn: getDashboards });

  const handleAiGenerate = async () => {
    if (!project) {
      return;
    }

    setAiGenerating(true);
    setAiContent('');

    // Gather project data
    const queryIdSet = new Set(project.savedQueryIds || []);
    const vizIdSet = new Set(project.visualizationIds || []);
    const dashIdSet = new Set(project.dashboardIds || []);

    const projQueries = (allQueries || []).filter(q => queryIdSet.has(q.id));
    const projVizs = (allVizs || []).filter(v => vizIdSet.has(v.id));
    const projDashboards = (allDashboards || []).filter(d => dashIdSet.has(d.id));

    const datasetsDesc = (project.datasets || []).map(d => {
      if (d.type === 'plugin') {
        return `- **${d.label}** — Plugin: all schemas in \`${d.schema}\``;
      }
      if (d.type === 'schema') {
        return `- **${d.label}** — Schema: all tables in \`${d.schema}\``;
      }
      if (d.type === 'saved_query') {
        return `- **${d.label}** — Saved Query: \`${d.savedQueryId}\``;
      }
      return `- **${d.label}** — Table: \`${d.schema}.${d.table}\``;
    }).join('\n') || '- No datasets configured';

    const queriesDesc = projQueries.map(q =>
      `- **${q.name}**${q.description ? `: ${q.description}` : ''} — SQL: \`${q.sql.substring(0, 120)}${q.sql.length > 120 ? '...' : ''}\``
    ).join('\n') || '- No saved queries';

    const vizsDesc = projVizs.map(v =>
      `- **${v.name}** (${v.chartType} chart)${v.description ? `: ${v.description}` : ''} — [View](/sqllab/projects/${projectId}/visualizations)`
    ).join('\n') || '- No visualizations';

    const dashDesc = projDashboards.map(d =>
      `- **${d.name}**${d.description ? `: ${d.description}` : ''} — ${d.panels?.length || 0} panels — [View](/sqllab/dashboards/${d.id})`
    ).join('\n') || '- No dashboards';

    const prompt = `You are a technical writer for a data analytics project. Generate a comprehensive project wiki page in Markdown for the project described below. The page should include:

1. **Executive Summary** — A concise overview of what this project is about and its purpose, inferred from the datasets, queries, and visualizations.
2. **Project Goals** — Inferred objectives based on the types of data and analyses present.
3. **Data Sources** — A description of each dataset with its type and purpose.
4. **Saved Queries** — A summary of each query and what it analyzes.
5. **Visualizations** — A description of each visualization with its chart type and a link. Explain what insights each provides.
6. **Dashboards** — A description of each dashboard with panel count and a link. Explain how the dashboards organize the visualizations.
7. **Getting Started** — Brief instructions for new team members on how to explore this project.

Use proper Markdown formatting with headers, bold text, and bullet points. Include the links provided. Be specific and descriptive, not generic.

---

## Project: ${project.name}
${project.description ? `\nDescription: ${project.description}` : ''}
${project.tags?.length ? `Tags: ${project.tags.join(', ')}` : ''}
Owner: ${project.owner}

### Datasets
${datasetsDesc}

### Saved Queries
${queriesDesc}

### Visualizations
${vizsDesc}

### Dashboards
${dashDesc}`;

    let accumulated = '';
    const messages: ChatMessage[] = [{ role: 'user', content: prompt }];

    abortRef.current = streamChat(
      { messages, tools: [], context: {} },
      (event: DeltaEvent) => {
        if (event.type === 'content') {
          accumulated += event.content;
          setAiContent(accumulated);
        }
      },
      async () => {
        // Done streaming — save as a wiki page
        try {
          await createWikiPage(projectId!, {
            title: 'Project Overview',
            content: accumulated,
          });
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
          message.success('AI-generated wiki page created');
        } catch (err) {
          message.error(`Failed to save page: ${(err as Error).message}`);
        }
        setAiGenerating(false);
        setAiContent('');
      },
      () => {
        setAiGenerating(false);
        message.error('AI generation failed. Check that Prospector is configured.');
      },
    );
  };

  const handleCancelAi = () => {
    abortRef.current?.abort();
    setAiGenerating(false);
    setAiContent('');
  };

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
          <Space size={4}>
            {aiEnabled && (
              <Tooltip title="Generate wiki page with AI">
                <Button
                  size="small"
                  icon={<RobotOutlined />}
                  loading={aiGenerating}
                  onClick={handleAiGenerate}
                />
              </Tooltip>
            )}
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
          </Space>
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
        {aiGenerating ? (
          <Card>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
              <Space>
                <Spin size="small" />
                <Text strong>Generating Project Overview...</Text>
              </Space>
              <Button size="small" onClick={handleCancelAi}>Cancel</Button>
            </div>
            <div
              style={{
                padding: '16px 0',
                borderTop: '1px solid var(--color-border-secondary)',
              }}
            >
              <div className="wiki-markdown-content">
                <Markdown rehypePlugins={[rehypeRaw]}>
                  {aiContent || '*Waiting for AI response...*'}
                </Markdown>
              </div>
            </div>
          </Card>
        ) : selectedPage ? (
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
              <div className="wiki-markdown-content">
                <Markdown rehypePlugins={[rehypeRaw]}>
                  {selectedPage.content}
                </Markdown>
              </div>
            </div>
          </Card>
        ) : (
          <Empty description="No wiki pages yet. Create one to get started!">
            <Space>
              {aiEnabled && (
                <Button
                  icon={<RobotOutlined />}
                  onClick={handleAiGenerate}
                  loading={aiGenerating}
                >
                  Generate with AI
                </Button>
              )}
              <Button
                type="primary"
                icon={<PlusOutlined />}
                onClick={() => {
                  setEditingPageId(null);
                  setWikiEditorOpen(true);
                }}
              >
                Create Page
              </Button>
            </Space>
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
