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
import { useMemo, useRef, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Button, Tooltip, message, Spin, Modal, Space } from 'antd';
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
import { usePageChrome } from '../contexts/AppChromeContext';
import type { ChatMessage, DeltaEvent } from '../types/ai';
import type { WikiPage } from '../types';

function formatRelative(ts: number): string {
  const date = new Date(ts);
  const diff = Date.now() - date.getTime();
  const min = Math.floor(diff / 60000);
  if (min < 1) {
    return 'just now';
  }
  if (min < 60) {
    return `${min}m ago`;
  }
  const hr = Math.floor(min / 60);
  if (hr < 24) {
    return `${hr}h ago`;
  }
  const days = Math.floor(hr / 24);
  if (days < 7) {
    return `${days}d ago`;
  }
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

/** Strip basic markdown for the page-list preview line. */
function previewFromMarkdown(md: string, max = 100): string {
  const stripped = md
    .replace(/^---[\s\S]*?---\n/, '')           // frontmatter
    .replace(/```[\s\S]*?```/g, '')             // code blocks
    .replace(/!\[[^\]]*\]\([^)]+\)/g, '')       // images
    .replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')    // links → text
    .replace(/[#>*_`~-]/g, '')                  // markdown markers
    .replace(/\s+/g, ' ')
    .trim();
  return stripped.length > max ? stripped.slice(0, max) + '…' : stripped;
}

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

  const { data: aiStatus } = useQuery({
    queryKey: ['ai-status'],
    queryFn: getAiStatus,
  });
  const aiEnabled = aiStatus?.enabled && aiStatus?.configured;

  const { data: allQueries } = useQuery({ queryKey: ['savedQueries'], queryFn: getSavedQueries });
  const { data: allVizs } = useQuery({ queryKey: ['visualizations'], queryFn: getVisualizations });
  const { data: allDashboards } = useQuery({ queryKey: ['dashboards'], queryFn: getDashboards });

  const sortedPages = useMemo<WikiPage[]>(
    () => [...(project?.wikiPages || [])].sort((a, b) => a.order - b.order),
    [project?.wikiPages],
  );

  const selectedPage = useMemo<WikiPage | null>(() => {
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
      if (pageId) {
        navigate(`/projects/${projectId}/wiki`);
      }
    },
  });

  const handleAiGenerate = async () => {
    if (!project) {
      return;
    }

    setAiGenerating(true);
    setAiContent('');

    const queryIdSet = new Set(project.savedQueryIds || []);
    const vizIdSet = new Set(project.visualizationIds || []);
    const dashIdSet = new Set(project.dashboardIds || []);

    const projQueries = (allQueries || []).filter((q) => queryIdSet.has(q.id));
    const projVizs = (allVizs || []).filter((v) => vizIdSet.has(v.id));
    const projDashboards = (allDashboards || []).filter((d) => dashIdSet.has(d.id));

    const datasetsDesc =
      (project.datasets || [])
        .map((d) => {
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
        })
        .join('\n') || '- No datasets configured';

    const queriesDesc =
      projQueries
        .map((q) =>
          `- **${q.name}**${q.description ? `: ${q.description}` : ''} — SQL: \`${q.sql.substring(0, 120)}${q.sql.length > 120 ? '...' : ''}\``,
        )
        .join('\n') || '- No saved queries';

    const vizsDesc =
      projVizs
        .map((v) =>
          `- **${v.name}** (${v.chartType} chart)${v.description ? `: ${v.description}` : ''} — [View](/sqllab/projects/${projectId}/visualizations)`,
        )
        .join('\n') || '- No visualizations';

    const dashDesc =
      projDashboards
        .map((d) =>
          `- **${d.name}**${d.description ? `: ${d.description}` : ''} — ${d.panels?.length || 0} panels — [View](/sqllab/dashboards/${d.id})`,
        )
        .join('\n') || '- No dashboards';

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

  const handleEditSelected = () => {
    if (!selectedPage) {
      return;
    }
    setEditingPageId(selectedPage.id);
    setWikiEditorOpen(true);
  };

  const handleDeleteSelected = () => {
    if (!selectedPage) {
      return;
    }
    Modal.confirm({
      title: 'Delete this wiki page?',
      content: 'This action cannot be undone.',
      okText: 'Delete',
      okButtonProps: { danger: true },
      onOk: () => deleteWikiMutation.mutate(selectedPage.id),
    });
  };

  // Page-level actions live in the unified shell toolbar
  const toolbarActions = useMemo(
    () => (
      <Space size={4}>
        {selectedPage && (
          <>
            <Tooltip title="Edit page">
              <Button type="text" size="small" icon={<EditOutlined />} onClick={handleEditSelected} />
            </Tooltip>
            <Tooltip title="Delete page">
              <Button type="text" size="small" icon={<DeleteOutlined />} danger onClick={handleDeleteSelected} />
            </Tooltip>
          </>
        )}
      </Space>
    ),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [selectedPage?.id],
  );
  usePageChrome({ toolbarActions });

  return (
    <div className="page-wiki">
      {/* Pages list — Notes-style middle column */}
      <aside className="wiki-pagelist">
        <header className="wiki-pagelist-header">
          <span className="wiki-pagelist-title">Wiki Pages</span>
          <Space size={2}>
            {aiEnabled && (
              <Tooltip title="Generate page with AI">
                <Button
                  type="text"
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
        </header>

        <div className="wiki-pagelist-scroll">
          {sortedPages.length === 0 ? (
            <div className="wiki-pagelist-empty">
              <FileTextOutlined className="wiki-pagelist-empty-glyph" />
              <p>No wiki pages yet.</p>
              <p className="wiki-pagelist-empty-hint">
                Create a page or generate one from project context using AI.
              </p>
            </div>
          ) : (
            <ul className="wiki-pagelist-items" role="list">
              {sortedPages.map((page) => {
                const selected = selectedPage?.id === page.id;
                return (
                  <li
                    key={page.id}
                    className={`wiki-pagelist-item${selected ? ' is-selected' : ''}`}
                    onClick={() => navigate(`/projects/${projectId}/wiki/${page.id}`)}
                  >
                    <div className="wiki-pagelist-item-title">{page.title}</div>
                    <div className="wiki-pagelist-item-preview">
                      {previewFromMarkdown(page.content) || <em>No content</em>}
                    </div>
                    <div className="wiki-pagelist-item-meta">{formatRelative(page.updatedAt)}</div>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      </aside>

      {/* Document content */}
      <section className="wiki-content">
        {aiGenerating ? (
          <article className="wiki-document">
            <header className="wiki-document-header">
              <div className="wiki-document-title-row">
                <h1 className="wiki-document-title">Generating Project Overview…</h1>
                <Space>
                  <Spin size="small" />
                  <Button size="small" onClick={handleCancelAi}>Cancel</Button>
                </Space>
              </div>
              <p className="wiki-document-meta">AI is drafting based on the project's datasets, queries, vizzes, and dashboards.</p>
            </header>
            <div className="wiki-document-body">
              <Markdown rehypePlugins={[rehypeRaw]}>
                {aiContent || '*Waiting for AI response…*'}
              </Markdown>
            </div>
          </article>
        ) : selectedPage ? (
          <article className="wiki-document">
            <header className="wiki-document-header">
              <h1 className="wiki-document-title">{selectedPage.title}</h1>
              <p className="wiki-document-meta">Last updated {formatRelative(selectedPage.updatedAt)}</p>
            </header>
            <div className="wiki-document-body">
              <Markdown rehypePlugins={[rehypeRaw]}>{selectedPage.content}</Markdown>
            </div>
          </article>
        ) : (
          <div className="wiki-document-empty">
            <FileTextOutlined className="wiki-document-empty-glyph" />
            <h2>Start the project wiki</h2>
            <p>
              Document the project's purpose, datasets, and findings in Markdown.
              Pick a page from the list, or create a new one.
            </p>
            <Space>
              {aiEnabled && (
                <Button icon={<RobotOutlined />} onClick={handleAiGenerate} loading={aiGenerating}>
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
                Create page
              </Button>
            </Space>
          </div>
        )}
      </section>

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
