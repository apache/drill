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
import { useMemo, useState } from 'react';
import { Tabs, Button, Space, Tag, Modal, message, Tooltip } from 'antd';
import {
  DatabaseOutlined,
  CodeOutlined,
  BarChartOutlined,
  DashboardOutlined,
  FileTextOutlined,
  GlobalOutlined,
  LockOutlined,
  UserOutlined,
  DeleteOutlined,
  PlusOutlined,
  ShareAltOutlined,
  DownloadOutlined,
  HistoryOutlined,
  BranchesOutlined,
} from '@ant-design/icons';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { removeDataset } from '../api/projects';
import { useProjectContext } from '../contexts/ProjectContext';
import {
  ShareModal,
  DatasetPickerModal,
  ProjectActivityFeed,
  ProjectLineage,
  ExportImportModal,
} from '../components/project';
import { usePageChrome } from '../contexts/AppChromeContext';

function formatDate(timestamp: number): string {
  return new Date(timestamp).toLocaleString();
}

function formatRelative(ts: number): string {
  const diff = Date.now() - new Date(ts).getTime();
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
  return new Date(ts).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
}

export default function ProjectDetailPage() {
  const { project, projectId } = useProjectContext();
  const queryClient = useQueryClient();
  const [activeTab, setActiveTab] = useState('overview');
  const [shareModalOpen, setShareModalOpen] = useState(false);
  const [datasetRefModalOpen, setDatasetRefModalOpen] = useState(false);
  const [exportModalOpen, setExportModalOpen] = useState(false);

  const removeDatasetMutation = useMutation({
    mutationFn: (datasetId: string) => removeDataset(projectId!, datasetId),
    onSuccess: () => {
      message.success('Dataset removed');
      queryClient.invalidateQueries({ queryKey: ['project', projectId] });
    },
  });

  const toolbarActions = useMemo(
    () => (
      <Space size={4}>
        <Tooltip title="Export project">
          <Button type="text" size="small" icon={<DownloadOutlined />} onClick={() => setExportModalOpen(true)} />
        </Tooltip>
        <Button type="primary" size="small" icon={<ShareAltOutlined />} onClick={() => setShareModalOpen(true)}>
          Share
        </Button>
      </Space>
    ),
    [],
  );
  usePageChrome({ toolbarActions });

  if (!project) {
    return null;
  }

  const tabItems = [
    {
      key: 'overview',
      label: 'Overview',
      children: (
        <div className="settings-section-stack">
          {project.description && (
            <SettingsSection title="Description">
              <p className="settings-prose">{project.description}</p>
            </SettingsSection>
          )}

          <SettingsSection title="Details">
            <dl className="settings-dlist">
              <div className="settings-drow">
                <dt>Owner</dt>
                <dd><UserOutlined /> {project.owner}</dd>
              </div>
              <div className="settings-drow">
                <dt>Visibility</dt>
                <dd>
                  {project.isPublic
                    ? <><GlobalOutlined style={{ color: 'var(--color-success)' }} /> Public</>
                    : <><LockOutlined style={{ color: 'var(--color-warning)' }} /> Private</>}
                </dd>
              </div>
              <div className="settings-drow">
                <dt>Created</dt>
                <dd>{formatDate(project.createdAt)}</dd>
              </div>
              <div className="settings-drow">
                <dt>Updated</dt>
                <dd>{formatDate(project.updatedAt)} <span className="settings-muted">· {formatRelative(project.updatedAt)}</span></dd>
              </div>
              {project.sharedWith && project.sharedWith.length > 0 && (
                <div className="settings-drow">
                  <dt>Shared with</dt>
                  <dd>
                    <Space size={4} wrap>
                      {project.sharedWith.map((user) => (
                        <Tag key={user} bordered={false}>{user}</Tag>
                      ))}
                    </Space>
                  </dd>
                </div>
              )}
              {project.tags && project.tags.length > 0 && (
                <div className="settings-drow">
                  <dt>Tags</dt>
                  <dd>
                    <Space size={4} wrap>
                      {project.tags.map((t) => (
                        <Tag key={t} color="blue" bordered={false}>{t}</Tag>
                      ))}
                    </Space>
                  </dd>
                </div>
              )}
            </dl>
          </SettingsSection>

          <SettingsSection title="Contents">
            <div className="settings-stat-grid">
              <StatCard icon={<DatabaseOutlined />} label="Datasets" value={project.datasets?.length ?? 0} />
              <StatCard icon={<CodeOutlined />} label="Saved Queries" value={project.savedQueryIds?.length ?? 0} />
              <StatCard icon={<BarChartOutlined />} label="Visualizations" value={project.visualizationIds?.length ?? 0} />
              <StatCard icon={<DashboardOutlined />} label="Dashboards" value={project.dashboardIds?.length ?? 0} />
              <StatCard icon={<FileTextOutlined />} label="Wiki Pages" value={project.wikiPages?.length ?? 0} />
            </div>
          </SettingsSection>
        </div>
      ),
    },
    {
      key: 'datasets',
      label: <span><DatabaseOutlined /> Datasets</span>,
      children: (
        <div className="settings-section-stack">
          <SettingsSection
            title="Datasets"
            trailing={
              <Button
                type="primary"
                size="small"
                icon={<PlusOutlined />}
                onClick={() => setDatasetRefModalOpen(true)}
              >
                Add Dataset
              </Button>
            }
          >
            {(!project.datasets || project.datasets.length === 0) ? (
              <p className="settings-empty">No datasets added yet.</p>
            ) : (
              <ul className="settings-row-list">
                {project.datasets.map((dataset) => {
                  const typeLabel = dataset.type === 'plugin' ? 'Plugin'
                    : dataset.type === 'schema' ? 'Schema'
                    : dataset.type === 'saved_query' ? 'Saved Query'
                    : 'Table';
                  const typeDesc = dataset.type === 'plugin'
                    ? `All schemas in ${dataset.schema}`
                    : dataset.type === 'schema'
                    ? `All tables in ${dataset.schema}`
                    : dataset.type === 'saved_query'
                    ? `Saved Query: ${dataset.savedQueryId}`
                    : `${dataset.schema}.${dataset.table}`;
                  return (
                    <li key={dataset.id} className="settings-row">
                      <DatabaseOutlined className="settings-row-icon" />
                      <div className="settings-row-body">
                        <div className="settings-row-title">{dataset.label || dataset.schema || '(unnamed)'}</div>
                        <code className="settings-row-sub">{typeDesc}</code>
                      </div>
                      <Tag bordered={false} className="settings-row-tag">{typeLabel}</Tag>
                      <Tooltip title="Remove">
                        <Button
                          type="text"
                          danger
                          size="small"
                          icon={<DeleteOutlined />}
                          onClick={() => Modal.confirm({
                            title: 'Remove this dataset?',
                            content: 'The dataset will be removed from this project.',
                            okText: 'Remove',
                            okButtonProps: { danger: true },
                            onOk: () => removeDatasetMutation.mutate(dataset.id),
                          })}
                        />
                      </Tooltip>
                    </li>
                  );
                })}
              </ul>
            )}
          </SettingsSection>
        </div>
      ),
    },
    {
      key: 'activity',
      label: <span><HistoryOutlined /> Activity</span>,
      children: (
        <div className="settings-section-stack">
          <SettingsSection title="Recent Activity" padded={false}>
            <ProjectActivityFeed project={project} />
          </SettingsSection>
        </div>
      ),
    },
    {
      key: 'lineage',
      label: <span><BranchesOutlined /> Lineage</span>,
      children: (
        <div className="settings-section-stack">
          <SettingsSection title="Lineage" padded={false}>
            <ProjectLineage project={project} />
          </SettingsSection>
        </div>
      ),
    },
  ];

  return (
    <div className="page-projectsettings">
      <header className="page-projectsettings-header">
        <div>
          <h1 className="page-projectsettings-title">{project.name}</h1>
          <div className="page-projectsettings-subtitle">
            {project.isPublic ? (
              <Tag bordered={false} icon={<GlobalOutlined />} color="success">Public</Tag>
            ) : (
              <Tag bordered={false} icon={<LockOutlined />}>Private</Tag>
            )}
            <span className="settings-muted">· {project.owner} · Updated {formatRelative(project.updatedAt)}</span>
          </div>
        </div>
      </header>

      <div className="page-projectsettings-tabs">
        <Tabs activeKey={activeTab} onChange={setActiveTab} items={tabItems} />
      </div>

      {/* Modals */}
      <ShareModal
        open={shareModalOpen}
        project={project}
        onClose={() => setShareModalOpen(false)}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
          setShareModalOpen(false);
        }}
      />

      <DatasetPickerModal
        open={datasetRefModalOpen}
        projectId={projectId!}
        existingDatasets={project.datasets || []}
        onClose={() => setDatasetRefModalOpen(false)}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
          setDatasetRefModalOpen(false);
        }}
      />

      <ExportImportModal
        open={exportModalOpen}
        mode="export"
        projectId={projectId}
        onClose={() => setExportModalOpen(false)}
      />
    </div>
  );
}

interface SettingsSectionProps {
  title: string;
  trailing?: React.ReactNode;
  /** When false, omit the inner padding (for components that bring their own scaffolding) */
  padded?: boolean;
  children: React.ReactNode;
}

function SettingsSection({ title, trailing, padded = true, children }: SettingsSectionProps) {
  return (
    <section className="settings-section">
      <header className="settings-section-header">
        <h2 className="settings-section-title">{title}</h2>
        {trailing && <span className="settings-section-trailing">{trailing}</span>}
      </header>
      <div className={`settings-section-body${padded ? '' : ' is-flush'}`}>
        {children}
      </div>
    </section>
  );
}

function StatCard({ icon, label, value }: { icon: React.ReactNode; label: string; value: number }) {
  return (
    <div className="settings-stat">
      <span className="settings-stat-icon">{icon}</span>
      <span className="settings-stat-value">{value.toLocaleString()}</span>
      <span className="settings-stat-label">{label}</span>
    </div>
  );
}
