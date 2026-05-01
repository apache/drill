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
import { useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Input,
  Button,
  Space,
  Popconfirm,
  message,
  Spin,
  Modal,
  Form,
  Select,
  Switch,
  Tooltip,
  Dropdown,
} from 'antd';
import type { MenuProps } from 'antd';
import {
  SearchOutlined,
  EditOutlined,
  DeleteOutlined,
  GlobalOutlined,
  LockOutlined,
  AppstoreOutlined,
  ClockCircleOutlined,
  LinkOutlined,
  StarOutlined,
  StarFilled,
  FolderOutlined,
  PlusOutlined,
  CheckOutlined,
  MoreOutlined,
  DashboardOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getDashboards,
  createDashboard,
  deleteDashboard,
  restoreDashboard,
  updateDashboard,
  getFavorites,
  toggleFavorite,
} from '../api/dashboards';
import { useCurrentUser } from '../hooks/useCurrentUser';
import { useUndoableDelete } from '../hooks/useUndoableDelete';
import { usePageChrome } from '../contexts/AppChromeContext';
import type { Dashboard, DashboardPanel, DashboardPanelType } from '../types';
import AddToProjectModal from '../components/common/AddToProjectModal';
import BulkAddToProjectModal from '../components/common/BulkAddToProjectModal';

const { TextArea } = Input;

type Filter = 'all' | 'favorites' | 'public';

function formatRelative(ts: number | string | undefined): string {
  if (!ts) {
    return 'unknown';
  }
  const date = typeof ts === 'number' ? new Date(ts) : new Date(ts);
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
  if (days < 30) {
    return `${Math.floor(days / 7)}w ago`;
  }
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

/**
 * Mini schematic of a dashboard's panel layout — Apple Photos-style "the content
 * is the cover". Each panel renders as a tinted rectangle in the same relative
 * position it occupies in the dashboard grid.
 */
function DashboardThumb({ dashboard, height = 180 }: { dashboard: Dashboard; height?: number }) {
  const panels = dashboard.panels ?? [];

  const { maxX, maxY } = useMemo(() => {
    let x = 0;
    let y = 0;
    panels.forEach((p) => {
      x = Math.max(x, p.x + p.width);
      y = Math.max(y, p.y + p.height);
    });
    return { maxX: Math.max(x, 1), maxY: Math.max(y, 1) };
  }, [panels]);

  if (panels.length === 0) {
    return (
      <div className="dashboard-tile-empty" style={{ height }}>
        <DashboardOutlined />
        <span>Empty dashboard</span>
      </div>
    );
  }

  return (
    <div className="dashboard-tile-thumb" style={{ height }}>
      {panels.map((p: DashboardPanel) => (
        <div
          key={p.id}
          className={`dashboard-tile-panel dashboard-tile-panel-${p.type}`}
          style={{
            left: `${(p.x / maxX) * 100}%`,
            top: `${(p.y / maxY) * 100}%`,
            width: `${(p.width / maxX) * 100}%`,
            height: `${(p.height / maxY) * 100}%`,
          }}
        />
      ))}
    </div>
  );
}

function panelTypeBreakdown(dashboard: Dashboard): { type: DashboardPanelType; count: number }[] {
  const counts = new Map<DashboardPanelType, number>();
  for (const p of dashboard.panels ?? []) {
    counts.set(p.type, (counts.get(p.type) ?? 0) + 1);
  }
  return Array.from(counts.entries()).map(([type, count]) => ({ type, count }));
}

interface DashboardsPageProps {
  filterIds?: string[];
  projectId?: string;
  projectName?: string;
  projectOwner?: string;
  onAdd?: () => void;
}

export default function DashboardsPage({ filterIds, projectId, projectName, projectOwner, onAdd }: DashboardsPageProps = {}) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { canEdit, canView, isProjectOwner } = useCurrentUser();
  const [searchText, setSearchText] = useState('');
  const [filter, setFilter] = useState<Filter>('all');
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [editingDashboard, setEditingDashboard] = useState<Dashboard | null>(null);
  const [createForm] = Form.useForm();
  const [editForm] = Form.useForm();
  const [addToProjectId, setAddToProjectId] = useState<string | null>(null);
  const [bulkSelected, setBulkSelected] = useState<Set<string>>(new Set());
  const [bulkProjectModalOpen, setBulkProjectModalOpen] = useState(false);

  const { data: dashboards, isLoading, error } = useQuery({
    queryKey: ['dashboards'],
    queryFn: getDashboards,
  });

  const { data: favorites } = useQuery({
    queryKey: ['dashboard-favorites'],
    queryFn: getFavorites,
  });

  const favoriteSet = useMemo(() => new Set(favorites ?? []), [favorites]);

  const favoriteMutation = useMutation({
    mutationFn: toggleFavorite,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dashboard-favorites'] });
    },
  });

  const createMutation = useMutation({
    mutationFn: createDashboard,
    onSuccess: (newDashboard) => {
      message.success('Dashboard created');
      queryClient.invalidateQueries({ queryKey: ['dashboards'] });
      setCreateModalOpen(false);
      createForm.resetFields();
      const dashboardPath = projectId
        ? `/projects/${projectId}/dashboards/${newDashboard.id}`
        : `/dashboards/${newDashboard.id}`;
      const navState = projectId
        ? { state: { from: `/projects/${projectId}/dashboards`, projectName, projectId } }
        : undefined;
      navigate(dashboardPath, navState);
    },
    onError: (err: Error) => message.error(`Failed to create dashboard: ${err.message}`),
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<Dashboard> }) => updateDashboard(id, data),
    onSuccess: () => {
      message.success('Dashboard updated');
      queryClient.invalidateQueries({ queryKey: ['dashboards'] });
      setEditModalOpen(false);
      setEditingDashboard(null);
    },
    onError: (err: Error) => message.error(`Failed to update dashboard: ${err.message}`),
  });

  const undoable = useUndoableDelete();

  const deleteDashboardWithUndo = (dashboard: Dashboard) => undoable.run({
    label: `Deleted "${dashboard.name}"`,
    capture: () => dashboard,
    remove: () => deleteDashboard(dashboard.id),
    restore: (snap) => restoreDashboard(snap.id),
    onDeleted: () => queryClient.invalidateQueries({ queryKey: ['dashboards'] }),
    onRestored: () => queryClient.invalidateQueries({ queryKey: ['dashboards'] }),
  });

  const bulkDeleteDashboardsWithUndo = (ids: string[]) => {
    if (ids.length === 0) {
      return;
    }
    undoable.run<string[]>({
      label: `Deleted ${ids.length} ${ids.length === 1 ? 'dashboard' : 'dashboards'}`,
      capture: () => [...ids],
      remove: async () => { await Promise.all(ids.map((id) => deleteDashboard(id))); },
      restore: async (snap) => { await Promise.all(snap.map((id) => restoreDashboard(id))); },
      onDeleted: () => queryClient.invalidateQueries({ queryKey: ['dashboards'] }),
      onRestored: () => queryClient.invalidateQueries({ queryKey: ['dashboards'] }),
    });
  };

  const visibleDashboards = useMemo(() => {
    if (!dashboards) {
      return [];
    }
    let result = dashboards.filter((d) => canView(d.owner, d.isPublic));
    if (filterIds) {
      const idSet = new Set(filterIds);
      result = result.filter((d) => idSet.has(d.id));
    }
    return result;
  }, [dashboards, filterIds, canView]);

  const filteredDashboards = useMemo(() => {
    let result = visibleDashboards;
    if (filter === 'favorites') {
      result = result.filter((d) => favoriteSet.has(d.id));
    } else if (filter === 'public') {
      result = result.filter((d) => d.isPublic);
    }
    if (searchText) {
      const lower = searchText.toLowerCase();
      result = result.filter(
        (d) =>
          d.name.toLowerCase().includes(lower) ||
          (d.description && d.description.toLowerCase().includes(lower)),
      );
    }
    // Most recent first
    return [...result].sort((a, b) => {
      const at = typeof a.updatedAt === 'number' ? a.updatedAt : new Date(a.updatedAt).getTime();
      const bt = typeof b.updatedAt === 'number' ? b.updatedAt : new Date(b.updatedAt).getTime();
      return bt - at;
    });
  }, [visibleDashboards, filter, searchText, favoriteSet]);

  const handleCreate = async () => {
    try {
      const values = await createForm.validateFields();
      createMutation.mutate({
        name: values.name,
        description: values.description,
        isPublic: values.isPublic || false,
        refreshInterval: values.refreshInterval || 0,
      });
    } catch {
      // Form validation failed
    }
  };

  const handleEdit = (dashboard: Dashboard) => {
    setEditingDashboard(dashboard);
    editForm.setFieldsValue({
      name: dashboard.name,
      description: dashboard.description,
      isPublic: dashboard.isPublic,
    });
    setEditModalOpen(true);
  };

  const handleSaveEdit = async () => {
    if (!editingDashboard) {
      return;
    }
    try {
      const values = await editForm.validateFields();
      updateMutation.mutate({
        id: editingDashboard.id,
        data: {
          name: values.name,
          description: values.description,
          isPublic: values.isPublic,
        },
      });
    } catch {
      // Form validation failed
    }
  };

  const navigateToDashboard = (dashboard: Dashboard) => {
    const dashboardPath = projectId
      ? `/projects/${projectId}/dashboards/${dashboard.id}`
      : `/dashboards/${dashboard.id}`;
    const navState = projectId
      ? { state: { from: `/projects/${projectId}/dashboards`, projectName, projectId } }
      : undefined;
    navigate(dashboardPath, navState);
  };

  const toggleBulk = (id: string) => {
    setBulkSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  // Toolbar actions registered in the unified shell toolbar
  const toolbarActions = useMemo(
    () => (
      <Space size={4}>
        {onAdd && (
          <Tooltip title="Add existing">
            <Button type="text" size="small" icon={<PlusOutlined />} onClick={onAdd} />
          </Tooltip>
        )}
        <Button
          type="primary"
          size="small"
          icon={<PlusOutlined />}
          onClick={() => setCreateModalOpen(true)}
        >
          New Dashboard
        </Button>
      </Space>
    ),
    [onAdd],
  );
  usePageChrome({ toolbarActions });

  if (error) {
    return (
      <div className="page-dashboards-error">
        <h2>Couldn't load dashboards</h2>
        <p>{(error as Error).message}</p>
      </div>
    );
  }

  return (
    <div className="page-dashboards">
      {undoable.contextHolder}
      <header className="page-dashboards-header">
        <div>
          <h1 className="page-dashboards-title">Dashboards</h1>
          <p className="page-dashboards-subtitle">
            {dashboards === undefined
              ? 'Loading…'
              : visibleDashboards.length === 0
                ? 'No dashboards yet'
                : filteredDashboards.length === visibleDashboards.length
                  ? `${visibleDashboards.length} ${visibleDashboards.length === 1 ? 'dashboard' : 'dashboards'}`
                  : `Showing ${filteredDashboards.length} of ${visibleDashboards.length}`}
          </p>
        </div>
      </header>

      <div className="page-dashboards-toolbar">
        <Input
          placeholder="Search dashboards…"
          prefix={<SearchOutlined style={{ color: 'var(--color-text-tertiary)' }} />}
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          allowClear
          className="page-dashboards-search"
        />

        <div className="page-dashboards-chips">
          {(['all', 'favorites', 'public'] as Filter[]).map((f) => (
            <button
              key={f}
              type="button"
              className={`page-dashboards-chip${filter === f ? ' is-active' : ''}`}
              onClick={() => setFilter(f)}
            >
              {f === 'all' && 'All'}
              {f === 'favorites' && (
                <>
                  <StarFilled style={{ fontSize: 11, marginRight: 4, color: '#FFD60A' }} />
                  Favorites
                </>
              )}
              {f === 'public' && (
                <>
                  <GlobalOutlined style={{ fontSize: 11, marginRight: 4 }} />
                  Public
                </>
              )}
            </button>
          ))}
        </div>

        {bulkSelected.size > 0 && (
          <div className="page-dashboards-bulkbar">
            <span>{bulkSelected.size} selected</span>
            {!projectId && (
              <Button size="small" onClick={() => setBulkProjectModalOpen(true)}>Add to project</Button>
            )}
            <Popconfirm
              title={`Delete ${bulkSelected.size} ${bulkSelected.size === 1 ? 'dashboard' : 'dashboards'}?`}
              onConfirm={() => {
                bulkDeleteDashboardsWithUndo(Array.from(bulkSelected));
                setBulkSelected(new Set());
              }}
              okText="Delete"
              cancelText="Cancel"
              okButtonProps={{ danger: true }}
            >
              <Button size="small" danger>Delete</Button>
            </Popconfirm>
            <Button size="small" type="text" onClick={() => setBulkSelected(new Set())}>Clear</Button>
          </div>
        )}
      </div>

      {isLoading ? (
        <div className="page-dashboards-loading">
          <Spin size="large" />
        </div>
      ) : filteredDashboards.length === 0 ? (
        <div className="page-dashboards-empty">
          <DashboardOutlined className="page-dashboards-empty-glyph" />
          <h2>
            {searchText || filter !== 'all'
              ? 'No matches'
              : visibleDashboards.length === 0
                ? 'No dashboards yet'
                : 'No dashboards'}
          </h2>
          <p>
            {searchText || filter !== 'all'
              ? 'Try a different search or filter.'
              : 'Create one to start visualizing your data.'}
          </p>
          {!searchText && filter === 'all' && (
            <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateModalOpen(true)}>
              Create Dashboard
            </Button>
          )}
          {(searchText || filter !== 'all') && (
            <Button onClick={() => { setSearchText(''); setFilter('all'); }}>Clear filters</Button>
          )}
        </div>
      ) : (
        <div className="page-dashboards-grid">
          {filteredDashboards.map((dashboard) => {
            const isBulk = bulkSelected.has(dashboard.id);
            const isFavorite = favoriteSet.has(dashboard.id);
            const editable = canEdit(dashboard.owner) || (projectOwner ? isProjectOwner(projectOwner) : false);
            const breakdown = panelTypeBreakdown(dashboard);

            const moreItems: MenuProps['items'] = [
              editable
                ? { key: 'edit', icon: <EditOutlined />, label: 'Edit details', onClick: () => handleEdit(dashboard) }
                : null,
              !projectId
                ? { key: 'addto', icon: <FolderOutlined />, label: 'Add to project…', onClick: () => setAddToProjectId(dashboard.id) }
                : null,
              editable ? { type: 'divider' as const } : null,
              editable
                ? {
                    key: 'delete',
                    icon: <DeleteOutlined />,
                    label: 'Delete',
                    danger: true,
                    onClick: () => {
                      Modal.confirm({
                        title: 'Delete this dashboard?',
                        content: 'This action cannot be undone.',
                        okText: 'Delete',
                        okButtonProps: { danger: true },
                        onOk: () => deleteDashboardWithUndo(dashboard),
                      });
                    },
                  }
                : null,
            ].filter(Boolean) as MenuProps['items'];

            return (
              <div
                key={dashboard.id}
                className={`dashboard-tile${isBulk ? ' is-bulk-selected' : ''}`}
                onClick={() => navigateToDashboard(dashboard)}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    navigateToDashboard(dashboard);
                  }
                }}
              >
                <div className="dashboard-tile-thumb-wrap">
                  <DashboardThumb dashboard={dashboard} />

                  <button
                    type="button"
                    className={`dashboard-tile-check${isBulk ? ' is-on' : ''}`}
                    aria-label={isBulk ? 'Deselect' : 'Select'}
                    onClick={(e) => {
                      e.stopPropagation();
                      toggleBulk(dashboard.id);
                    }}
                  >
                    {isBulk && <CheckOutlined />}
                  </button>

                  <button
                    type="button"
                    className={`dashboard-tile-fav${isFavorite ? ' is-on' : ''}`}
                    aria-label={isFavorite ? 'Unfavorite' : 'Favorite'}
                    onClick={(e) => {
                      e.stopPropagation();
                      favoriteMutation.mutate(dashboard.id);
                    }}
                  >
                    {isFavorite ? <StarFilled /> : <StarOutlined />}
                  </button>

                  <div className="dashboard-tile-actions" onClick={(e) => e.stopPropagation()}>
                    <Dropdown menu={{ items: moreItems }} placement="bottomRight" trigger={['click']}>
                      <button type="button" className="dashboard-tile-action-btn" aria-label="More">
                        <MoreOutlined />
                      </button>
                    </Dropdown>
                  </div>
                </div>

                <div className="dashboard-tile-meta">
                  <div className="dashboard-tile-name-row">
                    <h3 className="dashboard-tile-name" title={dashboard.name}>{dashboard.name}</h3>
                    {dashboard.isPublic ? (
                      <GlobalOutlined className="dashboard-tile-vis-icon" title="Public" />
                    ) : (
                      <LockOutlined className="dashboard-tile-vis-icon" title="Private" />
                    )}
                  </div>
                  <div className="dashboard-tile-sub">
                    <span className="dashboard-tile-counts">
                      <AppstoreOutlined />
                      {dashboard.panels?.length ?? 0} {(dashboard.panels?.length ?? 0) === 1 ? 'panel' : 'panels'}
                      {breakdown.length > 1 && (
                        <span className="dashboard-tile-types">
                          {breakdown.slice(0, 3).map((b) => (
                            <span key={b.type} className={`dashboard-tile-type-dot dashboard-tile-panel-${b.type}`} title={`${b.count} ${b.type}`} />
                          ))}
                        </span>
                      )}
                    </span>
                    {dashboard.refreshInterval > 0 && (
                      <span className="dashboard-tile-refresh">
                        <ClockCircleOutlined /> {dashboard.refreshInterval}s
                      </span>
                    )}
                    <span className="dashboard-tile-time">{formatRelative(dashboard.updatedAt)}</span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Create Dashboard Modal */}
      <Modal
        title="New Dashboard"
        open={createModalOpen}
        onOk={handleCreate}
        onCancel={() => {
          setCreateModalOpen(false);
          createForm.resetFields();
        }}
        confirmLoading={createMutation.isPending}
        okText="Create"
      >
        <Form form={createForm} layout="vertical">
          <Form.Item
            name="name"
            label="Name"
            rules={[{ required: true, message: 'Please enter a dashboard name' }]}
          >
            <Input placeholder="My Dashboard" />
          </Form.Item>
          <Form.Item name="description" label="Description">
            <TextArea rows={3} placeholder="What is this dashboard for?" />
          </Form.Item>
          <Form.Item name="refreshInterval" label="Auto-refresh">
            <Select
              placeholder="Select refresh interval"
              options={[
                { value: 0, label: 'Off' },
                { value: 10, label: '10 seconds' },
                { value: 30, label: '30 seconds' },
                { value: 60, label: '1 minute' },
                { value: 300, label: '5 minutes' },
              ]}
            />
          </Form.Item>
          <Form.Item name="isPublic" label="Public" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Modal>

      {/* Edit Dashboard Modal */}
      <Modal
        title="Edit Dashboard"
        open={editModalOpen}
        onOk={handleSaveEdit}
        onCancel={() => {
          setEditModalOpen(false);
          setEditingDashboard(null);
        }}
        confirmLoading={updateMutation.isPending}
      >
        <Form form={editForm} layout="vertical">
          <Form.Item
            name="name"
            label="Name"
            rules={[{ required: true, message: 'Please enter a name' }]}
          >
            <Input />
          </Form.Item>
          <Form.Item name="description" label="Description">
            <TextArea rows={3} />
          </Form.Item>
          <Form.Item name="isPublic" label="Visibility">
            <Select
              options={[
                { value: false, label: 'Private' },
                { value: true, label: 'Public' },
              ]}
            />
          </Form.Item>
        </Form>
        {editingDashboard?.isPublic && (
          <div className="dashboard-share-link">
            <span className="dashboard-share-link-label"><LinkOutlined /> Shareable link</span>
            <Input.Group compact>
              <Input
                value={`${window.location.origin}/sqllab/dashboards/${editingDashboard.id}`}
                readOnly
                style={{ width: 'calc(100% - 80px)' }}
              />
              <Button
                icon={<LinkOutlined />}
                onClick={() => {
                  navigator.clipboard.writeText(
                    `${window.location.origin}/sqllab/dashboards/${editingDashboard.id}`,
                  ).then(
                    () => message.success('Link copied'),
                    () => message.error('Failed to copy'),
                  );
                }}
              >
                Copy
              </Button>
            </Input.Group>
          </div>
        )}
      </Modal>

      <AddToProjectModal
        open={!!addToProjectId}
        onClose={() => setAddToProjectId(null)}
        itemId={addToProjectId || ''}
        itemType="dashboard"
      />

      <BulkAddToProjectModal
        open={bulkProjectModalOpen}
        onClose={() => setBulkProjectModalOpen(false)}
        itemIds={Array.from(bulkSelected)}
        itemType="dashboard"
      />
    </div>
  );
}
