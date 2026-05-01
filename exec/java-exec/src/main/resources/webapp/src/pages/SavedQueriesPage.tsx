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
import { useState, useMemo, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Input,
  Button,
  Space,
  Tag,
  Popconfirm,
  message,
  Tooltip,
  Spin,
  Modal,
  Form,
  Switch,
  Select,
  Dropdown,
} from 'antd';
import type { MenuProps } from 'antd';
import {
  SearchOutlined,
  PlayCircleOutlined,
  EditOutlined,
  DeleteOutlined,
  GlobalOutlined,
  LockOutlined,
  UserOutlined,
  CodeOutlined,
  FolderOutlined,
  ClockCircleOutlined,
  CopyOutlined,
  CalendarOutlined,
  PlusOutlined,
  MoreOutlined,
  CheckOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import Editor from '@monaco-editor/react';
import { getSavedQueries, deleteSavedQuery, updateSavedQuery, restoreSavedQuery } from '../api/savedQueries';
import { useUndoableDelete } from '../hooks/useUndoableDelete';
import { getProjects } from '../api/projects';
import { getSchedules } from '../api/schedules';
import { useCurrentUser } from '../hooks/useCurrentUser';
import { useTheme } from '../hooks/useTheme';
import { usePageChrome } from '../contexts/AppChromeContext';
import type { SavedQuery } from '../types';
import AddToProjectModal from '../components/common/AddToProjectModal';
import BulkAddToProjectModal from '../components/common/BulkAddToProjectModal';
import ScheduleModal from '../components/query-editor/ScheduleModal';

const { TextArea } = Input;

interface SavedQueriesPageProps {
  filterIds?: string[];
  projectId?: string;
  projectOwner?: string;
  onAdd?: () => void;
}

type Filter = 'all' | 'mine' | 'public' | 'scheduled';

function formatRelative(ts: number | string): string {
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

function firstLine(s: string, max = 80): string {
  const trimmed = s.replace(/\s+/g, ' ').trim();
  return trimmed.length > max ? trimmed.slice(0, max) + '…' : trimmed;
}

export default function SavedQueriesPage({ filterIds, projectId, projectOwner, onAdd }: SavedQueriesPageProps = {}) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { canEdit, canView, isProjectOwner } = useCurrentUser();
  const { isDark } = useTheme();
  const [searchText, setSearchText] = useState('');
  const [filter, setFilter] = useState<Filter>('all');
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [editingQuery, setEditingQuery] = useState<SavedQuery | null>(null);
  const [addToProjectId, setAddToProjectId] = useState<string | null>(null);
  const [bulkProjectModalOpen, setBulkProjectModalOpen] = useState(false);
  const [bulkSelected, setBulkSelected] = useState<Set<string>>(new Set());
  const [scheduleQueryId, setScheduleQueryId] = useState<string | null>(null);
  const [scheduleQueryName, setScheduleQueryName] = useState('');
  const [scheduleQuerySql, setScheduleQuerySql] = useState<string | undefined>(undefined);
  const [projectFilter, setProjectFilter] = useState<string | null>(null);
  const [form] = Form.useForm();

  const { data: queries, isLoading, error } = useQuery({
    queryKey: ['savedQueries'],
    queryFn: getSavedQueries,
  });

  const { data: projects } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
    enabled: !projectId,
  });

  const { data: schedules } = useQuery({
    queryKey: ['schedules'],
    queryFn: getSchedules,
  });

  const scheduledIds = useMemo(
    () => new Set((schedules ?? []).filter((s) => s.enabled).map((s) => s.savedQueryId)),
    [schedules],
  );

  const undoable = useUndoableDelete();

  const deleteSavedQueryWithUndo = (q: SavedQuery) => undoable.run({
    label: `Deleted "${q.name}"`,
    capture: () => q,
    remove: () => deleteSavedQuery(q.id),
    restore: (snap) => restoreSavedQuery(snap.id),
    onDeleted: () => queryClient.invalidateQueries({ queryKey: ['savedQueries'] }),
    onRestored: () => queryClient.invalidateQueries({ queryKey: ['savedQueries'] }),
  });

  const bulkDeleteWithUndo = (ids: string[]) => {
    if (ids.length === 0) {
      return;
    }
    undoable.run<string[]>({
      label: `Deleted ${ids.length} ${ids.length === 1 ? 'query' : 'queries'}`,
      capture: () => [...ids],
      remove: async () => { await Promise.all(ids.map((id) => deleteSavedQuery(id))); },
      restore: async (snap) => { await Promise.all(snap.map((id) => restoreSavedQuery(id))); },
      onDeleted: () => queryClient.invalidateQueries({ queryKey: ['savedQueries'] }),
      onRestored: () => queryClient.invalidateQueries({ queryKey: ['savedQueries'] }),
    });
  };

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<SavedQuery> }) =>
      updateSavedQuery(id, data),
    onSuccess: () => {
      message.success('Query updated');
      queryClient.invalidateQueries({ queryKey: ['savedQueries'] });
      setEditModalOpen(false);
      setEditingQuery(null);
    },
    onError: (err: Error) => message.error(`Failed to update: ${err.message}`),
  });

  const filteredQueries = useMemo(() => {
    if (!queries) {
      return [];
    }
    let result = queries.filter((q) => canView(q.owner, q.isPublic));

    if (filterIds) {
      const idSet = new Set(filterIds);
      result = result.filter((q) => idSet.has(q.id));
    }
    if (projectFilter && projects) {
      const proj = projects.find((p) => p.id === projectFilter);
      if (proj) {
        const idSet = new Set(proj.savedQueryIds || []);
        result = result.filter((q) => idSet.has(q.id));
      }
    }
    if (filter === 'mine') {
      result = result.filter((q) => canEdit(q.owner));
    } else if (filter === 'public') {
      result = result.filter((q) => q.isPublic);
    } else if (filter === 'scheduled') {
      result = result.filter((q) => scheduledIds.has(q.id));
    }
    if (searchText) {
      const lower = searchText.toLowerCase();
      result = result.filter(
        (q) =>
          q.name.toLowerCase().includes(lower) ||
          q.sql.toLowerCase().includes(lower) ||
          (q.description && q.description.toLowerCase().includes(lower)),
      );
    }
    // Most-recent first
    return [...result].sort((a, b) => {
      const at = typeof a.updatedAt === 'number' ? a.updatedAt : new Date(a.updatedAt).getTime();
      const bt = typeof b.updatedAt === 'number' ? b.updatedAt : new Date(b.updatedAt).getTime();
      return bt - at;
    });
  }, [queries, searchText, filter, filterIds, projectFilter, projects, scheduledIds, canView, canEdit]);

  // Auto-select first query when list changes (or current selection drops out of list)
  useEffect(() => {
    if (filteredQueries.length === 0) {
      setSelectedId(null);
      return;
    }
    if (!selectedId || !filteredQueries.some((q) => q.id === selectedId)) {
      setSelectedId(filteredQueries[0].id);
    }
  }, [filteredQueries, selectedId]);

  const selected = useMemo(
    () => filteredQueries.find((q) => q.id === selectedId) ?? null,
    [filteredQueries, selectedId],
  );

  const handleLoadQuery = (query: SavedQuery) => {
    const target = projectId ? `/projects/${projectId}/query` : '/query';
    navigate(target, { state: { loadQuery: query } });
  };

  const handleEdit = (query: SavedQuery) => {
    setEditingQuery(query);
    form.setFieldsValue({
      name: query.name,
      description: query.description,
      isPublic: query.isPublic,
    });
    setEditModalOpen(true);
  };

  const handleSaveEdit = async () => {
    if (!editingQuery) {
      return;
    }
    try {
      const values = await form.validateFields();
      updateMutation.mutate({
        id: editingQuery.id,
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

  const copyToClipboard = (text: string) => {
    navigator.clipboard
      .writeText(text)
      .then(() => message.success('Copied to clipboard'))
      .catch(() => message.error('Could not copy'));
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

  // Toolbar action: New Query (registers in unified shell toolbar)
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
          onClick={() => navigate(projectId ? `/projects/${projectId}/query` : '/query')}
        >
          New Query
        </Button>
      </Space>
    ),
    [navigate, onAdd, projectId],
  );
  usePageChrome({ toolbarActions });

  if (error) {
    return (
      <div className="page-savedqueries-error">
        <h2>Couldn't load queries</h2>
        <p>{(error as Error).message}</p>
      </div>
    );
  }

  return (
    <div className="page-savedqueries">
      {undoable.contextHolder}
      {/* Filter strip */}
      <div className="page-savedqueries-filterbar">
        <Input
          placeholder="Search by name, SQL, or description…"
          prefix={<SearchOutlined style={{ color: 'var(--color-text-tertiary)' }} />}
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          allowClear
          className="page-savedqueries-search"
        />

        <div className="page-savedqueries-chips">
          {(['all', 'mine', 'public', 'scheduled'] as Filter[]).map((f) => (
            <button
              key={f}
              type="button"
              className={`page-savedqueries-chip${filter === f ? ' is-active' : ''}`}
              onClick={() => setFilter(f)}
            >
              {f === 'all' && 'All'}
              {f === 'mine' && 'Mine'}
              {f === 'public' && 'Public'}
              {f === 'scheduled' && 'Scheduled'}
            </button>
          ))}
        </div>

        {!projectId && projects && projects.length > 0 && (
          <Select
            placeholder="Any project"
            value={projectFilter}
            onChange={setProjectFilter}
            allowClear
            size="small"
            style={{ width: 180 }}
            options={projects.map((p) => ({ value: p.id, label: p.name }))}
            suffixIcon={<FolderOutlined />}
          />
        )}

        {bulkSelected.size > 0 && (
          <div className="page-savedqueries-bulkbar">
            <span>{bulkSelected.size} selected</span>
            {!projectId && (
              <Button size="small" onClick={() => setBulkProjectModalOpen(true)}>Add to project</Button>
            )}
            <Popconfirm
              title={`Delete ${bulkSelected.size} ${bulkSelected.size === 1 ? 'query' : 'queries'}?`}
              onConfirm={() => {
                bulkDeleteWithUndo(Array.from(bulkSelected));
                setBulkSelected(new Set());
              }}
              okText="Delete"
              cancelText="Cancel"
              okButtonProps={{ danger: true }}
            >
              <Button size="small" danger>Delete</Button>
            </Popconfirm>
            <Button size="small" type="text" onClick={() => setBulkSelected(new Set())}>
              Clear
            </Button>
          </div>
        )}
      </div>

      {/* Two-column body */}
      <div className="page-savedqueries-body">
        {/* List column */}
        <ul className="savedquery-list" role="list">
          {isLoading ? (
            <li className="savedquery-list-loading"><Spin /></li>
          ) : filteredQueries.length === 0 ? (
            <li className="savedquery-list-empty">
              {searchText
                ? 'No queries match your search.'
                : onAdd
                  ? 'No saved queries in this project yet.'
                  : 'No saved queries yet — save one from SQL Lab.'}
            </li>
          ) : (
            filteredQueries.map((q) => {
              const isSelected = q.id === selectedId;
              const isBulk = bulkSelected.has(q.id);
              const subtitle = q.description?.trim()
                ? firstLine(q.description, 90)
                : firstLine(q.sql, 90);
              return (
                <li
                  key={q.id}
                  className={`savedquery-row${isSelected ? ' is-selected' : ''}${isBulk ? ' is-bulk-selected' : ''}`}
                  onClick={() => setSelectedId(q.id)}
                >
                  <button
                    type="button"
                    className={`savedquery-row-check${isBulk ? ' is-on' : ''}`}
                    aria-label={isBulk ? 'Deselect' : 'Select for bulk action'}
                    onClick={(e) => {
                      e.stopPropagation();
                      toggleBulk(q.id);
                    }}
                  >
                    {isBulk && <CheckOutlined />}
                  </button>

                  <div className="savedquery-row-body">
                    <div className="savedquery-row-title-line">
                      <span className="savedquery-row-name" title={q.name}>{q.name}</span>
                      {q.isPublic ? (
                        <GlobalOutlined className="savedquery-row-icon" title="Public" />
                      ) : (
                        <LockOutlined className="savedquery-row-icon" title="Private" />
                      )}
                      {scheduledIds.has(q.id) && (
                        <CalendarOutlined className="savedquery-row-icon savedquery-row-icon-scheduled" title="Scheduled" />
                      )}
                    </div>
                    <div className="savedquery-row-sub">{subtitle}</div>
                    <div className="savedquery-row-meta">
                      <span>{formatRelative(q.updatedAt)}</span>
                      <span aria-hidden="true">·</span>
                      <span>{q.owner}</span>
                      {q.defaultSchema && (
                        <>
                          <span aria-hidden="true">·</span>
                          <span className="savedquery-row-schema">{q.defaultSchema}</span>
                        </>
                      )}
                    </div>
                  </div>
                </li>
              );
            })
          )}
        </ul>

        {/* Preview column */}
        <section className="savedquery-preview">
          {selected ? (
            <SelectedQueryPreview
              query={selected}
              isDark={isDark}
              isScheduled={scheduledIds.has(selected.id)}
              canEditQuery={canEdit(selected.owner) || (projectOwner ? isProjectOwner(projectOwner) : false)}
              showAddToProject={!projectId}
              onRun={() => handleLoadQuery(selected)}
              onEdit={() => handleEdit(selected)}
              onCopy={() => copyToClipboard(selected.sql)}
              onSchedule={() => {
                setScheduleQueryId(selected.id);
                setScheduleQueryName(selected.name);
                setScheduleQuerySql(selected.sql);
              }}
              onAddToProject={() => setAddToProjectId(selected.id)}
              onDelete={() => deleteSavedQueryWithUndo(selected)}
            />
          ) : (
            <div className="savedquery-preview-empty">
              <CodeOutlined className="savedquery-preview-empty-glyph" />
              <h2>{filteredQueries.length === 0 ? 'No queries' : 'Select a query'}</h2>
              <p>
                {filteredQueries.length === 0
                  ? 'Save one from SQL Lab to see it here.'
                  : 'Choose a query from the list to preview its SQL.'}
              </p>
            </div>
          )}
        </section>
      </div>

      {/* Modals */}
      <Modal
        title="Edit Query"
        open={editModalOpen}
        onOk={handleSaveEdit}
        onCancel={() => {
          setEditModalOpen(false);
          setEditingQuery(null);
        }}
        confirmLoading={updateMutation.isPending}
      >
        <Form form={form} layout="vertical">
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
          <Form.Item name="isPublic" label="Public" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Modal>

      <AddToProjectModal
        open={!!addToProjectId}
        onClose={() => setAddToProjectId(null)}
        itemId={addToProjectId || ''}
        itemType="savedQuery"
      />

      <BulkAddToProjectModal
        open={bulkProjectModalOpen}
        onClose={() => setBulkProjectModalOpen(false)}
        itemIds={Array.from(bulkSelected)}
        itemType="savedQuery"
      />

      <ScheduleModal
        open={!!scheduleQueryId}
        onClose={() => {
          setScheduleQueryId(null);
          setScheduleQueryName('');
          setScheduleQuerySql(undefined);
        }}
        savedQueryId={scheduleQueryId || ''}
        savedQueryName={scheduleQueryName}
        savedQuerySql={scheduleQuerySql}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['schedules'] });
        }}
      />
    </div>
  );
}

interface SelectedQueryPreviewProps {
  query: SavedQuery;
  isDark: boolean;
  isScheduled: boolean;
  canEditQuery: boolean;
  showAddToProject: boolean;
  onRun: () => void;
  onEdit: () => void;
  onCopy: () => void;
  onSchedule: () => void;
  onAddToProject: () => void;
  onDelete: () => void;
}

function SelectedQueryPreview({
  query,
  isDark,
  isScheduled,
  canEditQuery,
  showAddToProject,
  onRun,
  onEdit,
  onCopy,
  onSchedule,
  onAddToProject,
  onDelete,
}: SelectedQueryPreviewProps) {
  const moreItems: MenuProps['items'] = [
    canEditQuery
      ? { key: 'edit', icon: <EditOutlined />, label: 'Edit details', onClick: onEdit }
      : null,
    canEditQuery
      ? { key: 'schedule', icon: <CalendarOutlined />, label: isScheduled ? 'Manage schedule' : 'Schedule…', onClick: onSchedule }
      : null,
    showAddToProject
      ? { key: 'addto', icon: <FolderOutlined />, label: 'Add to project…', onClick: onAddToProject }
      : null,
    { key: 'copy', icon: <CopyOutlined />, label: 'Copy SQL', onClick: onCopy },
    canEditQuery ? { type: 'divider' as const } : null,
    canEditQuery
      ? { key: 'delete', icon: <DeleteOutlined />, label: 'Delete', danger: true, onClick: onDelete }
      : null,
  ].filter(Boolean) as MenuProps['items'];

  return (
    <>
      <header className="savedquery-preview-header">
        <div className="savedquery-preview-titles">
          <h1 className="savedquery-preview-name">{query.name}</h1>
          <div className="savedquery-preview-badges">
            {query.isPublic ? (
              <Tag bordered={false} icon={<GlobalOutlined />} color="success">Public</Tag>
            ) : (
              <Tag bordered={false} icon={<LockOutlined />}>Private</Tag>
            )}
            {isScheduled && (
              <Tag bordered={false} icon={<CalendarOutlined />} color="processing">Scheduled</Tag>
            )}
            {query.defaultSchema && <Tag bordered={false}>{query.defaultSchema}</Tag>}
          </div>
        </div>
        <div className="savedquery-preview-actions">
          <Button type="primary" icon={<PlayCircleOutlined />} onClick={onRun}>
            Run in SQL Lab
          </Button>
          <Tooltip title="Copy SQL">
            <Button icon={<CopyOutlined />} onClick={onCopy} />
          </Tooltip>
          <Dropdown menu={{ items: moreItems }} placement="bottomRight" trigger={['click']}>
            <Button icon={<MoreOutlined />} />
          </Dropdown>
        </div>
      </header>

      {query.description && (
        <p className="savedquery-preview-desc">{query.description}</p>
      )}

      <div className="savedquery-preview-meta">
        <span><UserOutlined /> {query.owner}</span>
        <span><ClockCircleOutlined /> Updated {formatRelative(query.updatedAt)}</span>
      </div>

      <div className="savedquery-preview-sql">
        <Editor
          value={query.sql}
          language="sql"
          theme={isDark ? 'vs-dark' : 'light'}
          height="100%"
          options={{
            readOnly: true,
            minimap: { enabled: false },
            scrollBeyondLastLine: false,
            fontSize: 13,
            fontFamily: "'SF Mono', SFMono-Regular, Consolas, 'Liberation Mono', Menlo, monospace",
            renderLineHighlight: 'none',
            lineNumbers: 'on',
            folding: false,
            scrollbar: { useShadows: false, verticalScrollbarSize: 10 },
            padding: { top: 10, bottom: 10 },
          }}
        />
      </div>
    </>
  );
}
