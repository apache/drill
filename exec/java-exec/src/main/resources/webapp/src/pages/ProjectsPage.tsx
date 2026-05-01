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
  Modal,
  Form,
  Switch,
  Select,
  Tooltip,
  Tag,
  Spin,
} from 'antd';
import {
  SearchOutlined,
  EditOutlined,
  DeleteOutlined,
  GlobalOutlined,
  LockOutlined,
  PlusOutlined,
  StarOutlined,
  StarFilled,
  UploadOutlined,
  FolderOutlined,
  ClockCircleOutlined,
  DatabaseOutlined,
  CodeOutlined,
  BarChartOutlined,
  DashboardOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getProjects,
  createProject,
  deleteProject,
  restoreProject,
  updateProject,
  getFavorites,
  toggleFavorite,
} from '../api/projects';
import type { Project } from '../types';
import { ExportImportModal } from '../components/project';
import ProjectAppearanceField, { resolveTileBackground, TINT_PALETTE } from '../components/project/ProjectAppearanceField';
import {
  coverFromProject,
  coverToProjectFields,
  type CoverStyle,
} from '../hooks/useProjectAppearance';
import { useUndoableDelete } from '../hooks/useUndoableDelete';
import { usePageChrome } from '../contexts/AppChromeContext';

const { TextArea } = Input;

// Tint palette and resolveTileBackground live in ProjectAppearanceField for reuse.

function monogramFor(project: Project): string {
  const words = project.name.trim().split(/\s+/);
  if (words.length >= 2) {
    return (words[0][0] + words[1][0]).toUpperCase();
  }
  return project.name.slice(0, 2).toUpperCase();
}

function formatRelative(ts: number | string | undefined): string {
  if (!ts) {
    return '—';
  }
  const date = typeof ts === 'number' ? new Date(ts) : new Date(ts);
  const diff = Date.now() - date.getTime();
  const sec = Math.floor(diff / 1000);
  if (sec < 60) {
    return 'just now';
  }
  const min = Math.floor(sec / 60);
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
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
}

interface ProjectTileProps {
  project: Project;
  cover: CoverStyle;
  isFavorite: boolean;
  onOpen: () => void;
  onToggleFavorite: () => void;
  onEdit: (e: React.MouseEvent) => void;
  onDelete: (e?: React.MouseEvent) => void;
}

function ProjectTile({ project, cover, isFavorite, onOpen, onToggleFavorite, onEdit, onDelete }: ProjectTileProps) {
  const headerStyle = resolveTileBackground(cover, project.id);
  const isImageCover = cover.kind === 'image';
  const counts = [
    { icon: <DatabaseOutlined />, n: project.datasets?.length ?? 0, label: 'data' },
    { icon: <CodeOutlined />, n: project.savedQueryIds?.length ?? 0, label: 'queries' },
    { icon: <BarChartOutlined />, n: project.visualizationIds?.length ?? 0, label: 'viz' },
    { icon: <DashboardOutlined />, n: project.dashboardIds?.length ?? 0, label: 'dash' },
  ];

  return (
    <div
      className="project-tile"
      onClick={onOpen}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          onOpen();
        }
      }}
    >
      <div
        className={`project-tile-header${isImageCover ? ' has-image' : ''}`}
        style={headerStyle}
      >
        {!isImageCover && <div className="project-tile-monogram">{monogramFor(project)}</div>}

        <button
          type="button"
          className={`project-tile-star${isFavorite ? ' is-on' : ''}`}
          onClick={(e) => {
            e.stopPropagation();
            onToggleFavorite();
          }}
          aria-label={isFavorite ? 'Unfavorite' : 'Favorite'}
        >
          {isFavorite ? <StarFilled /> : <StarOutlined />}
        </button>

        <div className="project-tile-actions" onClick={(e) => e.stopPropagation()}>
          <Tooltip title="Edit details">
            <button type="button" className="project-tile-action-btn" onClick={onEdit}>
              <EditOutlined />
            </button>
          </Tooltip>
          {!project.isSystem && (
            <Popconfirm
              title="Delete this project?"
              description="This action cannot be undone."
              onConfirm={(e) => {
                e?.stopPropagation();
                onDelete(e);
              }}
              onCancel={(e) => e?.stopPropagation()}
              okText="Delete"
              cancelText="Cancel"
              okButtonProps={{ danger: true }}
            >
              <Tooltip title="Delete">
                <button
                  type="button"
                  className="project-tile-action-btn is-danger"
                  onClick={(e) => e.stopPropagation()}
                >
                  <DeleteOutlined />
                </button>
              </Tooltip>
            </Popconfirm>
          )}
        </div>
      </div>

      <div className="project-tile-body">
        <div className="project-tile-title-row">
          <h3 className="project-tile-name" title={project.name}>{project.name}</h3>
          <span className="project-tile-visibility" aria-label={project.isPublic ? 'Public' : 'Private'}>
            {project.isPublic ? <GlobalOutlined /> : <LockOutlined />}
          </span>
        </div>

        {project.description ? (
          <p className="project-tile-description">{project.description}</p>
        ) : (
          <p className="project-tile-description is-muted">No description</p>
        )}

        {project.tags && project.tags.length > 0 && (
          <div className="project-tile-tags">
            {project.tags.slice(0, 3).map((t) => (
              <Tag key={t} bordered={false} className="project-tile-tag">{t}</Tag>
            ))}
            {project.tags.length > 3 && (
              <Tag bordered={false} className="project-tile-tag">+{project.tags.length - 3}</Tag>
            )}
          </div>
        )}

        <div className="project-tile-meta">
          {counts.filter((c) => c.n > 0).slice(0, 3).map((c) => (
            <span key={c.label} className="project-tile-meta-item">
              {c.icon}
              <span>{c.n}</span>
            </span>
          ))}
          <span className="project-tile-meta-item project-tile-meta-time">
            <ClockCircleOutlined />
            <span>{formatRelative(project.updatedAt)}</span>
          </span>
        </div>
      </div>
    </div>
  );
}

interface NewTileProps {
  onClick: () => void;
}

function NewProjectTile({ onClick }: NewTileProps) {
  return (
    <button type="button" className="project-tile project-tile-new" onClick={onClick}>
      <div className="project-tile-new-glyph">
        <PlusOutlined />
      </div>
      <div className="project-tile-new-label">New Project</div>
      <div className="project-tile-new-sublabel">Start a fresh workspace</div>
    </button>
  );
}

interface WelcomeProps {
  onCreate: () => void;
  onConnect: () => void;
  onTryQuery: () => void;
}

/** First-run welcome state — appears when the user has no projects yet. */
function ProjectsWelcome({ onCreate, onConnect, onTryQuery }: WelcomeProps) {
  return (
    <div className="page-projects-welcome">
      <div className="page-projects-welcome-hero">
        <FolderOutlined className="page-projects-welcome-glyph" />
        <h2>Welcome to Drill</h2>
        <p>
          Apache Drill lets you query files, databases, and APIs with the SQL
          you already know. Start in three steps:
        </p>
      </div>

      <div className="page-projects-welcome-steps">
        <button type="button" className="page-projects-welcome-step" onClick={onConnect}>
          <span className="page-projects-welcome-step-num">1</span>
          <div>
            <div className="page-projects-welcome-step-title">Connect a data source</div>
            <div className="page-projects-welcome-step-sub">
              Add a file system, JDBC database, REST API, Mongo, Splunk — anything Drill can read.
            </div>
          </div>
        </button>

        <button type="button" className="page-projects-welcome-step" onClick={onTryQuery}>
          <span className="page-projects-welcome-step-num">2</span>
          <div>
            <div className="page-projects-welcome-step-title">Run a query</div>
            <div className="page-projects-welcome-step-sub">
              Open SQL Lab, write SQL against your data source, and see results instantly.
            </div>
          </div>
        </button>

        <button type="button" className="page-projects-welcome-step is-primary" onClick={onCreate}>
          <span className="page-projects-welcome-step-num">3</span>
          <div>
            <div className="page-projects-welcome-step-title">Create a project</div>
            <div className="page-projects-welcome-step-sub">
              Group queries, charts, dashboards, and a wiki around a topic. You can always come back here.
            </div>
          </div>
        </button>
      </div>

      <p className="page-projects-welcome-hint">
        Tip: press <kbd>?</kbd> at any time to see all keyboard shortcuts, or <kbd>⌘K</kbd> to jump anywhere.
      </p>
    </div>
  );
}

export default function ProjectsPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [searchText, setSearchText] = useState('');
  const [favoritesOnly, setFavoritesOnly] = useState(false);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [editingProject, setEditingProject] = useState<Project | null>(null);
  const [createForm] = Form.useForm();
  const [editForm] = Form.useForm();
  const [importModalOpen, setImportModalOpen] = useState(false);

  // Cover (color/image) is persisted on the Project record server-side.
  // These local states track the picker's value during a Create/Edit modal session.
  const [createCover, setCreateCover] = useState<CoverStyle>({ kind: 'auto' });
  const [createName, setCreateName] = useState('');
  const [editCover, setEditCover] = useState<CoverStyle>({ kind: 'auto' });

  const { data: projects, isLoading, error } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
  });

  const { data: favorites } = useQuery({
    queryKey: ['project-favorites'],
    queryFn: getFavorites,
  });

  const favoriteSet = useMemo(() => new Set(favorites ?? []), [favorites]);

  const favoriteMutation = useMutation({
    mutationFn: toggleFavorite,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['project-favorites'] });
    },
  });

  const createMutation = useMutation({
    mutationFn: createProject,
    onSuccess: (newProject) => {
      message.success('Project created');
      queryClient.invalidateQueries({ queryKey: ['projects'] });
      setCreateModalOpen(false);
      setCreateCover({ kind: 'auto' });
      setCreateName('');
      createForm.resetFields();
      navigate(`/projects/${newProject.id}`);
    },
    onError: (err: Error) => {
      message.error(`Failed to create project: ${err.message}`);
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<Project> }) =>
      updateProject(id, data),
    onSuccess: () => {
      message.success('Project updated');
      queryClient.invalidateQueries({ queryKey: ['projects'] });
      setEditModalOpen(false);
      setEditingProject(null);
    },
    onError: (err: Error) => {
      message.error(`Failed to update project: ${err.message}`);
    },
  });

  const undoable = useUndoableDelete();

  const deleteMutation = useMutation({
    mutationFn: deleteProject,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['projects'] });
    },
    onError: (err: Error) => {
      message.error(`Failed to delete project: ${err.message}`);
    },
  });

  const deleteProjectWithUndo = (id: string) => {
    const proj = projects?.find((p) => p.id === id);
    if (!proj) {
      deleteMutation.mutate(id);
      return;
    }
    undoable.run({
      label: `Deleted project "${proj.name}"`,
      capture: () => proj,
      remove: () => deleteProject(id),
      restore: (snap) => restoreProject(snap.id),
      onDeleted: () => queryClient.invalidateQueries({ queryKey: ['projects'] }),
      onRestored: () => queryClient.invalidateQueries({ queryKey: ['projects'] }),
    });
  };

  const filteredProjects = useMemo(() => {
    if (!projects) {
      return [];
    }
    let result = projects;
    if (favoritesOnly) {
      result = result.filter((p) => favoriteSet.has(p.id));
    }
    if (searchText) {
      const lower = searchText.toLowerCase();
      result = result.filter(
        (p) =>
          p.name.toLowerCase().includes(lower) ||
          (p.description && p.description.toLowerCase().includes(lower)) ||
          p.tags?.some((t) => t.toLowerCase().includes(lower)),
      );
    }
    // Pinned/favorites first within results
    return [...result].sort((a, b) => {
      const aFav = favoriteSet.has(a.id) ? 1 : 0;
      const bFav = favoriteSet.has(b.id) ? 1 : 0;
      if (aFav !== bFav) {
        return bFav - aFav;
      }
      return b.updatedAt - a.updatedAt;
    });
  }, [projects, searchText, favoritesOnly, favoriteSet]);

  const handleCreate = async () => {
    try {
      const values = await createForm.validateFields();
      const coverFields = coverToProjectFields(createCover, TINT_PALETTE);
      createMutation.mutate({
        name: values.name,
        description: values.description,
        tags: values.tags || [],
        isPublic: values.isPublic || false,
        ...coverFields,
      });
    } catch {
      // Form validation failed
    }
  };

  const handleEdit = (project: Project, e: React.MouseEvent) => {
    e.stopPropagation();
    setEditingProject(project);
    setEditCover(coverFromProject(project, TINT_PALETTE));
    editForm.setFieldsValue({
      name: project.name,
      description: project.description,
      tags: project.tags,
      isPublic: project.isPublic,
    });
    setEditModalOpen(true);
  };

  const handleSaveEdit = async () => {
    if (!editingProject) {
      return;
    }
    try {
      const values = await editForm.validateFields();
      const coverFields = coverToProjectFields(editCover, TINT_PALETTE);
      updateMutation.mutate({
        id: editingProject.id,
        data: {
          name: values.name,
          description: values.description,
          tags: values.tags || [],
          isPublic: values.isPublic,
          ...coverFields,
        },
      });
    } catch {
      // Form validation failed
    }
  };

  // Register toolbar actions in the unified shell toolbar
  const toolbarActions = useMemo(
    () => (
      <Space size={4}>
        <Tooltip title="Import project">
          <Button
            type="text"
            size="small"
            icon={<UploadOutlined />}
            onClick={() => setImportModalOpen(true)}
          />
        </Tooltip>
        <Button
          type="primary"
          size="small"
          icon={<PlusOutlined />}
          onClick={() => setCreateModalOpen(true)}
        >
          New Project
        </Button>
      </Space>
    ),
    [],
  );
  usePageChrome({ toolbarActions });

  const totalCount = projects?.length ?? 0;
  const showingCount = filteredProjects.length;

  return (
    <div className="page-projects">
      {undoable.contextHolder}
      <header className="page-projects-header">
        <div>
          <h1 className="page-projects-title">Projects</h1>
          <p className="page-projects-subtitle">
            {totalCount === 0
              ? 'No projects yet'
              : showingCount === totalCount
                ? `${totalCount} ${totalCount === 1 ? 'project' : 'projects'}`
                : `Showing ${showingCount} of ${totalCount}`}
          </p>
        </div>
      </header>

      <div className="page-projects-toolbar">
        <Input
          placeholder="Search projects, tags, descriptions…"
          prefix={<SearchOutlined style={{ color: 'var(--color-text-tertiary)' }} />}
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          allowClear
          className="page-projects-search"
        />
        <div className="page-projects-chips">
          <button
            type="button"
            className={`page-projects-chip${!favoritesOnly ? ' is-active' : ''}`}
            onClick={() => setFavoritesOnly(false)}
          >
            All
          </button>
          <button
            type="button"
            className={`page-projects-chip${favoritesOnly ? ' is-active' : ''}`}
            onClick={() => setFavoritesOnly(true)}
          >
            <StarFilled style={{ fontSize: 11, marginRight: 4 }} />
            Favorites
          </button>
        </div>
      </div>

      {error ? (
        <div className="page-projects-empty">
          <FolderOutlined className="page-projects-empty-glyph" />
          <h2>Couldn't load projects</h2>
          <p>{(error as Error).message}</p>
          <Button onClick={() => queryClient.invalidateQueries({ queryKey: ['projects'] })}>
            Try Again
          </Button>
        </div>
      ) : isLoading ? (
        <div className="page-projects-loading">
          <Spin size="large" />
        </div>
      ) : totalCount === 0 ? (
        <ProjectsWelcome
          onCreate={() => setCreateModalOpen(true)}
          onConnect={() => navigate('/datasources')}
          onTryQuery={() => navigate('/query')}
        />
      ) : (
        <div className="page-projects-grid">
          <NewProjectTile onClick={() => setCreateModalOpen(true)} />
          {filteredProjects.map((project) => (
            <ProjectTile
              key={project.id}
              project={project}
              cover={coverFromProject(project, TINT_PALETTE)}
              isFavorite={favoriteSet.has(project.id)}
              onOpen={() => navigate(`/projects/${project.id}/query`)}
              onToggleFavorite={() => favoriteMutation.mutate(project.id)}
              onEdit={(e) => handleEdit(project, e)}
              onDelete={() => deleteProjectWithUndo(project.id)}
            />
          ))}

          {filteredProjects.length === 0 && totalCount > 0 && (
            <div className="page-projects-empty page-projects-empty-inline">
              <SearchOutlined className="page-projects-empty-glyph" />
              <h2>No matches</h2>
              <p>
                {favoritesOnly
                  ? "You haven't favorited any matching projects."
                  : 'Try a different search term or clear the filter.'}
              </p>
              {(searchText || favoritesOnly) && (
                <Button
                  onClick={() => {
                    setSearchText('');
                    setFavoritesOnly(false);
                  }}
                >
                  Clear filters
                </Button>
              )}
            </div>
          )}
        </div>
      )}

      {/* Create Project Modal */}
      <Modal
        title="New Project"
        open={createModalOpen}
        onOk={handleCreate}
        onCancel={() => {
          setCreateModalOpen(false);
          setCreateCover({ kind: 'auto' });
          setCreateName('');
          createForm.resetFields();
        }}
        confirmLoading={createMutation.isPending}
        okText="Create"
      >
        <Form
          form={createForm}
          layout="vertical"
          onValuesChange={(changed) => {
            if (typeof changed.name === 'string') {
              setCreateName(changed.name);
            }
          }}
        >
          <Form.Item
            name="name"
            label="Name"
            rules={[{ required: true, message: 'Please enter a project name' }]}
          >
            <Input placeholder="Customer Analytics" />
          </Form.Item>
          <Form.Item name="description" label="Description">
            <TextArea rows={3} placeholder="What is this project about?" />
          </Form.Item>
          <Form.Item label="Appearance">
            <ProjectAppearanceField
              value={createCover}
              onChange={setCreateCover}
              projectName={createName}
              projectIdSeed={createName || 'new-project'}
            />
          </Form.Item>
          <Form.Item name="tags" label="Tags">
            <Select mode="tags" placeholder="Add tags…" style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="isPublic" label="Public" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Modal>

      {/* Edit Project Modal */}
      <Modal
        title="Edit Project"
        open={editModalOpen}
        onOk={handleSaveEdit}
        onCancel={() => {
          setEditModalOpen(false);
          setEditingProject(null);
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
          {editingProject && (
            <Form.Item label="Appearance">
              <ProjectAppearanceField
                value={editCover}
                onChange={setEditCover}
                projectName={editingProject.name}
                projectIdSeed={editingProject.id}
              />
            </Form.Item>
          )}
          <Form.Item name="tags" label="Tags">
            <Select mode="tags" placeholder="Add tags…" style={{ width: '100%' }} />
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
      </Modal>

      <ExportImportModal
        open={importModalOpen}
        mode="import"
        onClose={() => setImportModalOpen(false)}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['projects'] });
          setImportModalOpen(false);
        }}
      />
    </div>
  );
}
