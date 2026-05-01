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
import { Button, Modal, message } from 'antd';
import {
  DeleteOutlined,
  ReloadOutlined,
  UndoOutlined,
  FolderOpenOutlined,
  DatabaseOutlined,
  BarChartOutlined,
  AppstoreOutlined,
} from '@ant-design/icons';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  getTrashedSavedQueries,
  restoreSavedQuery,
  purgeSavedQuery,
} from '../../api/savedQueries';
import {
  getTrashedVisualizations,
  restoreVisualization,
  purgeVisualization,
} from '../../api/visualizations';
import {
  getTrashedDashboards,
  restoreDashboard,
  purgeDashboard,
} from '../../api/dashboards';
import {
  getTrashedProjects,
  restoreProject,
  purgeProject,
} from '../../api/projects';

type Category = 'projects' | 'savedQueries' | 'visualizations' | 'dashboards';

interface TrashItem {
  id: string;
  name: string;
  deletedAt: number;
  category: Category;
}

const CATEGORY_LABEL: Record<Category, string> = {
  projects: 'Projects',
  savedQueries: 'Saved Queries',
  visualizations: 'Visualizations',
  dashboards: 'Dashboards',
};

const CATEGORY_ICON: Record<Category, React.ReactNode> = {
  projects: <FolderOpenOutlined />,
  savedQueries: <DatabaseOutlined />,
  visualizations: <BarChartOutlined />,
  dashboards: <AppstoreOutlined />,
};

function formatRelative(ts: number): string {
  const diff = Date.now() - ts;
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
  return new Date(ts).toLocaleDateString(undefined, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
}

export default function TrashBody() {
  const queryClient = useQueryClient();
  const [busyId, setBusyId] = useState<string | null>(null);

  const projects = useQuery({
    queryKey: ['trash', 'projects'],
    queryFn: getTrashedProjects,
    staleTime: 30_000,
  });
  const savedQueries = useQuery({
    queryKey: ['trash', 'savedQueries'],
    queryFn: getTrashedSavedQueries,
    staleTime: 30_000,
  });
  const visualizations = useQuery({
    queryKey: ['trash', 'visualizations'],
    queryFn: getTrashedVisualizations,
    staleTime: 30_000,
  });
  const dashboards = useQuery({
    queryKey: ['trash', 'dashboards'],
    queryFn: getTrashedDashboards,
    staleTime: 30_000,
  });

  const isLoading =
    projects.isLoading ||
    savedQueries.isLoading ||
    visualizations.isLoading ||
    dashboards.isLoading;

  const items: TrashItem[] = useMemo(() => {
    const all: TrashItem[] = [];
    (projects.data ?? []).forEach((p) =>
      all.push({ id: p.id, name: p.name, deletedAt: p.deletedAt ?? 0, category: 'projects' }),
    );
    (savedQueries.data ?? []).forEach((q) =>
      all.push({ id: q.id, name: q.name, deletedAt: q.deletedAt ?? 0, category: 'savedQueries' }),
    );
    (visualizations.data ?? []).forEach((v) =>
      all.push({ id: v.id, name: v.name, deletedAt: v.deletedAt ?? 0, category: 'visualizations' }),
    );
    (dashboards.data ?? []).forEach((d) =>
      all.push({ id: d.id, name: d.name, deletedAt: d.deletedAt ?? 0, category: 'dashboards' }),
    );
    return all.sort((a, b) => b.deletedAt - a.deletedAt);
  }, [projects.data, savedQueries.data, visualizations.data, dashboards.data]);

  const refreshCategory = (cat: Category) => {
    queryClient.invalidateQueries({ queryKey: ['trash', cat] });
    queryClient.invalidateQueries({ queryKey: [cat] });
  };

  const refreshAll = () => {
    queryClient.invalidateQueries({ queryKey: ['trash'] });
    queryClient.invalidateQueries({ queryKey: ['projects'] });
    queryClient.invalidateQueries({ queryKey: ['savedQueries'] });
    queryClient.invalidateQueries({ queryKey: ['visualizations'] });
    queryClient.invalidateQueries({ queryKey: ['dashboards'] });
  };

  const handleRestore = async (item: TrashItem) => {
    setBusyId(item.id);
    try {
      switch (item.category) {
        case 'projects':
          await restoreProject(item.id);
          break;
        case 'savedQueries':
          await restoreSavedQuery(item.id);
          break;
        case 'visualizations':
          await restoreVisualization(item.id);
          break;
        case 'dashboards':
          await restoreDashboard(item.id);
          break;
      }
      message.success(`Restored "${item.name}"`);
      refreshCategory(item.category);
    } catch (err) {
      message.error(`Couldn't restore: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setBusyId(null);
    }
  };

  const handlePurge = (item: TrashItem) => {
    Modal.confirm({
      title: `Permanently delete "${item.name}"?`,
      content: 'This cannot be undone.',
      okText: 'Delete Forever',
      okButtonProps: { danger: true },
      cancelText: 'Cancel',
      onOk: async () => {
        setBusyId(item.id);
        try {
          switch (item.category) {
            case 'projects':
              await purgeProject(item.id);
              break;
            case 'savedQueries':
              await purgeSavedQuery(item.id);
              break;
            case 'visualizations':
              await purgeVisualization(item.id);
              break;
            case 'dashboards':
              await purgeDashboard(item.id);
              break;
          }
          message.success('Permanently deleted');
          refreshCategory(item.category);
        } catch (err) {
          message.error(`Couldn't purge: ${err instanceof Error ? err.message : String(err)}`);
        } finally {
          setBusyId(null);
        }
      },
    });
  };

  return (
    <div className="settings-body trash-body">
      <div className="trash-header">
        <p className="trash-explainer">
          Items deleted from Projects, Saved Queries, Visualizations, and Dashboards
          land here. Restore returns them with their original ID — references from
          other items stay intact.
        </p>
        <Button size="small" icon={<ReloadOutlined />} onClick={refreshAll}>
          Refresh
        </Button>
      </div>

      {isLoading ? (
        <div className="trash-empty">
          <p>Loading…</p>
        </div>
      ) : items.length === 0 ? (
        <div className="trash-empty">
          <DeleteOutlined className="trash-empty-glyph" />
          <h3>Trash is empty</h3>
          <p>Deleted items will appear here.</p>
        </div>
      ) : (
        <ul className="trash-list">
          {items.map((item) => (
            <li key={`${item.category}-${item.id}`} className="trash-entry">
              <span className={`trash-entry-icon trash-icon-${item.category}`}>
                {CATEGORY_ICON[item.category]}
              </span>
              <div className="trash-entry-info">
                <span className="trash-entry-name" title={item.name}>{item.name}</span>
                <span className="trash-entry-meta">
                  {CATEGORY_LABEL[item.category]} · deleted {formatRelative(item.deletedAt)}
                </span>
              </div>
              <div className="trash-entry-actions">
                <Button
                  size="small"
                  icon={<UndoOutlined />}
                  onClick={() => handleRestore(item)}
                  loading={busyId === item.id}
                >
                  Restore
                </Button>
                <Button
                  size="small"
                  danger
                  icon={<DeleteOutlined />}
                  onClick={() => handlePurge(item)}
                  disabled={busyId === item.id}
                >
                  Delete Forever
                </Button>
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
