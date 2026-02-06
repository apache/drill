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
import { useState, useCallback, useMemo, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Responsive, WidthProvider } from 'react-grid-layout';
import {
  Button,
  Space,
  Typography,
  Spin,
  Alert,
  Select,
  Modal,
  List,
  Empty,
  message,
  Tooltip,
} from 'antd';
import {
  EditOutlined,
  EyeOutlined,
  SaveOutlined,
  PlusOutlined,
  ArrowLeftOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import { getDashboard, updateDashboard } from '../api/dashboards';
import { getVisualizations } from '../api/visualizations';
import { DashboardPanelCard } from '../components/dashboard';
import type { DashboardPanel } from '../types';

import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';

const ResponsiveGridLayout = WidthProvider(Responsive);
const { Title, Text } = Typography;

const REFRESH_OPTIONS = [
  { label: 'Off', value: 0 },
  { label: '10s', value: 10 },
  { label: '30s', value: 30 },
  { label: '1m', value: 60 },
  { label: '5m', value: 300 },
];

export default function DashboardViewPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [editMode, setEditMode] = useState(false);
  const [panels, setPanels] = useState<DashboardPanel[]>([]);
  const [panelsInitialized, setPanelsInitialized] = useState(false);
  const [refreshInterval, setRefreshInterval] = useState(0);
  const [addPanelVisible, setAddPanelVisible] = useState(false);

  // Fetch dashboard
  const { data: dashboard, isLoading, error } = useQuery({
    queryKey: ['dashboard', id],
    queryFn: () => getDashboard(id!),
    enabled: !!id,
  });

  // Initialize panels from fetched dashboard data
  useEffect(() => {
    if (dashboard && !panelsInitialized) {
      setPanels(dashboard.panels || []);
      setRefreshInterval(dashboard.refreshInterval || 0);
      setPanelsInitialized(true);
    }
  }, [dashboard, panelsInitialized]);

  // Fetch available visualizations for "Add panel" modal
  const { data: visualizations } = useQuery({
    queryKey: ['visualizations'],
    queryFn: getVisualizations,
    enabled: addPanelVisible,
  });

  // Save dashboard mutation
  const saveMutation = useMutation({
    mutationFn: () => updateDashboard(id!, { panels, refreshInterval }),
    onSuccess: () => {
      message.success('Dashboard saved');
      queryClient.invalidateQueries({ queryKey: ['dashboard', id] });
    },
    onError: () => {
      message.error('Failed to save dashboard');
    },
  });

  // Convert panels to react-grid-layout format
  const layout = useMemo(() => {
    return panels.map((panel) => ({
      i: panel.id,
      x: panel.x,
      y: panel.y,
      w: panel.width,
      h: panel.height,
      minW: 2,
      minH: 2,
      static: !editMode,
    }));
  }, [panels, editMode]);

  // Handle layout change from drag/resize
  const handleLayoutChange = useCallback((newLayout: Array<{ i: string; x: number; y: number; w: number; h: number }>) => {
    if (!editMode) {
      return;
    }
    setPanels((prev) =>
      prev.map((panel) => {
        const layoutItem = newLayout.find((l) => l.i === panel.id);
        if (layoutItem) {
          return {
            ...panel,
            x: layoutItem.x,
            y: layoutItem.y,
            width: layoutItem.w,
            height: layoutItem.h,
          };
        }
        return panel;
      })
    );
  }, [editMode]);

  // Add a panel from a visualization
  const handleAddPanel = useCallback((visualizationId: string) => {
    const newPanel: DashboardPanel = {
      id: crypto.randomUUID(),
      visualizationId,
      x: 0,
      y: Infinity, // puts it at the bottom
      width: 6,
      height: 3,
    };
    setPanels((prev) => [...prev, newPanel]);
    setAddPanelVisible(false);
    message.success('Panel added');
  }, []);

  // Remove a panel
  const handleRemovePanel = useCallback((panelId: string) => {
    setPanels((prev) => prev.filter((p) => p.id !== panelId));
  }, []);

  // Toggle edit mode
  const handleToggleEdit = useCallback(() => {
    if (editMode) {
      // Exiting edit mode without saving - reset panels
      if (dashboard) {
        setPanels(dashboard.panels || []);
        setRefreshInterval(dashboard.refreshInterval || 0);
      }
    }
    setEditMode(!editMode);
  }, [editMode, dashboard]);

  // Save and exit edit mode
  const handleSave = useCallback(() => {
    saveMutation.mutate();
    setEditMode(false);
  }, [saveMutation]);

  if (isLoading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
        <Spin size="large" tip="Loading dashboard..." />
      </div>
    );
  }

  if (error || !dashboard) {
    return (
      <div style={{ padding: 24 }}>
        <Alert
          message="Error"
          description={error instanceof Error ? error.message : 'Dashboard not found'}
          type="error"
          showIcon
          action={
            <Button onClick={() => navigate('/dashboards')}>Back to Dashboards</Button>
          }
        />
      </div>
    );
  }

  return (
    <div className="dashboard-view">
      {/* Toolbar */}
      <div className="dashboard-toolbar">
        <Space>
          <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/dashboards')}>
            Back
          </Button>
          <div>
            <Title level={4} style={{ margin: 0 }}>{dashboard.name}</Title>
            {dashboard.description && (
              <Text type="secondary">{dashboard.description}</Text>
            )}
          </div>
        </Space>

        <Space>
          <Select
            value={refreshInterval}
            onChange={setRefreshInterval}
            options={REFRESH_OPTIONS}
            style={{ width: 100 }}
            prefix={<ReloadOutlined />}
            disabled={!editMode}
          />

          {editMode ? (
            <>
              <Button icon={<PlusOutlined />} onClick={() => setAddPanelVisible(true)}>
                Add Panel
              </Button>
              <Button
                icon={<SaveOutlined />}
                type="primary"
                onClick={handleSave}
                loading={saveMutation.isPending}
              >
                Save
              </Button>
              <Tooltip title="Cancel editing">
                <Button icon={<EyeOutlined />} onClick={handleToggleEdit}>
                  Cancel
                </Button>
              </Tooltip>
            </>
          ) : (
            <Button icon={<EditOutlined />} onClick={handleToggleEdit}>
              Edit
            </Button>
          )}
        </Space>
      </div>

      {/* Dashboard Grid */}
      {panels.length === 0 ? (
        <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 'calc(100% - 64px)' }}>
          <Empty
            description={
              <span>
                No panels yet.
                {editMode ? ' Click "Add Panel" to get started.' : ' Click "Edit" to add panels.'}
              </span>
            }
          >
            {!editMode && (
              <Button type="primary" icon={<EditOutlined />} onClick={() => setEditMode(true)}>
                Start Editing
              </Button>
            )}
          </Empty>
        </div>
      ) : (
        <div className="dashboard-grid-container">
          <ResponsiveGridLayout
            className={`dashboard-grid ${editMode ? 'edit-mode' : ''}`}
            layouts={{ lg: layout }}
            breakpoints={{ lg: 1200, md: 996, sm: 768, xs: 480 }}
            cols={{ lg: 12, md: 10, sm: 6, xs: 4 }}
            rowHeight={120}
            isDraggable={editMode}
            isResizable={editMode}
            onLayoutChange={handleLayoutChange}
            draggableHandle=".drag-handle"
            margin={[12, 12]}
          >
            {panels.map((panel) => (
              <div key={panel.id}>
                <DashboardPanelCard
                  panel={panel}
                  editMode={editMode}
                  refreshInterval={editMode ? 0 : refreshInterval}
                  onRemove={handleRemovePanel}
                />
              </div>
            ))}
          </ResponsiveGridLayout>
        </div>
      )}

      {/* Add Panel Modal */}
      <Modal
        title="Add Visualization Panel"
        open={addPanelVisible}
        onCancel={() => setAddPanelVisible(false)}
        footer={null}
        width={600}
      >
        {visualizations && visualizations.length > 0 ? (
          <List
            dataSource={visualizations}
            renderItem={(viz) => {
              const alreadyAdded = panels.some((p) => p.visualizationId === viz.id);
              return (
                <List.Item
                  actions={[
                    <Button
                      key="add"
                      type="primary"
                      size="small"
                      disabled={alreadyAdded}
                      onClick={() => handleAddPanel(viz.id)}
                    >
                      {alreadyAdded ? 'Added' : 'Add'}
                    </Button>,
                  ]}
                >
                  <List.Item.Meta
                    title={viz.name}
                    description={`${viz.chartType} chart${viz.description ? ` - ${viz.description}` : ''}`}
                  />
                </List.Item>
              );
            }}
          />
        ) : (
          <Empty description="No visualizations available. Create one first from the Visualizations page." />
        )}
      </Modal>
    </div>
  );
}
