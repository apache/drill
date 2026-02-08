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
import { useState, useCallback, useMemo, useEffect, useRef } from 'react';
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
  Tabs,
  Input,
  Upload,
} from 'antd';
import {
  EditOutlined,
  EyeOutlined,
  SaveOutlined,
  PlusOutlined,
  ArrowLeftOutlined,
  ReloadOutlined,
  FilePdfOutlined,
  ShareAltOutlined,
  BarChartOutlined,
  FileMarkdownOutlined,
  PictureOutlined,
  FontSizeOutlined,
  CloseOutlined,
  SettingOutlined,
  StarOutlined,
  StarFilled,
  InboxOutlined,
} from '@ant-design/icons';
import html2canvas from 'html2canvas';
import { jsPDF } from 'jspdf';
import { getDashboard, updateDashboard, getFavorites, toggleFavorite, uploadImage } from '../api/dashboards';
import { getVisualizations } from '../api/visualizations';
import { DashboardPanelCard, DashboardSettingsDrawer, DEFAULT_THEME } from '../components/dashboard';
import type { DashboardPanel, DashboardTab, DashboardTheme } from '../types';

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

type AddPanelTab = 'visualization' | 'markdown' | 'image' | 'title';

export default function DashboardViewPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const gridRef = useRef<HTMLDivElement>(null);

  const [editMode, setEditMode] = useState(false);
  const [panels, setPanels] = useState<DashboardPanel[]>([]);
  const [tabs, setTabs] = useState<DashboardTab[]>([]);
  const [theme, setTheme] = useState<DashboardTheme>(DEFAULT_THEME);
  const [panelsInitialized, setPanelsInitialized] = useState(false);
  const [refreshInterval, setRefreshInterval] = useState(0);
  const [addPanelVisible, setAddPanelVisible] = useState(false);
  const [addPanelTab, setAddPanelTab] = useState<AddPanelTab>('visualization');
  const [activeTabId, setActiveTabId] = useState<string | null>(null);
  const [renamingTabId, setRenamingTabId] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState('');
  const [exportingPdf, setExportingPdf] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);

  // New panel content inputs
  const [newMarkdownContent, setNewMarkdownContent] = useState('## Heading\n\nYour content here...');
  const [newImageUrl, setNewImageUrl] = useState('');
  const [newImageAlt, setNewImageAlt] = useState('');
  const [newTitleText, setNewTitleText] = useState('');
  const [newTitleSubtitle, setNewTitleSubtitle] = useState('');
  const [imageUploading, setImageUploading] = useState(false);

  // Fetch dashboard
  const { data: dashboard, isLoading, error } = useQuery({
    queryKey: ['dashboard', id],
    queryFn: () => getDashboard(id!),
    enabled: !!id,
  });

  // Fetch favorites
  const { data: favorites } = useQuery({
    queryKey: ['dashboard-favorites'],
    queryFn: getFavorites,
  });

  const isFavorited = useMemo(() => {
    return id ? (favorites || []).includes(id) : false;
  }, [favorites, id]);

  // Toggle favorite mutation
  const favoriteMutation = useMutation({
    mutationFn: () => toggleFavorite(id!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dashboard-favorites'] });
    },
  });

  // Initialize panels from fetched dashboard data
  useEffect(() => {
    if (dashboard && !panelsInitialized) {
      setPanels(dashboard.panels || []);
      setTabs(dashboard.tabs || []);
      setTheme(dashboard.theme || DEFAULT_THEME);
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
    mutationFn: () => updateDashboard(id!, { panels, tabs, theme, refreshInterval }),
    onSuccess: () => {
      message.success('Dashboard saved');
      queryClient.invalidateQueries({ queryKey: ['dashboard', id] });
    },
    onError: () => {
      message.error('Failed to save dashboard');
    },
  });

  // Filter panels by active tab
  const visiblePanels = useMemo(() => {
    if (!activeTabId) {
      if (tabs.length === 0) {
        return panels;
      }
      return panels.filter((p) => !p.tabId);
    }
    return panels.filter((p) => p.tabId === activeTabId);
  }, [panels, activeTabId, tabs]);

  // Convert panels to react-grid-layout format
  const layout = useMemo(() => {
    return visiblePanels.map((panel) => ({
      i: panel.id,
      x: panel.x,
      y: panel.y,
      w: panel.width,
      h: panel.height,
      minW: 2,
      minH: 2,
      static: !editMode,
    }));
  }, [visiblePanels, editMode]);

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

  // Add a visualization panel
  const handleAddVizPanel = useCallback((visualizationId: string) => {
    const newPanel: DashboardPanel = {
      id: crypto.randomUUID(),
      type: 'visualization',
      visualizationId,
      x: 0,
      y: Infinity,
      width: 6,
      height: 3,
      tabId: activeTabId || undefined,
    };
    setPanels((prev) => [...prev, newPanel]);
    setAddPanelVisible(false);
    message.success('Visualization panel added');
  }, [activeTabId]);

  // Add a content panel (markdown, image, title)
  const handleAddContentPanel = useCallback((type: 'markdown' | 'image' | 'title') => {
    let content = '';
    let config: Record<string, string> | undefined;

    if (type === 'markdown') {
      content = newMarkdownContent;
    } else if (type === 'image') {
      content = newImageUrl;
      if (newImageAlt) {
        config = { imageAlt: newImageAlt };
      }
    } else if (type === 'title') {
      content = newTitleText;
      if (newTitleSubtitle) {
        config = { subtitle: newTitleSubtitle, textAlign: 'center' };
      } else {
        config = { textAlign: 'center' };
      }
    }

    const newPanel: DashboardPanel = {
      id: crypto.randomUUID(),
      type,
      content,
      config,
      x: 0,
      y: Infinity,
      width: type === 'title' ? 12 : 6,
      height: type === 'title' ? 2 : 3,
      tabId: activeTabId || undefined,
    };
    setPanels((prev) => [...prev, newPanel]);
    setAddPanelVisible(false);
    setNewMarkdownContent('## Heading\n\nYour content here...');
    setNewImageUrl('');
    setNewImageAlt('');
    setNewTitleText('');
    setNewTitleSubtitle('');
    message.success(`${type.charAt(0).toUpperCase() + type.slice(1)} panel added`);
  }, [activeTabId, newMarkdownContent, newImageUrl, newImageAlt, newTitleText, newTitleSubtitle]);

  // Remove a panel
  const handleRemovePanel = useCallback((panelId: string) => {
    setPanels((prev) => prev.filter((p) => p.id !== panelId));
  }, []);

  // Update a panel's content/config inline
  const handlePanelChange = useCallback((updatedPanel: DashboardPanel) => {
    setPanels((prev) => prev.map((p) => p.id === updatedPanel.id ? updatedPanel : p));
  }, []);

  // Toggle edit mode
  const handleToggleEdit = useCallback(() => {
    if (editMode) {
      // Exiting edit mode without saving - reset
      if (dashboard) {
        setPanels(dashboard.panels || []);
        setTabs(dashboard.tabs || []);
        setTheme(dashboard.theme || DEFAULT_THEME);
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

  // Tab management
  const handleAddTab = useCallback(() => {
    const newTab: DashboardTab = {
      id: crypto.randomUUID(),
      name: `Tab ${tabs.length + 1}`,
      order: tabs.length,
    };
    setTabs((prev) => [...prev, newTab]);
  }, [tabs.length]);

  const handleRenameTab = useCallback((tabId: string, newName: string) => {
    setTabs((prev) => prev.map((t) => t.id === tabId ? { ...t, name: newName } : t));
    setRenamingTabId(null);
  }, []);

  const handleDeleteTab = useCallback((tabId: string) => {
    setTabs((prev) => prev.filter((t) => t.id !== tabId));
    setPanels((prev) => prev.map((p) => p.tabId === tabId ? { ...p, tabId: undefined } : p));
    if (activeTabId === tabId) {
      setActiveTabId(null);
    }
  }, [activeTabId]);

  // PDF export
  const handleExportPdf = useCallback(async () => {
    if (!gridRef.current) {
      return;
    }
    setExportingPdf(true);
    try {
      const canvas = await html2canvas(gridRef.current, {
        scale: 2,
        useCORS: true,
        logging: false,
      });
      const imgData = canvas.toDataURL('image/png');
      const pdf = new jsPDF({
        orientation: 'landscape',
        unit: 'mm',
        format: 'a4',
      });
      const pageWidth = pdf.internal.pageSize.getWidth();
      const pageHeight = pdf.internal.pageSize.getHeight();
      const imgWidth = pageWidth - 20;
      const imgHeight = (canvas.height * imgWidth) / canvas.width;

      pdf.setFontSize(16);
      pdf.text(dashboard?.name || 'Dashboard', 10, 12);
      pdf.setFontSize(10);
      pdf.setTextColor(128);
      pdf.text(`Exported ${new Date().toLocaleString()}`, 10, 18);

      const startY = 22;
      const availableHeight = pageHeight - startY - 5;

      if (imgHeight <= availableHeight) {
        pdf.addImage(imgData, 'PNG', 10, startY, imgWidth, imgHeight);
      } else {
        const scaledHeight = availableHeight;
        const scaledWidth = (canvas.width * scaledHeight) / canvas.height;
        pdf.addImage(imgData, 'PNG', 10, startY, scaledWidth, scaledHeight);
      }

      pdf.save(`${dashboard?.name || 'dashboard'}.pdf`);
      message.success('PDF exported');
    } catch {
      message.error('Failed to export PDF');
    } finally {
      setExportingPdf(false);
    }
  }, [dashboard?.name]);

  // Share link
  const handleCopyShareLink = useCallback(() => {
    const url = `${window.location.origin}/sqllab/dashboards/${id}`;
    navigator.clipboard.writeText(url).then(
      () => message.success('Link copied to clipboard!'),
      () => message.error('Failed to copy link')
    );
  }, [id]);

  // Theme CSS custom properties
  const themeStyle = useMemo((): React.CSSProperties => ({
    '--db-bg': theme.backgroundColor,
    '--db-font': theme.fontColor,
    '--db-panel-bg': theme.panelBackground,
    '--db-panel-border': theme.panelBorderColor,
    '--db-panel-radius': theme.panelBorderRadius,
    '--db-accent': theme.accentColor,
    '--db-header': theme.headerColor,
    fontFamily: theme.fontFamily,
    color: theme.fontColor,
    backgroundColor: theme.backgroundColor,
  } as React.CSSProperties), [theme]);

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

  // Build tab items for Ant Tabs
  const tabItems = tabs.length > 0 ? [
    {
      key: '__default__',
      label: 'All',
    },
    ...tabs
      .sort((a, b) => a.order - b.order)
      .map((tab) => ({
        key: tab.id,
        label: editMode && renamingTabId === tab.id ? (
          <Input
            className="tab-rename-input"
            value={renameValue}
            onChange={(e) => setRenameValue(e.target.value)}
            onBlur={() => handleRenameTab(tab.id, renameValue)}
            onPressEnter={() => handleRenameTab(tab.id, renameValue)}
            autoFocus
            size="small"
            onClick={(e) => e.stopPropagation()}
          />
        ) : (
          <span
            onDoubleClick={(e) => {
              if (editMode) {
                e.stopPropagation();
                setRenamingTabId(tab.id);
                setRenameValue(tab.name);
              }
            }}
          >
            {tab.name}
            {editMode && (
              <CloseOutlined
                style={{ marginLeft: 8, fontSize: 10, color: '#999' }}
                onClick={(e) => {
                  e.stopPropagation();
                  handleDeleteTab(tab.id);
                }}
              />
            )}
          </span>
        ),
      })),
  ] : [];

  return (
    <div className="dashboard-view">
      {/* PDF Export Overlay */}
      {exportingPdf && (
        <div className="pdf-export-overlay">
          <Spin size="large" tip="Generating PDF..." />
        </div>
      )}

      {/* Toolbar */}
      <div className="dashboard-toolbar">
        <Space>
          <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/dashboards')}>
            Back
          </Button>
          <div>
            <Space align="center">
              <Title level={4} style={{ margin: 0 }}>{dashboard.name}</Title>
              <Tooltip title={isFavorited ? 'Remove from favorites' : 'Add to favorites'}>
                <Button
                  type="text"
                  size="small"
                  icon={isFavorited
                    ? <StarFilled style={{ color: '#faad14', fontSize: 18 }} />
                    : <StarOutlined style={{ fontSize: 18 }} />
                  }
                  onClick={() => favoriteMutation.mutate()}
                  loading={favoriteMutation.isPending}
                />
              </Tooltip>
            </Space>
            {dashboard.description && (
              <Text type="secondary">{dashboard.description}</Text>
            )}
          </div>
        </Space>

        <Space>
          {/* PDF Export */}
          <Tooltip title="Export as PDF">
            <Button
              icon={<FilePdfOutlined />}
              onClick={handleExportPdf}
              disabled={exportingPdf || panels.length === 0}
            >
              PDF
            </Button>
          </Tooltip>

          {/* Share Link */}
          {dashboard.isPublic && (
            <Tooltip title="Copy shareable link">
              <Button
                icon={<ShareAltOutlined />}
                onClick={handleCopyShareLink}
              >
                Share
              </Button>
            </Tooltip>
          )}

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
              <Tooltip title="Dashboard settings">
                <Button icon={<SettingOutlined />} onClick={() => setSettingsOpen(true)} />
              </Tooltip>
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

      {/* Dashboard Tabs */}
      {(tabs.length > 0 || editMode) && (
        <div className="dashboard-tabs">
          <Tabs
            activeKey={activeTabId || '__default__'}
            onChange={(key) => setActiveTabId(key === '__default__' ? null : key)}
            items={tabItems}
            tabBarExtraContent={editMode ? (
              <Button
                type="text"
                size="small"
                icon={<PlusOutlined />}
                onClick={handleAddTab}
              >
                Add Tab
              </Button>
            ) : undefined}
          />
        </div>
      )}

      {/* Dashboard Grid */}
      {visiblePanels.length === 0 ? (
        <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 'calc(100% - 64px)' }}>
          <Empty
            description={
              <span>
                No panels{activeTabId ? ' in this tab' : ''}.
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
        <div className="dashboard-grid-container" ref={gridRef} style={themeStyle}>
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
            {visiblePanels.map((panel) => (
              <div key={panel.id}>
                <DashboardPanelCard
                  panel={panel}
                  editMode={editMode}
                  refreshInterval={editMode ? 0 : refreshInterval}
                  dashboardUpdatedAt={dashboard.updatedAt}
                  onRemove={handleRemovePanel}
                  onPanelChange={handlePanelChange}
                />
              </div>
            ))}
          </ResponsiveGridLayout>
        </div>
      )}

      {/* Settings Drawer */}
      <DashboardSettingsDrawer
        open={settingsOpen}
        theme={theme}
        onClose={() => setSettingsOpen(false)}
        onThemeChange={setTheme}
      />

      {/* Add Panel Modal */}
      <Modal
        title="Add Panel"
        open={addPanelVisible}
        onCancel={() => setAddPanelVisible(false)}
        footer={addPanelTab !== 'visualization' ? (
          <Space>
            <Button onClick={() => setAddPanelVisible(false)}>Cancel</Button>
            <Button
              type="primary"
              onClick={() => handleAddContentPanel(addPanelTab)}
              disabled={
                (addPanelTab === 'image' && !newImageUrl) ||
                (addPanelTab === 'title' && !newTitleText)
              }
            >
              Add Panel
            </Button>
          </Space>
        ) : null}
        width={600}
      >
        <Tabs
          activeKey={addPanelTab}
          onChange={(key) => setAddPanelTab(key as AddPanelTab)}
          items={[
            {
              key: 'visualization',
              label: <span><BarChartOutlined /> Visualization</span>,
              children: visualizations && visualizations.length > 0 ? (
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
                            onClick={() => handleAddVizPanel(viz.id)}
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
              ),
            },
            {
              key: 'markdown',
              label: <span><FileMarkdownOutlined /> Markdown</span>,
              children: (
                <div>
                  <Text type="secondary" style={{ display: 'block', marginBottom: 8 }}>
                    Add formatted text using Markdown syntax. Supports headers, bold, italic, links, lists, and more.
                  </Text>
                  <Input.TextArea
                    value={newMarkdownContent}
                    onChange={(e) => setNewMarkdownContent(e.target.value)}
                    rows={6}
                    placeholder="Enter markdown content..."
                    style={{ fontFamily: 'monospace' }}
                  />
                </div>
              ),
            },
            {
              key: 'image',
              label: <span><PictureOutlined /> Image</span>,
              children: (
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Tabs
                    defaultActiveKey="url"
                    items={[
                      {
                        key: 'url',
                        label: 'URL',
                        children: (
                          <Input
                            value={newImageUrl}
                            onChange={(e) => setNewImageUrl(e.target.value)}
                            placeholder="https://example.com/image.png"
                            addonBefore="URL"
                          />
                        ),
                      },
                      {
                        key: 'upload',
                        label: 'Upload',
                        children: (
                          <Spin spinning={imageUploading} tip="Uploading...">
                            <Upload.Dragger
                              accept=".jpg,.jpeg,.png,.gif,.svg,.webp"
                              showUploadList={false}
                              beforeUpload={(file) => {
                                const allowedTypes = ['image/jpeg', 'image/png', 'image/gif', 'image/svg+xml', 'image/webp'];
                                if (!allowedTypes.includes(file.type)) {
                                  message.error('Invalid file type. Allowed: JPG, PNG, GIF, SVG, WebP');
                                  return false;
                                }
                                if (file.size > 5 * 1024 * 1024) {
                                  message.error('File exceeds maximum size of 5 MB');
                                  return false;
                                }
                                setImageUploading(true);
                                uploadImage(file)
                                  .then((result) => {
                                    setNewImageUrl(result.url);
                                    message.success(`Uploaded ${result.filename}`);
                                  })
                                  .catch(() => {
                                    message.error('Failed to upload image');
                                  })
                                  .finally(() => {
                                    setImageUploading(false);
                                  });
                                return false;
                              }}
                            >
                              <p className="ant-upload-drag-icon">
                                <InboxOutlined />
                              </p>
                              <p className="ant-upload-text">Click or drag an image file here</p>
                              <p className="ant-upload-hint">
                                JPG, PNG, GIF, SVG, or WebP — max 5 MB
                              </p>
                            </Upload.Dragger>
                          </Spin>
                        ),
                      },
                    ]}
                  />
                  <Input
                    value={newImageAlt}
                    onChange={(e) => setNewImageAlt(e.target.value)}
                    placeholder="Image description (optional)"
                    addonBefore="Alt text"
                  />
                </Space>
              ),
            },
            {
              key: 'title',
              label: <span><FontSizeOutlined /> Title</span>,
              children: (
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Text type="secondary">Add a large heading with an optional subtitle.</Text>
                  <Input
                    value={newTitleText}
                    onChange={(e) => setNewTitleText(e.target.value)}
                    placeholder="Dashboard Section Title"
                    addonBefore="Title"
                  />
                  <Input
                    value={newTitleSubtitle}
                    onChange={(e) => setNewTitleSubtitle(e.target.value)}
                    placeholder="Optional subtitle"
                    addonBefore="Subtitle"
                  />
                </Space>
              ),
            },
          ]}
        />
      </Modal>
    </div>
  );
}
