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
import { useCallback, useState, useEffect, useRef } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { useLocation } from 'react-router-dom';
import { Tabs, message, Tooltip } from 'antd';
import { PlusOutlined, MenuFoldOutlined, MenuUnfoldOutlined } from '@ant-design/icons';
import type { RootState, AppDispatch } from '../store';
import {
  addTab,
  removeTab,
  setActiveTab,
  setDefaultSchema,
  renameTab,
} from '../store/querySlice';
import { useQueryExecution } from '../hooks/useQuery';
import { useSchemas } from '../hooks/useMetadata';
import SchemaExplorer from '../components/schema-explorer/SchemaExplorer';
import SqlEditor, { DEFAULT_EDITOR_SETTINGS } from '../components/query-editor/SqlEditor';
import type { EditorSettings } from '../components/query-editor/SqlEditor';
import QueryToolbar from '../components/query-editor/QueryToolbar';
import ResultsGrid from '../components/results/ResultsGrid';
import SaveQueryDialog from '../components/query-editor/SaveQueryDialog';
import { VisualizationBuilder } from '../components/visualization';
import type { SavedQuery } from '../types';

interface LocationState {
  loadQuery?: SavedQuery;
}

export default function SqlLabPage() {
  const dispatch = useDispatch<AppDispatch>();
  const location = useLocation();
  const { tabs, activeTabId } = useSelector((state: RootState) => state.query);
  const activeTab = tabs.find((t) => t.id === activeTabId);

  const [autoLimit, setAutoLimit] = useState<number | null>(1000);
  const [saveDialogOpen, setSaveDialogOpen] = useState(false);
  const [vizBuilderOpen, setVizBuilderOpen] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  // Editor settings with localStorage persistence
  const [editorSettings, setEditorSettings] = useState<EditorSettings>(() => {
    try {
      const stored = localStorage.getItem('drill-sqllab-editor-settings');
      if (stored) {
        return { ...DEFAULT_EDITOR_SETTINGS, ...JSON.parse(stored) };
      }
    } catch {
      // Ignore parse errors
    }
    return DEFAULT_EDITOR_SETTINGS;
  });

  const handleEditorSettingsChange = useCallback((settings: EditorSettings) => {
    setEditorSettings(settings);
    try {
      localStorage.setItem('drill-sqllab-editor-settings', JSON.stringify(settings));
    } catch {
      // Ignore storage errors
    }
  }, []);

  // Resizable editor panel
  const [editorHeight, setEditorHeight] = useState(300);
  const [isDragging, setIsDragging] = useState(false);
  const contentRef = useRef<HTMLDivElement>(null);

  // Tab renaming
  const [editingTabId, setEditingTabId] = useState<string | null>(null);
  const [editingTabName, setEditingTabName] = useState('');

  // Fetch schemas for the schema selector
  const { data: schemas } = useSchemas();

  // Query execution hook for active tab
  const {
    sql,
    results,
    error,
    isExecuting,
    executionTime,
    execute,
    cancel,
    updateSql,
  } = useQueryExecution(activeTabId);

  // Handle loading query from navigation state (from SavedQueriesPage)
  const locationState = location.state as LocationState | undefined;
  useEffect(() => {
    if (locationState?.loadQuery) {
      const query = locationState.loadQuery;
      updateSql(query.sql);
      if (query.defaultSchema) {
        dispatch(setDefaultSchema({ tabId: activeTabId, schema: query.defaultSchema }));
      }
      // Clear the location state to prevent reloading on re-render
      window.history.replaceState({}, document.title);
    }
  }, [locationState, updateSql, dispatch, activeTabId]);

  // Handle execute with current settings
  const handleExecute = useCallback(() => {
    execute({
      autoLimit: autoLimit ?? undefined,
      defaultSchema: activeTab?.defaultSchema,
    });
  }, [execute, autoLimit, activeTab?.defaultSchema]);

  // Handle inserting text from schema explorer
  const handleInsertText = useCallback((text: string) => {
    // Use the global function exposed by SqlEditor
    const insertFn = (window as unknown as { sqlEditorInsertText?: (text: string) => void }).sqlEditorInsertText;
    if (insertFn) {
      insertFn(text);
    }
  }, []);

  // Handle table selection from schema explorer
  const handleTableSelect = useCallback(
    (schema: string, table: string) => {
      const query = `SELECT * FROM \`${schema}\`.\`${table}\` LIMIT 100`;
      updateSql(query);
    },
    [updateSql]
  );

  // Handle schema change
  const handleSchemaChange = useCallback(
    (schema: string) => {
      dispatch(setDefaultSchema({ tabId: activeTabId, schema }));
    },
    [dispatch, activeTabId]
  );

  // Handle tab operations
  const handleTabChange = useCallback(
    (key: string) => {
      dispatch(setActiveTab(key));
    },
    [dispatch]
  );

  const handleTabEdit = useCallback(
    (targetKey: React.MouseEvent | React.KeyboardEvent | string, action: 'add' | 'remove') => {
      if (action === 'add') {
        dispatch(addTab());
      } else if (action === 'remove' && typeof targetKey === 'string') {
        if (tabs.length > 1) {
          dispatch(removeTab(targetKey));
        } else {
          message.warning('Cannot close the last tab');
        }
      }
    },
    [dispatch, tabs.length]
  );

  // Handle save query
  const handleSave = useCallback(() => {
    if (!sql || sql.trim() === '') {
      message.warning('Please enter a SQL query before saving');
      return;
    }
    setSaveDialogOpen(true);
  }, [sql]);

  // Handle create visualization
  const handleCreateVisualization = useCallback(() => {
    if (!results || results.rows.length === 0) {
      message.warning('Please run a query first to create a visualization');
      return;
    }
    setVizBuilderOpen(true);
  }, [results]);

  // ---- Resizable editor/results split ----
  const handleDragStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);

  useEffect(() => {
    if (!isDragging) {
      return;
    }

    const handleMouseMove = (e: MouseEvent) => {
      if (!contentRef.current) {
        return;
      }
      const contentRect = contentRef.current.getBoundingClientRect();
      // Subtract the tabs height (~46px) from the top
      const tabsHeight = 46;
      const newHeight = e.clientY - contentRect.top - tabsHeight;
      // Clamp between min (120) and max (contentHeight - 100 for results)
      const maxHeight = contentRect.height - tabsHeight - 100;
      setEditorHeight(Math.max(120, Math.min(newHeight, maxHeight)));
    };

    const handleMouseUp = () => {
      setIsDragging(false);
    };

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
    // Prevent text selection while dragging
    document.body.style.userSelect = 'none';
    document.body.style.cursor = 'row-resize';

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
      document.body.style.userSelect = '';
      document.body.style.cursor = '';
    };
  }, [isDragging]);

  // ---- Tab renaming ----
  const handleTabDoubleClick = useCallback((tabId: string, currentName: string) => {
    setEditingTabId(tabId);
    setEditingTabName(currentName);
  }, []);

  const handleTabRenameSubmit = useCallback(() => {
    if (editingTabId && editingTabName.trim()) {
      dispatch(renameTab({ tabId: editingTabId, name: editingTabName.trim() }));
    }
    setEditingTabId(null);
    setEditingTabName('');
  }, [dispatch, editingTabId, editingTabName]);

  const handleTabRenameKeyDown = useCallback((e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      handleTabRenameSubmit();
    } else if (e.key === 'Escape') {
      setEditingTabId(null);
      setEditingTabName('');
    }
  }, [handleTabRenameSubmit]);

  return (
    <div className="sqllab-main">
      {/* Schema Explorer Sidebar */}
      <div className={`sqllab-sidebar${sidebarCollapsed ? ' collapsed' : ''}`}>
        {!sidebarCollapsed && (
          <SchemaExplorer
            onInsertText={handleInsertText}
            onTableSelect={handleTableSelect}
          />
        )}
      </div>

      {/* Sidebar Toggle */}
      <Tooltip title={sidebarCollapsed ? 'Show Schema Explorer' : 'Hide Schema Explorer'}>
        <div
          className="sidebar-toggle"
          onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
        >
          {sidebarCollapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
        </div>
      </Tooltip>

      {/* Main Content */}
      <div className="sqllab-content" ref={contentRef}>
        {/* Query Tabs */}
        <Tabs
          type="editable-card"
          activeKey={activeTabId}
          onChange={handleTabChange}
          onEdit={handleTabEdit}
          items={tabs.map((tab) => ({
            key: tab.id,
            label: editingTabId === tab.id ? (
              <input
                className="tab-rename-input"
                value={editingTabName}
                onChange={(e) => setEditingTabName(e.target.value)}
                onBlur={handleTabRenameSubmit}
                onKeyDown={handleTabRenameKeyDown}
                autoFocus
                onClick={(e) => e.stopPropagation()}
              />
            ) : (
              <span onDoubleClick={() => handleTabDoubleClick(tab.id, tab.name)}>
                {tab.name}
              </span>
            ),
            closable: tabs.length > 1,
          }))}
          style={{ padding: '0 16px', marginBottom: 0 }}
          addIcon={<PlusOutlined />}
        />

        {/* Editor Panel */}
        <div className="sqllab-editor-panel" style={{ height: editorHeight }}>
          {/* Toolbar */}
          <QueryToolbar
            onExecute={handleExecute}
            onCancel={cancel}
            onSave={handleSave}
            isExecuting={isExecuting}
            executionTime={executionTime}
            schemas={schemas}
            selectedSchema={activeTab?.defaultSchema}
            onSchemaChange={handleSchemaChange}
            autoLimit={autoLimit ?? 1000}
            onAutoLimitChange={setAutoLimit}
            editorSettings={editorSettings}
            onEditorSettingsChange={handleEditorSettingsChange}
          />

          {/* SQL Editor */}
          <SqlEditor
            value={sql}
            onChange={updateSql}
            onExecute={handleExecute}
            settings={editorSettings}
          />
        </div>

        {/* Resize Handle */}
        <div
          className={`sqllab-resize-handle${isDragging ? ' dragging' : ''}`}
          onMouseDown={handleDragStart}
        />

        {/* Results Panel */}
        <div className="sqllab-results-panel">
          <ResultsGrid
            results={results}
            error={error}
            isLoading={isExecuting}
            onCreateVisualization={handleCreateVisualization}
          />
        </div>
      </div>

      {/* Save Query Dialog */}
      <SaveQueryDialog
        open={saveDialogOpen}
        onClose={() => setSaveDialogOpen(false)}
        sql={sql}
        defaultSchema={activeTab?.defaultSchema}
      />

      {/* Visualization Builder */}
      <VisualizationBuilder
        open={vizBuilderOpen}
        onClose={() => setVizBuilderOpen(false)}
        queryResult={results ?? null}
        sql={sql}
        defaultSchema={activeTab?.defaultSchema}
      />
    </div>
  );
}
