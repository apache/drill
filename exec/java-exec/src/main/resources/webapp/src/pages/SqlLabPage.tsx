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
import { useCallback, useState, useEffect, useRef, useMemo } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { useLocation } from 'react-router-dom';
import { Tabs, message, Tooltip, Modal, Alert, Button, Space, Spin } from 'antd';
import { PlusOutlined, MenuFoldOutlined, MenuUnfoldOutlined, RobotOutlined } from '@ant-design/icons';
import Markdown from 'react-markdown';
import type { RootState, AppDispatch } from '../store';
import {
  addTab,
  removeTab,
  setActiveTab,
  setDefaultSchema,
  renameTab,
  clearResultsExpired,
} from '../store/querySlice';
import { toggleSidebar, setEditorHeight } from '../store/uiSlice';
import { useWorkspacePersistence } from '../hooks/useWorkspacePersistence';
import { useQueryExecution } from '../hooks/useQuery';
import { useQueryHistory } from '../hooks/useQueryHistory';
import { useSchemas } from '../hooks/useMetadata';
import { useProspector } from '../hooks/useProspector';
import { getAiStatus, streamChat, transpileSql, convertDataType } from '../api/ai';
import { getSchemaTree } from '../api/metadata';
import SchemaExplorer from '../components/schema-explorer/SchemaExplorer';
import type { DatasetFilter } from '../components/schema-explorer/SchemaExplorer';
import SqlEditor, { DEFAULT_EDITOR_SETTINGS } from '../components/query-editor/SqlEditor';
import type { EditorSettings } from '../components/query-editor/SqlEditor';
import QueryToolbar from '../components/query-editor/QueryToolbar';
import ResultsGrid, { DEFAULT_RESULTS_SETTINGS } from '../components/results/ResultsGrid';
import type { ResultsSettings } from '../components/results/ResultsGrid';
import SaveQueryDialog from '../components/query-editor/SaveQueryDialog';
import QueryHistoryModal from '../components/query-editor/QueryHistoryModal';
import { VisualizationBuilder } from '../components/visualization';
import ShareApiModal from '../components/results/ShareApiModal';
import { ProspectorPanel } from '../components/prospector';
import type { SavedQuery } from '../types';
import type { ChatContext, ChatMessage } from '../types/ai';
import { applySqlTransformation, prettifySql, type ColumnTransformation } from '../utils/sqlTransformations';

function extractSqlFromMarkdown(markdown: string): string | null {
  const regex = /```sql\s*\n([\s\S]*?)```/g;
  let lastMatch: string | null = null;
  let match;
  while ((match = regex.exec(markdown)) !== null) {
    lastMatch = match[1].trim();
  }
  return lastMatch;
}

interface SqlLabPageProps {
  datasetFilter?: DatasetFilter;
  headerContent?: React.ReactNode;
  projectId?: string;
}

interface LocationState {
  loadQuery?: SavedQuery;
}

export default function SqlLabPage({ datasetFilter, headerContent, projectId }: SqlLabPageProps) {
  const dispatch = useDispatch<AppDispatch>();
  const location = useLocation();
  const { tabs, activeTabId } = useSelector((state: RootState) => state.query);
  const activeTab = tabs.find((t) => t.id === activeTabId);

  const sidebarCollapsed = useSelector((state: RootState) => state.ui.sidebarCollapsed);
  const editorHeight = useSelector((state: RootState) => state.ui.editorHeight);

  const { onResultsCached } = useWorkspacePersistence(projectId);

  const [autoLimit, setAutoLimit] = useState<number | null>(1000);
  const [saveDialogOpen, setSaveDialogOpen] = useState(false);
  const [vizBuilderOpen, setVizBuilderOpen] = useState(false);
  const [historyModalOpen, setHistoryModalOpen] = useState(false);
  const [shareApiModalOpen, setShareApiModalOpen] = useState(false);
  const [sharedQueryApiIds, setSharedQueryApiIds] = useState<Record<string, string | undefined>>({});

  // Optimize modal state
  const [optimizeModalOpen, setOptimizeModalOpen] = useState(false);
  const [optimizeStreaming, setOptimizeStreaming] = useState(false);
  const [optimizeContent, setOptimizeContent] = useState('');
  const [optimizeError, setOptimizeError] = useState<string | null>(null);
  const [optimizeDone, setOptimizeDone] = useState(false);
  const optimizeAbortRef = useRef<AbortController | null>(null);

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

  // Results settings with localStorage persistence
  const [resultsSettings, setResultsSettings] = useState<ResultsSettings>(() => {
    try {
      const stored = localStorage.getItem('drill-sqllab-results-settings');
      if (stored) {
        return { ...DEFAULT_RESULTS_SETTINGS, ...JSON.parse(stored) };
      }
    } catch {
      // Ignore parse errors
    }
    return DEFAULT_RESULTS_SETTINGS;
  });

  const handleResultsSettingsChange = useCallback((settings: ResultsSettings) => {
    setResultsSettings(settings);
    try {
      localStorage.setItem('drill-sqllab-results-settings', JSON.stringify(settings));
    } catch {
      // Ignore storage errors
    }
  }, []);

  // Resizable editor panel
  const [isDragging, setIsDragging] = useState(false);
  const contentRef = useRef<HTMLDivElement>(null);

  // Tab renaming
  const [editingTabId, setEditingTabId] = useState<string | null>(null);
  const [editingTabName, setEditingTabName] = useState('');

  // Prospector state
  const [prospectorOpen, setProspectorOpen] = useState(() => {
    try {
      return localStorage.getItem('drill-prospector-open') === 'true';
    } catch {
      return false;
    }
  });
  const [prospectorAvailable, setProspectorAvailable] = useState(false);

  // Fetch schemas for the schema selector
  const { data: schemas } = useSchemas();

  // Query history hook — scoped to project+tab when inside a project
  const { history, addEntry: addHistory, clearHistory } = useQueryHistory(projectId, activeTabId);

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
  } = useQueryExecution(activeTabId, addHistory);

  // Prospector hook
  const prospector = useProspector(updateSql);

  // Check Prospector status on mount
  useEffect(() => {
    getAiStatus()
      .then((status) => setProspectorAvailable(status.enabled))
      .catch(() => setProspectorAvailable(false));
  }, []);

  // Build AI context from current state
  const aiContext: ChatContext = useMemo(() => ({
    currentSql: sql || undefined,
    currentSchema: activeTab?.defaultSchema || undefined,
    availableSchemas: schemas?.map((s) => s.name),
    error: error?.message || undefined,
    resultSummary: results ? {
      rowCount: results.rows?.length ?? 0,
      columns: results.columns || [],
      columnTypes: results.metadata || [],
    } : undefined,
  }), [sql, activeTab?.defaultSchema, schemas, error, results]);

  // Prospector sidebar toggle with localStorage persistence
  const toggleProspector = useCallback(() => {
    setProspectorOpen((prev) => {
      const next = !prev;
      try {
        localStorage.setItem('drill-prospector-open', String(next));
      } catch {
        // Ignore storage errors
      }
      return next;
    });
  }, []);

  // Handle Prospector actions from toolbar/results (opens sidebar + sends message)
  const handleProspectorAction = useCallback(
    (prompt: string) => {
      if (!prospectorOpen) {
        setProspectorOpen(true);
        try {
          localStorage.setItem('drill-prospector-open', 'true');
        } catch {
          // Ignore storage errors
        }
      }
      prospector.sendMessage(prompt, aiContext);
    },
    [prospectorOpen, prospector, aiContext],
  );

  const handleExplainQuery = useCallback(() => {
    handleProspectorAction('Explain this SQL query. What does it do and how could it be improved?');
  }, [handleProspectorAction]);

  const handleFixError = useCallback(() => {
    handleProspectorAction('Fix the error in my SQL query. Explain what went wrong and provide a corrected version.');
  }, [handleProspectorAction]);

  const handleFixWithProspector = useCallback(() => {
    handleProspectorAction('Fix the error in my SQL query. Explain what went wrong and provide a corrected version.');
  }, [handleProspectorAction]);

  // Optimize query handler
  const handleOptimizeQuery = useCallback(() => {
    if (!sql || sql.trim().length === 0) {
      return;
    }

    setOptimizeContent('');
    setOptimizeError(null);
    setOptimizeDone(false);
    setOptimizeStreaming(true);
    setOptimizeModalOpen(true);

    const messages: ChatMessage[] = [
      {
        role: 'system',
        content: `You are an Apache Drill SQL optimization expert. The user will provide a SQL query. Your job:
1. Explain what the query does in 1-2 sentences.
2. List numbered optimization suggestions. For each, explain WHY it improves performance.
3. Provide the complete optimized SQL in a single \`\`\`sql code block.
4. Focus on: predicate pushdown, partition pruning, avoiding SELECT *, appropriate use of LIMIT, JOIN ordering, and Drill-specific optimizations.
5. If the query is already optimal, say so and return the original SQL in a \`\`\`sql code block.`,
      },
      {
        role: 'user',
        content: `Please optimize this SQL query:\n\n\`\`\`sql\n${sql}\n\`\`\``,
      },
    ];

    const controller = streamChat(
      { messages, tools: [], context: aiContext },
      (event) => {
        if (event.type === 'content') {
          setOptimizeContent((prev) => prev + event.content);
        }
      },
      () => {
        setOptimizeStreaming(false);
        setOptimizeDone(true);
      },
      (err) => {
        setOptimizeStreaming(false);
        setOptimizeError(err.message);
      },
    );

    optimizeAbortRef.current = controller;
  }, [sql, aiContext]);

  const handleOptimizeAccept = useCallback(async () => {
    const extractedSql = extractSqlFromMarkdown(optimizeContent);
    if (extractedSql) {
      const schemaNames = schemas?.map((s) => s.name) || [];
      let schemaTree: { name: string; tables: { name: string; columns: string[] }[] }[] = [];
      if (schemaNames.length > 0) {
        try {
          schemaTree = await getSchemaTree(schemaNames);
        } catch {
          // Fall back to empty schemas if fetch fails
        }
      }
      const transpiledSql = await transpileSql(extractedSql, 'mysql', 'drill', schemaTree);
      updateSql(transpiledSql);
    }
    setOptimizeModalOpen(false);
    optimizeAbortRef.current?.abort();
  }, [optimizeContent, updateSql, schemas]);

  const handleOptimizeClose = useCallback(() => {
    setOptimizeModalOpen(false);
    optimizeAbortRef.current?.abort();
  }, []);

  // Cleanup optimize streaming on unmount
  useEffect(() => {
    return () => {
      optimizeAbortRef.current?.abort();
    };
  }, []);

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

  // Show transformation preview modal and apply on confirm.
  const showTransformPreview = useCallback(
    (originalSql: string, transformedSql: string, hideLoading: () => void) => {
      hideLoading();
      const preStyle = {
        backgroundColor: 'var(--color-bg-elevated)',
        color: 'var(--color-text)',
        padding: 12,
        borderRadius: 4,
        fontSize: 12,
        maxHeight: 150,
        overflowY: 'auto' as const,
        whiteSpace: 'pre-wrap' as const,
      };

      const formattedOriginal = prettifySql(originalSql);
      const formattedTransformed = prettifySql(transformedSql);

      Modal.confirm({
        title: 'Apply Column Transformation?',
        width: 600,
        content: (
          <div>
            <Alert
              message="This will modify your SQL query"
              type="info"
              style={{ marginBottom: 16 }}
            />
            <div style={{ marginBottom: 8 }}>
              <strong>Original:</strong>
              <pre style={preStyle}>{formattedOriginal}</pre>
            </div>
            <div>
              <strong>Transformed:</strong>
              <pre style={preStyle}>{formattedTransformed}</pre>
            </div>
          </div>
        ),
        okText: 'Apply & Run',
        cancelText: 'Cancel',
        onOk: () => {
          updateSql(formattedTransformed);
          setTimeout(() => handleExecute(), 100);
        },
      });
    },
    [updateSql, handleExecute]
  );

  // Handle column transformation
  const handleTransformColumn = useCallback(
    (_columnName: string, transformation: ColumnTransformation) => {
      const currentSql = sql || '';
      const hideLoading = message.loading('Preparing transformation...', 0);

      // Route CAST transformations through the Python/sqlglot backend
      if (transformation.type === 'cast' && transformation.targetType) {
        // Build a columns map from result metadata (needed for star queries)
        const columnsMap: Record<string, string> | undefined = results?.columns
          ? Object.fromEntries(results.columns.map((c) => [c, 'VARCHAR']))
          : undefined;

        const fallbackToTs = () => {
          const fallback = applySqlTransformation(currentSql, transformation, results?.columns);
          if (!fallback) {
            hideLoading();
            message.error('Could not apply transformation to query');
            return;
          }
          showTransformPreview(currentSql, fallback, hideLoading);
        };

        convertDataType(currentSql, transformation.columnName, transformation.targetType, columnsMap)
          .then((response) => {
            if (!response.success) {
              fallbackToTs();
              return;
            }
            showTransformPreview(currentSql, response.sql, hideLoading);
          })
          .catch(() => {
            fallbackToTs();
          });
        return;
      }

      // Other transformations use the local TypeScript approach
      const transformedSql = applySqlTransformation(currentSql, transformation, results?.columns);
      if (!transformedSql) {
        hideLoading();
        message.error('Could not apply transformation to query');
        return;
      }
      showTransformPreview(currentSql, transformedSql, hideLoading);
    },
    [sql, results?.columns, showTransformPreview]
  );

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
      // Table function expressions (e.g. Excel sheets) are passed as the table arg
      if (table.startsWith('table(')) {
        const query = `SELECT *\nFROM ${table}\nLIMIT 100`;
        updateSql(query);
        return;
      }
      // Format schema: plugin unquoted, workspace parts backtick-quoted
      const schemaParts = schema.split('.');
      const formattedSchema = schemaParts.length <= 1
        ? schema
        : schemaParts[0] + '.' + schemaParts.slice(1).map((p) => `\`${p}\``).join('.');
      const query = `SELECT *\nFROM ${formattedSchema}.\`${table}\`\nLIMIT 100`;
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

  // Handle share as API
  const handleShareApi = useCallback(() => {
    if (!results || results.rows.length === 0) {
      message.warning('Please run a query first to share results as API');
      return;
    }
    setShareApiModalOpen(true);
  }, [results]);

  const handleSharedQueryApiIdChange = useCallback((id: string | undefined) => {
    setSharedQueryApiIds(prev => ({ ...prev, [activeTabId]: id }));
  }, [activeTabId]);

  // Handle query history
  const handleShowHistory = useCallback(() => {
    setHistoryModalOpen(true);
  }, []);

  const handleSelectHistoryQuery = useCallback(
    (selectedSql: string) => {
      updateSql(selectedSql);
    },
    [updateSql]
  );

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
      dispatch(setEditorHeight(Math.max(120, Math.min(newHeight, maxHeight))));
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
  }, [isDragging, dispatch]);

  // Cache results when they arrive
  useEffect(() => {
    if (results && activeTabId) {
      onResultsCached(activeTabId, results, executionTime ?? 0);
    }
  }, [results, activeTabId, executionTime, onResultsCached]);

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
            datasetFilter={datasetFilter}
          />
        )}
      </div>

      {/* Sidebar Toggle */}
      <Tooltip title={sidebarCollapsed ? 'Show Schema Explorer' : 'Hide Schema Explorer'}>
        <div
          className="sidebar-toggle"
          onClick={() => dispatch(toggleSidebar())}
        >
          {sidebarCollapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
        </div>
      </Tooltip>

      {/* Main Content */}
      <div className="sqllab-content" ref={contentRef}>
        {/* Optional Header (e.g. project bar) */}
        {headerContent}

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
            resultsSettings={resultsSettings}
            onResultsSettingsChange={handleResultsSettingsChange}
            onShowHistory={handleShowHistory}
            onExplainQuery={handleExplainQuery}
            onOptimizeQuery={handleOptimizeQuery}
            onFixError={handleFixError}
            onToggleProspector={toggleProspector}
            hasSql={!!sql && sql.trim().length > 0}
            hasError={!!error}
            prospectorOpen={prospectorOpen}
            prospectorAvailable={prospectorAvailable}
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
          {activeTab?.resultsExpired && (
            <Alert
              message="Cached results have expired"
              description="Re-run the query to see results."
              type="info"
              showIcon
              closable
              onClose={() => dispatch(clearResultsExpired(activeTabId))}
              action={
                <Button size="small" type="primary" onClick={handleExecute}>
                  Re-run Query
                </Button>
              }
              style={{ margin: '8px 16px' }}
            />
          )}
          <ResultsGrid
            results={results}
            error={error}
            isLoading={isExecuting}
            onCreateVisualization={handleCreateVisualization}
            resultsSettings={resultsSettings}
            onResultsSettingsChange={handleResultsSettingsChange}
            onFixWithProspector={handleFixWithProspector}
            prospectorAvailable={prospectorAvailable}
            onTransformColumn={handleTransformColumn}
            onShareApi={handleShareApi}
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

      {/* Share API Modal */}
      <ShareApiModal
        open={shareApiModalOpen}
        onClose={() => setShareApiModalOpen(false)}
        sql={sql}
        defaultSchema={activeTab?.defaultSchema}
        sharedQueryApiId={sharedQueryApiIds[activeTabId]}
        onSharedQueryApiIdChange={handleSharedQueryApiIdChange}
      />

      {/* Query History Modal */}
      <QueryHistoryModal
        open={historyModalOpen}
        onClose={() => setHistoryModalOpen(false)}
        history={history}
        onSelectQuery={handleSelectHistoryQuery}
        onClearHistory={clearHistory}
      />

      {/* Optimize Query Modal */}
      <Modal
        title="Optimize Query"
        open={optimizeModalOpen}
        onCancel={handleOptimizeClose}
        width={720}
        destroyOnClose
        footer={
          <Space>
            <Button onClick={handleOptimizeClose}>Cancel</Button>
            <Button
              type="primary"
              disabled={!optimizeDone || !extractSqlFromMarkdown(optimizeContent)}
              onClick={handleOptimizeAccept}
            >
              Accept
            </Button>
          </Space>
        }
      >
        {optimizeError && (
          <Alert
            message="Optimization Failed"
            description={optimizeError}
            type="error"
            showIcon
            style={{ marginBottom: 16 }}
          />
        )}
        {optimizeContent ? (
          <div className="optimize-modal-content">
            <div className="optimize-explanation">
              <Markdown>{optimizeContent}</Markdown>
              {optimizeStreaming && <span className="prospector-cursor" />}
            </div>
          </div>
        ) : optimizeStreaming ? (
          <div className="optimize-thinking">
            <Spin size="small" />
            <span style={{ marginLeft: 8 }}>Analyzing your query...</span>
          </div>
        ) : null}
      </Modal>

      {/* Prospector Sidebar Toggle */}
      {prospectorAvailable && (
        <Tooltip title={prospectorOpen ? 'Hide Prospector' : 'Show Prospector'}>
          <div className="prospector-sidebar-toggle" onClick={toggleProspector}>
            <RobotOutlined />
          </div>
        </Tooltip>
      )}

      {/* Prospector Sidebar */}
      {prospectorAvailable && (
        <div className={`prospector-sidebar${prospectorOpen ? '' : ' collapsed'}`}>
          {prospectorOpen && (
            <ProspectorPanel prospector={prospector} context={aiContext} />
          )}
        </div>
      )}
    </div>
  );
}
