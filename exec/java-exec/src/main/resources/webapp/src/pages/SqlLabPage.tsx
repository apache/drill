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
import { useCallback, useState, useEffect, useRef, useMemo, type MutableRefObject } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import { useLocation, useNavigate } from 'react-router-dom';
import { Tabs, message, notification, Tooltip, Modal, Alert, Button, Space, Spin, Dropdown, Grid } from 'antd';
import { PlusOutlined, MenuFoldOutlined, MenuUnfoldOutlined, RobotOutlined, MoreOutlined, EditOutlined, CopyOutlined, CloseOutlined, PlayCircleOutlined, StopOutlined, ExperimentOutlined, TableOutlined } from '@ant-design/icons';
import Markdown from 'react-markdown';
import type { RootState, AppDispatch } from '../store';
import {
  addTab,
  duplicateTab,
  removeTab,
  setActiveTab,
  setDefaultSchema,
  renameTab,
  clearResults,
  clearResultsExpired,
} from '../store/querySlice';
import { toggleSidebar, setEditorHeight } from '../store/uiSlice';
import { useWorkspacePersistence } from '../hooks/useWorkspacePersistence';
import { useQueryExecution } from '../hooks/useQuery';
import { useQueryHistory } from '../hooks/useQueryHistory';
import { useSchemas } from '../hooks/useMetadata';
import { useProspector } from '../hooks/useProspector';
import { useMonacoCompletion } from '../hooks/useMonacoCompletion';
import { getAiStatus, getAiConfig, streamChat, transpileSql, convertDataType } from '../api/ai';
import { getSchemaTree } from '../api/metadata';
import SchemaExplorer from '../components/schema-explorer/SchemaExplorer';
import type { DatasetFilter } from '../components/schema-explorer/SchemaExplorer';
import SqlEditor, { DEFAULT_EDITOR_SETTINGS } from '../components/query-editor/SqlEditor';
import type { EditorSettings } from '../components/query-editor/SqlEditor';
import QueryToolbar from '../components/query-editor/QueryToolbar';
import ResultsGrid, { DEFAULT_RESULTS_SETTINGS } from '../components/results/ResultsGrid';
import type { ResultsSettings } from '../components/results/ResultsGrid';
import SaveQueryDialog from '../components/query-editor/SaveQueryDialog';
import KeyboardShortcutsModal from '../components/query-editor/KeyboardShortcutsModal';
import QueryHistoryModal from '../components/query-editor/QueryHistoryModal';
import { VisualizationBuilder } from '../components/visualization';
import ShareApiModal from '../components/results/ShareApiModal';
import NotebookPanel from '../components/notebook/NotebookPanel';
import type { NotebookPanelHandle } from '../components/notebook/NotebookPanel';
import { ProspectorPanel } from '../components/prospector';
import type { SavedQuery } from '../types';
import type { ChatContext, ChatMessage } from '../types/ai';
import { applySqlTransformation, prettifySql, type ColumnTransformation } from '../utils/sqlTransformations';
import { useSqlValidation } from '../hooks/useSqlValidation';
import type { Monaco } from '@monaco-editor/react';

type IStandaloneCodeEditor = Parameters<import('@monaco-editor/react').OnMount>[0];

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
  savedQueryIds?: string[];
}

interface LocationState {
  loadQuery?: SavedQuery;
  initialSql?: string;
}

export default function SqlLabPage({ datasetFilter, headerContent, projectId, savedQueryIds }: SqlLabPageProps) {
  const dispatch = useDispatch<AppDispatch>();
  const location = useLocation();
  const navigate = useNavigate();
  const { tabs, activeTabId } = useSelector((state: RootState) => state.query);
  const activeTab = tabs.find((t) => t.id === activeTabId);

  const sidebarCollapsed = useSelector((state: RootState) => state.ui.sidebarCollapsed);
  const editorHeight = useSelector((state: RootState) => state.ui.editorHeight);

  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;

  const { onResultsCached } = useWorkspacePersistence(projectId);

  const [autoLimit, setAutoLimit] = useState<number | null>(1000);
  const [saveDialogOpen, setSaveDialogOpen] = useState(false);
  const [vizBuilderOpen, setVizBuilderOpen] = useState(false);
  const [historyModalOpen, setHistoryModalOpen] = useState(false);
  const [shareApiModalOpen, setShareApiModalOpen] = useState(false);
  const [resultsPanelTab, setResultsPanelTab] = useState<string>('results');
  const [maxNotebookRows, setMaxNotebookRows] = useState(50000);
  const [notebookHandle, setNotebookHandle] = useState<NotebookPanelHandle | null>(null);
  const [sharedQueryApiIds, setSharedQueryApiIds] = useState<Record<string, string | undefined>>({});
  const [shortcutsOpen, setShortcutsOpen] = useState(false);

  // Track "unsaved" state per tab: maps tabId -> sql at last explicit save
  const savedSqlRef = useRef<Record<string, string>>({});

  // Track editor text selection for "Run Selection"
  const [hasSelection, setHasSelection] = useState(false);

  // Editor/Monaco refs for SQL validation + selection tracking
  const editorInstanceRef = useRef<IStandaloneCodeEditor | null>(null) as MutableRefObject<IStandaloneCodeEditor | null>;
  const monacoInstanceRef = useRef<Monaco | null>(null) as MutableRefObject<Monaco | null>;
  // State version so hooks can react when the editor mounts
  const [monacoInstance, setMonacoInstance] = useState<Monaco | null>(null);

  const handleEditorReady = useCallback((editor: IStandaloneCodeEditor, monaco: Monaco) => {
    editorInstanceRef.current = editor;
    monacoInstanceRef.current = monaco;
    setMonacoInstance(monaco);
    // Track whether the user has a non-empty selection
    editor.onDidChangeCursorSelection(() => {
      const sel = editor.getSelection();
      setHasSelection(!!sel && !sel.isEmpty());
    });
  }, []);

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
  const [sendDataToAi, setSendDataToAi] = useState(true);
  const [maxToolRounds, setMaxToolRounds] = useState(15);

  // Fetch schemas for the schema selector
  const { data: schemas } = useSchemas();

  // Schema-aware SQL autocomplete (must come after schemas is declared)
  useMonacoCompletion(monacoInstance, schemas);

  // Query history hook — scoped to project+tab when inside a project
  const { history, addEntry: addHistory, clearHistory } = useQueryHistory(projectId, activeTabId);

  // Query execution hook for active tab
  const {
    sql,
    results,
    error,
    isExecuting,
    executionTime,
    cacheId,
    execute,
    cancel,
    updateSql,
  } = useQueryExecution(activeTabId, addHistory);

  // Prospector hook
  const prospector = useProspector(updateSql, useCallback((id: string, name: string) => {
    notification.success({
      message: 'Visualization Created',
      description: `"${name}" has been saved.`,
      btn: (
        <Button
          size="small"
          type="primary"
          onClick={() => {
            notification.destroy();
            navigate('/visualizations');
          }}
        >
          View Visualizations
        </Button>
      ),
      duration: 0,
      key: `viz-created-${id}`,
    });
  }, [navigate]), maxToolRounds);

  // Real-time SQL validation markers in the editor
  useSqlValidation(sql, editorInstanceRef, monacoInstanceRef);

  // Global keyboard shortcuts: ?, Ctrl+Shift+F (schema search), Ctrl+S (save)
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement;
      const inInput = target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.isContentEditable;
      const mod = e.metaKey || e.ctrlKey;

      // ? — show keyboard shortcuts (not while typing)
      if (e.key === '?' && !inInput && !mod) {
        e.preventDefault();
        setShortcutsOpen(true);
        return;
      }

      // Ctrl/Cmd+Shift+F — focus schema search
      if (mod && e.shiftKey && e.key === 'F') {
        e.preventDefault();
        const searchEl = document.getElementById('schema-search-input') as HTMLInputElement | null;
        if (searchEl) {
          if (sidebarCollapsed) {
            dispatch(toggleSidebar());
          }
          setTimeout(() => searchEl.focus(), 150);
        }
        return;
      }

      // Ctrl/Cmd+S — save query
      if (mod && e.key === 's' && !e.shiftKey) {
        e.preventDefault();
        if (sql && sql.trim()) {
          setSaveDialogOpen(true);
        }
        return;
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [sql, sidebarCollapsed, dispatch]);

  // Check Prospector status and config on mount
  useEffect(() => {
    getAiStatus()
      .then((status) => setProspectorAvailable(status.enabled))
      .catch(() => setProspectorAvailable(false));
    getAiConfig()
      .then((cfg) => {
        setSendDataToAi(cfg.sendDataToAi ?? true);
        setMaxToolRounds(cfg.maxToolRounds || 15);
      })
      .catch(() => {});
  }, []);

  // Derive notebook DataFrame name from tab name (same logic as NotebookPanel)
  const notebookDfName = useMemo(() => {
    const name = activeTab?.name || 'df';
    const sanitized = name.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, '').replace(/^[0-9]/, '_$&');
    return sanitized || 'df';
  }, [activeTab?.name]);

  // Build AI context from current state
  const isNotebookTab = resultsPanelTab === 'notebook';
  const aiContext: ChatContext = useMemo(() => {
    // When inside a project, scope schemas to project datasets only
    const projectSchemas = datasetFilter?.datasets?.length
      ? [...new Set(datasetFilter.datasets.map((d) => d.schema).filter(Boolean) as string[])]
      : undefined;

    const base: ChatContext = {
      currentSql: sql || undefined,
      currentSchema: activeTab?.defaultSchema || undefined,
      availableSchemas: projectSchemas || schemas?.map((s) => s.name),
      error: error?.message || undefined,
      resultSummary: results ? {
        rowCount: results.rows?.length ?? 0,
        columns: results.columns || [],
        columnTypes: results.metadata || [],
      } : undefined,
      projectDatasets: datasetFilter?.datasets?.length
        ? datasetFilter.datasets.map((d) => ({
            type: d.type,
            schema: d.schema,
            table: d.table,
            label: d.label,
          }))
        : undefined,
    };

    if (isNotebookTab) {
      base.notebookMode = true;
      base.notebookDfName = notebookDfName;
      base.notebookColumns = results?.columns;
      if (results) {
        base.notebookDfShape = `${results.rows?.length ?? 0} rows x ${results.columns?.length ?? 0} columns`;
      }
      if (notebookHandle) {
        const cellCode = notebookHandle.getLastCellCode();
        if (cellCode) {
          base.notebookCellCode = cellCode;
        }
        const cellError = notebookHandle.getLastCellError();
        if (cellError) {
          base.notebookCellError = cellError;
        }
      }
    }

    return base;
  }, [sql, activeTab?.defaultSchema, schemas, error, results, isNotebookTab, notebookDfName, notebookHandle, datasetFilter]);

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

  // Build schema context for the optimize prompt
  const buildSchemaContext = useCallback(
    (columns: string[], metadata: string[], rows: Record<string, unknown>[], includeData: boolean): string => {
      if (!columns || columns.length === 0) {
        return '';
      }
      let ctx = '\n\n## Result Schema\n\n| Column | Drill Type |\n|--------|------------|\n';
      columns.forEach((col, i) => {
        ctx += `| ${col} | ${metadata[i] || 'UNKNOWN'} |\n`;
      });

      if (includeData && rows && rows.length > 0) {
        const sampleRows = rows.slice(0, 5);
        ctx += '\n## Sample Data (first ' + sampleRows.length + ' rows)\n\n';
        ctx += '| ' + columns.join(' | ') + ' |\n';
        ctx += '| ' + columns.map(() => '---').join(' | ') + ' |\n';
        for (const row of sampleRows) {
          const vals = columns.map((col) => {
            const v = row[col];
            const s = v == null ? 'NULL' : String(v);
            return s.length > 40 ? s.substring(0, 37) + '...' : s;
          });
          ctx += '| ' + vals.join(' | ') + ' |\n';
        }
      }

      return ctx;
    },
    [],
  );

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

    const schemaContext = results
      ? buildSchemaContext(results.columns || [], results.metadata || [], results.rows || [], sendDataToAi)
      : '';

    const messages: ChatMessage[] = [
      {
        role: 'system',
        content: `You are an Apache Drill SQL optimization expert. The user will provide a SQL query. Your job:
1. Explain what the query does in 1-2 sentences.
2. Analyze the result schema for data type mismatches. If a VARCHAR column contains dates or numbers, suggest CAST expressions.
3. List numbered optimization suggestions. For each, explain WHY it improves performance.
4. Provide the complete optimized SQL in a single \`\`\`sql code block.
5. Focus on: predicate pushdown, partition pruning, avoiding SELECT *, appropriate use of LIMIT, JOIN ordering, Drill-specific optimizations, and data type corrections.
6. If the query is already optimal, say so and return the original SQL in a \`\`\`sql code block.`,
      },
      {
        role: 'user',
        content: `Please optimize this SQL query:\n\n\`\`\`sql\n${sql}\n\`\`\`` + schemaContext,
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
  }, [sql, aiContext, results, sendDataToAi, buildSchemaContext]);

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

  // Handle loading query from navigation state (from SavedQueriesPage or VisualizationsPage)
  const locationState = location.state as LocationState | undefined;
  useEffect(() => {
    if (locationState?.loadQuery) {
      const query = locationState.loadQuery;
      updateSql(query.sql);
      if (query.defaultSchema) {
        dispatch(setDefaultSchema({ tabId: activeTabId, schema: query.defaultSchema }));
      }
      window.history.replaceState({}, document.title);
    } else if (locationState?.initialSql) {
      updateSql(locationState.initialSql);
      window.history.replaceState({}, document.title);
    }
  }, [locationState, updateSql, dispatch, activeTabId]);

  // Handle format SQL
  const handleFormat = useCallback(() => {
    if (!sql || !sql.trim()) {
      return;
    }
    updateSql(prettifySql(sql));
  }, [sql, updateSql]);

  // Handle execute — runs selected text if available, otherwise full SQL
  const handleExecute = useCallback(() => {
    const editor = editorInstanceRef.current;
    let sqlToRun: string | undefined;
    if (editor) {
      const selection = editor.getSelection();
      if (selection && !selection.isEmpty()) {
        sqlToRun = editor.getModel()?.getValueInRange(selection) || undefined;
      }
    }
    execute({
      autoLimit: autoLimit ?? undefined,
      defaultSchema: activeTab?.defaultSchema,
      sqlOverride: sqlToRun,
    });
  }, [execute, autoLimit, activeTab?.defaultSchema, editorInstanceRef]);

  const handleClearResults = useCallback(() => {
    dispatch(clearResults(activeTabId));
  }, [dispatch, activeTabId]);

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
    (schema: string, table: string, columnNames?: string[]) => {
      const cols = columnNames && columnNames.length > 0
        ? columnNames.map((c) => `\`${c}\``).join(',\n       ')
        : '*';
      // Table function expressions (e.g. Excel sheets) are passed as the table arg
      if (table.startsWith('table(')) {
        const query = `SELECT ${cols}\nFROM ${table}\nLIMIT 100`;
        updateSql(query);
        return;
      }
      // Format schema: plugin unquoted, workspace parts backtick-quoted
      const schemaParts = schema.split('.');
      const formattedSchema = schemaParts.length <= 1
        ? schema
        : schemaParts[0] + '.' + schemaParts.slice(1).map((p) => `\`${p}\``).join('.');
      const query = `SELECT ${cols}\nFROM ${formattedSchema}.\`${table}\`\nLIMIT 100`;
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
        if (tabs.length <= 1) {
          message.warning('Cannot close the last tab');
          return;
        }
        const closingTab = tabs.find((t) => t.id === targetKey);
        const tabSql = closingTab?.sql?.trim() || '';
        const savedSql = (savedSqlRef.current[targetKey] ?? '').trim();
        if (tabSql && tabSql !== savedSql) {
          Modal.confirm({
            title: 'Close tab with unsaved query?',
            content: 'This tab has unsaved changes that will be lost.',
            okText: 'Close anyway',
            okButtonProps: { danger: true },
            cancelText: 'Keep open',
            onOk: () => dispatch(removeTab(targetKey)),
          });
        } else {
          dispatch(removeTab(targetKey));
        }
      }
    },
    [dispatch, tabs]
  );

  // Handle save query — record saved state
  const handleSave = useCallback(() => {
    if (!sql || sql.trim() === '') {
      message.warning('Please enter a SQL query before saving');
      return;
    }
    savedSqlRef.current[activeTabId] = sql;
    setSaveDialogOpen(true);
  }, [sql, activeTabId]);

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

  // ---- Resizable editor/results split (mouse + touch) ----
  const handleDragStart = useCallback((e: React.MouseEvent | React.TouchEvent) => {
    if (!('touches' in e)) {
      e.preventDefault();
    }
    setIsDragging(true);
  }, []);

  useEffect(() => {
    if (!isDragging) {
      return;
    }

    const getClientY = (e: MouseEvent | TouchEvent) =>
      'touches' in e ? e.touches[0].clientY : e.clientY;

    const handleMove = (e: MouseEvent | TouchEvent) => {
      if (!contentRef.current) {
        return;
      }
      if (e.cancelable) {
        e.preventDefault();
      }
      const contentRect = contentRef.current.getBoundingClientRect();
      const tabsHeight = 46;
      const newHeight = getClientY(e) - contentRect.top - tabsHeight;
      const maxHeight = contentRect.height - tabsHeight - 100;
      dispatch(setEditorHeight(Math.max(120, Math.min(newHeight, maxHeight))));
    };

    const handleEnd = () => {
      setIsDragging(false);
    };

    document.addEventListener('mousemove', handleMove);
    document.addEventListener('mouseup', handleEnd);
    document.addEventListener('touchmove', handleMove, { passive: false });
    document.addEventListener('touchend', handleEnd);
    document.body.style.userSelect = 'none';
    document.body.style.cursor = 'row-resize';

    return () => {
      document.removeEventListener('mousemove', handleMove);
      document.removeEventListener('mouseup', handleEnd);
      document.removeEventListener('touchmove', handleMove);
      document.removeEventListener('touchend', handleEnd);
      document.body.style.userSelect = '';
      document.body.style.cursor = '';
    };
  }, [isDragging, dispatch]);

  // Cache results when they arrive
  useEffect(() => {
    if (results && activeTabId) {
      onResultsCached(activeTabId, results, executionTime ?? 0, cacheId);
    }
  }, [results, activeTabId, executionTime, cacheId, onResultsCached]);

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
    // Always stop propagation so keystrokes (Delete, Backspace, etc.) don't
    // bubble up to Ant Design Tabs and accidentally close or switch tabs.
    e.stopPropagation();
    if (e.key === 'Enter') {
      handleTabRenameSubmit();
    } else if (e.key === 'Escape') {
      setEditingTabId(null);
      setEditingTabName('');
    }
  }, [handleTabRenameSubmit]);

  const handleDuplicateTab = useCallback((tabId: string) => {
    dispatch(duplicateTab(tabId));
  }, [dispatch]);

  return (
    <div className="sqllab-main">
      {/* Mobile backdrop — closes any open sidebar on tap */}
      {isMobile && (!sidebarCollapsed || (prospectorOpen && prospectorAvailable)) && (
        <div
          className="mobile-sidebar-backdrop"
          onClick={() => {
            if (!sidebarCollapsed) {
              dispatch(toggleSidebar());
            }
            if (prospectorOpen) {
              toggleProspector();
            }
          }}
        />
      )}

      {/* Schema Explorer Sidebar */}
      <div className={`sqllab-sidebar${sidebarCollapsed ? ' collapsed' : ''}`}>
        {!sidebarCollapsed && (
          <SchemaExplorer
            onInsertText={handleInsertText}
            onTableSelect={handleTableSelect}
            datasetFilter={datasetFilter}
            projectId={projectId}
            savedQueryIds={savedQueryIds}
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
              <span className="tab-label-wrapper" onDoubleClick={() => handleTabDoubleClick(tab.id, tab.name)}>
                <span className="tab-label-text">
                  {tab.name}
                  {tab.results?.rows && (
                    <span className="tab-row-count"> ({tab.results.rows.length.toLocaleString()})</span>
                  )}
                  {tab.sql?.trim() && tab.sql !== (savedSqlRef.current[tab.id] ?? '') && (
                    <span className="tab-unsaved-dot" title="Unsaved changes"> •</span>
                  )}
                </span>
                <Dropdown
                  trigger={['click']}
                  menu={{
                    items: [
                      {
                        key: 'rename',
                        icon: <EditOutlined />,
                        label: 'Rename',
                        onClick: ({ domEvent }) => {
                          domEvent.stopPropagation();
                          // Defer until after the dropdown fully closes, otherwise
                          // its cleanup blur fires onBlur and cancels the rename.
                          setTimeout(() => handleTabDoubleClick(tab.id, tab.name), 0);
                        },
                      },
                      {
                        key: 'duplicate',
                        icon: <CopyOutlined />,
                        label: 'Duplicate',
                        onClick: ({ domEvent }) => {
                          domEvent.stopPropagation();
                          handleDuplicateTab(tab.id);
                        },
                      },
                      { type: 'divider' },
                      {
                        key: 'close',
                        icon: <CloseOutlined />,
                        label: 'Close',
                        danger: true,
                        disabled: tabs.length <= 1,
                        onClick: ({ domEvent }) => {
                          domEvent.stopPropagation();
                          if (tabs.length > 1) {
                            dispatch(removeTab(tab.id));
                          } else {
                            message.warning('Cannot close the last tab');
                          }
                        },
                      },
                    ],
                  }}
                >
                  <span
                    className="tab-menu-btn"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <MoreOutlined />
                  </span>
                </Dropdown>
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
            onFormat={handleFormat}
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
            hasSelection={hasSelection}
            hasError={!!error}
            prospectorOpen={prospectorOpen}
            prospectorAvailable={prospectorAvailable}
          />

          {/* SQL Editor */}
          <SqlEditor
            value={sql}
            onChange={updateSql}
            onExecute={handleExecute}
            onEditorReady={handleEditorReady}
            settings={editorSettings}
          />
        </div>

        {/* Resize Handle */}
        <div
          className={`sqllab-resize-handle${isDragging ? ' dragging' : ''}`}
          onMouseDown={handleDragStart}
          onTouchStart={handleDragStart}
        />

        {/* Results Panel */}
        <div className="sqllab-results-panel">
          <Tabs
            activeKey={resultsPanelTab}
            onChange={setResultsPanelTab}
            size="small"
            style={{ height: '100%' }}
            tabBarStyle={{ margin: '0 12px', marginBottom: 0 }}
            items={[
              {
                key: 'results',
                label: <span><TableOutlined /> Results</span>,
                children: (
                  <div style={{ height: '100%', overflow: 'hidden' }}>
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
                      cacheId={cacheId}
                      sql={sql}
                      onClearResults={handleClearResults}
                      onRerun={handleExecute}
                    />
                  </div>
                ),
              },
              {
                key: 'notebook',
                label: <span><ExperimentOutlined /> Notebook</span>,
                children: (
                  <div style={{ height: '100%', overflow: 'hidden' }}>
                    <NotebookPanel
                      key={activeTabId}
                      tabId={activeTabId}
                      results={results}
                      maxNotebookRows={maxNotebookRows}
                      onMaxNotebookRowsChange={setMaxNotebookRows}
                      tabName={activeTab?.name}
                      onHandle={setNotebookHandle}
                    />
                  </div>
                ),
              },
            ]}
          />
        </div>
      </div>

      {/* Save Query Dialog */}
      <SaveQueryDialog
        open={saveDialogOpen}
        onClose={() => setSaveDialogOpen(false)}
        sql={sql}
        defaultSchema={activeTab?.defaultSchema}
        projectId={projectId}
        onSaved={(name) => dispatch(renameTab({ tabId: activeTabId, name }))}
      />

      {/* Visualization Builder */}
      <VisualizationBuilder
        open={vizBuilderOpen}
        onClose={() => setVizBuilderOpen(false)}
        queryResult={results ?? null}
        sql={sql}
        defaultSchema={activeTab?.defaultSchema}
        projectId={projectId}
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
            <ProspectorPanel
              prospector={prospector}
              context={aiContext}
              onInsertCell={notebookHandle ? (code) => {
                notebookHandle.addCell(code);
                // Switch to notebook tab if not already there
                setResultsPanelTab('notebook');
              } : undefined}
            />
          )}
        </div>
      )}

      {/* Mobile floating Run button */}
      {isMobile && (
        <button
          className={`mobile-run-fab${isExecuting ? ' executing' : ''}`}
          onClick={handleExecute}
          aria-label={isExecuting ? 'Cancel query' : (hasSelection ? 'Run selection' : 'Run query')}
        >
          {isExecuting ? <StopOutlined /> : <PlayCircleOutlined />}
        </button>
      )}

      {/* Keyboard Shortcuts Modal (global) */}
      <KeyboardShortcutsModal open={shortcutsOpen} onClose={() => setShortcutsOpen(false)} />
    </div>
  );
}
