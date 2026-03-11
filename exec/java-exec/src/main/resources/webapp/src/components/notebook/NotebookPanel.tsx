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
import { useState, useCallback, useRef, useEffect, useMemo } from 'react';
import {
  Button,
  Space,
  Typography,
  Spin,
  Alert,
  Tooltip,
  Tag,
  Empty,
  InputNumber,
  Dropdown,
  Modal,
  Input,
  Table,
  Popconfirm,
  Select,
  message,
} from 'antd';
import {
  PlayCircleOutlined,
  PlusOutlined,
  DeleteOutlined,
  ClearOutlined,
  LoadingOutlined,
  DatabaseOutlined,
  ReloadOutlined,
  DownOutlined,
  UpOutlined,
  CodeOutlined,
  AppstoreAddOutlined,
  SyncOutlined,
  SearchOutlined,
  DownloadOutlined,
  EyeOutlined,
  EyeInvisibleOutlined,
  FileTextOutlined,
  SaveOutlined,
  FolderOpenOutlined,
  UnorderedListOutlined,
  FormOutlined,
  CompressOutlined,
  ExpandOutlined,
  CloudUploadOutlined,
} from '@ant-design/icons';
import Editor from '@monaco-editor/react';
import Markdown from 'react-markdown';
import DOMPurify from 'dompurify';
import { usePyodide } from '../../hooks/usePyodide';
import type { PythonOutput, PackageInfo, VariableInfo, WorkspaceInfo } from '../../hooks/usePyodide';
import type { QueryResult } from '../../types';
import { useTheme } from '../../hooks/useTheme';

const { Text } = Typography;

// ─── Cell Types ──────────────────────────────────────────────────────

type CellType = 'code' | 'markdown';

interface NotebookCell {
  id: string;
  type: CellType;
  code: string;
  output: PythonOutput | null;
  running: boolean;
  executionCount: number | null;
  collapsed: boolean;        // collapse input
  outputCollapsed: boolean;  // collapse output
}

export interface NotebookPanelHandle {
  addCell: (code: string) => void;
  getLastCellCode: () => string;
  getLastCellError: () => string | null;
}

interface NotebookPanelProps {
  /** Query tab ID — notebook state is scoped per tab */
  tabId?: string;
  results?: QueryResult;
  maxNotebookRows?: number;
  onMaxNotebookRowsChange?: (rows: number) => void;
  tabName?: string;
  onHandle?: (handle: NotebookPanelHandle | null) => void;
}

// ─── Persistence Types ──────────────────────────────────────────────

interface SavedNotebook {
  name: string;
  cells: { type: CellType; code: string }[];
  savedAt: string;
}

const NOTEBOOKS_KEY = 'drill-notebooks';
const TAB_NOTEBOOKS_KEY = 'drill-tab-notebooks';

interface TabNotebookState {
  cells: { type: CellType; code: string }[];
}

function loadTabNotebook(tabId: string): TabNotebookState | null {
  try {
    const raw = localStorage.getItem(TAB_NOTEBOOKS_KEY);
    const map: Record<string, TabNotebookState> = raw ? JSON.parse(raw) : {};
    return map[tabId] || null;
  } catch {
    return null;
  }
}

function saveTabNotebook(tabId: string, state: TabNotebookState) {
  try {
    const raw = localStorage.getItem(TAB_NOTEBOOKS_KEY);
    const map: Record<string, TabNotebookState> = raw ? JSON.parse(raw) : {};
    map[tabId] = state;
    localStorage.setItem(TAB_NOTEBOOKS_KEY, JSON.stringify(map));
  } catch {
    // localStorage full or unavailable
  }
}

function loadNotebookList(): SavedNotebook[] {
  try {
    const raw = localStorage.getItem(NOTEBOOKS_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function saveNotebookList(notebooks: SavedNotebook[]) {
  localStorage.setItem(NOTEBOOKS_KEY, JSON.stringify(notebooks));
}

// ─── Helpers ────────────────────────────────────────────────────────

let cellIdCounter = 0;

function createCell(code = '', type: CellType = 'code'): NotebookCell {
  return {
    id: `cell-${++cellIdCounter}`,
    type,
    code,
    output: null,
    running: false,
    executionCount: null,
    collapsed: false,
    outputCollapsed: false,
  };
}

function starterCode(varName: string): string {
  return `# Your query results are available as '${varName}'
# Example: explore your data
${varName}.head(10)`;
}

function mlSnippets(v: string) { return [
  { key: 'describe', label: 'Describe Data', code: `${v}.describe()` },
  { key: 'info', label: 'Data Info', code: `print(${v}.info())
print(f"\\nShape: {${v}.shape}")
print(f"Null counts:\\n{${v}.isnull().sum()}")` },
  {
    key: 'correlation',
    label: 'Correlation Matrix',
    code: `import matplotlib.pyplot as plt
import numpy as np

numeric_df = ${v}.select_dtypes(include='number')
corr = numeric_df.corr()

fig, ax = plt.subplots(figsize=(10, 8))
im = ax.imshow(corr, cmap='coolwarm', aspect='auto', vmin=-1, vmax=1)
ax.set_xticks(range(len(corr.columns)))
ax.set_yticks(range(len(corr.columns)))
ax.set_xticklabels(corr.columns, rotation=45, ha='right')
ax.set_yticklabels(corr.columns)
for i in range(len(corr)):
    for j in range(len(corr)):
        ax.text(j, i, f'{corr.iloc[i, j]:.2f}', ha='center', va='center',
                color='white' if abs(corr.iloc[i, j]) > 0.5 else 'black', fontsize=9)
plt.colorbar(im, ax=ax)
ax.set_title('Correlation Matrix')
plt.tight_layout()
plt.show()`,
  },
  {
    key: 'histogram',
    label: 'Histograms',
    code: `import matplotlib.pyplot as plt

numeric_cols = ${v}.select_dtypes(include='number').columns
n = len(numeric_cols)
if n > 0:
    fig, axes = plt.subplots(1, min(n, 4), figsize=(4 * min(n, 4), 4))
    if n == 1:
        axes = [axes]
    for i, col in enumerate(numeric_cols[:4]):
        axes[i].hist(${v}[col].dropna(), bins=30, edgecolor='black', alpha=0.7)
        axes[i].set_title(col)
    plt.tight_layout()
    plt.show()
else:
    print("No numeric columns found")`,
  },
  {
    key: 'scatter',
    label: 'Scatter Plot',
    code: `import matplotlib.pyplot as plt

numeric_cols = ${v}.select_dtypes(include='number').columns.tolist()
if len(numeric_cols) >= 2:
    x_col, y_col = numeric_cols[0], numeric_cols[1]
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(${v}[x_col], ${v}[y_col], alpha=0.6, edgecolors='black', linewidths=0.5)
    ax.set_xlabel(x_col)
    ax.set_ylabel(y_col)
    ax.set_title(f'{y_col} vs {x_col}')
    plt.tight_layout()
    plt.show()
else:
    print("Need at least 2 numeric columns for a scatter plot")`,
  },
  {
    key: 'linear-regression',
    label: 'Linear Regression',
    code: `from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error
import matplotlib.pyplot as plt
import numpy as np

numeric_cols = ${v}.select_dtypes(include='number').columns.tolist()
if len(numeric_cols) >= 2:
    X = ${v}[[numeric_cols[0]]].dropna()
    y = ${v}[numeric_cols[1]].loc[X.index]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = LinearRegression()
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    print(f"R2 Score: {r2_score(y_test, y_pred):.4f}")
    print(f"RMSE: {np.sqrt(mean_squared_error(y_test, y_pred)):.4f}")
    print(f"Coefficient: {model.coef_[0]:.4f}, Intercept: {model.intercept_:.4f}")
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(X_test, y_test, alpha=0.6, label='Actual')
    X_sorted = X_test.sort_values(by=X_test.columns[0])
    ax.plot(X_sorted, model.predict(X_sorted), color='red', linewidth=2, label='Prediction')
    ax.set_xlabel(numeric_cols[0]); ax.set_ylabel(numeric_cols[1])
    ax.legend(); ax.set_title('Linear Regression')
    plt.tight_layout(); plt.show()
else:
    print("Need at least 2 numeric columns")`,
  },
  {
    key: 'kmeans',
    label: 'K-Means Clustering',
    code: `from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

numeric_df = ${v}.select_dtypes(include='number').dropna()
if numeric_df.shape[1] >= 2:
    scaler = StandardScaler()
    scaled = scaler.fit_transform(numeric_df)
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(scaled)
    cols = numeric_df.columns.tolist()
    fig, ax = plt.subplots(figsize=(8, 6))
    scatter = ax.scatter(numeric_df[cols[0]], numeric_df[cols[1]], c=clusters, cmap='viridis', alpha=0.6, edgecolors='black', linewidths=0.5)
    ax.set_xlabel(cols[0]); ax.set_ylabel(cols[1])
    ax.set_title('K-Means Clustering (k=3)')
    plt.colorbar(scatter, ax=ax, label='Cluster')
    plt.tight_layout(); plt.show()
    print(f"Cluster sizes: {dict(zip(*np.unique(clusters, return_counts=True)))}")
    print(f"Inertia: {kmeans.inertia_:.2f}")
else:
    print("Need at least 2 numeric columns for clustering")`,
  },
  {
    key: 'decision-tree',
    label: 'Decision Tree Classifier',
    code: `from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import numpy as np

target_col = ${v}.columns[-1]
feature_cols = ${v}.select_dtypes(include='number').columns.tolist()
feature_cols = [c for c in feature_cols if c != target_col]
if len(feature_cols) >= 1:
    X = ${v}[feature_cols].dropna()
    y = ${v}[target_col].loc[X.index]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = DecisionTreeClassifier(max_depth=5, random_state=42)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    print(classification_report(y_test, y_pred))
    print(f"\\nFeature importances:")
    for feat, imp in sorted(zip(feature_cols, model.feature_importances_), key=lambda x: -x[1]):
        print(f"  {feat}: {imp:.4f}")
else:
    print("Need at least 1 numeric feature column")`,
  },
  {
    key: 'save-to-drill',
    label: 'Save to Drill',
    code: `# Save DataFrame back to Drill
# Change workspace, table_name, and format as needed
await save_to_drill(${v}, workspace='tmp', table_name='my_analysis', format='json')`,
  },
  {
    key: 'pca',
    label: 'PCA Visualization',
    code: `from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

numeric_df = ${v}.select_dtypes(include='number').dropna()
if numeric_df.shape[1] >= 2:
    scaler = StandardScaler()
    scaled = scaler.fit_transform(numeric_df)
    pca = PCA(n_components=2)
    components = pca.fit_transform(scaled)
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(components[:, 0], components[:, 1], alpha=0.6, edgecolors='black', linewidths=0.5)
    ax.set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]:.1%} variance)')
    ax.set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]:.1%} variance)')
    ax.set_title('PCA - First 2 Components')
    plt.tight_layout(); plt.show()
    print(f"Explained variance: {pca.explained_variance_ratio_}")
    print(f"Total explained: {sum(pca.explained_variance_ratio_):.1%}")
else:
    print("Need at least 2 numeric columns for PCA")`,
  },
]; }

// ─── Export Helpers ─────────────────────────────────────────────────

function exportAsPython(cells: NotebookCell[]): string {
  const lines: string[] = ['# Exported from Apache Drill SQL Lab Notebook', ''];
  for (const cell of cells) {
    if (cell.type === 'markdown') {
      cell.code.split('\n').forEach((l) => lines.push(`# ${l}`));
      lines.push('');
    } else {
      lines.push(cell.code, '');
    }
  }
  return lines.join('\n');
}

function exportAsIpynb(cells: NotebookCell[]): string {
  const nb = {
    nbformat: 4,
    nbformat_minor: 5,
    metadata: {
      kernelspec: { display_name: 'Python 3 (Pyodide)', language: 'python', name: 'python3' },
      language_info: { name: 'python', version: '3.11' },
    },
    cells: cells.map((cell) => ({
      cell_type: cell.type === 'markdown' ? 'markdown' : 'code',
      source: cell.code.split('\n').map((l, i, arr) => (i < arr.length - 1 ? l + '\n' : l)),
      metadata: {},
      ...(cell.type === 'code' ? { outputs: [], execution_count: cell.executionCount } : {}),
    })),
  };
  return JSON.stringify(nb, null, 2);
}

function downloadFile(content: string, filename: string, mimeType: string) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function downloadImage(dataUrl: string, filename: string) {
  const a = document.createElement('a');
  a.href = dataUrl;
  a.download = filename;
  a.click();
}

// ─── Main Component ─────────────────────────────────────────────────

export default function NotebookPanel({
  tabId,
  results,
  maxNotebookRows = 50000,
  onMaxNotebookRowsChange,
  tabName = 'df',
  onHandle,
}: NotebookPanelProps) {
  const dfName = useMemo(() => {
    const sanitized = tabName.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, '').replace(/^[0-9]/, '_$&');
    return sanitized || 'df';
  }, [tabName]);

  const { isDark } = useTheme();
  const {
    loading: pyLoading,
    ready: pyReady,
    statusMessage,
    error: pyError,
    initialize,
    runPython,
    injectDataFrame,
    listDataFrames,
    installPackage,
    uninstallPackage,
    listInstalledPackages,
    listAllPackages,
    listVariables,
    resetNamespace,
    saveToDrill,
    listWritableWorkspaces,
  } = usePyodide();

  // ─── State ──────────────────────────────────────────────────────
  // Restore cells from per-tab storage, or create a starter cell
  const [cells, setCells] = useState<NotebookCell[]>(() => {
    if (tabId) {
      const saved = loadTabNotebook(tabId);
      if (saved && saved.cells.length > 0) {
        return saved.cells.map((c) => createCell(c.code, c.type));
      }
    }
    return [createCell(starterCode(dfName))];
  });
  const [dataInjected, setDataInjected] = useState(false);
  const [packagesModalOpen, setPackagesModalOpen] = useState(false);
  const [packageInput, setPackageInput] = useState('');
  const [installingPackage, setInstallingPackage] = useState(false);
  const [allPackages, setAllPackages] = useState<PackageInfo[]>([]);
  const [userInstalledNames, setUserInstalledNames] = useState<string[]>([]);
  const [packageFilter, setPackageFilter] = useState('');
  const [variablesPanelOpen, setVariablesPanelOpen] = useState(false);
  const [variables, setVariables] = useState<VariableInfo[]>([]);
  const [saveModalOpen, setSaveModalOpen] = useState(false);
  const [loadModalOpen, setLoadModalOpen] = useState(false);
  const [saveName, setSaveName] = useState('');
  const [savedNotebooks, setSavedNotebooks] = useState<SavedNotebook[]>([]);
  const [editingMarkdownId, setEditingMarkdownId] = useState<string | null>(null);
  const [exportToDrillOpen, setExportToDrillOpen] = useState(false);
  const [exportWorkspaces, setExportWorkspaces] = useState<WorkspaceInfo[]>([]);
  const [exportPlugin, setExportPlugin] = useState('dfs');
  const [exportWorkspace, setExportWorkspace] = useState('tmp');
  const [exportTableName, setExportTableName] = useState('notebook_export');
  const [exportFormat, setExportFormat] = useState('json');
  const [exportDfName, setExportDfName] = useState('');
  const [exportLoading, setExportLoading] = useState(false);
  const [availableDataFrames, setAvailableDataFrames] = useState<string[]>([]);
  const [exportWritePolicy, setExportWritePolicy] = useState<string>('filesystem');

  const executionCountRef = useRef(0);
  const cellRefs = useRef<Map<string, HTMLDivElement>>(new Map());
  const injectionPromiseRef = useRef<Promise<void> | null>(null);

  // ─── Auto-save cells per tab ─────────────────────────────────────
  useEffect(() => {
    if (tabId) {
      saveTabNotebook(tabId, {
        cells: cells.map((c) => ({ type: c.type, code: c.code })),
      });
    }
  }, [tabId, cells]);

  // ─── Auto-init & inject ─────────────────────────────────────────
  useEffect(() => {
    if (!pyReady && !pyLoading && !pyError) {
      initialize();
    }
  }, [pyReady, pyLoading, pyError, initialize]);

  useEffect(() => {
    if (pyReady && results && results.rows.length > 0) {
      const promise = injectDataFrame(dfName, results.columns, results.rows, maxNotebookRows)
        .then(() => setDataInjected(true))
        .catch(() => {});
      injectionPromiseRef.current = promise;
    }
  }, [pyReady, results, injectDataFrame, maxNotebookRows, dfName]);

  // ─── Cell operations ────────────────────────────────────────────
  const updateCell = useCallback((id: string, updates: Partial<NotebookCell>) => {
    setCells((prev) => prev.map((c) => (c.id === id ? { ...c, ...updates } : c)));
  }, []);

  const ensureDataInjected = useCallback(async () => {
    if (injectionPromiseRef.current) {
      await injectionPromiseRef.current;
    }
    if (!dataInjected && results && results.rows.length > 0 && pyReady) {
      const promise = injectDataFrame(dfName, results.columns, results.rows, maxNotebookRows)
        .then(() => setDataInjected(true))
        .catch(() => {});
      injectionPromiseRef.current = promise;
      await promise;
    }
  }, [dataInjected, results, pyReady, injectDataFrame, maxNotebookRows, dfName]);

  const runCell = useCallback(async (cellId: string) => {
    const cell = cells.find((c) => c.id === cellId);
    if (!cell || !pyReady || cell.type === 'markdown') {
      return;
    }
    updateCell(cellId, { running: true, output: null });
    await ensureDataInjected();
    const count = ++executionCountRef.current;
    const output = await runPython(cell.code);
    updateCell(cellId, { running: false, output, executionCount: count });
    // Refresh variables after each execution
    refreshVariables();
  }, [cells, pyReady, runPython, updateCell, ensureDataInjected]);

  const runAllCells = useCallback(async () => {
    for (const cell of cells) {
      if (cell.type === 'code') {
        await runCell(cell.id);
      }
    }
  }, [cells, runCell]);

  const addCell = useCallback((afterId?: string, code = '', type: CellType = 'code') => {
    const newCell = createCell(code, type);
    setCells((prev) => {
      if (afterId) {
        const idx = prev.findIndex((c) => c.id === afterId);
        const next = [...prev];
        next.splice(idx + 1, 0, newCell);
        return next;
      }
      return [...prev, newCell];
    });
    if (type === 'markdown') {
      setEditingMarkdownId(newCell.id);
    }
    setTimeout(() => {
      const el = cellRefs.current.get(newCell.id);
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    }, 100);
  }, []);

  // Expose handle to parent
  useEffect(() => {
    if (onHandle) {
      onHandle({
        addCell: (code: string) => addCell(undefined, code),
        getLastCellCode: () => {
          const last = cells[cells.length - 1];
          return last?.code || '';
        },
        getLastCellError: () => {
          for (let i = cells.length - 1; i >= 0; i--) {
            if (cells[i].output?.error && cells[i].output?.stderr) {
              return cells[i].output!.stderr;
            }
          }
          return null;
        },
      });
      return () => onHandle(null);
    }
  }, [onHandle, addCell, cells]);

  const deleteCell = useCallback((id: string) => {
    setCells((prev) => prev.length <= 1 ? prev : prev.filter((c) => c.id !== id));
  }, []);

  const moveCell = useCallback((id: string, direction: 'up' | 'down') => {
    setCells((prev) => {
      const idx = prev.findIndex((c) => c.id === id);
      if (idx < 0) { return prev; }
      const newIdx = direction === 'up' ? idx - 1 : idx + 1;
      if (newIdx < 0 || newIdx >= prev.length) { return prev; }
      const next = [...prev];
      [next[idx], next[newIdx]] = [next[newIdx], next[idx]];
      return next;
    });
  }, []);

  const clearOutputs = useCallback(() => {
    setCells((prev) => prev.map((c) => ({ ...c, output: null, executionCount: null })));
    executionCountRef.current = 0;
  }, []);

  const handleReset = useCallback(async () => {
    await resetNamespace();
    clearOutputs();
    setDataInjected(false);
    if (results && results.rows.length > 0) {
      await injectDataFrame(dfName, results.columns, results.rows, maxNotebookRows);
      setDataInjected(true);
    }
    refreshVariables();
  }, [resetNamespace, clearOutputs, results, injectDataFrame, maxNotebookRows, dfName]);

  const toggleCellType = useCallback((id: string) => {
    setCells((prev) => prev.map((c) => {
      if (c.id !== id) { return c; }
      const newType: CellType = c.type === 'code' ? 'markdown' : 'code';
      if (newType === 'markdown') { setEditingMarkdownId(c.id); }
      return { ...c, type: newType, output: null, executionCount: null };
    }));
  }, []);

  // ─── Variables ──────────────────────────────────────────────────
  const refreshVariables = useCallback(async () => {
    if (!pyReady) { return; }
    const vars = await listVariables();
    setVariables(vars);
  }, [pyReady, listVariables]);

  // ─── Packages ─────────────────────────────────────────────────
  const refreshPackages = useCallback(async () => {
    const [all, userInstalled] = await Promise.all([listAllPackages(), listInstalledPackages()]);
    setAllPackages(all);
    setUserInstalledNames(userInstalled);
  }, [listAllPackages, listInstalledPackages]);

  const handleInstallPackage = useCallback(async () => {
    const pkg = packageInput.trim();
    if (!pkg) { return; }
    setInstallingPackage(true);
    const output = await installPackage(pkg);
    setInstallingPackage(false);
    if (output.error) {
      message.error(`Failed to install '${pkg}': ${output.stderr}`);
    } else {
      message.success(`Installed '${pkg}'`);
      setPackageInput('');
      refreshPackages();
    }
  }, [packageInput, installPackage, refreshPackages]);

  const handleUpdatePackage = useCallback(async (pkg: string) => {
    setInstallingPackage(true);
    const output = await runPython(`await micropip.install('${pkg.replace(/'/g, "\\'")}', keep_going=True)\nprint(f"Updated '${pkg.replace(/'/g, "\\'")}' successfully")`);
    setInstallingPackage(false);
    if (output.error) { message.error(`Failed to update '${pkg}': ${output.stderr}`); }
    else { message.success(`Updated '${pkg}'`); refreshPackages(); }
  }, [runPython, refreshPackages]);

  const handleUninstallPackage = useCallback(async (pkg: string) => {
    setInstallingPackage(true);
    const output = await uninstallPackage(pkg);
    setInstallingPackage(false);
    if (output.error) { message.error(`Failed to uninstall '${pkg}': ${output.stderr}`); }
    else { message.success(`Uninstalled '${pkg}'`); refreshPackages(); }
  }, [uninstallPackage, refreshPackages]);

  const handleOpenPackages = useCallback(() => {
    setPackagesModalOpen(true);
    setPackageFilter('');
    refreshPackages();
  }, [refreshPackages]);

  // ─── Save/Load ────────────────────────────────────────────────
  const handleSave = useCallback(() => {
    const name = saveName.trim();
    if (!name) { return; }
    const notebooks = loadNotebookList();
    const existing = notebooks.findIndex((n) => n.name === name);
    const entry: SavedNotebook = {
      name,
      cells: cells.map((c) => ({ type: c.type, code: c.code })),
      savedAt: new Date().toISOString(),
    };
    if (existing >= 0) {
      notebooks[existing] = entry;
    } else {
      notebooks.push(entry);
    }
    saveNotebookList(notebooks);
    setSaveModalOpen(false);
    setSaveName('');
    message.success(`Notebook "${name}" saved`);
  }, [saveName, cells]);

  const handleLoad = useCallback((notebook: SavedNotebook) => {
    const loaded = notebook.cells.map((c) => createCell(c.code, c.type));
    setCells(loaded.length > 0 ? loaded : [createCell(starterCode(dfName))]);
    setLoadModalOpen(false);
    message.success(`Loaded "${notebook.name}"`);
  }, [dfName]);

  const handleDeleteNotebook = useCallback((name: string) => {
    const notebooks = loadNotebookList().filter((n) => n.name !== name);
    saveNotebookList(notebooks);
    setSavedNotebooks(notebooks);
    message.success(`Deleted "${name}"`);
  }, []);

  // ─── Save to Drill ──────────────────────────────────────────
  const handleOpenExportToDrill = useCallback(async () => {
    setExportToDrillOpen(true);
    setExportTableName('notebook_export');
    setExportFormat('json');
    setExportLoading(false);
    const [wsResult, dfs] = await Promise.all([listWritableWorkspaces(), listDataFrames()]);
    setExportWorkspaces(wsResult.workspaces);
    setExportWritePolicy(wsResult.writePolicy);
    setAvailableDataFrames(dfs);
    setExportDfName(dfs.length > 0 ? dfs[0] : dfName);
    if (wsResult.workspaces.length > 0) {
      setExportPlugin(wsResult.workspaces[0].plugin);
      setExportWorkspace(wsResult.workspaces[0].workspace);
    }
  }, [listWritableWorkspaces, listDataFrames, dfName]);

  const handleExportToDrill = useCallback(async () => {
    if (!exportDfName || !exportTableName.trim()) {
      message.warning('Please fill in all fields');
      return;
    }
    setExportLoading(true);
    const output = await saveToDrill(exportDfName, exportPlugin, exportWorkspace, exportTableName.trim(), exportFormat);
    setExportLoading(false);
    if (output.error) {
      message.error(`Export failed: ${output.stderr}`);
    } else {
      message.success('Data exported to Drill successfully');
      setExportToDrillOpen(false);
    }
  }, [exportDfName, exportPlugin, exportWorkspace, exportTableName, exportFormat, saveToDrill]);

  // ─── Export ───────────────────────────────────────────────────
  const handleExportPy = useCallback(() => {
    downloadFile(exportAsPython(cells), 'notebook.py', 'text/x-python');
  }, [cells]);

  const handleExportIpynb = useCallback(() => {
    downloadFile(exportAsIpynb(cells), 'notebook.ipynb', 'application/json');
  }, [cells]);

  // ─── Loading / Error / Empty states ───────────────────────────
  if (pyLoading) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100%', gap: 16, padding: 24 }}>
        <Spin indicator={<LoadingOutlined style={{ fontSize: 32 }} />} />
        <Text style={{ fontSize: 14 }}>{statusMessage}</Text>
        <Text type="secondary" style={{ fontSize: 12 }}>First load downloads ~30MB of Python packages. Subsequent loads are cached.</Text>
      </div>
    );
  }
  if (pyError) {
    return (
      <div style={{ padding: 24 }}>
        <Alert type="error" message="Failed to load Python runtime" description={pyError} action={<Button onClick={initialize}>Retry</Button>} />
      </div>
    );
  }
  if (!results || results.rows.length === 0) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
        <Empty description="Run a SQL query first, then switch to the Notebook tab to analyze results with Python" />
      </div>
    );
  }

  const snippetMenuItems = mlSnippets(dfName).map((s) => ({
    key: s.key,
    label: s.label,
    onClick: () => addCell(undefined, s.code),
  }));

  const exportMenuItems = [
    { key: 'py', label: 'Export as .py', icon: <CodeOutlined />, onClick: handleExportPy },
    { key: 'ipynb', label: 'Export as .ipynb', icon: <FileTextOutlined />, onClick: handleExportIpynb },
  ];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' }}>
      {/* ─── Toolbar ─── */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '6px 12px', borderBottom: '1px solid var(--color-border)',
        background: 'var(--color-bg-elevated)', flexShrink: 0, flexWrap: 'wrap', gap: 4,
      }}>
        <Space size={6} wrap>
          <Button size="small" icon={<PlayCircleOutlined />} onClick={runAllCells} disabled={!pyReady}>Run All</Button>
          <Dropdown menu={{ items: [
            { key: 'code', label: 'Code Cell', icon: <CodeOutlined />, onClick: () => addCell() },
            { key: 'markdown', label: 'Markdown Cell', icon: <FormOutlined />, onClick: () => addCell(undefined, '## Title\n\nWrite your notes here...', 'markdown') },
          ] }}>
            <Button size="small" icon={<PlusOutlined />}>Add Cell</Button>
          </Dropdown>
          <Dropdown menu={{ items: snippetMenuItems }}>
            <Button size="small" icon={<CodeOutlined />}>Snippets</Button>
          </Dropdown>
          <Button size="small" icon={<AppstoreAddOutlined />} onClick={handleOpenPackages}>Packages</Button>
          <Button size="small" icon={<UnorderedListOutlined />} onClick={() => { setVariablesPanelOpen(!variablesPanelOpen); refreshVariables(); }}
            type={variablesPanelOpen ? 'primary' : 'default'}>Variables</Button>
          <Button size="small" icon={<ClearOutlined />} onClick={clearOutputs}>Clear</Button>
          <Button size="small" icon={<ReloadOutlined />} onClick={handleReset}>Reset</Button>
          <Dropdown menu={{ items: [
            { key: 'save', label: 'Save Notebook', icon: <SaveOutlined />, onClick: () => setSaveModalOpen(true) },
            { key: 'load', label: 'Open Notebook', icon: <FolderOpenOutlined />, onClick: () => { setSavedNotebooks(loadNotebookList()); setLoadModalOpen(true); } },
            { type: 'divider' as const },
            ...exportMenuItems,
          ] }}>
            <Button size="small" icon={<DownloadOutlined />}>File</Button>
          </Dropdown>
          <Button size="small" icon={<CloudUploadOutlined />} onClick={handleOpenExportToDrill}
            disabled={!pyReady || !dataInjected}>Save to Drill</Button>
        </Space>
        <Space size={8}>
          {dataInjected && (
            <Tag color="green" icon={<DatabaseOutlined />}>{dfName}: {results.rows.length.toLocaleString()} rows</Tag>
          )}
          <Tooltip title="Maximum rows to load into the notebook">
            <Space size={4}>
              <Text type="secondary" style={{ fontSize: 11 }}>Max rows:</Text>
              <InputNumber size="small" min={100} max={200000} step={1000}
                value={maxNotebookRows} onChange={(v) => onMaxNotebookRowsChange?.(v ?? 50000)}
                style={{ width: 90 }} />
            </Space>
          </Tooltip>
        </Space>
      </div>

      {/* ─── Main content area ─── */}
      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>

        {/* ─── Cells ─── */}
        <div style={{ flex: 1, overflow: 'auto', padding: 12 }}>
          {cells.map((cell, idx) => (
            <div
              key={cell.id}
              ref={(el) => { if (el) { cellRefs.current.set(cell.id, el); } }}
              className={`notebook-cell notebook-cell-${cell.type}`}
              style={{
                marginBottom: 12, border: '1px solid var(--color-border)',
                borderRadius: 6, overflow: 'hidden', background: 'var(--color-bg-container)',
                borderLeft: cell.type === 'markdown' ? '3px solid #722ed1' : '3px solid #1677ff',
              }}
            >
              {/* Cell header */}
              <div style={{
                display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                padding: '3px 8px', background: 'var(--color-bg-elevated)',
                borderBottom: '1px solid var(--color-border)', minHeight: 30,
              }}>
                <Space size={4}>
                  {cell.type === 'code' && (
                    <Text type="secondary" style={{ fontSize: 11, fontFamily: 'monospace', width: 50 }}>
                      [{cell.executionCount ?? ' '}]
                    </Text>
                  )}
                  {cell.type === 'code' ? (
                    <Tooltip title="Run cell (Shift+Enter)">
                      <Button size="small" type="text"
                        icon={cell.running ? <LoadingOutlined /> : <PlayCircleOutlined />}
                        onClick={() => runCell(cell.id)} disabled={!pyReady || cell.running} />
                    </Tooltip>
                  ) : (
                    <Tag color="purple" style={{ fontSize: 10, margin: 0 }}>Markdown</Tag>
                  )}
                  <Tooltip title={cell.type === 'code' ? 'Convert to Markdown' : 'Convert to Code'}>
                    <Button size="small" type="text"
                      icon={cell.type === 'code' ? <FormOutlined /> : <CodeOutlined />}
                      onClick={() => toggleCellType(cell.id)} />
                  </Tooltip>
                </Space>
                <Space size={0}>
                  {cell.type === 'code' && (
                    <Tooltip title={cell.collapsed ? 'Show input' : 'Hide input'}>
                      <Button size="small" type="text"
                        icon={cell.collapsed ? <ExpandOutlined /> : <CompressOutlined />}
                        onClick={() => updateCell(cell.id, { collapsed: !cell.collapsed })} />
                    </Tooltip>
                  )}
                  {cell.output && (
                    <Tooltip title={cell.outputCollapsed ? 'Show output' : 'Hide output'}>
                      <Button size="small" type="text"
                        icon={cell.outputCollapsed ? <EyeOutlined /> : <EyeInvisibleOutlined />}
                        onClick={() => updateCell(cell.id, { outputCollapsed: !cell.outputCollapsed })} />
                    </Tooltip>
                  )}
                  <Tooltip title="Move up">
                    <Button size="small" type="text" icon={<UpOutlined />}
                      onClick={() => moveCell(cell.id, 'up')} disabled={idx === 0} />
                  </Tooltip>
                  <Tooltip title="Move down">
                    <Button size="small" type="text" icon={<DownOutlined />}
                      onClick={() => moveCell(cell.id, 'down')} disabled={idx === cells.length - 1} />
                  </Tooltip>
                  <Tooltip title="Delete cell">
                    <Button size="small" type="text" danger icon={<DeleteOutlined />}
                      onClick={() => deleteCell(cell.id)} disabled={cells.length <= 1} />
                  </Tooltip>
                </Space>
              </div>

              {/* Cell body */}
              {cell.type === 'markdown' ? (
                // Markdown cell
                editingMarkdownId === cell.id ? (
                  <div>
                    <Editor
                      height={Math.max(60, Math.min(200, (cell.code.split('\n').length + 1) * 19))}
                      language="markdown"
                      theme={isDark ? 'vs-dark' : 'light'}
                      value={cell.code}
                      onChange={(value) => updateCell(cell.id, { code: value || '' })}
                      options={{
                        minimap: { enabled: false }, lineNumbers: 'off', scrollBeyondLastLine: false,
                        fontSize: 13, tabSize: 2, wordWrap: 'on', automaticLayout: true,
                        folding: false, renderLineHighlight: 'none', overviewRulerLanes: 0,
                        scrollbar: { vertical: 'hidden', horizontal: 'auto' },
                        padding: { top: 4, bottom: 4 },
                      }}
                      onMount={(editor) => {
                        editor.addAction({
                          id: 'finish-edit',
                          label: 'Finish Editing',
                          keybindings: [
                            // eslint-disable-next-line @typescript-eslint/no-explicit-any
                            (window as any).monaco?.KeyMod.Shift | (window as any).monaco?.KeyCode.Enter,
                          ],
                          run: () => setEditingMarkdownId(null),
                        });
                      }}
                    />
                    <div style={{ padding: '4px 8px', borderTop: '1px solid var(--color-border)', background: 'var(--color-bg-elevated)' }}>
                      <Button size="small" type="link" onClick={() => setEditingMarkdownId(null)}>Done editing (Shift+Enter)</Button>
                    </div>
                  </div>
                ) : (
                  <div
                    style={{ padding: '8px 16px', cursor: 'pointer', minHeight: 32 }}
                    onDoubleClick={() => setEditingMarkdownId(cell.id)}
                  >
                    <div className="notebook-markdown-rendered">
                      <Markdown>{cell.code || '*Double-click to edit*'}</Markdown>
                    </div>
                  </div>
                )
              ) : (
                // Code cell
                !cell.collapsed && (
                  <div style={{ borderBottom: cell.output && !cell.outputCollapsed ? '1px solid var(--color-border)' : undefined }}>
                    <Editor
                      height={Math.max(60, Math.min(300, (cell.code.split('\n').length + 1) * 19))}
                      language="python"
                      theme={isDark ? 'vs-dark' : 'light'}
                      value={cell.code}
                      onChange={(value) => updateCell(cell.id, { code: value || '' })}
                      options={{
                        minimap: { enabled: false }, lineNumbers: 'on', scrollBeyondLastLine: false,
                        fontSize: 13, tabSize: 4, wordWrap: 'on', automaticLayout: true,
                        folding: false, renderLineHighlight: 'none', overviewRulerLanes: 0,
                        scrollbar: { vertical: 'hidden', horizontal: 'auto' },
                        padding: { top: 4, bottom: 4 },
                      }}
                      onMount={(editor) => {
                        // Shift+Enter: run cell
                        editor.addAction({
                          id: 'run-cell',
                          label: 'Run Cell',
                          keybindings: [
                            // eslint-disable-next-line @typescript-eslint/no-explicit-any
                            (window as any).monaco?.KeyMod.Shift | (window as any).monaco?.KeyCode.Enter,
                          ],
                          run: () => runCell(cell.id),
                        });
                      }}
                    />
                  </div>
                )
              )}

              {/* Code cell output */}
              {cell.type === 'code' && cell.output && !cell.outputCollapsed && (
                <div style={{ padding: '8px 12px', maxHeight: 500, overflow: 'auto' }}>
                  {/* HTML table output (rich DataFrames) */}
                  {cell.output.htmlResult && (
                    <div className="notebook-df-output"
                      dangerouslySetInnerHTML={{ __html: DOMPurify.sanitize(cell.output.htmlResult) }}
                    />
                  )}
                  {/* stdout */}
                  {cell.output.stdout && (
                    <pre style={{ margin: cell.output.htmlResult ? '8px 0 0' : 0, fontSize: 12, fontFamily: 'monospace', whiteSpace: 'pre-wrap', color: 'var(--color-text)' }}>
                      {cell.output.stdout}
                    </pre>
                  )}
                  {/* plain result (only if no HTML) */}
                  {cell.output.result && !cell.output.error && !cell.output.htmlResult && (
                    <pre style={{ margin: cell.output.stdout ? '4px 0 0' : 0, fontSize: 12, fontFamily: 'monospace', whiteSpace: 'pre-wrap', color: '#3b82f6' }}>
                      {cell.output.result}
                    </pre>
                  )}
                  {/* stderr / error */}
                  {cell.output.stderr && (
                    <pre style={{
                      margin: (cell.output.stdout || cell.output.result || cell.output.htmlResult) ? '4px 0 0' : 0,
                      fontSize: 12, fontFamily: 'monospace', whiteSpace: 'pre-wrap', color: '#ff4d4f',
                      background: isDark ? 'rgba(255,77,79,0.1)' : '#fff1f0', padding: '6px 8px', borderRadius: 4,
                    }}>
                      {cell.output.stderr}
                    </pre>
                  )}
                  {/* matplotlib image */}
                  {cell.output.imageData && (
                    <div style={{ marginTop: (cell.output.stdout || cell.output.result || cell.output.htmlResult) ? 8 : 0, position: 'relative' }}>
                      <img src={`data:image/png;base64,${cell.output.imageData}`} alt="Plot output"
                        style={{ maxWidth: '100%', borderRadius: 4 }} />
                      <Tooltip title="Download plot as PNG">
                        <Button size="small" type="default" icon={<DownloadOutlined />}
                          style={{ position: 'absolute', top: 4, right: 4, opacity: 0.8 }}
                          onClick={() => downloadImage(`data:image/png;base64,${cell.output!.imageData}`, `plot-${cell.executionCount || 'output'}.png`)}
                        />
                      </Tooltip>
                    </div>
                  )}
                </div>
              )}

              {/* Running indicator */}
              {cell.running && (
                <div style={{ padding: '8px 12px' }}>
                  <Spin size="small" /> <Text type="secondary" style={{ fontSize: 12, marginLeft: 8 }}>Running...</Text>
                </div>
              )}
            </div>
          ))}

          {/* Add cell buttons at bottom */}
          <div style={{ textAlign: 'center', padding: '8px 0' }}>
            <Space size={8}>
              <Button type="dashed" size="small" icon={<PlusOutlined />} onClick={() => addCell()} style={{ width: 150 }}>
                Code Cell
              </Button>
              <Button type="dashed" size="small" icon={<FormOutlined />}
                onClick={() => addCell(undefined, '## Title\n\nWrite your notes here...', 'markdown')}
                style={{ width: 150 }}>
                Markdown Cell
              </Button>
            </Space>
          </div>
        </div>

        {/* ─── Variables Panel ─── */}
        {variablesPanelOpen && (
          <div style={{
            width: 280, borderLeft: '1px solid var(--color-border)', overflow: 'auto',
            background: 'var(--color-bg-container)', flexShrink: 0,
          }}>
            <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--color-border)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Text strong style={{ fontSize: 13 }}>Variables</Text>
              <Button size="small" type="text" icon={<SyncOutlined />} onClick={refreshVariables} />
            </div>
            {variables.length === 0 ? (
              <div style={{ padding: 16, textAlign: 'center' }}>
                <Text type="secondary" style={{ fontSize: 12 }}>No variables yet. Run a cell to see variables here.</Text>
              </div>
            ) : (
              <div style={{ padding: 4 }}>
                {variables.map((v) => (
                  <div key={v.name} style={{
                    padding: '6px 8px', borderBottom: '1px solid var(--color-border)',
                    fontSize: 12,
                  }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 2 }}>
                      <Text strong style={{ fontSize: 12, fontFamily: 'monospace' }}>{v.name}</Text>
                      <Tag style={{ fontSize: 10, margin: 0 }}>{v.type}</Tag>
                    </div>
                    {v.shape && <Text type="secondary" style={{ fontSize: 11 }}>{v.shape}</Text>}
                    {v.size && <Text type="secondary" style={{ fontSize: 11, marginLeft: 8 }}>{v.size}</Text>}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      {/* ─── Packages Modal ─── */}
      <Modal title="Python Packages" open={packagesModalOpen} onCancel={() => setPackagesModalOpen(false)} footer={null} width={640}>
        <div style={{ marginBottom: 16 }}>
          <Text type="secondary">
            Install packages from PyPI. Pure Python packages generally work. Packages with C extensions need Pyodide-specific builds.
          </Text>
        </div>
        <Space.Compact style={{ width: '100%', marginBottom: 16 }}>
          <Input placeholder="Package name (e.g., seaborn)" value={packageInput}
            onChange={(e) => setPackageInput(e.target.value)} onPressEnter={handleInstallPackage} disabled={installingPackage} />
          <Button type="primary" onClick={handleInstallPackage} loading={installingPackage} disabled={!packageInput.trim()}>Install</Button>
        </Space.Compact>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
          <Text strong style={{ fontSize: 13 }}>Installed Packages ({allPackages.length})</Text>
          <Input placeholder="Filter..." prefix={<SearchOutlined />} size="small" value={packageFilter}
            onChange={(e) => setPackageFilter(e.target.value)} allowClear style={{ width: 200 }} />
        </div>
        <Table size="small"
          dataSource={allPackages.filter((p) => !packageFilter || p.name.toLowerCase().includes(packageFilter.toLowerCase()))}
          rowKey="name" pagination={{ pageSize: 15, size: 'small', showSizeChanger: false }} scroll={{ y: 400 }}
          columns={[
            {
              title: 'Package', dataIndex: 'name', key: 'name',
              sorter: (a: PackageInfo, b: PackageInfo) => a.name.localeCompare(b.name),
              defaultSortOrder: 'ascend' as const,
              render: (name: string) => {
                const isCore = ['pandas', 'numpy', 'matplotlib', 'scikit-learn', 'scipy', 'micropip'].includes(name);
                const isUser = userInstalledNames.includes(name);
                return (<span>{name}{isCore && <Tag color="green" style={{ marginLeft: 6, fontSize: 10 }}>core</Tag>}{isUser && <Tag color="blue" style={{ marginLeft: 6, fontSize: 10 }}>user</Tag>}</span>);
              },
            },
            { title: 'Version', dataIndex: 'version', key: 'version', width: 120, render: (v: string) => <Text code style={{ fontSize: 11 }}>{v}</Text> },
            {
              title: 'Actions', key: 'actions', width: 120,
              render: (_: unknown, record: PackageInfo) => {
                const isCore = ['pandas', 'numpy', 'matplotlib', 'scikit-learn', 'scipy', 'micropip'].includes(record.name);
                const isUser = userInstalledNames.includes(record.name);
                return (
                  <Space size={4}>
                    <Tooltip title="Update"><Button size="small" type="text" icon={<SyncOutlined />} onClick={() => handleUpdatePackage(record.name)} disabled={installingPackage || isCore} /></Tooltip>
                    {isUser && (
                      <Popconfirm title={`Uninstall ${record.name}?`} onConfirm={() => handleUninstallPackage(record.name)} okText="Uninstall" cancelText="Cancel">
                        <Tooltip title="Uninstall"><Button size="small" type="text" danger icon={<DeleteOutlined />} disabled={installingPackage} /></Tooltip>
                      </Popconfirm>
                    )}
                  </Space>
                );
              },
            },
          ]}
        />
        <div style={{ marginTop: 12, padding: '8px 12px', background: 'var(--color-bg-elevated)', borderRadius: 6 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>Install in a cell: <Text code style={{ fontSize: 12 }}>await install('seaborn')</Text></Text>
        </div>
      </Modal>

      {/* ─── Save Modal ─── */}
      <Modal title="Save Notebook" open={saveModalOpen} onCancel={() => setSaveModalOpen(false)}
        onOk={handleSave} okText="Save" okButtonProps={{ disabled: !saveName.trim() }}>
        <Input placeholder="Notebook name" value={saveName} onChange={(e) => setSaveName(e.target.value)}
          onPressEnter={handleSave} autoFocus />
        {loadNotebookList().some((n) => n.name === saveName.trim()) && (
          <Text type="warning" style={{ fontSize: 12, marginTop: 8, display: 'block' }}>
            A notebook with this name already exists and will be overwritten.
          </Text>
        )}
      </Modal>

      {/* ─── Load Modal ─── */}
      <Modal title="Open Notebook" open={loadModalOpen} onCancel={() => setLoadModalOpen(false)} footer={null} width={500}>
        {savedNotebooks.length === 0 ? (
          <Empty description="No saved notebooks" />
        ) : (
          <Table size="small" dataSource={savedNotebooks} rowKey="name" pagination={false}
            columns={[
              { title: 'Name', dataIndex: 'name', key: 'name' },
              { title: 'Cells', key: 'cells', width: 60, render: (_: unknown, r: SavedNotebook) => r.cells.length },
              { title: 'Saved', dataIndex: 'savedAt', key: 'savedAt', width: 160,
                render: (d: string) => new Date(d).toLocaleString() },
              {
                title: '', key: 'actions', width: 100,
                render: (_: unknown, r: SavedNotebook) => (
                  <Space size={4}>
                    <Button size="small" type="link" onClick={() => handleLoad(r)}>Open</Button>
                    <Popconfirm title="Delete this notebook?" onConfirm={() => handleDeleteNotebook(r.name)}>
                      <Button size="small" type="link" danger>Delete</Button>
                    </Popconfirm>
                  </Space>
                ),
              },
            ]}
          />
        )}
      </Modal>

      {/* ─── Save to Drill Modal ─── */}
      <Modal
        title={<><CloudUploadOutlined style={{ marginRight: 8 }} />Save to Drill</>}
        open={exportToDrillOpen}
        onCancel={() => setExportToDrillOpen(false)}
        onOk={handleExportToDrill}
        okText="Export"
        okButtonProps={{ loading: exportLoading, disabled: exportWritePolicy === 'disabled' || !exportDfName || !exportTableName.trim() }}
        width={480}
      >
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16, marginTop: 16 }}>
          {exportWritePolicy === 'disabled' && (
            <Alert
              type="warning"
              message="Export Disabled"
              description="Notebook data export has been disabled by the administrator."
              showIcon
            />
          )}
          {exportWritePolicy === 'admin_only' && exportWorkspaces.length === 0 && (
            <Alert
              type="info"
              message="Admin Only"
              description="Notebook data export is restricted to administrators."
              showIcon
            />
          )}
          <div>
            <Text strong style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>DataFrame</Text>
            <Select
              value={exportDfName}
              onChange={setExportDfName}
              style={{ width: '100%' }}
              options={availableDataFrames.map((name) => ({ value: name, label: name }))}
              placeholder="Select a DataFrame"
            />
          </div>
          <div>
            <Text strong style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>Workspace</Text>
            <Select
              value={`${exportPlugin}.${exportWorkspace}`}
              onChange={(val) => {
                const [p, w] = val.split('.');
                setExportPlugin(p);
                setExportWorkspace(w);
              }}
              style={{ width: '100%' }}
              options={exportWorkspaces.map((ws) => ({
                value: `${ws.plugin}.${ws.workspace}`,
                label: `${ws.plugin}.${ws.workspace}`,
              }))}
              placeholder="Select a writable workspace"
            />
            {exportWorkspaces.length === 0 && (
              <Text type="warning" style={{ fontSize: 12, marginTop: 4, display: 'block' }}>
                No writable workspaces found. Configure a writable workspace in Storage settings.
              </Text>
            )}
          </div>
          <div>
            <Text strong style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>Table Name</Text>
            <Input
              value={exportTableName}
              onChange={(e) => setExportTableName(e.target.value)}
              placeholder="e.g., my_analysis"
            />
          </div>
          <div>
            <Text strong style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>Format</Text>
            <Select
              value={exportFormat}
              onChange={setExportFormat}
              style={{ width: '100%' }}
              options={[
                { value: 'json', label: 'JSON' },
                { value: 'csv', label: 'CSV' },
              ]}
            />
          </div>
          <div style={{ padding: '8px 12px', background: 'var(--color-bg-elevated)', borderRadius: 6 }}>
            <Text type="secondary" style={{ fontSize: 12 }}>
              Or use in a cell: <Text code style={{ fontSize: 12 }}>await save_to_drill({exportDfName || 'df'}, workspace='tmp', table_name='my_data')</Text>
            </Text>
          </div>
        </div>
      </Modal>
    </div>
  );
}
