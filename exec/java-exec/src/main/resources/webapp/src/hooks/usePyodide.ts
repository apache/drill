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
import { useState, useRef, useCallback } from 'react';
import { tableFromJSON, tableToIPC } from 'apache-arrow';

/**
 * Pyodide runtime wrapper. Lazily loads Pyodide from CDN on first use
 * and pre-installs pandas, numpy, matplotlib, scikit-learn.
 */

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type PyodideInterface = any;

// Track whether pyarrow has been loaded
let pyarrowLoaded = false;

export interface PythonOutput {
  stdout: string;
  stderr: string;
  result: string | null;
  htmlResult: string | null; // HTML table from DataFrame rendering
  imageData: string | null; // base64 PNG from matplotlib
  error: boolean;
}

export interface PackageInfo {
  name: string;
  version: string;
}

export interface VariableInfo {
  name: string;
  type: string;
  shape: string;
  size: string;
}

export interface WorkspaceInfo {
  plugin: string;
  workspace: string;
  location: string;
  writable: boolean;
}

export interface WorkspacesResult {
  workspaces: WorkspaceInfo[];
  writePolicy: string;
}

// Get CSRF token from meta tag or cookie
function getCsrfToken(): string | null {
  const metaTag = document.querySelector('meta[name="csrf-token"]');
  if (metaTag) {
    return metaTag.getAttribute('content');
  }
  const cookies = document.cookie.split(';');
  for (const cookie of cookies) {
    const [name, value] = cookie.trim().split('=');
    if (name === 'drill.csrf.token') {
      return decodeURIComponent(value);
    }
  }
  return null;
}

interface UsePyodideReturn {
  loading: boolean;
  ready: boolean;
  statusMessage: string;
  error: string | null;
  initialize: () => Promise<void>;
  runPython: (code: string) => Promise<PythonOutput>;
  injectDataFrame: (
    name: string,
    columns: string[],
    rows: Record<string, unknown>[],
    maxRows?: number,
  ) => Promise<void>;
  listDataFrames: () => Promise<string[]>;
  installPackage: (packageName: string) => Promise<PythonOutput>;
  uninstallPackage: (packageName: string) => Promise<PythonOutput>;
  listInstalledPackages: () => Promise<string[]>;
  listAllPackages: () => Promise<PackageInfo[]>;
  listVariables: () => Promise<VariableInfo[]>;
  resetNamespace: () => Promise<void>;
  saveToDrill: (dfName: string, plugin: string, workspace: string, tableName: string, format: string) => Promise<PythonOutput>;
  listWritableWorkspaces: () => Promise<WorkspacesResult>;
}

// Singleton Pyodide instance shared across all hook consumers
let pyodideInstance: PyodideInterface | null = null;
let pyodidePromise: Promise<PyodideInterface> | null = null;

const PYODIDE_CDN = 'https://cdn.jsdelivr.net/pyodide/v0.26.4/full/';

async function loadPyodideRuntime(
  onStatus: (msg: string) => void,
): Promise<PyodideInterface> {
  if (pyodideInstance) {
    return pyodideInstance;
  }
  if (pyodidePromise) {
    return pyodidePromise;
  }

  pyodidePromise = (async () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  if (!(window as any).loadPyodide) {
      onStatus('Downloading Python runtime...');
      await new Promise<void>((resolve, reject) => {
        const script = document.createElement('script');
        script.src = `${PYODIDE_CDN}pyodide.js`;
        script.onload = () => resolve();
        script.onerror = () => reject(new Error('Failed to load Pyodide script'));
        document.head.appendChild(script);
      });
    }

    onStatus('Initializing Python runtime...');
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const loadPyodide = (window as any).loadPyodide;
    const pyodide = await loadPyodide({
      indexURL: PYODIDE_CDN,
    });

    onStatus('Installing pandas...');
    await pyodide.loadPackage('pandas');
    onStatus('Installing numpy...');
    await pyodide.loadPackage('numpy');
    onStatus('Installing matplotlib...');
    await pyodide.loadPackage('matplotlib');
    onStatus('Installing scikit-learn...');
    await pyodide.loadPackage('scikit-learn');
    onStatus('Installing scipy...');
    await pyodide.loadPackage('scipy');
    onStatus('Installing micropip...');
    await pyodide.loadPackage('micropip');

    await pyodide.runPythonAsync(`
import matplotlib
matplotlib.use('AGG')
import matplotlib.pyplot as plt
import io, base64, sys, json
import pandas as pd
import numpy as np
import micropip

# Helper to capture matplotlib output as base64 PNG
def _drill_capture_plot():
    buf = io.BytesIO()
    fig = plt.gcf()
    if fig.get_axes():
        fig.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        buf.seek(0)
        img = base64.b64encode(buf.read()).decode('utf-8')
        plt.close('all')
        return img
    return None

# Helper to render DataFrames as HTML
def _drill_try_html(obj):
    """If obj is a DataFrame or Series, return styled HTML table."""
    if isinstance(obj, pd.DataFrame):
        return obj.to_html(max_rows=100, max_cols=50, classes='drill-df-table', border=0)
    if isinstance(obj, pd.Series):
        return obj.to_frame().to_html(max_rows=100, classes='drill-df-table', border=0)
    return None

# Track injected DataFrames
_drill_dataframes = []

# Track installed packages
_drill_installed_packages = []

async def install(package_name):
    """Install a Python package from PyPI. Usage: await install('seaborn')"""
    await micropip.install(package_name)
    if package_name not in _drill_installed_packages:
        _drill_installed_packages.append(package_name)
    print(f"Installed '{package_name}' successfully")

def _drill_list_variables():
    """List user variables with type, shape, and size info."""
    import sys as _sys
    _skip = {
        'In', 'Out', '_', '__', '___',
        '_drill_capture_plot', '_drill_try_html', '_drill_dataframes',
        '_drill_installed_packages', '_drill_list_variables', 'install',
        '_drill_stdout', '_drill_stderr', '_drill_html_result', 'save_to_drill',
    }
    _builtins = set(dir(__builtins__)) if hasattr(__builtins__, '__iter__') else set()
    _result = []
    for _vname in sorted(dir()):
        if _vname.startswith('_') and _vname not in _drill_dataframes:
            continue
        if _vname in _skip or _vname in _builtins:
            continue
        try:
            _vobj = eval(_vname)
        except Exception:
            continue
        if callable(_vobj) and not isinstance(_vobj, (pd.DataFrame, pd.Series, np.ndarray)):
            continue
        _vtype = type(_vobj).__name__
        _vshape = ''
        _vsize = ''
        if isinstance(_vobj, pd.DataFrame):
            _vshape = f'{_vobj.shape[0]} x {_vobj.shape[1]}'
            _vsize = f'{_vobj.memory_usage(deep=True).sum() / 1024:.1f} KB'
        elif isinstance(_vobj, pd.Series):
            _vshape = f'{len(_vobj)}'
            _vsize = f'{_vobj.memory_usage(deep=True) / 1024:.1f} KB'
        elif isinstance(_vobj, np.ndarray):
            _vshape = ' x '.join(str(d) for d in _vobj.shape)
            _vsize = f'{_vobj.nbytes / 1024:.1f} KB'
        elif isinstance(_vobj, (list, dict, set, tuple)):
            _vshape = f'{len(_vobj)} items'
            _vsize = f'{_sys.getsizeof(_vobj) / 1024:.1f} KB'
        elif isinstance(_vobj, str):
            _vshape = f'{len(_vobj)} chars'
        elif isinstance(_vobj, (int, float, bool, complex)):
            _vshape = str(_vobj)
        _result.append({'name': _vname, 'type': _vtype, 'shape': _vshape, 'size': _vsize})
    return json.dumps(_result)

async def save_to_drill(df, plugin='dfs', workspace='tmp', table_name='notebook_export', format='json'):
    """Save a DataFrame to a Drill workspace.

    Usage:
        await save_to_drill(df)
        await save_to_drill(df, workspace='tmp', table_name='my_data', format='csv')

    Args:
        df: pandas DataFrame to export
        plugin: Storage plugin name (default: 'dfs')
        workspace: Workspace name (default: 'tmp')
        table_name: Name for the exported table/file (default: 'notebook_export')
        format: Output format - 'json' or 'csv' (default: 'json')

    Returns:
        The Drill query path to access the exported data
    """
    import json as _json
    if format == 'csv':
        _data = df.to_csv(index=False)
    else:
        _data = df.to_json(orient='records', date_format='iso')

    _payload = _json.dumps({
        'plugin': plugin,
        'workspace': workspace,
        'tableName': table_name,
        'format': format,
        'data': _data,
    })

    from pyodide.http import pyfetch
    from js import document
    _headers = {'Content-Type': 'application/json'}

    # Get CSRF token from meta tag
    _csrf_meta = document.querySelector('meta[name="csrf-token"]')
    if _csrf_meta:
        _csrf_token = _csrf_meta.getAttribute('content')
        if _csrf_token:
            _headers['X-CSRF-Token'] = _csrf_token
    else:
        # Try cookie
        from js import document as _doc
        for _c in (_doc.cookie or '').split(';'):
            _c = _c.strip()
            if _c.startswith('drill.csrf.token='):
                _headers['X-CSRF-Token'] = _c.split('=', 1)[1]
                break

    _resp = await pyfetch('/api/v1/notebook/export', method='POST', body=_payload, headers=_headers)
    _result = await _resp.json()

    if _result.get('success'):
        _path = _result.get('path', '')
        print(f"Exported to Drill: {_path}")
        print(f"Query with: SELECT * FROM {_path}")
        return _path
    else:
        raise RuntimeError(f"Export failed: {_result.get('message', 'Unknown error')}")
`);

    onStatus('Ready');
    pyodideInstance = pyodide;
    return pyodide;
  })();

  return pyodidePromise;
}

export function usePyodide(): UsePyodideReturn {
  const [loading, setLoading] = useState(false);
  const [ready, setReady] = useState(!!pyodideInstance);
  const [statusMessage, setStatusMessage] = useState(pyodideInstance ? 'Ready' : '');
  const [error, setError] = useState<string | null>(null);
  const pyodideRef = useRef<PyodideInterface>(pyodideInstance);

  const initialize = useCallback(async () => {
    if (pyodideRef.current) {
      setReady(true);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const pyodide = await loadPyodideRuntime(setStatusMessage);
      pyodideRef.current = pyodide;
      setReady(true);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to load Python runtime';
      setError(msg);
      setStatusMessage(msg);
    } finally {
      setLoading(false);
    }
  }, []);

  const runPython = useCallback(async (code: string): Promise<PythonOutput> => {
    const pyodide = pyodideRef.current;
    if (!pyodide) {
      return { stdout: '', stderr: 'Python runtime not initialized', result: null, htmlResult: null, imageData: null, error: true };
    }

    // Capture stdout/stderr and prepare HTML result holder
    await pyodide.runPythonAsync(`
import sys, io
_drill_stdout = io.StringIO()
_drill_stderr = io.StringIO()
sys.stdout = _drill_stdout
sys.stderr = _drill_stderr
_drill_html_result = None
`);

    let result: string | null = null;
    let hasError = false;

    try {
      // Wrap user code to capture the last expression for HTML rendering
      await pyodide.runPythonAsync(`_drill_last_expr = None`);
      const raw = await pyodide.runPythonAsync(code);
      if (raw !== undefined && raw !== null) {
        // Store the raw result in Python so _drill_try_html can inspect it
        pyodide.globals.set('_drill_last_expr', raw);
        try {
          const html = await pyodide.runPythonAsync(`_drill_try_html(_drill_last_expr)`);
          if (html) {
            await pyodide.runPythonAsync(`_drill_html_result = _drill_try_html(_drill_last_expr)`);
          }
        } catch {
          // Not a DataFrame/Series — skip HTML rendering
        }
        result = String(raw);
      }
    } catch (err) {
      hasError = true;
      const errMsg = err instanceof Error ? err.message : String(err);
      await pyodide.runPythonAsync(`_drill_stderr.write(${JSON.stringify(errMsg)})`);
    }

    // Capture matplotlib plots
    let imageData: string | null = null;
    try {
      const img = await pyodide.runPythonAsync('_drill_capture_plot()');
      if (img) {
        imageData = img as string;
      }
    } catch {
      // No plot
    }

    // Read captured output
    const stdout = pyodide.runPython('_drill_stdout.getvalue()') as string;
    const stderr = pyodide.runPython('_drill_stderr.getvalue()') as string;

    // Read HTML result
    let htmlResult: string | null = null;
    try {
      const html = pyodide.runPython('_drill_html_result');
      if (html) {
        htmlResult = html as string;
      }
    } catch {
      // No HTML
    }

    // Restore stdout/stderr
    await pyodide.runPythonAsync(`
sys.stdout = sys.__stdout__
sys.stderr = sys.__stderr__
`);

    return { stdout, stderr, result, htmlResult, imageData, error: hasError };
  }, []);

  const injectDataFrame = useCallback(async (
    name: string,
    columns: string[],
    rows: Record<string, unknown>[],
    maxRows = 50000,
  ): Promise<void> => {
    const pyodide = pyodideRef.current;
    if (!pyodide) {
      throw new Error('Python runtime not initialized');
    }

    const safeName = name.replace(/[^a-zA-Z0-9_]/g, '_').replace(/^[0-9]/, '_$&');
    const truncatedRows = rows.slice(0, maxRows);

    // Try Arrow IPC transfer first (faster for large datasets)
    let arrowSuccess = false;
    try {
      // Create Arrow table from row data
      const arrowTable = tableFromJSON(truncatedRows);
      const ipcBytes = tableToIPC(arrowTable, 'stream');

      // Load pyarrow in Pyodide if not yet loaded
      if (!pyarrowLoaded) {
        await pyodide.loadPackage('pyarrow');
        pyarrowLoaded = true;
      }

      // Transfer the IPC bytes to Pyodide
      pyodide.globals.set('_drill_ipc_bytes', ipcBytes);

      await pyodide.runPythonAsync(`
import pyarrow as pa
import pyarrow.ipc as ipc
import pandas as pd

_drill_reader = ipc.open_stream(bytes(_drill_ipc_bytes))
_drill_arrow_table = _drill_reader.read_all()
${safeName} = _drill_arrow_table.to_pandas()

if '${safeName}' not in _drill_dataframes:
    _drill_dataframes.append('${safeName}')

del _drill_ipc_bytes, _drill_reader, _drill_arrow_table
`);
      pyodide.globals.delete('_drill_ipc_bytes');
      arrowSuccess = true;
    } catch {
      // Arrow IPC failed — fall back to JSON
    }

    // Fallback: JSON-based transfer
    if (!arrowSuccess) {
      const jsonStr = JSON.stringify(truncatedRows);
      const colsStr = JSON.stringify(columns);
      const jsonB64 = btoa(unescape(encodeURIComponent(jsonStr)));
      const colsB64 = btoa(unescape(encodeURIComponent(colsStr)));

      await pyodide.runPythonAsync(`
import pandas as pd
import json
import base64

_drill_raw_data = base64.b64decode('${jsonB64}').decode('utf-8')
_drill_raw_cols = base64.b64decode('${colsB64}').decode('utf-8')
_drill_parsed_data = json.loads(_drill_raw_data)
_drill_parsed_cols = json.loads(_drill_raw_cols)
${safeName} = pd.DataFrame(_drill_parsed_data, columns=_drill_parsed_cols)

if '${safeName}' not in _drill_dataframes:
    _drill_dataframes.append('${safeName}')

del _drill_raw_data, _drill_raw_cols, _drill_parsed_data, _drill_parsed_cols
`);
    }
  }, []);

  const listDataFrames = useCallback(async (): Promise<string[]> => {
    const pyodide = pyodideRef.current;
    if (!pyodide) {
      return [];
    }
    const result = pyodide.runPython('_drill_dataframes') as string[];
    return Array.isArray(result) ? result : [];
  }, []);

  const installPackage = useCallback(async (packageName: string): Promise<PythonOutput> => {
    const pyodide = pyodideRef.current;
    if (!pyodide) {
      return { stdout: '', stderr: 'Python runtime not initialized', result: null, htmlResult: null, imageData: null, error: true };
    }
    return runPython(`await install('${packageName.replace(/'/g, "\\'")}')`);
  }, [runPython]);

  const uninstallPackage = useCallback(async (packageName: string): Promise<PythonOutput> => {
    const pyodide = pyodideRef.current;
    if (!pyodide) {
      return { stdout: '', stderr: 'Python runtime not initialized', result: null, htmlResult: null, imageData: null, error: true };
    }
    return runPython(`
micropip.uninstall('${packageName.replace(/'/g, "\\'")}')
if '${packageName.replace(/'/g, "\\'")}' in _drill_installed_packages:
    _drill_installed_packages.remove('${packageName.replace(/'/g, "\\'")}')
print(f"Uninstalled '${packageName.replace(/'/g, "\\'")}' successfully")
`);
  }, [runPython]);

  const listInstalledPackages = useCallback(async (): Promise<string[]> => {
    const pyodide = pyodideRef.current;
    if (!pyodide) {
      return [];
    }
    const result = pyodide.runPython('_drill_installed_packages') as string[];
    return Array.isArray(result) ? result : [];
  }, []);

  const listAllPackages = useCallback(async (): Promise<PackageInfo[]> => {
    const pyodide = pyodideRef.current;
    if (!pyodide) {
      return [];
    }
    try {
      const jsonStr = await pyodide.runPythonAsync(`
import json
import importlib.metadata
_pkgs = []
_dists = list(importlib.metadata.distributions())
_seen = set()
for _d in _dists:
    _n = _d.metadata['Name']
    _v = _d.metadata['Version']
    if _n and _n not in _seen:
        _seen.add(_n)
        _pkgs.append({'name': _n, 'version': _v or 'unknown'})
_pkgs.sort(key=lambda x: x['name'].lower())
json.dumps(_pkgs)
`);
      return JSON.parse(jsonStr as string);
    } catch {
      return [];
    }
  }, []);

  const listVariables = useCallback(async (): Promise<VariableInfo[]> => {
    const pyodide = pyodideRef.current;
    if (!pyodide) {
      return [];
    }
    try {
      const jsonStr = await pyodide.runPythonAsync('_drill_list_variables()');
      return JSON.parse(jsonStr as string);
    } catch {
      return [];
    }
  }, []);

  const resetNamespace = useCallback(async (): Promise<void> => {
    const pyodide = pyodideRef.current;
    if (!pyodide) {
      return;
    }
    await pyodide.runPythonAsync(`
for _name in list(_drill_dataframes):
    if _name in dir():
        exec(f"del {_name}")
_drill_dataframes.clear()
plt.close('all')
`);
  }, []);

  const saveToDrill = useCallback(async (
    dfName: string,
    plugin: string,
    workspace: string,
    tableName: string,
    format: string,
  ): Promise<PythonOutput> => {
    return runPython(
      `await save_to_drill(${dfName}, plugin='${plugin.replace(/'/g, "\\'")}', workspace='${workspace.replace(/'/g, "\\'")}', table_name='${tableName.replace(/'/g, "\\'")}', format='${format.replace(/'/g, "\\'")}')`
    );
  }, [runPython]);

  const listWritableWorkspaces = useCallback(async (): Promise<WorkspacesResult> => {
    try {
      const headers: Record<string, string> = {};
      const csrfToken = getCsrfToken();
      if (csrfToken) {
        headers['X-CSRF-Token'] = csrfToken;
      }
      const response = await fetch('/api/v1/notebook/workspaces', {
        credentials: 'include',
        headers,
      });
      if (!response.ok) {
        return { workspaces: [], writePolicy: 'disabled' };
      }
      const data = await response.json();
      return {
        workspaces: data.workspaces || [],
        writePolicy: data.writePolicy || 'filesystem',
      };
    } catch {
      return { workspaces: [], writePolicy: 'filesystem' };
    }
  }, []);

  return {
    loading,
    ready,
    statusMessage,
    error,
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
  };
}
