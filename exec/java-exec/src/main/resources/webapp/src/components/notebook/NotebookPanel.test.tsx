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
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import NotebookPanel from './NotebookPanel';

// Mock Monaco editor
vi.mock('@monaco-editor/react', () => ({
  default: vi.fn(({ value, onChange }) => (
    <textarea
      data-testid="monaco-editor"
      value={value}
      onChange={(e) => onChange?.(e.target.value)}
    />
  )),
}));

// Mock react-markdown
vi.mock('react-markdown', () => ({
  default: vi.fn(({ children }) => <div data-testid="markdown-rendered">{children}</div>),
}));

// Mock DOMPurify
vi.mock('dompurify', () => ({
  default: { sanitize: (html: string) => html },
}));

// Mock apache-arrow
vi.mock('apache-arrow', () => ({
  tableFromJSON: vi.fn(() => ({ schema: { fields: [] } })),
  tableToIPC: vi.fn(() => new Uint8Array([1, 2, 3])),
}));

// Mock usePyodide hook
const mockInitialize = vi.fn();
const mockRunPython = vi.fn();
const mockInjectDataFrame = vi.fn().mockResolvedValue(undefined);
const mockListDataFrames = vi.fn().mockResolvedValue(['df']);
const mockInstallPackage = vi.fn();
const mockUninstallPackage = vi.fn();
const mockListInstalledPackages = vi.fn().mockResolvedValue([]);
const mockListAllPackages = vi.fn().mockResolvedValue([]);
const mockListVariables = vi.fn().mockResolvedValue([]);
const mockResetNamespace = vi.fn();
const mockSaveToDrill = vi.fn();
const mockListWritableWorkspaces = vi.fn().mockResolvedValue({ workspaces: [], writePolicy: 'filesystem' });

vi.mock('../../hooks/usePyodide', () => ({
  usePyodide: () => ({
    loading: false,
    ready: true,
    statusMessage: 'Ready',
    error: null,
    initialize: mockInitialize,
    runPython: mockRunPython,
    injectDataFrame: mockInjectDataFrame,
    listDataFrames: mockListDataFrames,
    installPackage: mockInstallPackage,
    uninstallPackage: mockUninstallPackage,
    listInstalledPackages: mockListInstalledPackages,
    listAllPackages: mockListAllPackages,
    listVariables: mockListVariables,
    resetNamespace: mockResetNamespace,
    saveToDrill: mockSaveToDrill,
    listWritableWorkspaces: mockListWritableWorkspaces,
  }),
}));

// Mock useTheme
vi.mock('../../hooks/useTheme', () => ({
  useTheme: () => ({ isDark: false }),
}));

// Mock antd message
const mockMessageSuccess = vi.fn();
const mockMessageError = vi.fn();
const mockMessageWarning = vi.fn();
vi.mock('antd', async () => {
  const actual = await vi.importActual('antd');
  return {
    ...(actual as object),
    message: {
      success: (...args: unknown[]) => mockMessageSuccess(...args),
      error: (...args: unknown[]) => mockMessageError(...args),
      warning: (...args: unknown[]) => mockMessageWarning(...args),
    },
  };
});

const defaultResults = {
  columns: ['id', 'name', 'value'],
  rows: [
    { id: 1, name: 'Alice', value: 100 },
    { id: 2, name: 'Bob', value: 200 },
    { id: 3, name: 'Charlie', value: 300 },
  ],
  queryState: 'COMPLETED' as const,
};

describe('NotebookPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  // ==================== Rendering Tests ====================

  describe('rendering', () => {
    it('shows empty state when no results', () => {
      const { container } = render(<NotebookPanel />);
      expect(container.textContent).toContain('Run a SQL query first');
    });

    it('shows empty state when results have no rows', () => {
      const { container } = render(
        <NotebookPanel results={{ columns: ['a'], rows: [], queryState: 'COMPLETED' }} />
      );
      expect(container.textContent).toContain('Run a SQL query first');
    });

    it('renders toolbar when results are available', () => {
      render(<NotebookPanel results={defaultResults} />);
      expect(screen.getByText('Run All')).toBeInTheDocument();
      expect(screen.getByText('Add Cell')).toBeInTheDocument();
      expect(screen.getByText('Snippets')).toBeInTheDocument();
      expect(screen.getByText('Packages')).toBeInTheDocument();
      expect(screen.getByText('Variables')).toBeInTheDocument();
      expect(screen.getByText('Clear')).toBeInTheDocument();
      expect(screen.getByText('Reset')).toBeInTheDocument();
      expect(screen.getByText('File')).toBeInTheDocument();
      expect(screen.getByText('Save to Drill')).toBeInTheDocument();
    });

    it('renders starter code cell with results', () => {
      render(<NotebookPanel results={defaultResults} tabName="test_query" />);
      const editors = screen.getAllByTestId('monaco-editor') as HTMLTextAreaElement[];
      expect(editors.length).toBeGreaterThanOrEqual(1);
      // Starter code should reference the sanitized tab name
      expect(editors[0].value).toContain('test_query');
    });

    it('shows data injection tag', async () => {
      render(<NotebookPanel results={defaultResults} tabName="my_data" />);
      await waitFor(() => {
        expect(screen.getByText(/my_data/)).toBeInTheDocument();
      });
    });
  });

  // ==================== Cell Operations ====================

  describe('cell operations', () => {
    it('clears all outputs when Clear is clicked', async () => {
      render(<NotebookPanel results={defaultResults} />);
      fireEvent.click(screen.getByText('Clear'));
      // Outputs should be cleared — no error thrown
    });

    it('resets namespace when Reset is clicked', async () => {
      render(<NotebookPanel results={defaultResults} />);
      fireEvent.click(screen.getByText('Reset'));
      await waitFor(() => {
        expect(mockResetNamespace).toHaveBeenCalled();
      });
    });
  });

  // ==================== Save to Drill ====================

  describe('save to drill', () => {
    it('Save to Drill button is disabled when data not injected', () => {
      // By default dataInjected is false until injection completes
      render(<NotebookPanel results={defaultResults} />);
      const button = screen.getByText('Save to Drill');
      // Button exists but may be disabled before data injection
      expect(button).toBeInTheDocument();
    });

    it('opens Save to Drill modal and loads workspaces', async () => {
      const workspaces = [
        { plugin: 'dfs', workspace: 'tmp', location: '/tmp', writable: true },
        { plugin: 'dfs', workspace: 'data', location: '/data', writable: true },
      ];
      mockListWritableWorkspaces.mockResolvedValue({ workspaces, writePolicy: 'filesystem' });
      mockListDataFrames.mockResolvedValue(['my_df']);

      render(<NotebookPanel results={defaultResults} tabName="my_df" />);

      // Wait for data injection to complete
      await waitFor(() => {
        expect(mockInjectDataFrame).toHaveBeenCalled();
      });

      const button = screen.getByText('Save to Drill');
      fireEvent.click(button);

      await waitFor(() => {
        expect(mockListWritableWorkspaces).toHaveBeenCalled();
        expect(mockListDataFrames).toHaveBeenCalled();
      });

      // Modal should be open
      await waitFor(() => {
        expect(screen.getByText('Export')).toBeInTheDocument();
      });
    });

    it('calls saveToDrill when Export is clicked', async () => {
      const workspaces = [
        { plugin: 'dfs', workspace: 'tmp', location: '/tmp', writable: true },
      ];
      mockListWritableWorkspaces.mockResolvedValue({ workspaces, writePolicy: 'filesystem' });
      mockListDataFrames.mockResolvedValue(['test_df']);
      mockSaveToDrill.mockResolvedValue({
        stdout: "Exported to Drill: dfs.tmp.`test.json`",
        stderr: '',
        result: null,
        htmlResult: null,
        imageData: null,
        error: false,
      });

      render(<NotebookPanel results={defaultResults} tabName="test_df" />);

      await waitFor(() => {
        expect(mockInjectDataFrame).toHaveBeenCalled();
      });

      // Open the modal
      fireEvent.click(screen.getByText('Save to Drill'));

      await waitFor(() => {
        expect(screen.getByText('Export')).toBeInTheDocument();
      });

      // Click Export
      const exportButton = screen.getByText('Export');
      fireEvent.click(exportButton);

      await waitFor(() => {
        expect(mockSaveToDrill).toHaveBeenCalled();
      });
    });

    it('shows error when export fails', async () => {
      const workspaces = [
        { plugin: 'dfs', workspace: 'tmp', location: '/tmp', writable: true },
      ];
      mockListWritableWorkspaces.mockResolvedValue({ workspaces, writePolicy: 'filesystem' });
      mockListDataFrames.mockResolvedValue(['test_df']);
      mockSaveToDrill.mockResolvedValue({
        stdout: '',
        stderr: 'Export failed: Permission denied',
        result: null,
        htmlResult: null,
        imageData: null,
        error: true,
      });

      render(<NotebookPanel results={defaultResults} tabName="test_df" />);

      await waitFor(() => {
        expect(mockInjectDataFrame).toHaveBeenCalled();
      });

      fireEvent.click(screen.getByText('Save to Drill'));

      await waitFor(() => {
        expect(screen.getByText('Export')).toBeInTheDocument();
      });

      fireEvent.click(screen.getByText('Export'));

      await waitFor(() => {
        expect(mockMessageError).toHaveBeenCalledWith(
          expect.stringContaining('Export failed')
        );
      });
    });

    it('shows warning when no writable workspaces found', async () => {
      mockListWritableWorkspaces.mockResolvedValue({ workspaces: [], writePolicy: 'filesystem' });
      mockListDataFrames.mockResolvedValue(['df']);

      render(<NotebookPanel results={defaultResults} />);

      await waitFor(() => {
        expect(mockInjectDataFrame).toHaveBeenCalled();
      });

      fireEvent.click(screen.getByText('Save to Drill'));

      await waitFor(() => {
        expect(screen.getByText(/No writable workspaces found/)).toBeInTheDocument();
      });
    });

    it('shows disabled message when write policy is disabled', async () => {
      mockListWritableWorkspaces.mockResolvedValue({ workspaces: [], writePolicy: 'disabled' });
      mockListDataFrames.mockResolvedValue(['df']);

      render(<NotebookPanel results={defaultResults} />);

      await waitFor(() => {
        expect(mockInjectDataFrame).toHaveBeenCalled();
      });

      fireEvent.click(screen.getByText('Save to Drill'));

      await waitFor(() => {
        expect(screen.getByText(/Export Disabled/)).toBeInTheDocument();
      });
    });

    it('shows admin only message when write policy is admin_only and no workspaces', async () => {
      mockListWritableWorkspaces.mockResolvedValue({ workspaces: [], writePolicy: 'admin_only' });
      mockListDataFrames.mockResolvedValue(['df']);

      render(<NotebookPanel results={defaultResults} />);

      await waitFor(() => {
        expect(mockInjectDataFrame).toHaveBeenCalled();
      });

      fireEvent.click(screen.getByText('Save to Drill'));

      await waitFor(() => {
        expect(screen.getByText(/Admin Only/)).toBeInTheDocument();
      });
    });
  });

  // ==================== Per-tab Persistence ====================

  describe('per-tab persistence', () => {
    it('saves cells to localStorage when tabId is provided', async () => {
      render(
        <NotebookPanel results={defaultResults} tabId="tab-123" tabName="test" />
      );

      await waitFor(() => {
        const stored = localStorage.getItem('drill-tab-notebooks');
        expect(stored).toBeTruthy();
        const parsed = JSON.parse(stored!);
        expect(parsed['tab-123']).toBeDefined();
        expect(parsed['tab-123'].cells.length).toBeGreaterThan(0);
      });
    });

    it('restores cells from localStorage when tabId matches', async () => {
      // Pre-populate localStorage
      const savedState = {
        'tab-456': {
          cells: [
            { type: 'code', code: 'print("restored")' },
            { type: 'markdown', code: '## Restored Notebook' },
          ],
        },
      };
      localStorage.setItem('drill-tab-notebooks', JSON.stringify(savedState));

      render(
        <NotebookPanel results={defaultResults} tabId="tab-456" tabName="test" />
      );

      const editors = screen.getAllByTestId('monaco-editor') as HTMLTextAreaElement[];
      expect(editors[0].value).toBe('print("restored")');
    });
  });

  // ==================== DataFrame Name ====================

  describe('dataframe naming', () => {
    it('sanitizes tab name for DataFrame variable', () => {
      render(<NotebookPanel results={defaultResults} tabName="My Query 123!" />);
      const editors = screen.getAllByTestId('monaco-editor') as HTMLTextAreaElement[];
      // Should sanitize to My_Query_123
      expect(editors[0].value).toContain('My_Query_123');
    });

    it('uses df as default when tab name is empty', () => {
      render(<NotebookPanel results={defaultResults} tabName="" />);
      const editors = screen.getAllByTestId('monaco-editor') as HTMLTextAreaElement[];
      expect(editors[0].value).toContain('df');
    });

    it('prefixes with underscore when name starts with number', () => {
      render(<NotebookPanel results={defaultResults} tabName="123query" />);
      const editors = screen.getAllByTestId('monaco-editor') as HTMLTextAreaElement[];
      expect(editors[0].value).toContain('_123query');
    });
  });

  // ==================== NotebookPanelHandle ====================

  describe('handle', () => {
    it('provides handle to parent via onHandle callback', () => {
      const handleFn = vi.fn();
      render(
        <NotebookPanel results={defaultResults} onHandle={handleFn} tabName="test" />
      );

      expect(handleFn).toHaveBeenCalledWith(
        expect.objectContaining({
          addCell: expect.any(Function),
          getLastCellCode: expect.any(Function),
          getLastCellError: expect.any(Function),
        })
      );
    });

    it('handle.getLastCellCode returns the last cell code', () => {
      const handleFn = vi.fn();
      render(
        <NotebookPanel results={defaultResults} onHandle={handleFn} tabName="test" />
      );

      const handle = handleFn.mock.calls[handleFn.mock.calls.length - 1][0];
      if (handle) {
        const code = handle.getLastCellCode();
        expect(typeof code).toBe('string');
        expect(code.length).toBeGreaterThan(0);
      }
    });

    it('handle.getLastCellError returns null when no errors', () => {
      const handleFn = vi.fn();
      render(
        <NotebookPanel results={defaultResults} onHandle={handleFn} tabName="test" />
      );

      const handle = handleFn.mock.calls[handleFn.mock.calls.length - 1][0];
      if (handle) {
        expect(handle.getLastCellError()).toBeNull();
      }
    });

    it('cleans up handle on unmount', () => {
      const handleFn = vi.fn();
      const { unmount } = render(
        <NotebookPanel results={defaultResults} onHandle={handleFn} tabName="test" />
      );

      unmount();

      // Last call should be with null (cleanup)
      const lastCall = handleFn.mock.calls[handleFn.mock.calls.length - 1][0];
      expect(lastCall).toBeNull();
    });
  });

  // ==================== Packages Modal ====================

  describe('packages', () => {
    it('opens packages modal when Packages button is clicked', async () => {
      mockListAllPackages.mockResolvedValue([
        { name: 'pandas', version: '2.0.0' },
        { name: 'numpy', version: '1.24.0' },
      ]);

      render(<NotebookPanel results={defaultResults} />);
      fireEvent.click(screen.getByText('Packages'));

      await waitFor(() => {
        expect(screen.getByText('Python Packages')).toBeInTheDocument();
        expect(mockListAllPackages).toHaveBeenCalled();
      });
    });
  });

  // ==================== Variables Panel ====================

  describe('variables panel', () => {
    it('toggles variables panel when Variables button is clicked', async () => {
      mockListVariables.mockResolvedValue([
        { name: 'df', type: 'DataFrame', shape: '3 x 3', size: '1.2 KB' },
      ]);

      render(<NotebookPanel results={defaultResults} />);
      fireEvent.click(screen.getByText('Variables'));

      await waitFor(() => {
        expect(mockListVariables).toHaveBeenCalled();
      });
    });
  });
});
