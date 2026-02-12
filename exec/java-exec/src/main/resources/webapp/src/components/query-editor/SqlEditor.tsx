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
import { useState, useRef, useCallback, useEffect, useMemo } from 'react';
import Editor, { OnMount, OnChange, Monaco } from '@monaco-editor/react';
import { useTheme } from '../../hooks/useTheme';

// Use Monaco's editor type from the package
type IStandaloneCodeEditor = Parameters<OnMount>[0];

export interface EditorSettings {
  theme: 'vs-light' | 'vs-dark' | 'hc-black';
  fontSize: number;
  tabSize: number;
  wordWrap: boolean;
  minimap: boolean;
  lineNumbers: boolean;
}

export const DEFAULT_EDITOR_SETTINGS: EditorSettings = {
  theme: 'vs-light',
  fontSize: 14,
  tabSize: 2,
  wordWrap: true,
  minimap: false,
  lineNumbers: true,
};

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  onExecute?: () => void;
  readOnly?: boolean;
  height?: string | number;
  settings?: EditorSettings;
}

export default function SqlEditor({
  value,
  onChange,
  onExecute,
  readOnly = false,
  height = '100%',
  settings = DEFAULT_EDITOR_SETTINGS,
}: SqlEditorProps) {
  const editorRef = useRef<IStandaloneCodeEditor | null>(null);
  const monacoRef = useRef<Monaco | null>(null);
  const { isDark } = useTheme();

  // Compute effective Monaco theme: respect explicit hc-black choice,
  // otherwise sync with global dark mode
  const effectiveTheme = useMemo(() => {
    if (settings.theme === 'hc-black') {
      return 'hc-black';
    }
    return isDark ? 'vs-dark' : 'vs-light';
  }, [isDark, settings.theme]);

  const handleEditorDidMount: OnMount = useCallback(
    (editor, monaco) => {
      editorRef.current = editor;
      monacoRef.current = monaco;

      // Add keyboard shortcut for executing query (Ctrl/Cmd + Enter)
      editor.addAction({
        id: 'execute-query',
        label: 'Execute Query',
        keybindings: [
          monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
        ],
        run: () => {
          onExecute?.();
        },
      });

      // Add keyboard shortcut for formatting (Ctrl/Cmd + Shift + F)
      editor.addAction({
        id: 'format-query',
        label: 'Format Query',
        keybindings: [
          monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KeyF,
        ],
        run: () => {
          editor.getAction('editor.action.formatDocument')?.run();
        },
      });

      // Configure SQL language settings
      monaco.languages.registerCompletionItemProvider('sql', {
        provideCompletionItems: (model: unknown, position: { lineNumber: number; column: number }) => {
          const textModel = model as { getWordUntilPosition: (pos: { lineNumber: number; column: number }) => { startColumn: number; endColumn: number } };
          const word = textModel.getWordUntilPosition(position);
          const range = {
            startLineNumber: position.lineNumber,
            endLineNumber: position.lineNumber,
            startColumn: word.startColumn,
            endColumn: word.endColumn,
          };

          // SQL keywords
          const keywords = [
            'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN',
            'IS', 'NULL', 'TRUE', 'FALSE', 'AS', 'ON', 'JOIN', 'LEFT', 'RIGHT',
            'INNER', 'OUTER', 'FULL', 'CROSS', 'GROUP', 'BY', 'ORDER', 'HAVING',
            'LIMIT', 'OFFSET', 'UNION', 'ALL', 'DISTINCT', 'CASE', 'WHEN', 'THEN',
            'ELSE', 'END', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
            'CREATE', 'TABLE', 'VIEW', 'INDEX', 'DROP', 'ALTER', 'ADD', 'COLUMN',
            'WITH', 'RECURSIVE', 'ASC', 'DESC', 'NULLS', 'FIRST', 'LAST',
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'CAST', 'CONVERT', 'COALESCE',
            'INFORMATION_SCHEMA', 'SCHEMATA', 'TABLES', 'COLUMNS',
          ];

          const suggestions = keywords.map((keyword) => ({
            label: keyword,
            kind: monaco.languages.CompletionItemKind.Keyword,
            insertText: keyword,
            range,
          }));

          return { suggestions };
        },
      });

      // Focus the editor
      editor.focus();
    },
    [onExecute]
  );

  const handleEditorChange: OnChange = useCallback(
    (value) => {
      onChange(value || '');
    },
    [onChange]
  );

  // Method to insert text at cursor position (called from parent)
  const insertText = useCallback((text: string) => {
    const editor = editorRef.current;
    if (editor) {
      const selection = editor.getSelection();
      if (selection) {
        editor.executeEdits('insert', [
          {
            range: selection,
            text: text,
            forceMoveMarkers: true,
          },
        ]);
        editor.focus();
      }
    }
  }, []);

  // Expose insertText method via ref
  useEffect(() => {
    // Attach to window for parent component access
    (window as unknown as { sqlEditorInsertText?: (text: string) => void }).sqlEditorInsertText = insertText;
    return () => {
      delete (window as unknown as { sqlEditorInsertText?: (text: string) => void }).sqlEditorInsertText;
    };
  }, [insertText]);

  // ---- Drag-and-drop support (Phase 3) ----
  const [dragOver, setDragOver] = useState(false);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'copy';
    setDragOver(true);
  }, []);

  const handleDragLeave = useCallback(() => {
    setDragOver(false);
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const text = e.dataTransfer.getData('text/plain');
    if (!text) {
      return;
    }
    const editor = editorRef.current;
    if (editor) {
      // Try to place the text at the mouse drop position
      const target = editor.getTargetAtClientPoint(e.clientX, e.clientY);
      if (target?.position) {
        const pos = target.position;
        editor.executeEdits('drop', [
          {
            range: {
              startLineNumber: pos.lineNumber,
              startColumn: pos.column,
              endLineNumber: pos.lineNumber,
              endColumn: pos.column,
            },
            text,
            forceMoveMarkers: true,
          },
        ]);
      } else {
        // Fallback: insert at current cursor position
        insertText(text);
      }
      editor.focus();
    }
  }, [insertText]);

  return (
    <div
      className="monaco-container"
      style={{
        height,
        border: dragOver ? '2px solid #1890ff' : '2px solid transparent',
        borderRadius: 4,
        transition: 'border-color 0.2s',
      }}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
    >
      <Editor
        height="100%"
        defaultLanguage="sql"
        value={value}
        onChange={handleEditorChange}
        onMount={handleEditorDidMount}
        options={{
          readOnly,
          minimap: { enabled: settings.minimap },
          fontSize: settings.fontSize,
          lineNumbers: settings.lineNumbers ? 'on' : 'off',
          scrollBeyondLastLine: false,
          wordWrap: settings.wordWrap ? 'on' : 'off',
          automaticLayout: true,
          tabSize: settings.tabSize,
          insertSpaces: true,
          folding: true,
          renderLineHighlight: 'line',
          selectOnLineNumbers: true,
          roundedSelection: true,
          cursorStyle: 'line',
          cursorBlinking: 'smooth',
          smoothScrolling: true,
          contextmenu: true,
          fontFamily: "'JetBrains Mono', 'Fira Code', 'Consolas', 'Monaco', monospace",
          fontLigatures: true,
          suggest: {
            showKeywords: true,
            showSnippets: true,
          },
          quickSuggestions: {
            other: true,
            comments: false,
            strings: false,
          },
        }}
        theme={effectiveTheme}
      />
    </div>
  );
}
