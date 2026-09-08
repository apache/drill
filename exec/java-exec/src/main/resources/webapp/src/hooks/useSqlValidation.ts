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
import { useEffect, useRef } from 'react';
import type { Monaco } from '@monaco-editor/react';
import { validateSql } from '../api/queries';

type IStandaloneCodeEditor = Parameters<import('@monaco-editor/react').OnMount>[0];

const MARKER_OWNER = 'drill-sql-validation';
const DEBOUNCE_MS = 750;

/**
 * Debounced SQL validation hook that sets Monaco editor markers
 * based on Drill's Calcite SQL parser output.
 */
export function useSqlValidation(
  sql: string,
  editorRef: React.RefObject<IStandaloneCodeEditor | null>,
  monacoRef: React.RefObject<Monaco | null>,
  enabled = true,
): void {
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const lastValidatedRef = useRef<string>('');

  useEffect(() => {
    const editor = editorRef.current;
    const monaco = monacoRef.current;

    if (!enabled || !editor || !monaco) {
      return;
    }

    const model = editor.getModel();
    if (!model) {
      return;
    }

    // Clear markers on empty/whitespace SQL
    if (!sql || sql.trim().length === 0) {
      monaco.editor.setModelMarkers(model, MARKER_OWNER, []);
      lastValidatedRef.current = '';
      return;
    }

    // Skip if SQL hasn't changed since last validation
    if (sql === lastValidatedRef.current) {
      return;
    }

    // Cancel any pending timer
    if (timerRef.current !== null) {
      clearTimeout(timerRef.current);
    }

    // Cancel any in-flight request
    if (abortRef.current) {
      abortRef.current.abort();
    }

    timerRef.current = setTimeout(async () => {
      const controller = new AbortController();
      abortRef.current = controller;

      try {
        const result = await validateSql(sql, controller.signal);
        lastValidatedRef.current = sql;

        // Ensure the model is still valid (editor may have unmounted)
        const currentModel = editor.getModel();
        if (!currentModel) {
          return;
        }

        if (result.valid) {
          monaco.editor.setModelMarkers(currentModel, MARKER_OWNER, []);
        } else {
          const markers = result.errors.map((err) => ({
            message: err.message,
            severity: err.severity === 'warning'
              ? monaco.MarkerSeverity.Warning
              : monaco.MarkerSeverity.Error,
            startLineNumber: err.line,
            startColumn: err.column,
            endLineNumber: err.endLine,
            // Monaco endColumn is exclusive, SqlParserPos endColumn is inclusive
            endColumn: err.endColumn + 1,
          }));
          monaco.editor.setModelMarkers(currentModel, MARKER_OWNER, markers);
        }
      } catch {
        // Silently ignore network errors (AbortError, timeouts, etc.)
        // Keep previous markers in place
      }
    }, DEBOUNCE_MS);

    return () => {
      if (timerRef.current !== null) {
        clearTimeout(timerRef.current);
        timerRef.current = null;
      }
    };
  }, [sql, enabled, editorRef, monacoRef]);

  // Cleanup on unmount: cancel timer, abort request, clear markers
  useEffect(() => {
    // Capture ref values at effect time so the cleanup closure uses stable references
    const editor = editorRef.current;
    const monaco = monacoRef.current;
    return () => {
      if (timerRef.current !== null) {
        clearTimeout(timerRef.current);
      }
      if (abortRef.current) {
        abortRef.current.abort();
      }
      if (editor && monaco) {
        const model = editor.getModel();
        if (model) {
          monaco.editor.setModelMarkers(model, MARKER_OWNER, []);
        }
      }
    };
  }, [editorRef, monacoRef]);
}
