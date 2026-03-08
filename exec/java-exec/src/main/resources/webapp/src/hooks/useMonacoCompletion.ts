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
import { getTables, getColumns } from '../api/metadata';
import type { Monaco } from '@monaco-editor/react';
import type { SchemaInfo } from '../types';

// Drill-specific SQL functions and keywords beyond the basic set in SqlEditor
const DRILL_KEYWORDS = [
  'FLATTEN', 'KVGEN', 'REPEATED_COUNT', 'REPEATED_CONTAINS',
  'TO_DATE', 'TO_TIMESTAMP', 'TO_TIME', 'DATE_PART', 'DATE_DIFF', 'DATE_ADD',
  'CONVERT_TO', 'CONVERT_FROM', 'TYPEOF',
  'ILIKE', 'SIMILAR TO', 'NULLIF', 'COALESCE',
  'UNION ALL', 'EXCEPT', 'INTERSECT',
  'LATERAL', 'UNNEST',
];

/**
 * Registers a Monaco SQL completion provider that suggests:
 *   - Schema names (top-level)
 *   - Table names after "schema."
 *   - Column names after "schema.table."
 *   - Drill-specific keywords
 *
 * Results are cached per session to avoid redundant API calls.
 * The provider is disposed and re-registered whenever schemas change.
 */
export function useMonacoCompletion(
  monaco: Monaco | null,
  schemas: SchemaInfo[] | undefined,
) {
  // Caches survive provider re-registrations (schemas list reload)
  const tableCache = useRef<Map<string, string[]>>(new Map());
  const columnCache = useRef<Map<string, string[]>>(new Map());
  const disposableRef = useRef<{ dispose: () => void } | null>(null);

  useEffect(() => {
    if (!monaco || !schemas || schemas.length === 0) {
      return;
    }

    const schemaNames = schemas.map((s) => s.name);
    const schemaSet = new Set(schemaNames);

    // Dispose previous registration before creating a new one
    disposableRef.current?.dispose();

    disposableRef.current = monaco.languages.registerCompletionItemProvider('sql', {
      triggerCharacters: ['.'],

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      provideCompletionItems: async (model: any, position: any) => {
        const word = model.getWordUntilPosition(position);
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        };

        // Text on this line up to (not including) the current word
        const lineUpToCursor = model.getValueInRange({
          startLineNumber: position.lineNumber,
          startColumn: 1,
          endLineNumber: position.lineNumber,
          endColumn: word.startColumn,
        });

        // ── Dot-triggered: what's immediately before the dot? ──────────────
        if (lineUpToCursor.endsWith('.')) {
          // Strip the trailing dot and grab the qualifier
          const beforeDot = lineUpToCursor.slice(0, -1);

          // Extract the last continuous identifier (may contain dots and backticks)
          const qualMatch = beforeDot.match(/[`\w][\w.\s`]*$/);
          const qualifier = qualMatch
            ? qualMatch[0].replace(/`/g, '').replace(/\s+/g, ' ').trim()
            : '';

          if (!qualifier) {
            return { suggestions: [] };
          }

          // ── Case 1: qualifier is a known schema → suggest tables ─────────
          if (schemaSet.has(qualifier)) {
            let tables = tableCache.current.get(qualifier);
            if (!tables) {
              try {
                const result = await getTables(qualifier);
                tables = result.map((t) => t.name);
                tableCache.current.set(qualifier, tables);
              } catch {
                tables = [];
              }
            }
            return {
              suggestions: tables.map((t) => ({
                label: t,
                kind: monaco.languages.CompletionItemKind.Class,
                insertText: `\`${t}\``,
                range,
                detail: `Table · ${qualifier}`,
                sortText: `0${t}`,
              })),
            };
          }

          // ── Case 2: qualifier is "schema.table" → suggest columns ────────
          const lastDot = qualifier.lastIndexOf('.');
          if (lastDot >= 0) {
            const parentSchema = qualifier.slice(0, lastDot);
            const tableName = qualifier.slice(lastDot + 1);

            if (schemaSet.has(parentSchema) && tableName) {
              const cacheKey = `${parentSchema}\x00${tableName}`;
              let cols = columnCache.current.get(cacheKey);
              if (!cols) {
                try {
                  const result = await getColumns(parentSchema, tableName);
                  cols = result.map((c) => c.name);
                  columnCache.current.set(cacheKey, cols);
                } catch {
                  cols = [];
                }
              }
              return {
                suggestions: cols.map((c) => ({
                  label: c,
                  kind: monaco.languages.CompletionItemKind.Field,
                  insertText: `\`${c}\``,
                  range,
                  detail: `Column · ${tableName}`,
                  sortText: `0${c}`,
                })),
              };
            }
          }

          return { suggestions: [] };
        }

        // ── Non-dot context: offer schemas + Drill keywords ─────────────────
        const schemaSuggestions = schemaNames.map((name) => ({
          label: name,
          kind: monaco.languages.CompletionItemKind.Module,
          insertText: name,
          range,
          detail: 'Schema',
          sortText: `1${name}`,
        }));

        const drillKeywordSuggestions = DRILL_KEYWORDS.map((kw) => ({
          label: kw,
          kind: monaco.languages.CompletionItemKind.Keyword,
          insertText: kw,
          range,
          detail: 'Drill function',
          sortText: `3${kw}`,
        }));

        return { suggestions: [...schemaSuggestions, ...drillKeywordSuggestions] };
      },
    });

    return () => {
      disposableRef.current?.dispose();
      disposableRef.current = null;
    };
  }, [monaco, schemas]);
}
