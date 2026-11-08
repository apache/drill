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
import { Typography } from 'antd';
import ResultsGrid from '../results/ResultsGrid';
import type { QueryResult } from '../../types';

/** The secondary tabs this viewer supports. CanvasHost renders the strip. */
export const RESULTS_TABS = [
  { key: 'results', label: 'Results' },
  { key: 'sql', label: 'SQL' },
];

interface ResultsCanvasProps {
  sql: string;
  result: QueryResult;
  /** The active secondary tab, one of RESULTS_TABS. */
  tab: string;
}

/**
 * The rows of a query Prospector ran, or the SQL behind them. The grid is the same one
 * the SQL Lab uses, so export, profiling and column tools come with it. Only the props
 * that make sense beside a chat are passed: the result and its SQL. Nothing here re-runs
 * or clears, because a canvas result is a snapshot that the next target replaces. The grid
 * also gets no cache id (the hook hands over the rows rather than a cached query), so data
 * profiling re-derives from the SQL at the cost of one extra round trip. Create Chart and
 * Share as API are left out of the Analyze menu automatically, as no handlers are passed.
 */
export default function ResultsCanvas({ sql, result, tab }: ResultsCanvasProps) {
  if (tab === 'sql') {
    return (
      <pre
        style={{
          margin: 0,
          padding: 12,
          height: '100%',
          overflow: 'auto',
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
          fontFamily: 'var(--font-mono, monospace)',
          fontSize: 12,
        }}
      >
        {sql.trim() ? sql : <Typography.Text type="secondary">No SQL recorded.</Typography.Text>}
      </pre>
    );
  }
  return <ResultsGrid results={result} sql={sql} />;
}
