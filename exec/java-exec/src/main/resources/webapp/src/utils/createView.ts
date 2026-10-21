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
import { executeQuery } from '../api/queries';
import { addDataset } from '../api/projects';
import { buildViewDdl, type ViewMode } from './sql';

export interface CreateViewOptions {
  mode: ViewMode;
  schema: string;
  name: string;
  sql: string;
  replace: boolean;
  projectId?: string;
}

export interface CreateViewResult {
  /** The statement sent to Drill. */
  ddl: string;
  /** Set when the view was created but could not be linked to the project. */
  projectError?: string;
}

/**
 * Create a view or materialized view, then link it to the active project.
 *
 * Creating a view is DDL, not metadata: it executes immediately and is visible to
 * anyone who can read the schema. The view exists the moment executeQuery resolves, so
 * a later failure to link it to a project is reported as a partial outcome rather than
 * thrown — the same rule the visualization tool follows when addVisualization fails.
 *
 * Throws only if the DDL itself fails, in which case nothing was created.
 */
export async function createViewFromQuery(opts: CreateViewOptions): Promise<CreateViewResult> {
  const { mode, schema, name, sql, replace, projectId } = opts;
  const ddl = buildViewDdl({ mode, schema, name, sql, replace });

  // No autoLimitRowCount: it makes Drill cancel the query once N rows arrive, which
  // would truncate a materialized view's data.
  await executeQuery({ query: ddl, queryType: 'SQL' });

  if (!projectId) {
    return { ddl };
  }

  try {
    await addDataset(projectId, {
      id: '',
      type: mode === 'materialized_view' ? 'materialized_view' : 'view',
      schema,
      table: name,
      label: name,
    });
    return { ddl };
  } catch (err) {
    // The result goes back to a user who was told the view was created, so this must
    // not be silent.
    console.error('View created but could not be added to the project', err);
    return { ddl, projectError: err instanceof Error ? err.message : String(err) };
  }
}
