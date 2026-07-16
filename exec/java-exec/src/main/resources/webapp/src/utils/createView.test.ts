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
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { createViewFromQuery } from './createView';
import { executeQuery } from '../api/queries';
import { addDataset } from '../api/projects';

vi.mock('../api/queries', () => ({ executeQuery: vi.fn() }));
vi.mock('../api/projects', () => ({ addDataset: vi.fn() }));

const base = {
  mode: 'view' as const,
  schema: 'dfs.tmp',
  name: 'sales',
  sql: 'SELECT 1 FROM t',
  replace: false,
};

describe('createViewFromQuery', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(executeQuery).mockResolvedValue({} as never);
    vi.mocked(addDataset).mockResolvedValue({} as never);
  });

  it('sends the CREATE VIEW statement to Drill', async () => {
    await createViewFromQuery(base);
    expect(vi.mocked(executeQuery).mock.calls[0][0].query)
      .toBe('CREATE VIEW dfs.`tmp`.`sales` AS SELECT 1 FROM t');
  });

  /** Auto-limit makes Drill cancel once N rows arrive, truncating an MV's data. */
  it('never sets an auto limit, which would truncate a materialized view', async () => {
    await createViewFromQuery({ ...base, mode: 'materialized_view' });
    expect(vi.mocked(executeQuery).mock.calls[0][0].autoLimitRowCount).toBeUndefined();
  });

  it('does not touch the project when there is no active project', async () => {
    await createViewFromQuery(base);
    expect(addDataset).not.toHaveBeenCalled();
  });

  it('links a view to the active project as a view dataset', async () => {
    await createViewFromQuery({ ...base, projectId: 'p1' });
    expect(addDataset).toHaveBeenCalledWith('p1', expect.objectContaining({
      type: 'view', schema: 'dfs.tmp', table: 'sales', label: 'sales',
    }));
  });

  it('links a materialized view as a materialized_view dataset', async () => {
    await createViewFromQuery({ ...base, mode: 'materialized_view', projectId: 'p1' });
    expect(addDataset).toHaveBeenCalledWith('p1', expect.objectContaining({
      type: 'materialized_view',
    }));
  });

  /**
   * The view already exists at this point. Failing the whole call would tell the user
   * nothing was created and invite a retry, which would then fail on the name conflict.
   */
  it('reports a project-linking failure without losing the created view', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(addDataset).mockRejectedValue(new Error('project not found'));

    const result = await createViewFromQuery({ ...base, projectId: 'p1' });

    expect(result.ddl).toContain('CREATE VIEW');
    expect(result.projectError).toContain('project not found');
    expect(consoleError).toHaveBeenCalled();
    consoleError.mockRestore();
  });

  it('throws when the DDL itself fails, because nothing was created', async () => {
    vi.mocked(executeQuery).mockRejectedValue(new Error('already exists'));
    await expect(createViewFromQuery(base)).rejects.toThrow('already exists');
    expect(addDataset).not.toHaveBeenCalled();
  });
});
