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
import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Button,
  Card,
  Checkbox,
  Space,
  Spin,
  Alert,
  Descriptions,
  Tag,
  Empty,
  Collapse,
  Input,
  Modal,
  Tooltip,
  message,
} from 'antd';
import {
  ArrowLeftOutlined,
  EditOutlined,
  PlayCircleOutlined,
  CopyOutlined,
  DeleteOutlined,
  SaveOutlined,
  CloseOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import {
  getVisualization,
  updateVisualization,
  deleteVisualization,
} from '../api/visualizations';
import { executeQuery } from '../api/queries';
import { createSavedQuery, updateSavedQuery } from '../api/savedQueries';
import { addSavedQuery } from '../api/projects';
import { getEffectiveQuery } from '../utils/sqlTransformations';
import { findMissingColumnRefs, groupMissingByColumn } from '../utils/vizColumnDeps';
import ChartPreview from '../components/visualization/ChartPreview';
import SqlEditor from '../components/query-editor/SqlEditor';
import type { QueryResult } from '../types';

const chartColors: Record<string, string> = {
  bar: '#1890ff',
  line: '#52c41a',
  pie: '#faad14',
  scatter: '#f5222d',
  table: '#722ed1',
  number: '#eb2f96',
};

interface VisualizationDetailPageProps {
  projectId?: string;
}

export default function VisualizationDetailPage({ projectId: propProjectId }: VisualizationDetailPageProps = {}) {
  const { vizId } = useParams<{ vizId: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const projectId = propProjectId;
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);
  const [queryLoading, setQueryLoading] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);
  const ranQueryRef = useRef<string | null>(null);

  // ── Inline edit state ─────────────────────────────────────────────────
  // The detail page doubles as the SQL editor for the visualization. The
  // "Edit query" button toggles isEditing; entering edit mode seeds editedSql
  // from viz.sql but leaves the chart showing the last successful run until
  // the user Runs the new SQL.
  const [isEditing, setIsEditing] = useState(false);
  const [editedSql, setEditedSql] = useState('');
  const [hasRunSinceEdit, setHasRunSinceEdit] = useState(false);
  const [saving, setSaving] = useState(false);

  const { data: viz, isLoading, error } = useQuery({
    queryKey: ['visualization', vizId],
    queryFn: () => getVisualization(vizId || ''),
    enabled: !!vizId,
  });

  /**
   * Run a SQL string and update the chart. When called from edit mode, the
   * provided sql is the user's draft; otherwise we fall back to the saved
   * viz.sql. Result columns are also stored on the QueryResult so the missing-
   * column check can run off the latest run.
   */
  const runSql = useCallback(async (sql: string | undefined): Promise<QueryResult | null> => {
    if (!sql || !sql.trim()) {
      message.warning('No SQL query available');
      return null;
    }
    setQueryLoading(true);
    setQueryError(null);
    setQueryResult(null);
    try {
      const query = await getEffectiveQuery(sql, viz?.config || {});
      const result = await executeQuery({
        query,
        queryType: 'SQL',
        autoLimitRowCount: 10000,
        defaultSchema: viz?.defaultSchema || 'public',
      });
      setQueryResult(result);
      return result;
    } catch (err) {
      setQueryError(err instanceof Error ? err.message : 'Failed to run query');
      return null;
    } finally {
      setQueryLoading(false);
    }
  }, [viz?.config, viz?.defaultSchema]);

  const handleRunSavedSql = useCallback(() => {
    void runSql(viz?.sql);
  }, [runSql, viz?.sql]);

  const handleRunEditedSql = useCallback(async () => {
    const result = await runSql(editedSql);
    if (result) {
      setHasRunSinceEdit(true);
    }
  }, [runSql, editedSql]);

  // Auto-run query when visualization loads
  useEffect(() => {
    if (viz?.sql && viz.id !== ranQueryRef.current) {
      ranQueryRef.current = viz.id;
      handleRunSavedSql();
    }
  }, [viz?.id, viz?.sql, handleRunSavedSql]);

  // Seed editedSql from the saved viz when entering edit mode the first time
  // for this viz, and reset hasRunSinceEdit on toggle.
  const enterEditMode = useCallback(() => {
    setEditedSql(viz?.sql || '');
    setHasRunSinceEdit(false);
    setIsEditing(true);
  }, [viz?.sql]);

  const exitEditMode = useCallback(() => {
    setIsEditing(false);
    setHasRunSinceEdit(false);
    // Restore the saved-SQL chart so the preview matches the persisted state.
    handleRunSavedSql();
  }, [handleRunSavedSql]);

  // Compute missing-column refs against the columns from the most recent run.
  // The check only fires once we have a result — empty columns means "we don't
  // know yet" rather than "everything is missing".
  const missingRefs = useMemo(() => {
    return findMissingColumnRefs(viz?.config, queryResult?.columns);
  }, [viz?.config, queryResult?.columns]);
  const missingGroups = useMemo(() => groupMissingByColumn(missingRefs), [missingRefs]);

  const sqlIsDirty = isEditing && editedSql.trim() !== (viz?.sql || '').trim();

  /**
   * Save the edited SQL onto the visualization. Detaches from savedQueryId by
   * default so the linked saved query keeps its original SQL. Optionally also
   * updates the saved query if the user opted in.
   */
  const saveMutation = useMutation({
    mutationFn: async ({ alsoUpdateSavedQuery }: { alsoUpdateSavedQuery: boolean }) => {
      if (!viz?.id) {
        throw new Error('Missing visualization id');
      }
      // Update the saved query in-place if requested AND it still exists.
      if (alsoUpdateSavedQuery && viz.savedQueryId) {
        try {
          await updateSavedQuery(viz.savedQueryId, { sql: editedSql });
        } catch (err) {
          // Non-fatal — surface to the user but keep going with the viz update.
          message.warning(
            `Couldn't update linked saved query: ${err instanceof Error ? err.message : 'unknown error'}`,
          );
        }
      }
      // Detach the savedQueryId by default; only keep it if the user chose to
      // keep the saved query in sync. Either way the inline sql becomes the
      // source of truth from the viz's perspective.
      const payload = {
        sql: editedSql,
        savedQueryId: alsoUpdateSavedQuery ? viz.savedQueryId : '',
      };
      return updateVisualization(viz.id, payload);
    },
    onSuccess: () => {
      message.success('Visualization updated');
      queryClient.invalidateQueries({ queryKey: ['visualization', vizId] });
      queryClient.invalidateQueries({ queryKey: ['visualizations'] });
      setIsEditing(false);
      setHasRunSinceEdit(false);
    },
    onError: (e: Error) => {
      message.error(`Failed to save: ${e.message}`);
    },
    onSettled: () => setSaving(false),
  });

  const performSave = useCallback((alsoUpdateSavedQuery: boolean) => {
    setSaving(true);
    saveMutation.mutate({ alsoUpdateSavedQuery });
  }, [saveMutation]);

  /**
   * Manually save the visualization's SQL as a saved query and link the viz
   * to it. Only meaningful when the viz currently has inline sql but no
   * savedQueryId — surfaced as a "Save query" button next to "Copy" / "Edit".
   * Adds to the active project when one is in scope.
   */
  const saveQueryMutation = useMutation({
    mutationFn: async () => {
      if (!viz?.id || !viz?.sql) {
        throw new Error('Visualization has no SQL to save');
      }
      const created = await createSavedQuery({
        name: viz.name,
        description: `Query backing visualization "${viz.name}"`,
        sql: viz.sql,
        defaultSchema: viz.defaultSchema,
        isPublic: viz.isPublic,
      });
      if (projectId && created.id) {
        try {
          await addSavedQuery(projectId, created.id);
        } catch {
          // Project linkage best-effort — query still exists.
        }
      }
      await updateVisualization(viz.id, { savedQueryId: created.id });
      return created;
    },
    onSuccess: (created) => {
      message.success(`Saved query "${created.name}"`);
      queryClient.invalidateQueries({ queryKey: ['visualization', vizId] });
      queryClient.invalidateQueries({ queryKey: ['saved-queries'] });
      if (projectId) {
        queryClient.invalidateQueries({ queryKey: ['project', projectId] });
      }
    },
    onError: (e: Error) => {
      message.error(`Failed to save query: ${e.message}`);
    },
  });

  /**
   * Save click handler. Confirms when the new SQL drops columns the chart
   * config still references; otherwise saves directly.
   */
  const handleSave = useCallback(() => {
    if (!hasRunSinceEdit) {
      message.warning('Run the updated query first so we can check which columns it returns.');
      return;
    }
    const showSavedQueryPrompt = !!viz?.savedQueryId;
    let alsoUpdateSavedQuery = false;
    const askSavedQueryToggle = showSavedQueryPrompt ? (
      <Checkbox onChange={(e) => { alsoUpdateSavedQuery = e.target.checked; }}>
        Also update the linked saved query
      </Checkbox>
    ) : null;

    if (missingGroups.length > 0) {
      Modal.confirm({
        title: 'Some chart mappings will break',
        icon: <WarningOutlined style={{ color: '#faad14' }} />,
        content: (
          <div>
            <p>The updated query no longer returns:</p>
            <ul style={{ paddingLeft: 20 }}>
              {missingGroups.map((g) => (
                <li key={g.column}>
                  <code>{g.column}</code> — used as {g.slots.join(', ')}
                </li>
              ))}
            </ul>
            <p style={{ marginTop: 12 }}>
              The chart may render with empty values until you remap these.
              Save anyway?
            </p>
            {askSavedQueryToggle}
          </div>
        ),
        okText: 'Save anyway',
        okButtonProps: { danger: true },
        cancelText: 'Cancel',
        onOk: () => performSave(alsoUpdateSavedQuery),
      });
      return;
    }

    if (showSavedQueryPrompt) {
      Modal.confirm({
        title: 'Save updated query',
        content: (
          <div>
            <p>This visualization is linked to a saved query. By default we'll
              detach the link and store the new SQL on the visualization only.</p>
            {askSavedQueryToggle}
          </div>
        ),
        okText: 'Save',
        cancelText: 'Cancel',
        onOk: () => performSave(alsoUpdateSavedQuery),
      });
      return;
    }

    performSave(false);
  }, [hasRunSinceEdit, missingGroups, performSave, viz?.savedQueryId]);

  const handleDelete = () => {
    if (!viz?.id) return;

    Modal.confirm({
      title: 'Delete Visualization?',
      content: `Are you sure you want to delete "${viz.name}"? This cannot be undone.`,
      okText: 'Delete',
      okButtonProps: { danger: true },
      onOk: async () => {
        try {
          if (viz.id) {
            await deleteVisualization(viz.id);
          }
          message.success('Visualization deleted');
          navigate('/visualizations');
        } catch (err) {
          message.error(err instanceof Error ? err.message : 'Failed to delete visualization');
        }
      },
    });
  };

  if (isLoading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '100vh' }}>
        <Spin size="large" tip="Loading visualization..." />
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: '24px' }}>
        <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/visualizations')}>
          Back to Visualizations
        </Button>
        <Alert
          type="error"
          message="Failed to load visualization"
          description={error instanceof Error ? error.message : 'Unknown error'}
          style={{ marginTop: 24 }}
        />
      </div>
    );
  }

  if (!viz) {
    return (
      <div style={{ padding: '24px' }}>
        <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/visualizations')}>
          Back to Visualizations
        </Button>
        <Empty description="Visualization not found" style={{ marginTop: 40 }} />
      </div>
    );
  }

  const handleBack = () => {
    if (projectId) {
      navigate(`/projects/${projectId}/visualizations`);
    } else {
      navigate('/visualizations');
    }
  };

  return (
    <div style={{ padding: '24px' }}>
      <Space style={{ marginBottom: '24px' }}>
        <Button icon={<ArrowLeftOutlined />} onClick={handleBack}>
          Back
        </Button>
      </Space>

      <Card
        title={
          <Space>
            <span>{viz.name}</span>
            <Tag color={chartColors[viz.chartType]}>{viz.chartType}</Tag>
          </Space>
        }
        extra={
          <Space>
            {!isEditing && (
              <Button
                icon={<PlayCircleOutlined />}
                type="primary"
                onClick={handleRunSavedSql}
                loading={queryLoading}
                disabled={!viz.sql}
              >
                Run Query
              </Button>
            )}
            {!isEditing ? (
              <Button icon={<EditOutlined />} onClick={enterEditMode} disabled={!viz.sql}>
                Edit query
              </Button>
            ) : (
              <Tooltip title="Discard changes">
                <Button icon={<CloseOutlined />} onClick={exitEditMode}>
                  Cancel
                </Button>
              </Tooltip>
            )}
            <Button danger icon={<DeleteOutlined />} onClick={handleDelete} disabled={isEditing}>
              Delete
            </Button>
          </Space>
        }
      >
        <Descriptions column={2}>
          <Descriptions.Item label="Type">{viz.chartType}</Descriptions.Item>
          <Descriptions.Item label="Owner">{viz.owner || 'Unknown'}</Descriptions.Item>
          <Descriptions.Item label="Status">
            <Tag color={viz.isPublic ? 'green' : 'orange'}>{viz.isPublic ? 'Public' : 'Private'}</Tag>
          </Descriptions.Item>
          <Descriptions.Item label="Created">{new Date(viz.createdAt || '').toLocaleString()}</Descriptions.Item>
        </Descriptions>

        {viz.description && (
          <div style={{ marginTop: '16px' }}>
            <strong>Description:</strong>
            <p>{viz.description}</p>
          </div>
        )}
      </Card>

      {viz.sql && !isEditing && (
        <Card title="SQL Query" style={{ marginTop: '24px' }}>
          <Collapse
            items={[
              {
                key: '1',
                label: 'SQL Query',
                children: (
                  <div>
                    <Input.TextArea
                      value={viz.sql}
                      readOnly
                      rows={10}
                      style={{ marginBottom: '12px', fontFamily: 'monospace' }}
                    />
                    <Space wrap>
                      <Button
                        icon={<CopyOutlined />}
                        onClick={() => {
                          if (viz.sql) {
                            navigator.clipboard.writeText(viz.sql);
                            message.success('SQL copied to clipboard');
                          }
                        }}
                      >
                        Copy
                      </Button>
                      <Button icon={<EditOutlined />} onClick={enterEditMode}>
                        Edit query
                      </Button>
                      {!viz.savedQueryId && (
                        <Tooltip title="Persist this SQL as a Saved Query so it shows up alongside other reusable queries.">
                          <Button
                            icon={<SaveOutlined />}
                            onClick={() => saveQueryMutation.mutate()}
                            loading={saveQueryMutation.isPending}
                          >
                            Save query
                          </Button>
                        </Tooltip>
                      )}
                      {viz.savedQueryId && (
                        <Tag color="blue">Linked to saved query</Tag>
                      )}
                    </Space>
                  </div>
                ),
              },
            ]}
          />
        </Card>
      )}

      {isEditing && (
        <Card
          title={
            <Space>
              SQL Query
              {sqlIsDirty && <Tag color="blue">unsaved changes</Tag>}
            </Space>
          }
          style={{ marginTop: '24px' }}
          extra={
            <Space>
              <Button
                icon={<PlayCircleOutlined />}
                onClick={handleRunEditedSql}
                loading={queryLoading}
                disabled={!editedSql.trim()}
              >
                Run
              </Button>
              <Button
                type="primary"
                icon={<SaveOutlined />}
                onClick={handleSave}
                loading={saving}
                disabled={!sqlIsDirty || !hasRunSinceEdit}
              >
                Save
              </Button>
            </Space>
          }
        >
          <SqlEditor
            value={editedSql}
            onChange={setEditedSql}
            onExecute={handleRunEditedSql}
            height={260}
          />
          {hasRunSinceEdit && missingGroups.length > 0 && (
            <Alert
              type="warning"
              showIcon
              icon={<WarningOutlined />}
              style={{ marginTop: 12 }}
              message="Some chart mappings will break"
              description={
                <div>
                  <p style={{ margin: '0 0 6px' }}>
                    The updated query no longer returns:
                  </p>
                  <ul style={{ margin: 0, paddingLeft: 20 }}>
                    {missingGroups.map((g) => (
                      <li key={g.column}>
                        <code>{g.column}</code> — used as {g.slots.join(', ')}
                      </li>
                    ))}
                  </ul>
                </div>
              }
            />
          )}
          {queryResult && hasRunSinceEdit && (
            <Alert
              type="info"
              style={{ marginTop: 12 }}
              message={`Query returned ${queryResult.columns?.length ?? 0} column${queryResult.columns?.length === 1 ? '' : 's'}: ${(queryResult.columns ?? []).join(', ')}`}
            />
          )}
        </Card>
      )}

      {queryError && (
        <Alert
          type="error"
          message="Query Failed"
          description={queryError}
          closable
          onClose={() => setQueryError(null)}
          style={{ marginTop: '24px' }}
        />
      )}

      {(queryLoading || queryResult) && (
        <Card title="Visualization" style={{ marginTop: '24px' }}>
          <ChartPreview
            chartType={viz.chartType}
            config={viz.config || {}}
            data={queryResult}
            loading={queryLoading}
            height={600}
          />
        </Card>
      )}
    </div>
  );
}
