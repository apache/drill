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
import { useState, useMemo, useEffect, useCallback } from 'react';
import {
  Modal,
  Button,
  Form,
  Input,
  Switch,
  Select,
  Card,
  Row,
  Col,
  Typography,
  message,
  Spin,
  Alert,
} from 'antd';
import { ReloadOutlined, SaveOutlined, CodeOutlined, PlayCircleOutlined, WarningOutlined } from '@ant-design/icons';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { updateVisualization } from '../../api/visualizations';
import { executeQuery } from '../../api/queries';
import ChartTypeSelector from './ChartTypeSelector';
import ColumnMapper from './ColumnMapper';
import ChartPreview from './ChartPreview';
import { getEffectiveQuery } from '../../utils/sqlTransformations';
import type { ChartType, VisualizationConfig, QueryResult, VisualizationCreate, Visualization } from '../../types';

const { Text } = Typography;
const { TextArea } = Input;

interface VisualizationEditorProps {
  open: boolean;
  visualization: Visualization | null;
  onClose: () => void;
}

const colorSchemeOptions = [
  { value: 'default', label: 'Default' },
  { value: 'warm', label: 'Warm' },
  { value: 'cool', label: 'Cool' },
  { value: 'earth', label: 'Earth' },
];

export default function VisualizationEditor({
  open,
  visualization,
  onClose,
}: VisualizationEditorProps) {
  const [chartType, setChartType] = useState<ChartType>('bar');
  const [config, setConfig] = useState<VisualizationConfig>({});
  const [form] = Form.useForm();
  const queryClient = useQueryClient();
  const [showSql, setShowSql] = useState(true);

  // Base data (original SQL) — used for column list in ColumnMapper
  const [baseData, setBaseData] = useState<QueryResult | null>(null);
  const [dataLoading, setDataLoading] = useState(false);
  const [dataError, setDataError] = useState<string | null>(null);

  // Aggregated data — used for chart preview when aggregation is active
  const [aggregatedData, setAggregatedData] = useState<QueryResult | null>(null);
  const [aggregatedLoading, setAggregatedLoading] = useState(false);
  const [aggregatedError, setAggregatedError] = useState<string | null>(null);

  // Editable SQL state
  const [editedSql, setEditedSql] = useState<string>('');
  const [sqlDirty, setSqlDirty] = useState(false);
  const [staleMapping, setStaleMapping] = useState(false);

  // Guard: prevent effective query from firing before the saved config is loaded
  const [configReady, setConfigReady] = useState(false);

  // Watch colorScheme from form so preview updates live
  const selectedColorScheme = Form.useWatch('colorScheme', form) || 'default';

  // Pre-populate state when opening
  useEffect(() => {
    if (open && visualization) {
      setChartType(visualization.chartType);
      setConfig(visualization.config || {});
      setShowSql(true);
      setEditedSql(visualization.sql || '');
      setSqlDirty(false);
      form.setFieldsValue({
        name: visualization.name,
        description: visualization.description || '',
        colorScheme: visualization.config?.colorScheme || 'default',
        isPublic: visualization.isPublic || false,
      });
      // Signal that the saved config has been fully loaded — the effective
      // query effect is gated on this flag to avoid computing queries with
      // stale/incomplete config (e.g. missing timeGrain).
      setConfigReady(true);
    }
  }, [open, visualization, form]);

  // Fetch base data (original SQL, no time grain) — provides columns for ColumnMapper
  const fetchData = useCallback(async () => {
    if (!editedSql.trim()) {
      setDataError('SQL query is empty. Please enter a query.');
      return;
    }
    setDataLoading(true);
    setDataError(null);
    try {
      const result = await executeQuery({
        query: editedSql,
        queryType: 'SQL',
        autoLimitRowCount: 10000,
        defaultSchema: visualization?.defaultSchema,
      });
      setBaseData(result);
      setSqlDirty(false);
    } catch (err: unknown) {
      let msg = 'Unknown error';
      if (err instanceof Error) {
        msg = err.message;
      } else if (typeof err === 'object' && err !== null && 'response' in err) {
        msg = JSON.stringify((err as Record<string, unknown>).response);
      }
      setDataError(`Failed to execute query: ${msg}`);
    } finally {
      setDataLoading(false);
    }
  }, [editedSql, visualization?.defaultSchema]);

  // Auto-fetch base data on open (gated on editedSql so it waits for the init effect)
  useEffect(() => {
    if (open && visualization && editedSql) {
      fetchData();
    }
  }, [open, visualization, editedSql, fetchData]);

  // Extract column info from base data (always the full column set)
  const columns = useMemo(() => {
    if (!baseData || !baseData.columns || !baseData.metadata) {
      return [];
    }
    return baseData.columns.map((name, idx) => ({
      name,
      type: baseData.metadata[idx] || 'VARCHAR',
    }));
  }, [baseData]);

  // Detect stale column mappings after data changes
  useEffect(() => {
    if (columns.length === 0) {
      setStaleMapping(false);
      return;
    }
    const colNames = new Set(columns.map(c => c.name));
    const mappedCols: string[] = [];
    if (config.xAxis) {
      mappedCols.push(config.xAxis);
    }
    if (config.yAxis) {
      mappedCols.push(config.yAxis);
    }
    if (config.metrics) {
      mappedCols.push(...config.metrics);
    }
    if (config.dimensions) {
      mappedCols.push(...config.dimensions);
    }
    const hasStale = mappedCols.length > 0 && mappedCols.some(col => !colNames.has(col));
    setStaleMapping(hasStale);
  }, [columns, config.xAxis, config.yAxis, config.metrics, config.dimensions]);

  const [effectiveQuery, setEffectiveQuery] = useState<string>('');

  // Compute effective query asynchronously — gated on configReady so we don't
  // fire with stale/incomplete config before the saved visualization is loaded.
  useEffect(() => {
    if (!configReady || !editedSql) {
      setEffectiveQuery('');
      return;
    }
    let cancelled = false;
    getEffectiveQuery(editedSql, config)
      .then((result) => {
        if (!cancelled) {
          setEffectiveQuery(result);
        }
      })
      .catch((err) => {
        console.error('[VisualizationEditor] getEffectiveQuery failed:', err);
        if (!cancelled) {
          setEffectiveQuery(editedSql);
        }
      });
    return () => { cancelled = true; };
  }, [configReady, editedSql, config]);

  // Data for chart preview: prefer aggregated data when available
  const previewData = aggregatedData || baseData;

  // Fetch aggregated data when the effective query differs from the base SQL
  useEffect(() => {
    if (!open || !editedSql) {
      return;
    }
    if (effectiveQuery === editedSql || !effectiveQuery) {
      setAggregatedData(null);
      setAggregatedError(null);
      return;
    }
    let cancelled = false;
    const fetchAggregated = async () => {
      setAggregatedLoading(true);
      setAggregatedError(null);
      try {
        const result = await executeQuery({
          query: effectiveQuery,
          queryType: 'SQL',
          autoLimitRowCount: 10000,
          defaultSchema: visualization?.defaultSchema,
        });
        if (!cancelled) {
          setAggregatedData(result);
          setAggregatedError(null);
        }
      } catch (err) {
        if (!cancelled) {
          const errMsg = (err as Error).message;
          setAggregatedData(null);
          setAggregatedError(`Aggregation query failed: ${errMsg}\n\nGenerated SQL:\n${effectiveQuery}`);
        }
      } finally {
        if (!cancelled) {
          setAggregatedLoading(false);
        }
      }
    };
    fetchAggregated();
    return () => { cancelled = true; };
  }, [effectiveQuery, editedSql, open, visualization?.defaultSchema]);

  // Update mutation
  const editMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<VisualizationCreate> }) =>
      updateVisualization(id, data),
    onSuccess: () => {
      message.success('Visualization updated successfully');
      queryClient.invalidateQueries({ queryKey: ['visualizations'] });
      handleClose();
    },
    onError: (error: Error) => {
      message.error(`Failed to update visualization: ${error.message}`);
    },
  });

  const handleClose = () => {
    setConfigReady(false);
    setChartType('bar');
    setConfig({});
    setBaseData(null);
    setDataLoading(false);
    setDataError(null);
    setAggregatedData(null);
    setAggregatedLoading(false);
    setAggregatedError(null);
    setShowSql(true);
    setEditedSql('');
    setSqlDirty(false);
    setStaleMapping(false);
    form.resetFields();
    onClose();
  };

  const handleSave = async () => {
    if (!visualization) {
      return;
    }
    try {
      const values = await form.validateFields();

      const payload: Partial<VisualizationCreate> = {
        name: values.name,
        description: values.description,
        chartType,
        config: {
          ...config,
          colorScheme: values.colorScheme,
        },
        isPublic: values.isPublic || false,
        sql: editedSql,
      };

      editMutation.mutate({ id: visualization.id, data: payload });
    } catch {
      message.error('Please fill in all required fields');
    }
  };

  return (
    <Modal
      title="Edit Visualization"
      open={open}
      onCancel={handleClose}
      width="95vw"
      style={{ maxWidth: 1400, top: 20 }}
      footer={null}
      destroyOnClose
      styles={{
        body: {
          height: '80vh',
          overflow: 'hidden',
          padding: 0,
        },
      }}
    >
      <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
        <Row style={{ flex: 1, overflow: 'hidden' }}>
          {/* Left Panel - Controls */}
          <Col
            span={8}
            style={{
              height: '100%',
              overflowY: 'auto',
              borderRight: '1px solid var(--color-border)',
              padding: 16,
            }}
          >
            {/* Chart Type Section */}
            <Card size="small" title="Chart Type" style={{ marginBottom: 12 }}>
              <ChartTypeSelector value={chartType} onChange={setChartType} compact />
            </Card>

            {/* Data Mapping Section */}
            <Card size="small" title="Data Mapping" style={{ marginBottom: 12 }}>
              {dataLoading ? (
                <div style={{ padding: 16, textAlign: 'center' }}>
                  <Spin size="small" />
                  <div style={{ marginTop: 8 }}>
                    <Text type="secondary">Loading columns...</Text>
                  </div>
                </div>
              ) : dataError ? (
                <Alert
                  type="warning"
                  message="Could not load data"
                  description={dataError}
                  showIcon
                  style={{ marginBottom: 8 }}
                  action={
                    <Button size="small" icon={<ReloadOutlined />} onClick={fetchData}>
                      Retry
                    </Button>
                  }
                />
              ) : (
                <>
                  {staleMapping && (
                    <Alert
                      type="warning"
                      message="Column mappings may be stale"
                      description="Some mapped columns no longer exist in the query results. Please update the mappings below."
                      showIcon
                      icon={<WarningOutlined />}
                      style={{ marginBottom: 8 }}
                    />
                  )}
                  <ColumnMapper
                    columns={columns}
                    chartType={chartType}
                    config={config}
                    onChange={setConfig}
                  />
                </>
              )}
            </Card>

            {/* Details Section */}
            <Card size="small" title="Details">
              <Form
                form={form}
                layout="vertical"
                size="small"
                initialValues={{ colorScheme: 'default', isPublic: false }}
              >
                <Form.Item
                  name="name"
                  label="Name"
                  rules={[{ required: true, message: 'Please enter a name' }]}
                >
                  <Input placeholder="Visualization name" />
                </Form.Item>

                <Form.Item name="description" label="Description">
                  <TextArea placeholder="Optional description" rows={2} />
                </Form.Item>

                <Form.Item name="colorScheme" label="Color Scheme">
                  <Select options={colorSchemeOptions} />
                </Form.Item>

                <Form.Item name="isPublic" label="Public" valuePropName="checked">
                  <Switch />
                </Form.Item>
              </Form>
            </Card>
          </Col>

          {/* Right Panel - Preview */}
          <Col
            span={16}
            style={{
              height: '100%',
              display: 'flex',
              flexDirection: 'column',
              overflow: 'hidden',
            }}
          >
            {/* Toolbar */}
            <div
              style={{
                padding: '8px 16px',
                borderBottom: '1px solid var(--color-border)',
                display: 'flex',
                justifyContent: 'flex-end',
                gap: 8,
              }}
            >
              <Button
                icon={<ReloadOutlined />}
                onClick={fetchData}
                loading={dataLoading}
              >
                Refresh Data
              </Button>
              <Button
                type="primary"
                icon={<SaveOutlined />}
                onClick={handleSave}
                loading={editMutation.isPending}
              >
                Update Visualization
              </Button>
            </div>

            {/* Chart Preview */}
            <div style={{ flex: 1, padding: 16, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
              {aggregatedError && (
                <Alert
                  message="Aggregation Error"
                  description={
                    <pre style={{ whiteSpace: 'pre-wrap', fontSize: 11, margin: 0 }}>
                      {aggregatedError}
                    </pre>
                  }
                  type="error"
                  showIcon
                  closable
                  onClose={() => setAggregatedError(null)}
                  style={{ marginBottom: 8, flexShrink: 0 }}
                />
              )}
              <div style={{ flex: 1, minHeight: 0 }}>
                <ChartPreview
                  chartType={chartType}
                  config={{ ...config, colorScheme: selectedColorScheme }}
                  data={previewData}
                  loading={dataLoading || aggregatedLoading}
                  height="100%"
                />
              </div>
            </div>

            {/* SQL Panel (collapsible) */}
            <div
              style={{
                borderTop: '1px solid var(--color-border)',
                padding: '0 16px',
              }}
            >
              <Button
                type="text"
                size="small"
                icon={<CodeOutlined />}
                onClick={() => setShowSql(!showSql)}
                style={{ margin: '4px 0' }}
              >
                {showSql ? 'Hide SQL' : 'Edit SQL'}
              </Button>
              {showSql && (
                <>
                  {sqlDirty && (
                    <Text type="warning" style={{ fontSize: 11, display: 'block', marginBottom: 4 }}>
                      <WarningOutlined /> SQL modified but not yet run. Click Run to refresh the preview.
                    </Text>
                  )}
                  {effectiveQuery !== editedSql && editedSql && (
                    <>
                      <Text type="secondary" style={{ fontSize: 11, display: 'block', marginBottom: 4 }}>
                        Effective Query (with aggregation)
                      </Text>
                      <pre
                        style={{
                          background: 'var(--color-bg-elevated)',
                          padding: 12,
                          borderRadius: 4,
                          marginBottom: 8,
                          maxHeight: 80,
                          overflow: 'auto',
                          fontFamily: 'monospace',
                          fontSize: 12,
                          border: '2px solid #722ed1',
                        }}
                      >
                        {effectiveQuery}
                      </pre>
                    </>
                  )}
                  <div style={{ display: 'flex', gap: 8, alignItems: 'flex-start' }}>
                    <TextArea
                      value={editedSql}
                      onChange={(e) => {
                        setEditedSql(e.target.value);
                        setSqlDirty(true);
                      }}
                      autoSize={{ minRows: 3, maxRows: 8 }}
                      style={{
                        flex: 1,
                        fontFamily: 'monospace',
                        fontSize: 12,
                        border: sqlDirty ? '2px solid #faad14' : '1px solid var(--color-border)',
                      }}
                    />
                    <Button
                      type="primary"
                      icon={<PlayCircleOutlined />}
                      onClick={fetchData}
                      loading={dataLoading}
                      style={{ flexShrink: 0 }}
                    >
                      Run
                    </Button>
                  </div>
                </>
              )}
            </div>
          </Col>
        </Row>
      </div>
    </Modal>
  );
}
