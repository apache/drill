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
import { useState, useMemo, useEffect, useCallback, useRef } from 'react';
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
import { buildTimeGrainQuery, hasCompleteTimeGrainConfig } from '../../utils/sqlTransformations';
import type { TimeGrain, AggregationFunction } from '../../utils/sqlTransformations';
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

  // Data fetching state
  const [fetchedData, setFetchedData] = useState<QueryResult | null>(null);
  const [dataLoading, setDataLoading] = useState(false);
  const [dataError, setDataError] = useState<string | null>(null);

  // Editable SQL state
  const [editedSql, setEditedSql] = useState<string>('');
  const [sqlDirty, setSqlDirty] = useState(false);
  const [staleMapping, setStaleMapping] = useState(false);

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
    }
  }, [open, visualization, form]);

  // Build effective query with time grain if applicable
  const getEffectiveQuery = useCallback((originalSql: string, cfg: VisualizationConfig): string => {
    if (!hasCompleteTimeGrainConfig(cfg.chartOptions, cfg.metrics)) {
      return originalSql;
    }
    const grain = cfg.chartOptions!.timeGrain as TimeGrain;
    const aggregations = cfg.chartOptions!.metricAggregations as Record<string, AggregationFunction>;
    const wrapped = buildTimeGrainQuery(originalSql, {
      grain,
      temporalColumn: cfg.xAxis!,
      metricAggregations: aggregations,
    });
    return wrapped || originalSql;
  }, []);

  // Fetch data from edited SQL
  const fetchData = useCallback(async () => {
    if (!editedSql.trim()) {
      setDataError('SQL query is empty. Please enter a query.');
      return;
    }
    setDataLoading(true);
    setDataError(null);
    try {
      const query = getEffectiveQuery(editedSql, config);
      const result = await executeQuery({
        query,
        queryType: 'SQL',
        autoLimitRowCount: 10000,
        defaultSchema: visualization?.defaultSchema,
      });
      setFetchedData(result);
      setSqlDirty(false);
    } catch (err) {
      setDataError(`Failed to execute query: ${(err as Error).message}`);
    } finally {
      setDataLoading(false);
    }
  }, [editedSql, visualization?.defaultSchema, config, getEffectiveQuery]);

  // Auto-fetch on open
  useEffect(() => {
    if (open && visualization) {
      fetchData();
    }
  }, [open, visualization, fetchData]);

  // Extract column info from fetched data
  const columns = useMemo(() => {
    if (!fetchedData || !fetchedData.columns || !fetchedData.metadata) {
      return [];
    }
    return fetchedData.columns.map((name, idx) => ({
      name,
      type: fetchedData.metadata[idx] || 'VARCHAR',
    }));
  }, [fetchedData]);

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

  const effectiveQuery = useMemo(() => {
    if (!editedSql) {
      return '';
    }
    return getEffectiveQuery(editedSql, config);
  }, [editedSql, config, getEffectiveQuery]);

  // Auto-refetch when effective query changes (time grain / aggregation changes)
  const prevEffectiveQuery = useRef<string>('');
  useEffect(() => {
    if (!open || !editedSql) {
      return;
    }
    // Skip initial render — fetchData is already called on open
    if (prevEffectiveQuery.current === '') {
      prevEffectiveQuery.current = effectiveQuery;
      return;
    }
    if (effectiveQuery !== prevEffectiveQuery.current) {
      prevEffectiveQuery.current = effectiveQuery;
      // Re-fetch with the new effective query
      const refetch = async () => {
        setDataLoading(true);
        setDataError(null);
        try {
          const result = await executeQuery({
            query: effectiveQuery,
            queryType: 'SQL',
            autoLimitRowCount: 10000,
            defaultSchema: visualization?.defaultSchema,
          });
          setFetchedData(result);
          setSqlDirty(false);
        } catch (err) {
          setDataError(`Failed to execute query: ${(err as Error).message}`);
        } finally {
          setDataLoading(false);
        }
      };
      refetch();
    }
  }, [effectiveQuery, open, editedSql, visualization?.defaultSchema]);

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
    setChartType('bar');
    setConfig({});
    setFetchedData(null);
    setDataLoading(false);
    setDataError(null);
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
            <div style={{ flex: 1, padding: 16, overflow: 'hidden' }}>
              <ChartPreview
                chartType={chartType}
                config={{ ...config, colorScheme: selectedColorScheme }}
                data={fetchedData}
                loading={dataLoading}
                height={showSql ? 250 : 450}
              />
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
                        Effective Query (with time grain)
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
