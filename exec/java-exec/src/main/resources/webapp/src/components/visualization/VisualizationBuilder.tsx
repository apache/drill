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
  Steps,
  Button,
  Form,
  Input,
  Switch,
  Select,
  Space,
  Card,
  Row,
  Col,
  Typography,
  message,
  Divider,
  Spin,
  Alert,
} from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { createVisualization, updateVisualization } from '../../api/visualizations';
import { executeQuery } from '../../api/queries';
import ChartTypeSelector from './ChartTypeSelector';
import ColumnMapper from './ColumnMapper';
import ChartPreview from './ChartPreview';
import type { ChartType, VisualizationConfig, QueryResult, VisualizationCreate, Visualization } from '../../types';

const { Title, Text } = Typography;
const { TextArea } = Input;

interface VisualizationBuilderProps {
  open: boolean;
  onClose: () => void;
  queryResult?: QueryResult | null;
  savedQueryId?: string;
  sql?: string;
  defaultSchema?: string;
  visualization?: Visualization | null;
}

const colorSchemeOptions = [
  { value: 'default', label: 'Default' },
  { value: 'warm', label: 'Warm' },
  { value: 'cool', label: 'Cool' },
  { value: 'earth', label: 'Earth' },
];

export default function VisualizationBuilder({
  open,
  onClose,
  queryResult,
  savedQueryId,
  sql,
  defaultSchema,
  visualization,
}: VisualizationBuilderProps) {
  const isEditMode = !!visualization;

  const [currentStep, setCurrentStep] = useState(0);
  const [chartType, setChartType] = useState<ChartType>('bar');
  const [config, setConfig] = useState<VisualizationConfig>({});
  const [form] = Form.useForm();
  const queryClient = useQueryClient();

  // Internal data fetching for edit mode
  const [fetchedData, setFetchedData] = useState<QueryResult | null>(null);
  const [dataLoading, setDataLoading] = useState(false);
  const [dataError, setDataError] = useState<string | null>(null);

  const effectiveData = queryResult || fetchedData;

  // Pre-populate state when opening in edit mode
  useEffect(() => {
    if (open && visualization) {
      setChartType(visualization.chartType);
      setConfig(visualization.config || {});
      form.setFieldsValue({
        name: visualization.name,
        description: visualization.description || '',
        colorScheme: visualization.config?.colorScheme || 'default',
        isPublic: visualization.isPublic || false,
      });
    }
  }, [open, visualization, form]);

  // Fetch data when in edit mode with no external queryResult
  const fetchData = useCallback(async () => {
    if (!visualization?.sql) {
      setDataError('This visualization has no saved SQL query.');
      return;
    }
    setDataLoading(true);
    setDataError(null);
    try {
      const result = await executeQuery({
        query: visualization.sql,
        queryType: 'SQL',
        autoLimitRowCount: 10000,
        defaultSchema: visualization.defaultSchema,
      });
      setFetchedData(result);
    } catch (err) {
      setDataError(`Failed to execute query: ${(err as Error).message}`);
    } finally {
      setDataLoading(false);
    }
  }, [visualization?.sql, visualization?.defaultSchema]);

  useEffect(() => {
    if (open && isEditMode && !queryResult) {
      fetchData();
    }
  }, [open, isEditMode, queryResult, fetchData]);

  // Watch colorScheme from form so preview updates live
  const selectedColorScheme = Form.useWatch('colorScheme', form) || 'default';

  // Extract column info from effective data
  const columns = useMemo(() => {
    if (!effectiveData || !effectiveData.columns || !effectiveData.metadata) {
      return [];
    }
    return effectiveData.columns.map((name, idx) => ({
      name,
      type: effectiveData.metadata[idx] || 'VARCHAR',
    }));
  }, [effectiveData]);

  const createMutation = useMutation({
    mutationFn: createVisualization,
    onSuccess: () => {
      message.success('Visualization created successfully');
      queryClient.invalidateQueries({ queryKey: ['visualizations'] });
      handleClose();
    },
    onError: (error: Error) => {
      message.error(`Failed to create visualization: ${error.message}`);
    },
  });

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

  const isSaving = createMutation.isPending || editMutation.isPending;

  const handleClose = () => {
    setCurrentStep(0);
    setChartType('bar');
    setConfig({});
    setFetchedData(null);
    setDataLoading(false);
    setDataError(null);
    form.resetFields();
    onClose();
  };

  const handleNext = () => {
    if (currentStep === 2) {
      handleSave();
    } else {
      setCurrentStep(currentStep + 1);
    }
  };

  const handleBack = () => {
    setCurrentStep(currentStep - 1);
  };

  const handleSave = async () => {
    try {
      const values = await form.validateFields();

      const payload: VisualizationCreate = {
        name: values.name,
        description: values.description,
        savedQueryId: savedQueryId || visualization?.savedQueryId,
        chartType,
        config: {
          ...config,
          colorScheme: values.colorScheme,
        },
        isPublic: values.isPublic || false,
        sql: sql || visualization?.sql,
        defaultSchema: defaultSchema || visualization?.defaultSchema,
      };

      if (isEditMode) {
        editMutation.mutate({ id: visualization.id, data: payload });
      } else {
        createMutation.mutate(payload);
      }
    } catch {
      message.error('Please fill in all required fields');
    }
  };

  const canProceed = () => {
    switch (currentStep) {
      case 0:
        return !!chartType;
      case 1:
        if (chartType === 'table') {
          return true;
        }
        if (chartType === 'gauge' || chartType === 'bigNumber') {
          return config.metrics && config.metrics.length > 0;
        }
        if (['pie', 'funnel', 'treemap'].includes(chartType)) {
          return config.dimensions && config.dimensions.length > 0 && config.metrics && config.metrics.length > 0;
        }
        if (chartType === 'scatter') {
          return !!config.xAxis && !!config.yAxis;
        }
        return !!config.xAxis && config.metrics && config.metrics.length > 0;
      case 2:
        return true;
      default:
        return false;
    }
  };

  const hasData = effectiveData && effectiveData.rows && effectiveData.rows.length > 0;

  const steps = [
    {
      title: 'Chart Type',
      content: (
        <div style={{ padding: '16px 0' }}>
          <Title level={5}>Select a chart type</Title>
          <ChartTypeSelector value={chartType} onChange={setChartType} />
        </div>
      ),
    },
    {
      title: 'Configure',
      content: (
        <Row gutter={16} style={{ padding: '16px 0' }}>
          <Col span={10}>
            <Card title="Column Mapping" size="small">
              <ColumnMapper
                columns={columns}
                chartType={chartType}
                config={config}
                onChange={setConfig}
              />
            </Card>
          </Col>
          <Col span={14}>
            <Card title="Preview" size="small">
              <ChartPreview
                chartType={chartType}
                config={config}
                data={effectiveData}
                height={350}
              />
            </Card>
          </Col>
        </Row>
      ),
    },
    {
      title: 'Save',
      content: (
        <Row gutter={16} style={{ padding: '16px 0' }}>
          <Col span={12}>
            <Card title="Visualization Details" size="small">
              <Form
                form={form}
                layout="vertical"
                initialValues={{ colorScheme: 'default', isPublic: false }}
              >
                <Form.Item
                  name="name"
                  label="Name"
                  rules={[{ required: true, message: 'Please enter a name' }]}
                >
                  <Input placeholder="Enter visualization name" />
                </Form.Item>

                <Form.Item name="description" label="Description">
                  <TextArea placeholder="Optional description" rows={3} />
                </Form.Item>

                <Form.Item name="colorScheme" label="Color Scheme">
                  <Select options={colorSchemeOptions} />
                </Form.Item>

                <Form.Item name="isPublic" label="Make Public" valuePropName="checked">
                  <Switch />
                </Form.Item>
              </Form>
            </Card>
          </Col>
          <Col span={12}>
            <Card title="Final Preview" size="small">
              <ChartPreview
                chartType={chartType}
                config={{ ...config, colorScheme: selectedColorScheme }}
                data={effectiveData}
                height={300}
              />
            </Card>
          </Col>
        </Row>
      ),
    },
  ];

  return (
    <Modal
      title={isEditMode ? 'Edit Visualization' : 'Create Visualization'}
      open={open}
      onCancel={handleClose}
      width={900}
      footer={null}
      destroyOnClose
    >
      <Steps current={currentStep} items={steps.map((s) => ({ title: s.title }))} />

      <Divider />

      {dataLoading ? (
        <div style={{ padding: 40, textAlign: 'center' }}>
          <Spin size="large" />
          <div style={{ marginTop: 12 }}>
            <Text type="secondary">Loading query data...</Text>
          </div>
        </div>
      ) : dataError ? (
        <div style={{ padding: 24 }}>
          <Alert
            type="warning"
            message="Could not load data"
            description={dataError}
            showIcon
            action={
              <Button size="small" icon={<ReloadOutlined />} onClick={fetchData}>
                Retry
              </Button>
            }
          />
          <div style={{ marginTop: 16 }}>
            {steps[currentStep].content}

            <Divider />

            <div style={{ textAlign: 'right' }}>
              <Space>
                {currentStep > 0 && (
                  <Button onClick={handleBack}>Back</Button>
                )}
                <Button onClick={handleClose}>Cancel</Button>
                <Button
                  type="primary"
                  onClick={handleNext}
                  disabled={!canProceed()}
                  loading={isSaving}
                >
                  {currentStep === 2
                    ? (isEditMode ? 'Update Visualization' : 'Save Visualization')
                    : 'Next'}
                </Button>
              </Space>
            </div>
          </div>
        </div>
      ) : !hasData && !isEditMode ? (
        <div style={{ padding: 40, textAlign: 'center' }}>
          <Text type="secondary">
            No query results available. Please run a query first to create a visualization.
          </Text>
        </div>
      ) : (
        <>
          {isEditMode && !dataLoading && (
            <div style={{ marginBottom: 8, textAlign: 'right' }}>
              <Button size="small" icon={<ReloadOutlined />} onClick={fetchData}>
                Re-run Query
              </Button>
            </div>
          )}

          {steps[currentStep].content}

          <Divider />

          <div style={{ textAlign: 'right' }}>
            <Space>
              {currentStep > 0 && (
                <Button onClick={handleBack}>Back</Button>
              )}
              <Button onClick={handleClose}>Cancel</Button>
              <Button
                type="primary"
                onClick={handleNext}
                disabled={!canProceed()}
                loading={isSaving}
              >
                {currentStep === 2
                  ? (isEditMode ? 'Update Visualization' : 'Save Visualization')
                  : 'Next'}
              </Button>
            </Space>
          </div>
        </>
      )}
    </Modal>
  );
}
