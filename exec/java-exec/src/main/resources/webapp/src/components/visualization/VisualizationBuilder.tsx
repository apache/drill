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
import { useState, useMemo } from 'react';
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
} from 'antd';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { createVisualization } from '../../api/visualizations';
import ChartTypeSelector from './ChartTypeSelector';
import ColumnMapper from './ColumnMapper';
import ChartPreview from './ChartPreview';
import type { ChartType, VisualizationConfig, QueryResult, VisualizationCreate } from '../../types';

const { Title, Text } = Typography;
const { TextArea } = Input;

interface VisualizationBuilderProps {
  open: boolean;
  onClose: () => void;
  queryResult: QueryResult | null;
  savedQueryId?: string;
  sql?: string;
  defaultSchema?: string;
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
}: VisualizationBuilderProps) {
  const [currentStep, setCurrentStep] = useState(0);
  const [chartType, setChartType] = useState<ChartType>('bar');
  const [config, setConfig] = useState<VisualizationConfig>({});
  const [form] = Form.useForm();
  const queryClient = useQueryClient();

  // Watch colorScheme from form so preview updates live
  const selectedColorScheme = Form.useWatch('colorScheme', form) || 'default';

  // Extract column info from query result
  const columns = useMemo(() => {
    if (!queryResult || !queryResult.columns || !queryResult.metadata) {
      return [];
    }
    return queryResult.columns.map((name, idx) => ({
      name,
      type: queryResult.metadata[idx] || 'VARCHAR',
    }));
  }, [queryResult]);

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

  const handleClose = () => {
    setCurrentStep(0);
    setChartType('bar');
    setConfig({});
    form.resetFields();
    onClose();
  };

  const handleNext = () => {
    if (currentStep === 2) {
      // Final step - save
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

      const visualization: VisualizationCreate = {
        name: values.name,
        description: values.description,
        savedQueryId: savedQueryId,
        chartType,
        config: {
          ...config,
          colorScheme: values.colorScheme,
        },
        isPublic: values.isPublic || false,
        sql,
        defaultSchema,
      };

      createMutation.mutate(visualization);
    } catch {
      // Form validation failed
      message.error('Please fill in all required fields');
    }
  };

  const canProceed = () => {
    switch (currentStep) {
      case 0:
        return !!chartType;
      case 1:
        // Check if minimum config is set based on chart type
        if (chartType === 'table') {
          return true; // Table doesn't require config
        }
        if (chartType === 'gauge') {
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
                data={queryResult}
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
                data={queryResult}
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
      title="Create Visualization"
      open={open}
      onCancel={handleClose}
      width={900}
      footer={null}
      destroyOnClose
    >
      <Steps current={currentStep} items={steps.map((s) => ({ title: s.title }))} />

      <Divider />

      {!queryResult || queryResult.rows.length === 0 ? (
        <div style={{ padding: 40, textAlign: 'center' }}>
          <Text type="secondary">
            No query results available. Please run a query first to create a visualization.
          </Text>
        </div>
      ) : (
        <>
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
                loading={createMutation.isPending}
              >
                {currentStep === 2 ? 'Save Visualization' : 'Next'}
              </Button>
            </Space>
          </div>
        </>
      )}
    </Modal>
  );
}
