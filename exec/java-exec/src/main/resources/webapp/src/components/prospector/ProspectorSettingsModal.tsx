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
import { useState, useEffect, useCallback } from 'react';
import {
  Modal,
  Form,
  Input,
  Select,
  Switch,
  Slider,
  InputNumber,
  Button,
  message,
  Space,
  Alert,
} from 'antd';
import { getAiConfig, updateAiConfig, testAiConfig, getAiProviders } from '../../api/ai';
import type { AiConfig, AiProvider } from '../../types/ai';

const { TextArea } = Input;

interface ProspectorSettingsModalProps {
  open: boolean;
  onClose: () => void;
}

export default function ProspectorSettingsModal({ open, onClose }: ProspectorSettingsModalProps) {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [testing, setTesting] = useState(false);
  const [providers, setProviders] = useState<AiProvider[]>([]);
  const [config, setConfig] = useState<AiConfig | null>(null);
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null);

  const loadConfig = useCallback(async () => {
    try {
      const cfg = await getAiConfig();
      setConfig(cfg);
      form.setFieldsValue({
        provider: cfg.provider || 'openai',
        apiEndpoint: cfg.apiEndpoint || '',
        model: cfg.model || '',
        maxTokens: cfg.maxTokens || 4096,
        temperature: cfg.temperature ?? 0.7,
        enabled: cfg.enabled || false,
        systemPrompt: cfg.systemPrompt || '',
        sendDataToAi: cfg.sendDataToAi ?? true,
      });
    } catch {
      // Config endpoint may 403 if not admin
      message.error('Failed to load AI configuration. Admin access required.');
    }
  }, [form]);

  const loadProviders = useCallback(async () => {
    try {
      const provs = await getAiProviders();
      setProviders(provs);
    } catch {
      // Use defaults
      setProviders([
        { id: 'openai', displayName: 'OpenAI Compatible' },
        { id: 'anthropic', displayName: 'Anthropic Claude' },
      ]);
    }
  }, []);

  useEffect(() => {
    if (open) {
      loadConfig();
      loadProviders();
    }
  }, [open, loadConfig, loadProviders]);

  const handleSave = useCallback(async () => {
    try {
      const values = await form.validateFields();
      setLoading(true);
      await updateAiConfig({
        provider: values.provider,
        apiEndpoint: values.apiEndpoint || undefined,
        apiKey: values.apiKey || undefined,
        model: values.model,
        maxTokens: values.maxTokens,
        temperature: values.temperature,
        enabled: values.enabled,
        systemPrompt: values.systemPrompt || undefined,
        sendDataToAi: values.sendDataToAi,
      });
      message.success('AI configuration saved');
      onClose();
    } catch (err) {
      if (err && typeof err === 'object' && 'errorFields' in err) {
        return; // Form validation error
      }
      message.error('Failed to save AI configuration');
    } finally {
      setLoading(false);
    }
  }, [form, onClose]);

  const handleTest = useCallback(async () => {
    try {
      const values = await form.validateFields();
      setTesting(true);
      setTestResult(null);
      const result = await testAiConfig({
        provider: values.provider,
        apiEndpoint: values.apiEndpoint || undefined,
        apiKey: values.apiKey || undefined,
        model: values.model,
      });
      setTestResult(result);
    } catch {
      setTestResult({ success: false, message: 'Test failed' });
    } finally {
      setTesting(false);
    }
  }, [form]);

  return (
    <Modal
      title="Prospector Settings"
      open={open}
      onCancel={onClose}
      width={560}
      footer={
        <Space>
          <Button onClick={onClose}>Cancel</Button>
          <Button onClick={handleTest} loading={testing}>
            Test Connection
          </Button>
          <Button type="primary" onClick={handleSave} loading={loading}>
            Save
          </Button>
        </Space>
      }
    >
      {testResult && (
        <Alert
          type={testResult.success ? 'success' : 'error'}
          message={testResult.message}
          showIcon
          closable
          onClose={() => setTestResult(null)}
          style={{ marginBottom: 16 }}
        />
      )}

      <Form
        form={form}
        layout="vertical"
        initialValues={{
          provider: 'openai',
          maxTokens: 4096,
          temperature: 0.7,
          enabled: false,
          sendDataToAi: true,
        }}
      >
        <Form.Item name="enabled" label="Enable Prospector" valuePropName="checked">
          <Switch />
        </Form.Item>

        <Form.Item name="provider" label="LLM Provider" rules={[{ required: true }]}>
          <Select>
            {providers.map((p) => (
              <Select.Option key={p.id} value={p.id}>
                {p.displayName}
              </Select.Option>
            ))}
          </Select>
        </Form.Item>

        <Form.Item
          name="apiEndpoint"
          label="API Endpoint"
          help="Leave empty for default endpoint"
        >
          <Input placeholder="https://api.openai.com/v1" />
        </Form.Item>

        <Form.Item
          name="apiKey"
          label="API Key"
          help={config?.apiKeySet ? 'API key is set. Enter a new value to change it.' : undefined}
        >
          <Input.Password
            placeholder={config?.apiKeySet ? '(unchanged)' : 'Enter API key'}
          />
        </Form.Item>

        <Form.Item name="model" label="Model" rules={[{ required: true }]}>
          <Input placeholder="gpt-4o, claude-sonnet-4-20250514, llama3, etc." />
        </Form.Item>

        <Form.Item name="maxTokens" label="Max Tokens">
          <InputNumber min={100} max={128000} style={{ width: '100%' }} />
        </Form.Item>

        <Form.Item name="temperature" label="Temperature">
          <Slider min={0} max={2} step={0.1} marks={{ 0: '0', 1: '1', 2: '2' }} />
        </Form.Item>

        <Form.Item name="systemPrompt" label="Custom System Prompt">
          <TextArea
            rows={3}
            placeholder="Additional instructions for the AI assistant..."
          />
        </Form.Item>

        <Form.Item
          name="sendDataToAi"
          label="Send Data Samples to AI"
          valuePropName="checked"
          help="When enabled, a sample of query result rows is included when optimizing queries, helping the AI detect data type mismatches."
        >
          <Switch />
        </Form.Item>
      </Form>
    </Modal>
  );
}
