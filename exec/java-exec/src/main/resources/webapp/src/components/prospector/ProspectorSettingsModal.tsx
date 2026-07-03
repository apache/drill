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
  Alert,
  Collapse,
  Space,
  Divider,
} from 'antd';
import { getAiConfig, updateAiConfig, testAiConfig, getAiProviders } from '../../api/ai';
import type { AiConfig, AiProvider } from '../../types/ai';

const { TextArea } = Input;

interface BodyProps {
  /** Called after a successful save. May close the host modal/sheet. */
  onSaved?: () => void;
  /** Show a "Cancel" button alongside Save/Test. */
  showCancel?: boolean;
  onCancel?: () => void;
}

/**
 * The form body — usable inside the standalone modal or the consolidated Preferences sheet.
 */
export function ProspectorSettingsBody({ onSaved, showCancel, onCancel }: BodyProps) {
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

      // Stringify JSON fields for textarea display
      const customHeadersStr = cfg.customHeaders ? JSON.stringify(cfg.customHeaders, null, 2) : '';
      const additionalParametersStr = cfg.additionalParameters ? JSON.stringify(cfg.additionalParameters, null, 2) : '';

      form.setFieldsValue({
        provider: cfg.provider || 'openai',
        apiEndpoint: cfg.apiEndpoint || '',
        model: cfg.model || '',
        maxTokens: cfg.maxTokens || 4096,
        temperature: cfg.temperature ?? 0.7,
        enabled: cfg.enabled || false,
        systemPrompt: cfg.systemPrompt || '',
        sendDataToAi: cfg.sendDataToAi ?? true,
        maxToolRounds: cfg.maxToolRounds || 15,

        // Network Configuration
        customHeaders: customHeadersStr,
        proxyUrl: cfg.proxyUrl || '',
        proxyUsername: cfg.proxyUsername || '',
        connectTimeoutSeconds: cfg.connectTimeoutSeconds || 30,
        readTimeoutSeconds: cfg.readTimeoutSeconds || 120,
        writeTimeoutSeconds: cfg.writeTimeoutSeconds || 30,

        // SSL/TLS Configuration
        keystorePath: cfg.keystorePath || '',
        keystoreType: cfg.keystoreType || 'JKS',
        truststorePath: cfg.truststorePath || '',
        truststoreType: cfg.truststoreType || 'JKS',
        verifySSL: cfg.verifySSL !== false,

        // Additional Request Parameters
        additionalParameters: additionalParametersStr,

        // Custom API Format
        requestTemplate: cfg.requestTemplate || '',
        responseMapping: cfg.responseMapping || '',
      });
    } catch {
      message.error('Failed to load AI configuration. Admin access required.');
    }
  }, [form]);

  const loadProviders = useCallback(async () => {
    try {
      const provs = await getAiProviders();
      setProviders(provs);
    } catch {
      setProviders([
        { id: 'openai', displayName: 'OpenAI Compatible' },
        { id: 'anthropic', displayName: 'Anthropic Claude' },
      ]);
    }
  }, []);

  useEffect(() => {
    loadConfig();
    loadProviders();
  }, [loadConfig, loadProviders]);

  const handleSave = useCallback(async () => {
    try {
      const values = await form.validateFields();
      setLoading(true);

      // Parse JSON fields
      let customHeaders = undefined;
      if (values.customHeaders && typeof values.customHeaders === 'string') {
        try {
          customHeaders = values.customHeaders ? JSON.parse(values.customHeaders) : undefined;
        } catch (e) {
          message.error('Invalid JSON in Custom Headers');
          return;
        }
      }

      let additionalParameters = undefined;
      if (values.additionalParameters && typeof values.additionalParameters === 'string') {
        try {
          additionalParameters = values.additionalParameters ? JSON.parse(values.additionalParameters) : undefined;
        } catch (e) {
          message.error('Invalid JSON in Additional Parameters');
          return;
        }
      }

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
        maxToolRounds: values.maxToolRounds,

        // Network Configuration
        customHeaders,
        proxyUrl: values.proxyUrl || undefined,
        proxyUsername: values.proxyUsername || undefined,
        proxyPassword: values.proxyPassword || undefined,
        connectTimeoutSeconds: values.connectTimeoutSeconds || undefined,
        readTimeoutSeconds: values.readTimeoutSeconds || undefined,
        writeTimeoutSeconds: values.writeTimeoutSeconds || undefined,

        // SSL/TLS Configuration
        keystorePath: values.keystorePath || undefined,
        keystorePassword: values.keystorePassword || undefined,
        keystoreType: values.keystoreType || undefined,
        truststorePath: values.truststorePath || undefined,
        truststorePassword: values.truststorePassword || undefined,
        truststoreType: values.truststoreType || undefined,
        verifySSL: values.verifySSL,

        // Additional Request Parameters
        additionalParameters,

        // Custom API Format
        requestTemplate: values.requestTemplate || undefined,
        responseMapping: values.responseMapping || undefined,
      });
      message.success('AI configuration saved');
      onSaved?.();
    } catch (err) {
      if (err && typeof err === 'object' && 'errorFields' in err) {
        return;
      }
      message.error('Failed to save AI configuration');
    } finally {
      setLoading(false);
    }
  }, [form, onSaved]);

  const handleTest = useCallback(async () => {
    try {
      const values = await form.validateFields();
      setTesting(true);
      setTestResult(null);

      // Parse JSON fields for test
      let customHeaders = undefined;
      if (values.customHeaders) {
        try {
          customHeaders = JSON.parse(values.customHeaders);
        } catch (e) {
          setTestResult({ success: false, message: 'Invalid JSON in Custom Headers' });
          return;
        }
      }

      let additionalParameters = undefined;
      if (values.additionalParameters) {
        try {
          additionalParameters = JSON.parse(values.additionalParameters);
        } catch (e) {
          setTestResult({ success: false, message: 'Invalid JSON in Additional Parameters' });
          return;
        }
      }

      const result = await testAiConfig({
        provider: values.provider,
        apiEndpoint: values.apiEndpoint || undefined,
        apiKey: values.apiKey || undefined,
        model: values.model,
        customHeaders,
        proxyUrl: values.proxyUrl || undefined,
        proxyUsername: values.proxyUsername || undefined,
        proxyPassword: values.proxyPassword || undefined,
        connectTimeoutSeconds: values.connectTimeoutSeconds || undefined,
        readTimeoutSeconds: values.readTimeoutSeconds || undefined,
        writeTimeoutSeconds: values.writeTimeoutSeconds || undefined,
        keystorePath: values.keystorePath || undefined,
        keystorePassword: values.keystorePassword || undefined,
        keystoreType: values.keystoreType || undefined,
        truststorePath: values.truststorePath || undefined,
        truststorePassword: values.truststorePassword || undefined,
        truststoreType: values.truststoreType || undefined,
        verifySSL: values.verifySSL,
        additionalParameters,
        requestTemplate: values.requestTemplate || undefined,
        responseMapping: values.responseMapping || undefined,
      });
      setTestResult(result);
    } catch {
      setTestResult({ success: false, message: 'Test failed' });
    } finally {
      setTesting(false);
    }
  }, [form]);

  return (
    <div className="settings-body">
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
          maxToolRounds: 15,
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

        <Form.Item name="apiEndpoint" label="API Endpoint" help="Leave empty for default endpoint">
          <Input placeholder="https://api.openai.com/v1" />
        </Form.Item>

        <Form.Item
          name="apiKey"
          label="API Key"
          help={config?.apiKeySet ? 'API key is set. Enter a new value to change it.' : undefined}
        >
          <Input.Password placeholder={config?.apiKeySet ? '(unchanged)' : 'Enter API key'} />
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
          <TextArea rows={3} placeholder="Additional instructions for the AI assistant..." />
        </Form.Item>

        <Form.Item
          name="sendDataToAi"
          label="Send Data Samples to AI"
          valuePropName="checked"
          help="When enabled, a sample of query result rows is included when optimizing queries."
        >
          <Switch />
        </Form.Item>

        <Form.Item
          name="maxToolRounds"
          label="Max Tool Rounds"
          help="Maximum number of tool call rounds per message."
        >
          <InputNumber min={1} max={50} style={{ width: '100%' }} />
        </Form.Item>

        {/* Additional Request Parameters Section */}
        <Collapse
          ghost
          items={[
            {
              key: 'params',
              label: 'Additional Request Parameters',
              children: (
                <>
                  <Alert
                    message="Static key-value pairs sent with every API request"
                    type="info"
                    style={{ marginBottom: 16 }}
                  />
                  <Form.Item
                    name="additionalParameters"
                    label="Parameters (JSON)"
                    help="Enter as JSON object, e.g., {tenant_id: abc123, environment: production}"
                  >
                    <TextArea rows={4} placeholder='{"tenant_id": "abc123"}' />
                  </Form.Item>
                </>
              ),
            },
          ]}
        />

        {/* Network Configuration Section */}
        <Collapse
          ghost
          items={[
            {
              key: 'network',
              label: 'Network Configuration',
              children: (
                <>
                  <Form.Item name="proxyUrl" label="HTTP Proxy URL">
                    <Input placeholder="http://proxy.example.com:8080" />
                  </Form.Item>

                  <Form.Item name="proxyUsername" label="Proxy Username">
                    <Input autoComplete="off" />
                  </Form.Item>

                  <Form.Item name="proxyPassword" label="Proxy Password">
                    <Input.Password autoComplete="new-password" />
                  </Form.Item>

                  <Form.Item label="Timeouts (seconds)">
                    <Space>
                      <Form.Item name="connectTimeoutSeconds" noStyle>
                        <InputNumber min={1} max={300} placeholder="Connect (30)" />
                      </Form.Item>
                      <span>Connect</span>
                      <Form.Item name="readTimeoutSeconds" noStyle>
                        <InputNumber min={1} max={600} placeholder="Read (120)" />
                      </Form.Item>
                      <span>Read</span>
                      <Form.Item name="writeTimeoutSeconds" noStyle>
                        <InputNumber min={1} max={300} placeholder="Write (30)" />
                      </Form.Item>
                      <span>Write</span>
                    </Space>
                  </Form.Item>
                </>
              ),
            },
          ]}
        />

        {/* SSL/TLS Configuration Section */}
        <Collapse
          ghost
          items={[
            {
              key: 'ssl',
              label: 'SSL/TLS Configuration',
              children: (
                <>
                  <Form.Item
                    name="verifySSL"
                    label="Verify SSL Certificates"
                    valuePropName="checked"
                    help="Disable only for testing with self-signed certificates"
                  >
                    <Switch />
                  </Form.Item>

                  <Divider orientation="left" plain>
                    Custom CA Certificate (Truststore)
                  </Divider>

                  <Form.Item
                    name="truststorePath"
                    label="Truststore Path"
                    help="Absolute path to truststore file (JKS or PKCS12)"
                  >
                    <Input placeholder="/opt/drill/certs/truststore.jks" />
                  </Form.Item>

                  <Form.Item name="truststorePassword" label="Truststore Password">
                    <Input.Password autoComplete="new-password" />
                  </Form.Item>

                  <Form.Item name="truststoreType" label="Truststore Type">
                    <Select>
                      <Select.Option value="JKS">JKS</Select.Option>
                      <Select.Option value="PKCS12">PKCS12</Select.Option>
                    </Select>
                  </Form.Item>

                  <Divider orientation="left" plain>
                    Client Certificate (mTLS)
                  </Divider>

                  <Form.Item
                    name="keystorePath"
                    label="Keystore Path"
                    help="Absolute path to client certificate keystore"
                  >
                    <Input placeholder="/opt/drill/certs/client-keystore.jks" />
                  </Form.Item>

                  <Form.Item name="keystorePassword" label="Keystore Password">
                    <Input.Password autoComplete="new-password" />
                  </Form.Item>

                  <Form.Item name="keystoreType" label="Keystore Type">
                    <Select>
                      <Select.Option value="JKS">JKS</Select.Option>
                      <Select.Option value="PKCS12">PKCS12</Select.Option>
                    </Select>
                  </Form.Item>
                </>
              ),
            },
          ]}
        />

        {/* Custom Headers Section */}
        <Collapse
          ghost
          items={[
            {
              key: 'headers',
              label: 'Custom HTTP Headers',
              children: (
                <>
                  <Alert
                    message="Headers sent with every API request (e.g., X-Tenant-ID, X-Environment)"
                    type="info"
                    style={{ marginBottom: 16 }}
                  />
                  <Form.Item
                    name="customHeaders"
                    label="Headers (JSON)"
                    help="Enter as JSON object, e.g., {X-Tenant-ID: tenant123}"
                  >
                    <TextArea rows={4} placeholder='{"X-Tenant-ID": "tenant123"}' />
                  </Form.Item>
                </>
              ),
            },
          ]}
        />

        {/* Custom API Format Section (only show for enterprise provider) */}
        {form.getFieldValue('provider') === 'enterprise' && (
          <Collapse
            ghost
            items={[
              {
                key: 'custom-api',
                label: 'Custom API Format',
                children: (
                  <>
                    <Form.Item
                      name="requestTemplate"
                      label="Request Template (JSON)"
                      help="Template for custom API request format. Supports: {model}, {temperature}, {maxTokens}, {messages}, {tools}"
                      rules={[
                        { required: true, message: 'Request template is required for enterprise provider' },
                      ]}
                    >
                      <TextArea rows={6} placeholder='{"query": "{messages}", "model": "{model}"}' />
                    </Form.Item>

                    <Form.Item
                      name="responseMapping"
                      label="Response Mapping (JSONPath)"
                      help="Map custom API response to standard format (optional). Example: contentPath: $.data.text, donePath: $.data.finish_reason"
                    >
                      <TextArea
                        rows={4}
                        placeholder='{"contentPath": "$.data.text", "donePath": "$.data.finish_reason"}'
                      />
                    </Form.Item>
                  </>
                ),
              },
            ]}
          />
        )}
      </Form>

      <div className="settings-body-actions">
        {showCancel && <Button onClick={onCancel}>Cancel</Button>}
        <Button onClick={handleTest} loading={testing}>Test Connection</Button>
        <Button type="primary" onClick={handleSave} loading={loading}>Save</Button>
      </div>
    </div>
  );
}

interface ProspectorSettingsModalProps {
  open: boolean;
  onClose: () => void;
}

export default function ProspectorSettingsModal({ open, onClose }: ProspectorSettingsModalProps) {
  return (
    <Modal title="Prospector Settings" open={open} onCancel={onClose} width={560} footer={null} destroyOnClose>
      <ProspectorSettingsBody onSaved={onClose} showCancel onCancel={onClose} />
    </Modal>
  );
}
