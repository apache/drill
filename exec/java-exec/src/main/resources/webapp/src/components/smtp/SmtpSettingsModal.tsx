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
  InputNumber,
  Button,
  message,
  Space,
  Alert,
} from 'antd';
import { getSmtpConfig, updateSmtpConfig, testSmtpConfig } from '../../api/smtp';
import type { SmtpConfig } from '../../api/smtp';

interface SmtpSettingsModalProps {
  open: boolean;
  onClose: () => void;
}

export default function SmtpSettingsModal({ open, onClose }: SmtpSettingsModalProps) {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [testing, setTesting] = useState(false);
  const [config, setConfig] = useState<SmtpConfig | null>(null);
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null);

  const loadConfig = useCallback(async () => {
    try {
      const cfg = await getSmtpConfig();
      setConfig(cfg);
      form.setFieldsValue({
        host: cfg.host || '',
        port: cfg.port || 587,
        username: cfg.username || '',
        fromAddress: cfg.fromAddress || '',
        fromName: cfg.fromName || 'Apache Drill',
        encryption: cfg.encryption || 'starttls',
        enabled: cfg.enabled || false,
      });
    } catch {
      message.error('Failed to load SMTP configuration. Admin access required.');
    }
  }, [form]);

  useEffect(() => {
    if (open) {
      setTestResult(null);
      loadConfig();
    }
  }, [open, loadConfig]);

  const handleSave = useCallback(async () => {
    try {
      const values = await form.validateFields();
      setLoading(true);
      await updateSmtpConfig({
        host: values.host,
        port: values.port,
        username: values.username || undefined,
        password: values.password || undefined,
        fromAddress: values.fromAddress,
        fromName: values.fromName,
        encryption: values.encryption,
        enabled: values.enabled,
      });
      message.success('SMTP configuration saved');
      onClose();
    } catch (err) {
      if (err && typeof err === 'object' && 'errorFields' in err) {
        return;
      }
      message.error('Failed to save SMTP configuration');
    } finally {
      setLoading(false);
    }
  }, [form, onClose]);

  const handleTest = useCallback(async () => {
    try {
      await form.validateFields();
      setTesting(true);
      setTestResult(null);
      const result = await testSmtpConfig();
      setTestResult(result);
    } catch (err) {
      if (err && typeof err === 'object' && 'errorFields' in err) {
        return;
      }
      setTestResult({ success: false, message: 'Test failed' });
    } finally {
      setTesting(false);
    }
  }, [form]);

  return (
    <Modal
      title="Email (SMTP) Settings"
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
          port: 587,
          fromName: 'Apache Drill',
          encryption: 'starttls',
          enabled: false,
        }}
      >
        <Form.Item name="enabled" label="Enable Email Alerts" valuePropName="checked">
          <Switch />
        </Form.Item>

        <Form.Item
          name="host"
          label="SMTP Host"
          rules={[{ required: true, message: 'SMTP host is required' }]}
        >
          <Input placeholder="smtp.gmail.com" />
        </Form.Item>

        <Form.Item
          name="port"
          label="SMTP Port"
          rules={[{ required: true, message: 'Port is required' }]}
        >
          <InputNumber min={1} max={65535} style={{ width: '100%' }} />
        </Form.Item>

        <Form.Item name="encryption" label="Encryption">
          <Select>
            <Select.Option value="starttls">STARTTLS (port 587)</Select.Option>
            <Select.Option value="ssl">SSL/TLS (port 465)</Select.Option>
            <Select.Option value="none">None (port 25)</Select.Option>
          </Select>
        </Form.Item>

        <Form.Item name="username" label="Username">
          <Input placeholder="user@example.com" />
        </Form.Item>

        <Form.Item
          name="password"
          label="Password"
          help={config?.passwordSet ? 'Password is set. Enter a new value to change it.' : undefined}
        >
          <Input.Password
            placeholder={config?.passwordSet ? '(unchanged)' : 'Enter password'}
          />
        </Form.Item>

        <Form.Item
          name="fromAddress"
          label="From Address"
          rules={[
            { required: true, message: 'From address is required' },
            { type: 'email', message: 'Please enter a valid email address' },
          ]}
        >
          <Input placeholder="drill-alerts@example.com" />
        </Form.Item>

        <Form.Item name="fromName" label="From Name">
          <Input placeholder="Apache Drill" />
        </Form.Item>
      </Form>
    </Modal>
  );
}
