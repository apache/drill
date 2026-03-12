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
import { useEffect, useState } from 'react';
import { Modal, Form, InputNumber, Spin, message } from 'antd';
import { getProfileConfig, updateProfileConfig } from '../../api/profile';
import type { ProfileConfig } from '../../types/profile';

interface Props {
  open: boolean;
  onClose: () => void;
}

export default function ProfileSettingsModal({ open, onClose }: Props) {
  const [form] = Form.useForm<ProfileConfig>();
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (open) {
      setLoading(true);
      getProfileConfig()
        .then((config) => form.setFieldsValue(config))
        .finally(() => setLoading(false));
    }
  }, [open, form]);

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      setSaving(true);
      await updateProfileConfig(values);
      message.success('Profile settings saved');
      onClose();
    } catch (err) {
      if (err instanceof Error) {
        message.error(`Failed to save: ${err.message}`);
      }
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal
      title="Data Profiler Settings"
      open={open}
      onOk={handleSave}
      onCancel={onClose}
      confirmLoading={saving}
      okText="Save"
    >
      <Spin spinning={loading}>
        <Form form={form} layout="vertical">
          <Form.Item
            name="correlationMaxColumns"
            label="Correlation Max Columns"
            tooltip="Maximum number of numeric columns to include in the correlation matrix"
            rules={[{ type: 'number', min: 2, max: 100 }]}
          >
            <InputNumber min={2} max={100} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item
            name="profileMaxRows"
            label="Profile Max Rows"
            tooltip="Maximum rows scanned by aggregate SQL queries"
            rules={[{ type: 'number', min: 1000, max: 1000000 }]}
          >
            <InputNumber min={1000} max={1000000} step={1000} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item
            name="profileSampleSize"
            label="Sample Size"
            tooltip="Rows fetched for client-side distribution computation"
            rules={[{ type: 'number', min: 100, max: 100000 }]}
          >
            <InputNumber min={100} max={100000} step={1000} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item
            name="histogramBins"
            label="Histogram Bins"
            tooltip="Number of bins for numeric histograms"
            rules={[{ type: 'number', min: 5, max: 100 }]}
          >
            <InputNumber min={5} max={100} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item
            name="topKValues"
            label="Top K Values"
            tooltip="Number of frequent values to show for categorical columns"
            rules={[{ type: 'number', min: 3, max: 50 }]}
          >
            <InputNumber min={3} max={50} style={{ width: '100%' }} />
          </Form.Item>
        </Form>
      </Spin>
    </Modal>
  );
}
