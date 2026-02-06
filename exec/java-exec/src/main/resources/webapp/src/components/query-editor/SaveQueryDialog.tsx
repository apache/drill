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
import { useState } from 'react';
import { Modal, Form, Input, Switch, message } from 'antd';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { createSavedQuery } from '../../api/savedQueries';
import type { SavedQueryCreate } from '../../types';

const { TextArea } = Input;

interface SaveQueryDialogProps {
  open: boolean;
  onClose: () => void;
  sql: string;
  defaultSchema?: string;
}

export default function SaveQueryDialog({
  open,
  onClose,
  sql,
  defaultSchema,
}: SaveQueryDialogProps) {
  const [form] = Form.useForm();
  const queryClient = useQueryClient();
  const [saving, setSaving] = useState(false);

  const mutation = useMutation({
    mutationFn: createSavedQuery,
    onSuccess: () => {
      message.success('Query saved successfully');
      queryClient.invalidateQueries({ queryKey: ['savedQueries'] });
      form.resetFields();
      onClose();
    },
    onError: (error: Error) => {
      message.error(`Failed to save query: ${error.message}`);
    },
    onSettled: () => {
      setSaving(false);
    },
  });

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      setSaving(true);

      const query: SavedQueryCreate = {
        name: values.name,
        description: values.description,
        sql: sql,
        defaultSchema: defaultSchema,
        isPublic: values.isPublic || false,
      };

      mutation.mutate(query);
    } catch {
      // Form validation failed
    }
  };

  const handleCancel = () => {
    form.resetFields();
    onClose();
  };

  return (
    <Modal
      title="Save Query"
      open={open}
      onOk={handleSave}
      onCancel={handleCancel}
      okText="Save"
      confirmLoading={saving}
      destroyOnClose
    >
      <Form
        form={form}
        layout="vertical"
        initialValues={{ isPublic: false }}
      >
        <Form.Item
          name="name"
          label="Query Name"
          rules={[{ required: true, message: 'Please enter a query name' }]}
        >
          <Input placeholder="Enter a name for this query" autoFocus />
        </Form.Item>

        <Form.Item
          name="description"
          label="Description"
        >
          <TextArea
            placeholder="Optional description"
            rows={3}
          />
        </Form.Item>

        <Form.Item
          name="isPublic"
          label="Make Public"
          valuePropName="checked"
        >
          <Switch />
        </Form.Item>

        <div style={{ marginTop: 16, padding: 12, background: '#f5f5f5', borderRadius: 4 }}>
          <div style={{ fontSize: 12, color: '#666', marginBottom: 4 }}>SQL Preview:</div>
          <pre style={{ margin: 0, fontSize: 12, maxHeight: 100, overflow: 'auto' }}>
            {sql.length > 500 ? sql.substring(0, 500) + '...' : sql}
          </pre>
        </div>
      </Form>
    </Modal>
  );
}
