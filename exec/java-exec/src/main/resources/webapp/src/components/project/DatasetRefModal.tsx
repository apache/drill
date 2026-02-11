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
import { Modal, Form, Input, Radio, Select, message } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { addDataset } from '../../api/projects';
import { getSavedQueries } from '../../api/savedQueries';
import type { DatasetRef } from '../../types';

interface DatasetRefModalProps {
  open: boolean;
  projectId: string;
  onClose: () => void;
  onSuccess: () => void;
}

export default function DatasetRefModal({ open, projectId, onClose, onSuccess }: DatasetRefModalProps) {
  const [form] = Form.useForm();
  const [saving, setSaving] = useState(false);
  const [datasetType, setDatasetType] = useState<'table' | 'saved_query'>('table');

  const { data: savedQueries } = useQuery({
    queryKey: ['saved-queries'],
    queryFn: getSavedQueries,
    enabled: open,
  });

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      setSaving(true);

      const dataset: DatasetRef = {
        id: '',
        type: datasetType,
        schema: datasetType === 'table' ? values.schema : undefined,
        table: datasetType === 'table' ? values.table : undefined,
        savedQueryId: datasetType === 'saved_query' ? values.savedQueryId : undefined,
        label: values.label,
      };

      await addDataset(projectId, dataset);
      message.success('Dataset added');
      form.resetFields();
      setDatasetType('table');
      onSuccess();
    } catch (err) {
      if ((err as Error).message) {
        message.error(`Failed to add dataset: ${(err as Error).message}`);
      }
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal
      title="Add Dataset Reference"
      open={open}
      onOk={handleSave}
      onCancel={() => {
        form.resetFields();
        setDatasetType('table');
        onClose();
      }}
      confirmLoading={saving}
      okText="Add"
    >
      <Form form={form} layout="vertical">
        <Form.Item label="Type">
          <Radio.Group
            value={datasetType}
            onChange={(e) => setDatasetType(e.target.value)}
          >
            <Radio.Button value="table">Table</Radio.Button>
            <Radio.Button value="saved_query">Saved Query</Radio.Button>
          </Radio.Group>
        </Form.Item>

        <Form.Item
          name="label"
          label="Label"
          rules={[{ required: true, message: 'Please enter a label' }]}
        >
          <Input placeholder="Descriptive label for this dataset" />
        </Form.Item>

        {datasetType === 'table' ? (
          <>
            <Form.Item
              name="schema"
              label="Schema"
              rules={[{ required: true, message: 'Please enter a schema' }]}
            >
              <Input placeholder="e.g. dfs.data" />
            </Form.Item>
            <Form.Item
              name="table"
              label="Table"
              rules={[{ required: true, message: 'Please enter a table name' }]}
            >
              <Input placeholder="e.g. my_table.csv" />
            </Form.Item>
          </>
        ) : (
          <Form.Item
            name="savedQueryId"
            label="Saved Query"
            rules={[{ required: true, message: 'Please select a saved query' }]}
          >
            <Select
              placeholder="Select a saved query..."
              showSearch
              optionFilterProp="label"
              options={(savedQueries || []).map((q) => ({
                value: q.id,
                label: q.name,
              }))}
            />
          </Form.Item>
        )}
      </Form>
    </Modal>
  );
}
