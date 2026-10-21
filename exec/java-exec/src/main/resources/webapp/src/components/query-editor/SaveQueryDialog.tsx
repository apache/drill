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
import { Modal, Form, Input, Switch, message, Radio, Select, Checkbox, Tooltip } from 'antd';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { createSavedQuery } from '../../api/savedQueries';
import { addSavedQuery } from '../../api/projects';
import { getViewTargets } from '../../api/metadata';
import { isCreatableAsView, isValidViewName } from '../../utils/sql';
import type { ViewMode } from '../../utils/sql';
import { createViewFromQuery } from '../../utils/createView';
import type { SavedQueryCreate } from '../../types';

const { TextArea } = Input;

interface SaveQueryDialogProps {
  open: boolean;
  onClose: () => void;
  sql: string;
  defaultSchema?: string;
  projectId?: string;
  onSaved?: (name: string) => void;
  /** Called after a view or materialized view is created, with its schema. */
  onViewCreated?: (schema: string) => void;
}

export default function SaveQueryDialog({
  open,
  onClose,
  sql,
  defaultSchema,
  projectId,
  onSaved,
  onViewCreated,
}: SaveQueryDialogProps) {
  const [form] = Form.useForm();
  const queryClient = useQueryClient();
  const [saving, setSaving] = useState(false);
  const [mode, setMode] = useState<'query' | 'view' | 'materialized_view'>('query');
  const isViewMode = mode !== 'query';

  // A view can only wrap a SELECT. Disabling the radios is friendlier than letting
  // Drill reject the DDL, and saving a non-SELECT as a plain query stays valid.
  const canCreateView = isCreatableAsView(sql);

  const { data: viewTargets = [], isLoading: loadingTargets } = useQuery({
    queryKey: ['viewTargets'],
    queryFn: getViewTargets,
    enabled: open && isViewMode,
  });

  const mutation = useMutation({
    mutationFn: createSavedQuery,
    onSuccess: async (savedQuery) => {
      // Auto-add to project if in project context
      if (projectId && savedQuery?.id) {
        try {
          await addSavedQuery(projectId, savedQuery.id);
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
        } catch {
          // Non-fatal — query was saved, just not linked
        }
      }
      message.success('Query saved successfully');
      queryClient.invalidateQueries({ queryKey: ['savedQueries'] });
      onSaved?.(savedQuery.name);
      form.resetFields();
      setMode('query');
      onClose();
    },
    onError: (error: Error) => {
      message.error(`Failed to save query: ${error.message}`);
    },
    onSettled: () => {
      setSaving(false);
    },
  });

  const createView = async (values: { schema: string; name: string; replace?: boolean }) => {
    const { projectError } = await createViewFromQuery({
      mode: mode as ViewMode,
      schema: values.schema,
      name: values.name,
      sql,
      replace: values.replace ?? false,
      projectId,
    });

    const label = mode === 'materialized_view' ? 'Materialized view' : 'View';
    if (projectError) {
      message.warning(`${label} created, but not added to the project: ${projectError}`);
    } else {
      message.success(`${label} ${values.schema}.${values.name} created`);
      if (projectId) {
        queryClient.invalidateQueries({ queryKey: ['project', projectId] });
      }
    }

    onViewCreated?.(values.schema);
    form.resetFields();
    setMode('query');
    onClose();
  };

  const handleSave = async () => {
    let values;
    try {
      values = await form.validateFields();
    } catch {
      return; // Form validation failed; antd has already shown the messages.
    }

    setSaving(true);

    if (isViewMode) {
      try {
        await createView(values);
      } catch (err) {
        const detail = err instanceof Error ? err.message : String(err);
        message.error(`Failed to create ${mode === 'view' ? 'view' : 'materialized view'}: ${detail}`);
      } finally {
        setSaving(false);
      }
      return;
    }

    const query: SavedQueryCreate = {
      name: values.name,
      description: values.description,
      sql: sql,
      defaultSchema: defaultSchema,
      isPublic: values.isPublic || false,
    };

    mutation.mutate(query);
  };

  const handleCancel = () => {
    form.resetFields();
    setMode('query');
    onClose();
  };

  return (
    <Modal
      title={mode === 'query' ? 'Save Query'
        : mode === 'view' ? 'Save as View' : 'Save as Materialized View'}
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
        <Form.Item label="Save as">
          <Radio.Group value={mode} onChange={(e) => setMode(e.target.value)}>
            <Radio value="query">Query</Radio>
            <Tooltip
              title={canCreateView ? undefined
                : 'Only SELECT queries (including CTEs) can be saved as a view'}
            >
              <Radio value="view" disabled={!canCreateView}>View</Radio>
            </Tooltip>
            <Tooltip
              title={canCreateView ? undefined
                : 'Only SELECT queries (including CTEs) can be saved as a materialized view'}
            >
              <Radio value="materialized_view" disabled={!canCreateView}>
                Materialized View
              </Radio>
            </Tooltip>
          </Radio.Group>
        </Form.Item>

        {isViewMode && (
          <>
            <Form.Item
              name="schema"
              label="Schema"
              rules={[{ required: true, message: 'Please choose a schema' }]}
              extra="Only writable file-based schemas can hold a view."
            >
              <Select
                loading={loadingTargets}
                placeholder="Choose where to save the view"
                options={viewTargets.map((s) => ({ label: s.name, value: s.name }))}
                showSearch
              />
            </Form.Item>

            <Form.Item
              name="replace"
              valuePropName="checked"
            >
              <Checkbox>Replace if it already exists</Checkbox>
            </Form.Item>
          </>
        )}

        <Form.Item
          name="name"
          label={isViewMode ? 'View Name' : 'Query Name'}
          extra={isViewMode
            ? 'Use a slash to place the view in a subfolder, e.g. reports/daily_sales'
            : undefined}
          rules={[
            { required: true, message: `Please enter a ${isViewMode ? 'view' : 'query'} name` },
            {
              validator: (_, value) =>
                !isViewMode || !value || isValidViewName(value)
                  ? Promise.resolve()
                  : Promise.reject(new Error(
                      'Use letters, numbers, _ - . and / only')),
            },
          ]}
        >
          <Input placeholder={isViewMode ? 'e.g. sales_summary' : 'Enter a name for this query'} autoFocus />
        </Form.Item>

        {!isViewMode && (
          <>
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
          </>
        )}

        <div style={{ marginTop: 16, padding: 12, background: 'var(--color-bg-elevated)', borderRadius: 4 }}>
          <div style={{ fontSize: 12, color: 'var(--color-text-secondary)', marginBottom: 4 }}>SQL Preview:</div>
          <pre style={{ margin: 0, fontSize: 12, maxHeight: 100, overflow: 'auto' }}>
            {sql.length > 500 ? sql.substring(0, 500) + '...' : sql}
          </pre>
        </div>
      </Form>
    </Modal>
  );
}
