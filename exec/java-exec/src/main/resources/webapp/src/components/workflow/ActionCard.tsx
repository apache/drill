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
import { Button, Card, Input, Space, Tag, Tooltip, Typography } from 'antd';
import { ArrowDownOutlined, ArrowUpOutlined, DeleteOutlined } from '@ant-design/icons';
import type { ActionTypeInfo, ValidationError, WorkflowAction } from '../../types/workflow';
import ConditionEditor from './ConditionEditor';
import SchemaForm from './SchemaForm';

interface Props {
  action: WorkflowAction;
  index: number;
  count: number;
  type?: ActionTypeInfo;
  refs: string[];
  errors: ValidationError[];
  onChange: (a: WorkflowAction) => void;
  onMove: (delta: -1 | 1) => void;
  onDelete: () => void;
}

export default function ActionCard({ action, index, count, type, refs, errors, onChange, onMove, onDelete }: Props) {
  const prefix = `actions[${index}]`;
  const errorAt = (p: string) => errors.find((e) => e.path === `${prefix}${p}`)?.message;
  const configErrors: Record<string, string> = {};
  errors.forEach((e) => {
    const m = e.path.match(/^actions\[\d+\]\.config\.([^.[]+)/);
    if (m) {
      configErrors[m[1]] = e.message;
    }
  });
  const generalConfigError = errorAt('.config');

  return (
    <Card size="small"
      title={<Space>{type?.displayName ?? action.type}<Tag>{action.id}</Tag>
        {type?.hasSideEffects && <Tooltip title="Suppressed during dry runs"><Tag color="orange">side effects</Tag></Tooltip>}</Space>}
      extra={<Space>
        <Button size="small" icon={<ArrowUpOutlined />} aria-label="Move up" disabled={index === 0} onClick={() => onMove(-1)} />
        <Button size="small" icon={<ArrowDownOutlined />} aria-label="Move down" disabled={index === count - 1} onClick={() => onMove(1)} />
        <Button size="small" danger icon={<DeleteOutlined />} aria-label="Delete action" onClick={onDelete} />
      </Space>}>
      <Space direction="vertical" style={{ width: '100%' }}>
        <div>
          <Typography.Text strong>Action id</Typography.Text>
          <Input aria-label="Action id" value={action.id} status={errorAt('.id') ? 'error' : undefined}
            onChange={(e) => onChange({ ...action, id: e.target.value })} />
          {errorAt('.id') && <Typography.Text type="danger">{errorAt('.id')}</Typography.Text>}
          {errorAt('.type') && <Typography.Text type="danger">{errorAt('.type')}</Typography.Text>}
        </div>
        <div>
          <Typography.Text strong>Run when</Typography.Text>
          <ConditionEditor value={action.condition} refs={refs} error={errorAt('.condition')}
            onChange={(condition) => onChange({ ...action, condition })} />
        </div>
        {type && (
          <SchemaForm schema={type.configSchema} value={action.config} refs={refs} errors={configErrors}
            onChange={(config) => onChange({ ...action, config })} />
        )}
        {generalConfigError && <Typography.Text type="danger">{generalConfigError}</Typography.Text>}
      </Space>
    </Card>
  );
}
