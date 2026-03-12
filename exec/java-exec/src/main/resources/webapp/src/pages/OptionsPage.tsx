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
import { useState, useMemo, useCallback } from 'react';
import {
  Table,
  Input,
  Select,
  Switch,
  Button,
  Tag,
  Typography,
  Space,
  Spin,
  message,
  Tooltip,
  InputNumber,
} from 'antd';
import {
  SearchOutlined,
  UndoOutlined,
  CheckOutlined,
} from '@ant-design/icons';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import type { ColumnsType } from 'antd/es/table';
import {
  getOptions,
  getInternalOptions,
  getOptionDescriptions,
  updateOption,
} from '../api/options';
import type { DrillOption } from '../api/options';

const { Text } = Typography;

interface OptionRow extends DrillOption {
  description: string;
  isModified: boolean;
}

function OptionValueEditor({
  option,
  onSave,
}: {
  option: OptionRow;
  onSave: (name: string, value: string, kind: string) => Promise<void>;
}) {
  const [value, setValue] = useState<string>(String(option.value));
  const [saving, setSaving] = useState(false);
  const isChanged = value !== String(option.value);

  const handleSave = async (newValue: string) => {
    setSaving(true);
    try {
      await onSave(option.name, newValue, option.kind);
      setValue(newValue);
    } finally {
      setSaving(false);
    }
  };

  const handleReset = async () => {
    setSaving(true);
    try {
      await onSave(option.name, option.defaultValue, option.kind);
      setValue(option.defaultValue);
    } finally {
      setSaving(false);
    }
  };

  if (option.kind === 'BOOLEAN') {
    return (
      <Space>
        <Switch
          checked={value === 'true'}
          loading={saving}
          onChange={(checked) => handleSave(String(checked))}
          size="small"
        />
        {option.isModified && (
          <Tooltip title="Reset to default">
            <Button
              type="text"
              size="small"
              icon={<UndoOutlined />}
              onClick={handleReset}
              loading={saving}
            />
          </Tooltip>
        )}
      </Space>
    );
  }

  if (option.kind === 'LONG' || option.kind === 'DOUBLE') {
    return (
      <Space.Compact size="small">
        <InputNumber
          value={Number(value)}
          onChange={(v) => setValue(String(v ?? option.defaultValue))}
          style={{ width: 140 }}
          size="small"
          step={option.kind === 'DOUBLE' ? 0.1 : 1}
        />
        {isChanged && (
          <Button
            type="primary"
            icon={<CheckOutlined />}
            onClick={() => handleSave(value)}
            loading={saving}
            size="small"
          />
        )}
        {option.isModified && (
          <Tooltip title="Reset to default">
            <Button
              icon={<UndoOutlined />}
              onClick={handleReset}
              loading={saving}
              size="small"
            />
          </Tooltip>
        )}
      </Space.Compact>
    );
  }

  // STRING
  return (
    <Space.Compact size="small">
      <Input
        value={value}
        onChange={(e) => setValue(e.target.value)}
        style={{ width: 200 }}
        size="small"
      />
      {isChanged && (
        <Button
          type="primary"
          icon={<CheckOutlined />}
          onClick={() => handleSave(value)}
          loading={saving}
          size="small"
        />
      )}
      {option.isModified && (
        <Tooltip title="Reset to default">
          <Button
            icon={<UndoOutlined />}
            onClick={handleReset}
            loading={saving}
            size="small"
          />
        </Tooltip>
      )}
    </Space.Compact>
  );
}

export default function OptionsPage() {
  const [search, setSearch] = useState('');
  const [scopeFilter, setScopeFilter] = useState<string>('all');
  const [showInternal, setShowInternal] = useState(false);
  const [modifiedFilter, setModifiedFilter] = useState<string>('all');
  const queryClient = useQueryClient();

  const { data: publicOptions, isLoading: loadingPublic } = useQuery({
    queryKey: ['options', 'public'],
    queryFn: getOptions,
  });

  const { data: internalOptions, isLoading: loadingInternal } = useQuery({
    queryKey: ['options', 'internal'],
    queryFn: getInternalOptions,
    enabled: showInternal,
  });

  const { data: descriptions } = useQuery({
    queryKey: ['option-descriptions'],
    queryFn: getOptionDescriptions,
    staleTime: Infinity,
  });

  const handleSave = useCallback(async (name: string, value: string, kind: string) => {
    try {
      await updateOption(name, value, kind);
      message.success(`Updated ${name}`);
      queryClient.invalidateQueries({ queryKey: ['options'] });
    } catch (err) {
      message.error(`Failed to update ${name}: ${err instanceof Error ? err.message : 'Unknown error'}`);
      throw err;
    }
  }, [queryClient]);

  const options = useMemo((): OptionRow[] => {
    const raw = showInternal
      ? [...(publicOptions || []), ...(internalOptions || [])]
      : (publicOptions || []);

    return raw.map((opt) => ({
      ...opt,
      description: descriptions?.[opt.name] || '',
      isModified: String(opt.value) !== opt.defaultValue,
    }));
  }, [publicOptions, internalOptions, descriptions, showInternal]);

  const filtered = useMemo(() => {
    let result = options;

    if (search) {
      const q = search.toLowerCase();
      result = result.filter(
        (o) =>
          o.name.toLowerCase().includes(q) ||
          o.description.toLowerCase().includes(q),
      );
    }

    if (scopeFilter !== 'all') {
      result = result.filter((o) => o.optionScope === scopeFilter);
    }

    if (modifiedFilter === 'modified') {
      result = result.filter((o) => o.isModified);
    } else if (modifiedFilter === 'default') {
      result = result.filter((o) => !o.isModified);
    }

    return result;
  }, [options, search, scopeFilter, modifiedFilter]);

  const scopeCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const o of options) {
      counts[o.optionScope] = (counts[o.optionScope] || 0) + 1;
    }
    return counts;
  }, [options]);

  const modifiedCount = useMemo(
    () => options.filter((o) => o.isModified).length,
    [options],
  );

  const columns: ColumnsType<OptionRow> = [
    {
      title: 'Option',
      dataIndex: 'name',
      key: 'name',
      sorter: (a, b) => a.name.localeCompare(b.name),
      defaultSortOrder: 'ascend',
      render: (name: string, record) => (
        <div>
          <Text strong copyable={{ text: name }} style={{ fontSize: 13 }}>
            {name}
          </Text>
          {record.isModified && (
            <Tag color="blue" style={{ marginLeft: 8, fontSize: 10 }}>
              MODIFIED
            </Tag>
          )}
          {record.description && (
            <div>
              <Text type="secondary" style={{ fontSize: 12 }}>
                {record.description}
              </Text>
            </div>
          )}
        </div>
      ),
    },
    {
      title: 'Value',
      key: 'value',
      width: 280,
      render: (_, record) => (
        <OptionValueEditor option={record} onSave={handleSave} />
      ),
    },
    {
      title: 'Default',
      dataIndex: 'defaultValue',
      key: 'default',
      width: 150,
      render: (val: string) => (
        <Text type="secondary" style={{ fontSize: 12 }}>
          {val}
        </Text>
      ),
    },
    {
      title: 'Type',
      dataIndex: 'kind',
      key: 'kind',
      width: 90,
      render: (kind: string) => {
        const colors: Record<string, string> = {
          BOOLEAN: 'green',
          LONG: 'blue',
          DOUBLE: 'purple',
          STRING: 'orange',
        };
        return <Tag color={colors[kind] || 'default'}>{kind}</Tag>;
      },
    },
    {
      title: 'Scope',
      dataIndex: 'optionScope',
      key: 'scope',
      width: 90,
      render: (scope: string) => (
        <Text type="secondary" style={{ fontSize: 12 }}>{scope}</Text>
      ),
    },
  ];

  const isLoading = loadingPublic || (showInternal && loadingInternal);

  return (
    <div style={{ padding: 24, height: '100%', overflow: 'auto' }}>
      <div style={{ marginBottom: 16 }}>
        <Space wrap>
          <Input
            placeholder="Search options..."
            prefix={<SearchOutlined />}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            allowClear
            style={{ width: 300 }}
          />
          <Select
            value={scopeFilter}
            onChange={setScopeFilter}
            style={{ width: 160 }}
            options={[
              { value: 'all', label: `All Scopes (${options.length})` },
              ...Object.entries(scopeCounts).sort().map(([scope, count]) => ({
                value: scope,
                label: `${scope} (${count})`,
              })),
            ]}
          />
          <Select
            value={modifiedFilter}
            onChange={setModifiedFilter}
            style={{ width: 160 }}
            options={[
              { value: 'all', label: 'All Values' },
              { value: 'modified', label: `Modified (${modifiedCount})` },
              { value: 'default', label: 'Default Only' },
            ]}
          />
          <Switch
            checked={showInternal}
            onChange={setShowInternal}
            checkedChildren="Internal"
            unCheckedChildren="Public"
          />
        </Space>
      </div>

      {isLoading ? (
        <div style={{ textAlign: 'center', padding: 60 }}>
          <Spin tip="Loading options..." />
        </div>
      ) : (
        <Table
          dataSource={filtered}
          columns={columns}
          rowKey="name"
          size="small"
          pagination={{ pageSize: 50, showSizeChanger: true, pageSizeOptions: ['25', '50', '100', '200'] }}
          scroll={{ x: 800 }}
          showSorterTooltip={false}
        />
      )}
    </div>
  );
}
