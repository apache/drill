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
  Form,
  Input,
  Switch,
  InputNumber,
  AutoComplete,
  Button,
  Space,
  Table,
} from 'antd';
import { PlusOutlined, DeleteOutlined } from '@ant-design/icons';

interface JdbcFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

interface ParamRow {
  key: string;
  paramKey: string;
  paramValue: string;
}

const commonDrivers = [
  { value: 'org.postgresql.Driver' },
  { value: 'com.mysql.cj.jdbc.Driver' },
  { value: 'org.mariadb.jdbc.Driver' },
  { value: 'oracle.jdbc.OracleDriver' },
  { value: 'com.microsoft.sqlserver.jdbc.SQLServerDriver' },
  { value: 'org.h2.Driver' },
  { value: 'org.sqlite.JDBC' },
  { value: 'org.apache.derby.jdbc.ClientDriver' },
];

export default function JdbcForm({ config, onChange }: JdbcFormProps) {
  const [driver, setDriver] = useState<string>((config.driver as string) || '');
  const [url, setUrl] = useState<string>((config.url as string) || '');
  const [username, setUsername] = useState<string>((config.username as string) || '');
  const [password, setPassword] = useState<string>((config.password as string) || '');
  const [writable, setWritable] = useState<boolean>((config.writable as boolean) || false);
  const [caseInsensitive, setCaseInsensitive] = useState<boolean>(
    (config.caseInsensitiveTableNames as boolean) || false
  );
  const [batchSize, setBatchSize] = useState<number>((config.writerBatchSize as number) || 10000);
  const [params, setParams] = useState<ParamRow[]>([]);

  useEffect(() => {
    setDriver((config.driver as string) || '');
    setUrl((config.url as string) || '');
    setUsername((config.username as string) || '');
    setPassword((config.password as string) || '');
    setWritable((config.writable as boolean) || false);
    setCaseInsensitive((config.caseInsensitiveTableNames as boolean) || false);
    setBatchSize((config.writerBatchSize as number) || 10000);

    const sp = (config.sourceParameters as Record<string, string>) || {};
    setParams(
      Object.entries(sp).map(([k, v], i) => ({
        key: `p_${i}`,
        paramKey: k,
        paramValue: v,
      }))
    );
  }, [config]);

  const emitChange = useCallback(
    (updates: Partial<Record<string, unknown>>) => {
      onChange({ ...config, ...updates });
    },
    [config, onChange]
  );

  const handleParamsChange = (newParams: ParamRow[]) => {
    setParams(newParams);
    const sp: Record<string, string> = {};
    newParams.forEach((p) => {
      if (p.paramKey) {
        sp[p.paramKey] = p.paramValue;
      }
    });
    emitChange({ sourceParameters: Object.keys(sp).length > 0 ? sp : undefined });
  };

  const paramColumns = [
    {
      title: 'Key',
      dataIndex: 'paramKey',
      key: 'paramKey',
      render: (_: string, record: ParamRow) => (
        <Input
          size="small"
          value={record.paramKey}
          onChange={(e) => {
            const updated = params.map((p) =>
              p.key === record.key ? { ...p, paramKey: e.target.value } : p
            );
            handleParamsChange(updated);
          }}
          placeholder="parameter"
        />
      ),
    },
    {
      title: 'Value',
      dataIndex: 'paramValue',
      key: 'paramValue',
      render: (_: string, record: ParamRow) => (
        <Input
          size="small"
          value={record.paramValue}
          onChange={(e) => {
            const updated = params.map((p) =>
              p.key === record.key ? { ...p, paramValue: e.target.value } : p
            );
            handleParamsChange(updated);
          }}
          placeholder="value"
        />
      ),
    },
    {
      title: '',
      key: 'actions',
      width: 40,
      render: (_: unknown, record: ParamRow) => (
        <Button
          type="text"
          size="small"
          danger
          icon={<DeleteOutlined />}
          onClick={() => handleParamsChange(params.filter((p) => p.key !== record.key))}
        />
      ),
    },
  ];

  return (
    <Form layout="vertical">
      <Form.Item label="Driver Class">
        <AutoComplete
          value={driver}
          onChange={(val) => {
            setDriver(val);
            emitChange({ driver: val });
          }}
          options={commonDrivers}
          placeholder="org.postgresql.Driver"
          filterOption={(input, option) =>
            (option?.value as string).toLowerCase().includes(input.toLowerCase())
          }
        />
      </Form.Item>

      <Form.Item label="JDBC URL">
        <Input
          value={url}
          onChange={(e) => {
            setUrl(e.target.value);
            emitChange({ url: e.target.value });
          }}
          placeholder="jdbc:postgresql://host:5432/database"
        />
      </Form.Item>

      <Form.Item label="Username">
        <Input
          value={username}
          onChange={(e) => {
            setUsername(e.target.value);
            emitChange({ username: e.target.value });
          }}
          placeholder="username"
        />
      </Form.Item>

      <Form.Item label="Password">
        <Input.Password
          value={password}
          onChange={(e) => {
            setPassword(e.target.value);
            emitChange({ password: e.target.value });
          }}
          placeholder="password"
        />
      </Form.Item>

      <Form.Item label="Writable">
        <Switch
          checked={writable}
          onChange={(checked) => {
            setWritable(checked);
            emitChange({ writable: checked });
          }}
        />
      </Form.Item>

      <Form.Item label="Case Insensitive Table Names">
        <Switch
          checked={caseInsensitive}
          onChange={(checked) => {
            setCaseInsensitive(checked);
            emitChange({ caseInsensitiveTableNames: checked });
          }}
        />
      </Form.Item>

      <Form.Item label="Writer Batch Size">
        <InputNumber
          value={batchSize}
          onChange={(val) => {
            const v = val || 10000;
            setBatchSize(v);
            emitChange({ writerBatchSize: v });
          }}
          min={1}
          style={{ width: 200 }}
        />
      </Form.Item>

      <Form.Item
        label={
          <Space>
            Source Parameters
            <Button
              type="link"
              size="small"
              icon={<PlusOutlined />}
              onClick={() =>
                handleParamsChange([
                  ...params,
                  { key: `p_${Date.now()}`, paramKey: '', paramValue: '' },
                ])
              }
            >
              Add
            </Button>
          </Space>
        }
      >
        {params.length > 0 && (
          <Table
            dataSource={params}
            columns={paramColumns}
            pagination={false}
            size="small"
            rowKey="key"
          />
        )}
      </Form.Item>
    </Form>
  );
}
