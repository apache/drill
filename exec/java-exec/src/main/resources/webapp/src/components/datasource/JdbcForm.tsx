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
  Alert,
} from 'antd';
import { PlusOutlined, DeleteOutlined, LinkOutlined } from '@ant-design/icons';
import { testConnection } from '../../api/storage';

interface JdbcFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

interface ParamRow {
  key: string;
  paramKey: string;
  paramValue: string;
}

/**
 * Where a JDBC plugin's credentials live. Drill writes them at the top level of the config
 * when they were given as `username`/`password`, and inside a `PlainCredentialsProvider`
 * otherwise; both shapes come back from the server, so the form reads and writes whichever
 * one this config uses.
 */
function credentialsOf(config: Record<string, unknown>): { username: string; password: string } {
  const provider = config.credentialsProvider as { credentials?: Record<string, string> } | undefined;
  const creds = provider?.credentials ?? {};
  return {
    username: (config.username as string) || creds.username || '',
    password: (config.password as string) || creds.password || '',
  };
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
  const [username, setUsername] = useState<string>(() => credentialsOf(config).username);
  const [password, setPassword] = useState<string>(() => credentialsOf(config).password);
  const [writable, setWritable] = useState<boolean>((config.writable as boolean) || false);
  const [caseInsensitive, setCaseInsensitive] = useState<boolean>(
    (config.caseInsensitiveTableNames as boolean) || false
  );
  const [batchSize, setBatchSize] = useState<number>((config.writerBatchSize as number) || 10000);
  const [params, setParams] = useState<ParamRow[]>([]);
  const [testLoading, setTestLoading] = useState<boolean>(false);
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null);

  useEffect(() => {
    setDriver((config.driver as string) || '');
    setUrl((config.url as string) || '');
    setUsername(credentialsOf(config).username);
    setPassword(credentialsOf(config).password);
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

  // A credential goes back where it came from: changing one inside a credentials provider
  // must not leave a second, stale copy at the top level for the server to choose between.
  const emitCredential = useCallback(
    (field: 'username' | 'password', value: string) => {
      const provider = config.credentialsProvider as Record<string, unknown> | undefined;
      if (provider) {
        onChange({
          ...config,
          credentialsProvider: {
            ...provider,
            credentials: { ...(provider.credentials as Record<string, string> | undefined), [field]: value },
          },
        });
      } else {
        onChange({ ...config, [field]: value });
      }
    },
    [config, onChange],
  );

  const handleTestConnection = useCallback(async () => {
    if (!driver || !url) {
      setTestResult({ success: false, message: 'Please enter a Driver Class and JDBC URL' });
      return;
    }

    setTestLoading(true);
    setTestResult(null);

    try {
      // Start from the stored config, then overlay what the form models. Rebuilding the
      // config from this form's own fields tested a different data source than the one
      // being saved: everything the form does not model -- sourceParameters, authMode,
      // a credentials provider -- was dropped, so a connection could pass or fail here
      // for reasons that had nothing to do with the plugin as configured.
      const provider = config.credentialsProvider as Record<string, unknown> | undefined;
      const testConfig: Record<string, unknown> = {
        ...config,
        type: 'jdbc',
        driver,
        url,
        writable,
        caseInsensitiveTableNames: caseInsensitive,
        writerBatchSize: batchSize,
      };
      // A credential goes where it came from, as elsewhere in this form: a provider's
      // credentials plus a stale top-level copy would leave the server choosing between
      // two. An empty value is not a credential -- the server would try to connect as a
      // user named "" rather than without credentials.
      delete testConfig.username;
      delete testConfig.password;
      if (provider) {
        testConfig.credentialsProvider = {
          ...provider,
          credentials: {
            ...(provider.credentials as Record<string, string> | undefined),
            ...(username ? { username } : {}),
            ...(password ? { password } : {}),
          },
        };
      } else {
        if (username) {
          testConfig.username = username;
        }
        if (password) {
          testConfig.password = password;
        }
      }

      const result = await testConnection('test-connection', testConfig);
      setTestResult({
        success: result.success || false,
        message: result.message || 'Unknown error',
      });
    } catch (error) {
      setTestResult({
        success: false,
        message: `Error testing connection: ${error instanceof Error ? error.message : 'Unknown error'}`,
      });
    } finally {
      setTestLoading(false);
    }
  }, [config, driver, url, username, password, writable, caseInsensitive, batchSize]);

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
            emitCredential('username', e.target.value);
          }}
          placeholder="username"
        />
      </Form.Item>

      <Form.Item label="Password">
        <Input.Password
          value={password}
          onChange={(e) => {
            setPassword(e.target.value);
            emitCredential('password', e.target.value);
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

      <Form.Item>
        <Space>
          <Button
            type="primary"
            icon={<LinkOutlined />}
            loading={testLoading}
            onClick={handleTestConnection}
            disabled={!driver || !url}
          >
            Test Connection
          </Button>
        </Space>
      </Form.Item>

      {testResult && (
        <Form.Item>
          <Alert
            message={testResult.success ? 'Connection Successful' : 'Connection Failed'}
            description={testResult.message}
            type={testResult.success ? 'success' : 'error'}
            showIcon
            closable
            onClose={() => setTestResult(null)}
          />
        </Form.Item>
      )}
    </Form>
  );
}
