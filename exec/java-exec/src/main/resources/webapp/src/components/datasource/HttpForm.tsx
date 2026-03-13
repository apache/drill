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
  Select,
  Button,
  Space,
  Card,
  Table,
  Collapse,
  Typography,
} from 'antd';
import { PlusOutlined, DeleteOutlined } from '@ant-design/icons';

const { Text } = Typography;

interface HttpFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

interface HttpConnection {
  url: string;
  method?: string;
  headers?: Record<string, string>;
  [key: string]: unknown;
}

interface HeaderRow {
  key: string;
  headerKey: string;
  headerValue: string;
}

function HeadersEditor({
  headers,
  onChange,
}: {
  headers: Record<string, string>;
  onChange: (h: Record<string, string>) => void;
}) {
  const rows: HeaderRow[] = Object.entries(headers).map(([k, v], i) => ({
    key: `h_${i}`,
    headerKey: k,
    headerValue: v,
  }));

  const handleChange = (newRows: HeaderRow[]) => {
    const result: Record<string, string> = {};
    newRows.forEach((r) => {
      if (r.headerKey) {
        result[r.headerKey] = r.headerValue;
      }
    });
    onChange(result);
  };

  return (
    <>
      <Table
        dataSource={rows}
        pagination={false}
        size="small"
        rowKey="key"
        columns={[
          {
            title: 'Header',
            dataIndex: 'headerKey',
            render: (_: string, record: HeaderRow) => (
              <Input
                size="small"
                value={record.headerKey}
                onChange={(e) =>
                  handleChange(
                    rows.map((r) =>
                      r.key === record.key ? { ...r, headerKey: e.target.value } : r
                    )
                  )
                }
                placeholder="Content-Type"
              />
            ),
          },
          {
            title: 'Value',
            dataIndex: 'headerValue',
            render: (_: string, record: HeaderRow) => (
              <Input
                size="small"
                value={record.headerValue}
                onChange={(e) =>
                  handleChange(
                    rows.map((r) =>
                      r.key === record.key ? { ...r, headerValue: e.target.value } : r
                    )
                  )
                }
                placeholder="application/json"
              />
            ),
          },
          {
            title: '',
            key: 'actions',
            width: 40,
            render: (_: unknown, record: HeaderRow) => (
              <Button
                type="text"
                size="small"
                danger
                icon={<DeleteOutlined />}
                onClick={() => handleChange(rows.filter((r) => r.key !== record.key))}
              />
            ),
          },
        ]}
      />
      <Button
        type="link"
        size="small"
        icon={<PlusOutlined />}
        onClick={() =>
          handleChange([...rows, { key: `h_${Date.now()}`, headerKey: '', headerValue: '' }])
        }
      >
        Add Header
      </Button>
    </>
  );
}

export default function HttpForm({ config, onChange }: HttpFormProps) {
  const [connections, setConnections] = useState<Record<string, HttpConnection>>({});
  const [cacheResults, setCacheResults] = useState<boolean>(false);
  const [timeout, setTimeout_] = useState<number>(0);
  const [proxyHost, setProxyHost] = useState<string>('');
  const [proxyPort, setProxyPort] = useState<number | undefined>(undefined);
  const [proxyType, setProxyType] = useState<string>('');
  const [proxyUsername, setProxyUsername] = useState<string>('');
  const [proxyPassword, setProxyPassword] = useState<string>('');

  useEffect(() => {
    setConnections((config.connections as Record<string, HttpConnection>) || {});
    setCacheResults((config.cacheResults as boolean) || false);
    setTimeout_((config.timeout as number) || 0);

    const proxy = config.proxyConfig as Record<string, unknown> | undefined;
    if (proxy) {
      setProxyHost((proxy.host as string) || '');
      setProxyPort(proxy.port as number | undefined);
      setProxyType((proxy.type as string) || '');
      setProxyUsername((proxy.username as string) || '');
      setProxyPassword((proxy.password as string) || '');
    }
  }, [config]);

  const emitChange = useCallback(
    (updates: Partial<Record<string, unknown>>) => {
      onChange({ ...config, ...updates });
    },
    [config, onChange]
  );

  const handleConnectionChange = (name: string, field: string, value: unknown) => {
    const updated = { ...connections };
    if (!updated[name]) {
      updated[name] = { url: '' };
    }
    (updated[name] as Record<string, unknown>)[field] = value;
    setConnections(updated);
    emitChange({ connections: updated });
  };

  const addConnection = () => {
    const name = `connection_${Object.keys(connections).length + 1}`;
    const updated = { ...connections, [name]: { url: '', method: 'GET' } };
    setConnections(updated);
    emitChange({ connections: updated });
  };

  const removeConnection = (name: string) => {
    const updated = { ...connections };
    delete updated[name];
    setConnections(updated);
    emitChange({ connections: updated });
  };

  const renameConnection = (oldName: string, newName: string) => {
    if (!newName || newName === oldName) {
      return;
    }
    const updated: Record<string, HttpConnection> = {};
    Object.entries(connections).forEach(([k, v]) => {
      updated[k === oldName ? newName : k] = v;
    });
    setConnections(updated);
    emitChange({ connections: updated });
  };

  const buildProxy = (
    host: string,
    port: number | undefined,
    type: string,
    uname: string,
    pwd: string
  ) => {
    if (!host) {
      return undefined;
    }
    const proxy: Record<string, unknown> = { host };
    if (port) {
      proxy.port = port;
    }
    if (type) {
      proxy.type = type;
    }
    if (uname) {
      proxy.username = uname;
    }
    if (pwd) {
      proxy.password = pwd;
    }
    return proxy;
  };

  return (
    <Form layout="vertical">
      <Form.Item
        label={
          <Space>
            <Text>Connections</Text>
            <Button type="link" size="small" icon={<PlusOutlined />} onClick={addConnection}>
              Add
            </Button>
          </Space>
        }
      >
        <Space direction="vertical" style={{ width: '100%' }}>
          {Object.entries(connections).map(([name, conn]) => (
            <Card
              key={name}
              size="small"
              title={
                <Input
                  size="small"
                  value={name}
                  onBlur={(e) => renameConnection(name, e.target.value)}
                  style={{ width: 200 }}
                />
              }
              extra={
                <Button
                  type="text"
                  size="small"
                  danger
                  icon={<DeleteOutlined />}
                  onClick={() => removeConnection(name)}
                />
              }
            >
              <Form layout="vertical">
                <Form.Item label="URL" style={{ marginBottom: 8 }}>
                  <Input
                    value={conn.url}
                    onChange={(e) => handleConnectionChange(name, 'url', e.target.value)}
                    placeholder="https://api.example.com/data"
                  />
                </Form.Item>
                <Form.Item label="Method" style={{ marginBottom: 8 }}>
                  <Select
                    value={conn.method || 'GET'}
                    onChange={(val) => handleConnectionChange(name, 'method', val)}
                    options={[
                      { value: 'GET', label: 'GET' },
                      { value: 'POST', label: 'POST' },
                    ]}
                    style={{ width: 120 }}
                  />
                </Form.Item>
                <Form.Item label="Headers" style={{ marginBottom: 0 }}>
                  <HeadersEditor
                    headers={conn.headers || {}}
                    onChange={(h) => handleConnectionChange(name, 'headers', h)}
                  />
                </Form.Item>
              </Form>
            </Card>
          ))}
        </Space>
      </Form.Item>

      <Form.Item label="Cache Results">
        <Switch
          checked={cacheResults}
          onChange={(checked) => {
            setCacheResults(checked);
            emitChange({ cacheResults: checked });
          }}
        />
      </Form.Item>

      <Form.Item label="Timeout (seconds)">
        <InputNumber
          value={timeout}
          onChange={(val) => {
            const v = val || 0;
            setTimeout_(v);
            emitChange({ timeout: v });
          }}
          min={0}
          style={{ width: 200 }}
        />
      </Form.Item>

      <Collapse
        ghost
        items={[
          {
            key: 'proxy',
            label: 'Proxy Configuration',
            children: (
              <Form layout="vertical">
                <Form.Item label="Host" style={{ marginBottom: 8 }}>
                  <Input
                    value={proxyHost}
                    onChange={(e) => {
                      setProxyHost(e.target.value);
                      emitChange({
                        proxyConfig: buildProxy(
                          e.target.value,
                          proxyPort,
                          proxyType,
                          proxyUsername,
                          proxyPassword
                        ),
                      });
                    }}
                    placeholder="proxy.example.com"
                  />
                </Form.Item>
                <Form.Item label="Port" style={{ marginBottom: 8 }}>
                  <InputNumber
                    value={proxyPort}
                    onChange={(val) => {
                      setProxyPort(val || undefined);
                      emitChange({
                        proxyConfig: buildProxy(
                          proxyHost,
                          val || undefined,
                          proxyType,
                          proxyUsername,
                          proxyPassword
                        ),
                      });
                    }}
                    min={1}
                    max={65535}
                    style={{ width: 120 }}
                  />
                </Form.Item>
                <Form.Item label="Type" style={{ marginBottom: 8 }}>
                  <Select
                    value={proxyType || undefined}
                    onChange={(val) => {
                      setProxyType(val || '');
                      emitChange({
                        proxyConfig: buildProxy(
                          proxyHost,
                          proxyPort,
                          val || '',
                          proxyUsername,
                          proxyPassword
                        ),
                      });
                    }}
                    allowClear
                    placeholder="Select type"
                    options={[
                      { value: 'HTTP', label: 'HTTP' },
                      { value: 'SOCKS', label: 'SOCKS' },
                    ]}
                    style={{ width: 120 }}
                  />
                </Form.Item>
                <Form.Item label="Username" style={{ marginBottom: 8 }}>
                  <Input
                    value={proxyUsername}
                    onChange={(e) => {
                      setProxyUsername(e.target.value);
                      emitChange({
                        proxyConfig: buildProxy(
                          proxyHost,
                          proxyPort,
                          proxyType,
                          e.target.value,
                          proxyPassword
                        ),
                      });
                    }}
                  />
                </Form.Item>
                <Form.Item label="Password" style={{ marginBottom: 0 }}>
                  <Input.Password
                    value={proxyPassword}
                    onChange={(e) => {
                      setProxyPassword(e.target.value);
                      emitChange({
                        proxyConfig: buildProxy(
                          proxyHost,
                          proxyPort,
                          proxyType,
                          proxyUsername,
                          e.target.value
                        ),
                      });
                    }}
                  />
                </Form.Item>
              </Form>
            ),
          },
        ]}
      />
    </Form>
  );
}
