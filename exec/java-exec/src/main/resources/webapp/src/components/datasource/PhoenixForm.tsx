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
import { Form, Input, InputNumber, Button, Collapse, Tooltip, Typography } from 'antd';
import { QuestionCircleOutlined, PlusOutlined, DeleteOutlined } from '@ant-design/icons';

const { Text } = Typography;

const helpIcon = { color: '#999', cursor: 'help' as const };

function label(text: string, tip: string) {
  return (
    <span>
      {text}{' '}
      <Tooltip title={tip}>
        <QuestionCircleOutlined style={helpIcon} />
      </Tooltip>
    </span>
  );
}

interface PhoenixFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

export default function PhoenixForm({ config, onChange }: PhoenixFormProps) {
  const [zkQuorum, setZkQuorum] = useState<string>((config.zkQuorum as string) || '');
  const [port, setPort] = useState<number | undefined>((config.port as number) || undefined);
  const [zkPath, setZkPath] = useState<string>((config.zkPath as string) || '');
  const [jdbcURL, setJdbcURL] = useState<string>((config.jdbcURL as string) || '');
  const [userName, setUserName] = useState<string>((config.userName as string) || '');
  const [password, setPassword] = useState<string>((config.password as string) || '');
  const [extraProps, setExtraProps] = useState<{ key: string; value: string }[]>(() => {
    const props = (config.props as Record<string, unknown>) || {};
    return Object.entries(props).map(([key, value]) => ({ key, value: String(value) }));
  });

  useEffect(() => {
    setZkQuorum((config.zkQuorum as string) || '');
    setPort((config.port as number) || undefined);
    setZkPath((config.zkPath as string) || '');
    setJdbcURL((config.jdbcURL as string) || '');
    setUserName((config.userName as string) || '');
    setPassword((config.password as string) || '');
    const props = (config.props as Record<string, unknown>) || {};
    setExtraProps(Object.entries(props).map(([key, value]) => ({ key, value: String(value) })));
  }, [config]);

  const emitChange = useCallback(
    (updates: Partial<Record<string, unknown>>) => {
      onChange({ ...config, ...updates });
    },
    [config, onChange]
  );

  const buildProps = useCallback(
    (extras: { key: string; value: string }[]): Record<string, string> => {
      const m: Record<string, string> = {};
      for (const { key, value } of extras) {
        if (key) {
          m[key] = value;
        }
      }
      return m;
    },
    []
  );

  return (
    <Form layout="vertical">
      <Text strong style={{ display: 'block', marginBottom: 12 }}>
        Connection
      </Text>

      <Form.Item label={label('JDBC URL', 'Optional full Phoenix JDBC connection URL (e.g. "jdbc:phoenix:zk-host:2181:/hbase"). If provided, zkQuorum, port, and zkPath are ignored.')}>
        <Input
          value={jdbcURL}
          onChange={(e) => {
            setJdbcURL(e.target.value);
            emitChange({ jdbcURL: e.target.value || undefined });
          }}
          placeholder="jdbc:phoenix:localhost:2181:/hbase (optional)"
        />
      </Form.Item>

      <Text type="secondary" style={{ display: 'block', marginBottom: 12 }}>
        Or specify the connection components individually:
      </Text>

      <Form.Item label={label('ZooKeeper Quorum', 'Comma-separated list of ZooKeeper hosts used by Phoenix / HBase.')}>
        <Input
          value={zkQuorum}
          onChange={(e) => {
            setZkQuorum(e.target.value);
            emitChange({ zkQuorum: e.target.value || undefined });
          }}
          placeholder="localhost"
        />
      </Form.Item>

      <Form.Item label={label('Port', 'ZooKeeper client port. Default is typically 2181.')}>
        <InputNumber
          value={port}
          onChange={(val) => {
            setPort(val || undefined);
            emitChange({ port: val || undefined });
          }}
          min={1}
          max={65535}
          placeholder="2181"
          style={{ width: 200 }}
        />
      </Form.Item>

      <Form.Item label={label('ZooKeeper Path', 'The ZooKeeper znode path where HBase is registered (e.g. "/hbase").')}>
        <Input
          value={zkPath}
          onChange={(e) => {
            setZkPath(e.target.value);
            emitChange({ zkPath: e.target.value || undefined });
          }}
          placeholder="/hbase"
        />
      </Form.Item>

      <Text strong style={{ display: 'block', marginTop: 16, marginBottom: 12 }}>
        Authentication
      </Text>

      <Form.Item label={label('Username', 'Phoenix / HBase username for authentication. Leave empty if authentication is not required.')}>
        <Input
          value={userName}
          onChange={(e) => {
            setUserName(e.target.value);
            emitChange({ userName: e.target.value || undefined });
          }}
          placeholder="Username (optional)"
        />
      </Form.Item>

      <Form.Item label={label('Password', 'Phoenix / HBase password for authentication.')}>
        <Input.Password
          value={password}
          onChange={(e) => {
            setPassword(e.target.value);
            emitChange({ password: e.target.value || undefined });
          }}
        />
      </Form.Item>

      <Collapse
        ghost
        style={{ marginTop: 16 }}
        items={[
          {
            key: 'advanced',
            label: <Text strong>Advanced Options</Text>,
            children: (
              <>
                <Text strong style={{ display: 'block', marginBottom: 12 }}>
                  Additional Phoenix Properties
                </Text>
                <Text type="secondary" style={{ display: 'block', marginBottom: 12 }}>
                  Phoenix tuning and configuration properties (e.g. phoenix.query.timeoutMs).
                </Text>

                {extraProps.map((prop, index) => (
                  <div key={index} style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
                    <Input
                      value={prop.key}
                      onChange={(e) => {
                        const updated = [...extraProps];
                        updated[index] = { ...updated[index], key: e.target.value };
                        setExtraProps(updated);
                        emitChange({ props: buildProps(updated) });
                      }}
                      placeholder="Property name"
                      style={{ flex: 1 }}
                    />
                    <Input
                      value={prop.value}
                      onChange={(e) => {
                        const updated = [...extraProps];
                        updated[index] = { ...updated[index], value: e.target.value };
                        setExtraProps(updated);
                        emitChange({ props: buildProps(updated) });
                      }}
                      placeholder="Value"
                      style={{ flex: 1 }}
                    />
                    <Button
                      icon={<DeleteOutlined />}
                      onClick={() => {
                        const updated = extraProps.filter((_, i) => i !== index);
                        setExtraProps(updated);
                        emitChange({ props: buildProps(updated) });
                      }}
                      danger
                    />
                  </div>
                ))}
                <Button
                  type="dashed"
                  onClick={() => {
                    setExtraProps([...extraProps, { key: '', value: '' }]);
                  }}
                  icon={<PlusOutlined />}
                  style={{ width: '100%' }}
                >
                  Add Property
                </Button>
              </>
            ),
          },
        ]}
      />
    </Form>
  );
}
