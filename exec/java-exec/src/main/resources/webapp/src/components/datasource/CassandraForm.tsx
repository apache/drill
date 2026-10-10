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
import { Form, Input, InputNumber, Select, Tooltip, Typography } from 'antd';
import { QuestionCircleOutlined } from '@ant-design/icons';

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

interface CassandraFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

export default function CassandraForm({ config, onChange }: CassandraFormProps) {
  const [host, setHost] = useState<string>((config.host as string) || 'localhost');
  const [port, setPort] = useState<number>((config.port as number) || 9042);
  const [username, setUsername] = useState<string>((config.username as string) || '');
  const [password, setPassword] = useState<string>((config.password as string) || '');
  const [authMode, setAuthMode] = useState<string>((config.authMode as string) || 'SHARED_USER');

  useEffect(() => {
    setHost((config.host as string) || 'localhost');
    setPort((config.port as number) || 9042);
    setUsername((config.username as string) || '');
    setPassword((config.password as string) || '');
    setAuthMode((config.authMode as string) || 'SHARED_USER');
  }, [config]);

  const emitChange = useCallback(
    (updates: Partial<Record<string, unknown>>) => {
      onChange({ ...config, ...updates });
    },
    [config, onChange]
  );

  return (
    <Form layout="vertical">
      <Text strong style={{ display: 'block', marginBottom: 12 }}>
        Connection
      </Text>

      <Form.Item label={label('Host', 'The hostname or IP address of the Cassandra / ScyllaDB node to connect to.')}>
        <Input
          value={host}
          onChange={(e) => {
            setHost(e.target.value);
            emitChange({ host: e.target.value });
          }}
          placeholder="localhost"
        />
      </Form.Item>

      <Form.Item label={label('Port', 'The CQL native transport port. The default for Cassandra and ScyllaDB is 9042.')}>
        <InputNumber
          value={port}
          onChange={(val) => {
            const v = val || 9042;
            setPort(v);
            emitChange({ port: v });
          }}
          min={1}
          max={65535}
          style={{ width: 200 }}
        />
      </Form.Item>

      <Text strong style={{ display: 'block', marginTop: 16, marginBottom: 12 }}>
        Authentication
      </Text>

      <Form.Item label={label('Auth Mode', 'Shared User \u2014 All queries use the credentials configured here. User Translation \u2014 Each user provides their own Cassandra credentials.')}>
        <Select
          value={authMode}
          onChange={(val) => {
            setAuthMode(val);
            emitChange({ authMode: val });
          }}
          style={{ width: 300 }}
          options={[
            { value: 'SHARED_USER', label: 'Shared User' },
            { value: 'USER_TRANSLATION', label: 'User Translation' },
          ]}
        />
      </Form.Item>

      <Form.Item label={label('Username', 'Cassandra / ScyllaDB username for authentication. Leave empty if authentication is not enabled on the cluster.')}>
        <Input
          value={username}
          onChange={(e) => {
            setUsername(e.target.value);
            emitChange({ username: e.target.value || undefined });
          }}
          placeholder="cassandra"
        />
      </Form.Item>

      <Form.Item label={label('Password', 'Cassandra / ScyllaDB password for authentication.')}>
        <Input.Password
          value={password}
          onChange={(e) => {
            setPassword(e.target.value);
            emitChange({ password: e.target.value || undefined });
          }}
        />
      </Form.Item>
    </Form>
  );
}
