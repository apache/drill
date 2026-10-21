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
import { Form, Input, Switch, Select, Button, Collapse, Tooltip, Typography } from 'antd';
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

interface ElasticsearchFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

export default function ElasticsearchForm({ config, onChange }: ElasticsearchFormProps) {
  const [hosts, setHosts] = useState<string[]>(
    (config.hosts as string[]) || ['http://localhost:9200']
  );
  const [username, setUsername] = useState<string>((config.username as string) || '');
  const [password, setPassword] = useState<string>((config.password as string) || '');
  const [authMode, setAuthMode] = useState<string>((config.authMode as string) || 'SHARED_USER');
  const [pathPrefix, setPathPrefix] = useState<string>((config.pathPrefix as string) || '');
  const [disableSSLVerification, setDisableSSLVerification] = useState<boolean>(
    (config.disableSSLVerification as boolean) || false
  );

  useEffect(() => {
    setHosts((config.hosts as string[]) || ['http://localhost:9200']);
    setUsername((config.username as string) || '');
    setPassword((config.password as string) || '');
    setAuthMode((config.authMode as string) || 'SHARED_USER');
    setPathPrefix((config.pathPrefix as string) || '');
    setDisableSSLVerification((config.disableSSLVerification as boolean) || false);
  }, [config]);

  const emitChange = useCallback(
    (updates: Partial<Record<string, unknown>>) => {
      onChange({ ...config, ...updates });
    },
    [config, onChange]
  );

  const updateHost = (index: number, value: string) => {
    const updated = [...hosts];
    updated[index] = value;
    setHosts(updated);
    emitChange({ hosts: updated });
  };

  const addHost = () => {
    const updated = [...hosts, ''];
    setHosts(updated);
    emitChange({ hosts: updated });
  };

  const removeHost = (index: number) => {
    const updated = hosts.filter((_, i) => i !== index);
    setHosts(updated);
    emitChange({ hosts: updated });
  };

  return (
    <Form layout="vertical">
      <Text strong style={{ display: 'block', marginBottom: 12 }}>
        Connection
      </Text>

      <Form.Item label={label('Hosts', 'One or more Elasticsearch node URLs. Include the scheme and port (e.g. http://localhost:9200).')}>
        {hosts.map((host, index) => (
          <div key={index} style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
            <Input
              value={host}
              onChange={(e) => updateHost(index, e.target.value)}
              placeholder="http://localhost:9200"
              style={{ flex: 1 }}
            />
            {hosts.length > 1 && (
              <Button
                icon={<DeleteOutlined />}
                onClick={() => removeHost(index)}
                danger
              />
            )}
          </div>
        ))}
        <Button
          type="dashed"
          onClick={addHost}
          icon={<PlusOutlined />}
          style={{ width: '100%' }}
        >
          Add Host
        </Button>
      </Form.Item>

      <Text strong style={{ display: 'block', marginTop: 16, marginBottom: 12 }}>
        Authentication
      </Text>

      <Form.Item label={label('Auth Mode', 'Shared User \u2014 All queries use the credentials configured here. User Translation \u2014 Each user provides their own Elasticsearch credentials.')}>
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

      <Form.Item label={label('Username', 'Elasticsearch username for authentication. Leave empty if security is not enabled.')}>
        <Input
          value={username}
          onChange={(e) => {
            setUsername(e.target.value);
            emitChange({ username: e.target.value || undefined });
          }}
          placeholder="elastic"
        />
      </Form.Item>

      <Form.Item label={label('Password', 'Elasticsearch password for authentication.')}>
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
                <Form.Item label={label('Path Prefix', 'A path prefix added to all Elasticsearch requests. Useful when Elasticsearch is behind a reverse proxy.')}>
                  <Input
                    value={pathPrefix}
                    onChange={(e) => {
                      setPathPrefix(e.target.value);
                      emitChange({ pathPrefix: e.target.value || undefined });
                    }}
                    placeholder="/elasticsearch (optional)"
                  />
                </Form.Item>

                <Form.Item label={label('Disable SSL Verification', 'When enabled, SSL certificate verification is skipped. Use only for development with self-signed certificates.')} style={{ marginBottom: 8 }}>
                  <Switch
                    checked={disableSSLVerification}
                    onChange={(checked) => {
                      setDisableSSLVerification(checked);
                      emitChange({ disableSSLVerification: checked });
                    }}
                  />
                </Form.Item>
              </>
            ),
          },
        ]}
      />
    </Form>
  );
}
