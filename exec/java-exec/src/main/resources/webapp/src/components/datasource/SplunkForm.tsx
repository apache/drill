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
import { Form, Input, InputNumber, Switch, Select, Collapse, Tooltip, Typography } from 'antd';
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

interface SplunkFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

export default function SplunkForm({ config, onChange }: SplunkFormProps) {
  const [authMode, setAuthMode] = useState<string>((config.authMode as string) || 'SHARED_USER');
  const [username, setUsername] = useState<string>((config.username as string) || '');
  const [password, setPassword] = useState<string>((config.password as string) || '');
  const [scheme, setScheme] = useState<string>((config.scheme as string) || 'https');
  const [hostname, setHostname] = useState<string>((config.hostname as string) || 'localhost');
  const [port, setPort] = useState<number>((config.port as number) || 8089);
  const [earliestTime, setEarliestTime] = useState<string>((config.earliestTime as string) || '');
  const [latestTime, setLatestTime] = useState<string>((config.latestTime as string) || 'now');
  const [app, setApp] = useState<string>((config.app as string) || '');
  const [owner, setOwner] = useState<string>((config.owner as string) || '');
  const [token, setToken] = useState<string>((config.token as string) || '');
  const [cookie, setCookie] = useState<string>((config.cookie as string) || '');
  const [validateCertificates, setValidateCertificates] = useState<boolean>(
    (config.validateCertificates as boolean) || false
  );
  const [validateHostname, setValidateHostname] = useState<boolean>(
    (config.validateHostname as boolean) || false
  );
  const [reconnectRetries, setReconnectRetries] = useState<number>(
    (config.reconnectRetries as number) || 1
  );
  const [maxColumns, setMaxColumns] = useState<number>(
    (config.maxColumns as number) || 1024
  );
  const [maxCacheSize, setMaxCacheSize] = useState<number>(
    (config.maxCacheSize as number) || 10000
  );
  const [cacheExpiration, setCacheExpiration] = useState<number>(
    (config.cacheExpiration as number) || 1024
  );

  useEffect(() => {
    setAuthMode((config.authMode as string) || 'SHARED_USER');
    setUsername((config.username as string) || '');
    setPassword((config.password as string) || '');
    setScheme((config.scheme as string) || 'https');
    setHostname((config.hostname as string) || 'localhost');
    setPort((config.port as number) || 8089);
    setEarliestTime((config.earliestTime as string) || '');
    setLatestTime((config.latestTime as string) || 'now');
    setApp((config.app as string) || '');
    setOwner((config.owner as string) || '');
    setToken((config.token as string) || '');
    setCookie((config.cookie as string) || '');
    setValidateCertificates((config.validateCertificates as boolean) || false);
    setValidateHostname((config.validateHostname as boolean) || false);
    setReconnectRetries((config.reconnectRetries as number) || 1);
    setMaxColumns((config.maxColumns as number) || 1024);
    setMaxCacheSize((config.maxCacheSize as number) || 10000);
    setCacheExpiration((config.cacheExpiration as number) || 1024);
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

      <Form.Item label={label('Scheme', 'Protocol used to connect to the Splunk host (HTTPS recommended for production).')}>
        <Select
          value={scheme}
          onChange={(val) => {
            setScheme(val);
            emitChange({ scheme: val });
          }}
          style={{ width: 200 }}
          options={[
            { value: 'https', label: 'HTTPS' },
            { value: 'http', label: 'HTTP' },
          ]}
        />
      </Form.Item>

      <Form.Item label={label('Hostname', 'The hostname or IP address of the Splunk server.')}>
        <Input
          value={hostname}
          onChange={(e) => {
            setHostname(e.target.value);
            emitChange({ hostname: e.target.value });
          }}
          placeholder="localhost"
        />
      </Form.Item>

      <Form.Item label={label('Port', 'The Splunk management port. The default Splunk management port is 8089.')}>
        <InputNumber
          value={port}
          onChange={(val) => {
            const v = val || 8089;
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

      <Form.Item label={label('Auth Mode', 'Shared User \u2014 All queries use the credentials configured here. User Translation \u2014 Each user provides their own Splunk credentials.')}>
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

      <Form.Item label={label('Username', 'Splunk account username used to authenticate Drill queries.')}>
        <Input
          value={username}
          onChange={(e) => {
            setUsername(e.target.value);
            emitChange({ username: e.target.value });
          }}
          placeholder="admin"
        />
      </Form.Item>

      <Form.Item label={label('Password', 'Splunk account password.')}>
        <Input.Password
          value={password}
          onChange={(e) => {
            setPassword(e.target.value);
            emitChange({ password: e.target.value });
          }}
        />
      </Form.Item>

      <Text strong style={{ display: 'block', marginTop: 16, marginBottom: 12 }}>
        Time Boundaries
      </Text>

      <Form.Item label={label('Earliest Time', 'Default earliest time boundary for Splunk searches. Uses Splunk relative time syntax (e.g. "-14d", "-1h@h").')}>
        <Input
          value={earliestTime}
          onChange={(e) => {
            setEarliestTime(e.target.value);
            emitChange({ earliestTime: e.target.value || undefined });
          }}
          placeholder="-14d"
        />
      </Form.Item>

      <Form.Item label={label('Latest Time', 'Default latest time boundary for Splunk searches. Uses Splunk relative time syntax (e.g. "now", "-1d@d").')}>
        <Input
          value={latestTime}
          onChange={(e) => {
            setLatestTime(e.target.value);
            emitChange({ latestTime: e.target.value });
          }}
          placeholder="now"
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
                  Alternative Authentication
                </Text>

                <Form.Item label={label('Token', 'A Splunk authentication token. Can be used instead of username/password for token-based authentication.')}>
                  <Input.Password
                    value={token}
                    onChange={(e) => {
                      setToken(e.target.value);
                      emitChange({ token: e.target.value || undefined });
                    }}
                    placeholder="Session token (optional)"
                  />
                </Form.Item>

                <Form.Item label={label('Cookie', 'A valid Splunk login cookie. Can be used for session-based authentication.')}>
                  <Input
                    value={cookie}
                    onChange={(e) => {
                      setCookie(e.target.value);
                      emitChange({ cookie: e.target.value || undefined });
                    }}
                    placeholder="Login cookie (optional)"
                  />
                </Form.Item>

                <Text strong style={{ display: 'block', marginTop: 16, marginBottom: 12 }}>
                  Service Context
                </Text>

                <Form.Item label={label('Application', 'The Splunk application context for the service. Limits searches to a specific Splunk app.')}>
                  <Input
                    value={app}
                    onChange={(e) => {
                      setApp(e.target.value);
                      emitChange({ app: e.target.value || undefined });
                    }}
                    placeholder="Application context (optional)"
                  />
                </Form.Item>

                <Form.Item label={label('Owner', 'The Splunk owner context for the service. Limits searches to objects owned by a specific user.')}>
                  <Input
                    value={owner}
                    onChange={(e) => {
                      setOwner(e.target.value);
                      emitChange({ owner: e.target.value || undefined });
                    }}
                    placeholder="Owner context (optional)"
                  />
                </Form.Item>

                <Text strong style={{ display: 'block', marginTop: 16, marginBottom: 12 }}>
                  Security
                </Text>

                <Form.Item label={label('Validate SSL Certificates', "When enabled, the Splunk client will validate the server's SSL certificate. Disable only for development with self-signed certificates.")} style={{ marginBottom: 8 }}>
                  <Switch
                    checked={validateCertificates}
                    onChange={(checked) => {
                      setValidateCertificates(checked);
                      emitChange({ validateCertificates: checked });
                    }}
                  />
                </Form.Item>

                <Form.Item label={label('Validate Hostname', 'When enabled, the Splunk client will verify that the server hostname matches the certificate. Disable only for development.')} style={{ marginBottom: 8 }}>
                  <Switch
                    checked={validateHostname}
                    onChange={(checked) => {
                      setValidateHostname(checked);
                      emitChange({ validateHostname: checked });
                    }}
                  />
                </Form.Item>

                <Text strong style={{ display: 'block', marginTop: 16, marginBottom: 12 }}>
                  Performance
                </Text>

                <Form.Item label={label('Reconnect Retries', 'Number of times Drill will attempt to reconnect to Splunk if the connection is lost.')}>
                  <InputNumber
                    value={reconnectRetries}
                    onChange={(val) => {
                      const v = val || 1;
                      setReconnectRetries(v);
                      emitChange({ reconnectRetries: v });
                    }}
                    min={0}
                    style={{ width: 200 }}
                  />
                </Form.Item>

                <Form.Item label={label('Max Columns', 'The maximum number of columns Drill will accept from a Splunk query result.')}>
                  <InputNumber
                    value={maxColumns}
                    onChange={(val) => {
                      const v = val || 1024;
                      setMaxColumns(v);
                      emitChange({ maxColumns: v });
                    }}
                    min={1}
                    style={{ width: 200 }}
                  />
                </Form.Item>

                <Form.Item label={label('Max Cache Size (bytes)', 'Maximum size of the schema cache in bytes. Caching avoids repeated schema lookups against Splunk.')}>
                  <InputNumber
                    value={maxCacheSize}
                    onChange={(val) => {
                      const v = val || 10000;
                      setMaxCacheSize(v);
                      emitChange({ maxCacheSize: v });
                    }}
                    min={0}
                    style={{ width: 200 }}
                  />
                </Form.Item>

                <Form.Item label={label('Cache Expiration (minutes)', 'How long cached schema entries persist before being refreshed from Splunk.')}>
                  <InputNumber
                    value={cacheExpiration}
                    onChange={(val) => {
                      const v = val || 1024;
                      setCacheExpiration(v);
                      emitChange({ cacheExpiration: v });
                    }}
                    min={0}
                    style={{ width: 200 }}
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
