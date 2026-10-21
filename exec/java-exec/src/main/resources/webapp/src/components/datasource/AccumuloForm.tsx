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
import { Form, Input, InputNumber, Select, Switch, Collapse, Tooltip, Typography } from 'antd';
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

interface AccumuloFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

// ponytail: reads straight from `config` instead of mirroring into local state;
// the parent owns the config and re-renders on every change.
export default function AccumuloForm({ config, onChange }: AccumuloFormProps) {
  const str = (key: string) => (config[key] as string) || '';
  const num = (key: string) => (config[key] as number) ?? undefined;
  const set = (updates: Record<string, unknown>) => onChange({ ...config, ...updates });

  const isKerberos = str('authenticationType').toUpperCase() === 'KERBEROS';
  const authMode = str('authMode') || 'SHARED_USER';

  return (
    <Form layout="vertical">
      <Text strong style={{ display: 'block', marginBottom: 12 }}>
        Connection
      </Text>

      <Form.Item label={label('ZooKeeper Quorum', 'Comma-separated list of ZooKeeper servers in host:port form, e.g. "zk1:2181,zk2:2181".')}>
        <Input
          value={str('zookeeperQuorum')}
          onChange={(e) => set({ zookeeperQuorum: e.target.value || undefined })}
          placeholder="localhost:2181"
        />
      </Form.Item>

      <Form.Item label={label('Instance Name', 'The Accumulo instance name registered in ZooKeeper.')}>
        <Input
          value={str('instanceName')}
          onChange={(e) => set({ instanceName: e.target.value || undefined })}
          placeholder="accumulo"
        />
      </Form.Item>

      <Text strong style={{ display: 'block', marginTop: 16, marginBottom: 12 }}>
        Authentication
      </Text>

      <Form.Item label={label('Authentication Type', 'PASSWORD uses an Accumulo username and password. KERBEROS authenticates with a principal and keytab over SASL.')}>
        <Select
          value={isKerberos ? 'KERBEROS' : 'PASSWORD'}
          onChange={(value) => set({ authenticationType: value })}
          style={{ width: 240 }}
          options={[
            { value: 'PASSWORD', label: 'Password' },
            { value: 'KERBEROS', label: 'Kerberos' },
          ]}
        />
      </Form.Item>

      <Form.Item label={label('Auth Mode', 'SHARED_USER runs every query as the configured user. USER_TRANSLATION looks up per-user credentials in the credentials provider. USER_IMPERSONATION runs queries as the Drill user (Kerberos only).')}>
        <Select
          value={authMode}
          onChange={(value) => set({ authMode: value })}
          style={{ width: 240 }}
          options={[
            { value: 'SHARED_USER', label: 'Shared User' },
            { value: 'USER_TRANSLATION', label: 'User Translation' },
            { value: 'USER_IMPERSONATION', label: 'User Impersonation' },
          ]}
        />
      </Form.Item>

      {!isKerberos && authMode !== 'USER_TRANSLATION' && (
        <>
          <Form.Item label={label('Username', 'Accumulo username used for password authentication.')}>
            <Input
              value={str('username')}
              onChange={(e) => set({ username: e.target.value || undefined })}
              placeholder="root"
            />
          </Form.Item>

          <Form.Item label={label('Password', 'Password for the Accumulo user.')}>
            <Input.Password
              value={str('password')}
              onChange={(e) => set({ password: e.target.value || undefined })}
            />
          </Form.Item>
        </>
      )}

      {!isKerberos && authMode === 'USER_TRANSLATION' && (
        <Text type="secondary" style={{ display: 'block', marginBottom: 12 }}>
          Per-user credentials are stored in the credentials provider. Edit them on the JSON tab
          or in the Credentials page.
        </Text>
      )}

      {isKerberos && (
        <>
          <Form.Item label={label('Principal', 'Kerberos service principal, e.g. "drill/host.example.com@EXAMPLE.COM".')}>
            <Input
              value={str('principal')}
              onChange={(e) => set({ principal: e.target.value || undefined })}
              placeholder="drill/hostname@EXAMPLE.COM"
            />
          </Form.Item>

          <Form.Item label={label('Keytab Path', 'Path on the Drillbit host to the keytab holding the principal.')}>
            <Input
              value={str('keytabPath')}
              onChange={(e) => set({ keytabPath: e.target.value || undefined })}
              placeholder="/etc/security/keytabs/drill.keytab"
            />
          </Form.Item>

          <Form.Item label={label('SASL Quality of Protection', 'auth = authentication only, auth-int adds integrity checks, auth-conf adds encryption.')}>
            <Select
              value={str('saslQop') || 'auth'}
              onChange={(value) => set({ saslQop: value })}
              style={{ width: 240 }}
              options={[
                { value: 'auth', label: 'auth' },
                { value: 'auth-int', label: 'auth-int' },
                { value: 'auth-conf', label: 'auth-conf' },
              ]}
            />
          </Form.Item>

          <Form.Item label={label('Use Delegation Tokens', 'Obtain Accumulo delegation tokens so scans on remote Drillbits authenticate as the query user.')}>
            <Switch
              checked={Boolean(config.useDelegationTokens)}
              onChange={(checked) => set({ useDelegationTokens: checked })}
            />
          </Form.Item>
        </>
      )}

      <Collapse
        ghost
        style={{ marginTop: 16 }}
        items={[
          {
            key: 'advanced',
            label: <Text strong>Advanced Options</Text>,
            children: (
              <>
                <Form.Item label={label('Schema Metadata Table', 'Accumulo table holding Drill schema definitions. Defaults to "_drill_schema".')}>
                  <Input
                    value={str('schemaMetadataTable')}
                    onChange={(e) => set({ schemaMetadataTable: e.target.value || undefined })}
                    placeholder="_drill_schema"
                  />
                </Form.Item>

                <Form.Item label={label('Client Timeout (ms)', 'Timeout for Accumulo client operations. Defaults to 30000.')}>
                  <InputNumber
                    value={num('clientTimeout')}
                    onChange={(value) => set({ clientTimeout: value ?? undefined })}
                    min={1}
                    placeholder="30000"
                    style={{ width: 200 }}
                  />
                </Form.Item>

                <Form.Item label={label('BatchScanner Threads', 'Number of threads used by Accumulo BatchScanner. Defaults to 10.')}>
                  <InputNumber
                    value={num('batchScannerThreads')}
                    onChange={(value) => set({ batchScannerThreads: value ?? undefined })}
                    min={1}
                    placeholder="10"
                    style={{ width: 200 }}
                  />
                </Form.Item>

                <Form.Item label={label('Accumulo Service Primary', 'SASL service primary name for the Accumulo servers. Defaults to "accumulo".')}>
                  <Input
                    value={str('accumuloServicePrimary')}
                    onChange={(e) => set({ accumuloServicePrimary: e.target.value || undefined })}
                    placeholder="accumulo"
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
