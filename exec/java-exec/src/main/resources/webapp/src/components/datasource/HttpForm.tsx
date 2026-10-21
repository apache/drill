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
import { useCallback, useState, type ReactNode } from 'react';
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

type Obj = Record<string, unknown>;
type Setter = (field: string, value: unknown) => void;

interface KvRow {
  key: string;
  k: string;
  v: string;
}

/** Editor for a string->string map (headers, OAuth authorization params). */
function KeyValueEditor({
  value,
  onChange,
  keyPlaceholder,
  valuePlaceholder,
  addLabel,
}: {
  value: Record<string, string>;
  onChange: (v: Record<string, string>) => void;
  keyPlaceholder?: string;
  valuePlaceholder?: string;
  addLabel: string;
}) {
  const rows: KvRow[] = Object.entries(value).map(([k, v], i) => ({ key: `kv_${i}`, k, v }));

  const handleChange = (newRows: KvRow[]) => {
    const result: Record<string, string> = {};
    newRows.forEach((r) => {
      if (r.k) {
        result[r.k] = r.v;
      }
    });
    onChange(result);
  };

  const cell = (field: 'k' | 'v', placeholder?: string) =>
    function render(_: unknown, record: KvRow) {
      return (
        <Input
          size="small"
          value={record[field]}
          onChange={(e) =>
            handleChange(
              rows.map((r) => (r.key === record.key ? { ...r, [field]: e.target.value } : r))
            )
          }
          placeholder={placeholder}
        />
      );
    };

  return (
    <>
      <Table
        dataSource={rows}
        pagination={false}
        size="small"
        rowKey="key"
        columns={[
          { title: 'Name', dataIndex: 'k', render: cell('k', keyPlaceholder) },
          { title: 'Value', dataIndex: 'v', render: cell('v', valuePlaceholder) },
          {
            title: '',
            key: 'actions',
            width: 40,
            render: (_: unknown, record: KvRow) => (
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
        onClick={() => handleChange([...rows, { key: `kv_${Date.now()}`, k: '', v: '' }])}
      >
        {addLabel}
      </Button>
    </>
  );
}

const item = { marginBottom: 8 };

/** Provided schema (TupleMetadata) for json/xml readers; edited as raw JSON. */
function SchemaEditor({ value, onChange }: { value: unknown; onChange: (v: unknown) => void }) {
  const [text, setText] = useState(value ? JSON.stringify(value, null, 2) : '');
  const [error, setError] = useState('');

  const handleChange = (raw: string) => {
    setText(raw);
    if (!raw.trim()) {
      setError('');
      onChange(undefined);
      return;
    }
    try {
      onChange(JSON.parse(raw));
      setError('');
    } catch (e) {
      setError((e as Error).message);
    }
  };

  return (
    <Form.Item
      label="Provided Schema"
      style={item}
      validateStatus={error ? 'error' : undefined}
      help={error || 'Optional TupleMetadata JSON, e.g. {"columns": [...]}'}
    >
      <Input.TextArea rows={4} value={text} onChange={(e) => handleChange(e.target.value)} />
    </Form.Item>
  );
}


// Small field renderers: obj holds the current values, set writes one field.
const txt = (
  obj: Obj,
  set: Setter,
  key: string,
  label: string,
  opts: { placeholder?: string; help?: ReactNode; password?: boolean; width?: number } = {}
) => {
  const Component = opts.password ? Input.Password : Input;
  return (
    <Form.Item key={key} label={label} style={{ ...item, width: opts.width }} help={opts.help}>
      <Component
        value={(obj[key] as string) ?? ''}
        onChange={(e) => set(key, e.target.value)}
        placeholder={opts.placeholder}
      />
    </Form.Item>
  );
};

const num = (
  obj: Obj,
  set: Setter,
  key: string,
  label: string,
  opts: { min?: number; max?: number; placeholder?: string; help?: ReactNode } = {}
) => (
  <Form.Item key={key} label={label} style={item} help={opts.help}>
    <InputNumber
      value={(obj[key] as number) ?? undefined}
      onChange={(val) => set(key, val ?? undefined)}
      min={opts.min}
      max={opts.max}
      placeholder={opts.placeholder}
      style={{ width: 160 }}
    />
  </Form.Item>
);

const bool = (obj: Obj, set: Setter, key: string, label: string, dflt = false) => (
  <Space key={key}>
    <Switch checked={(obj[key] as boolean) ?? dflt} onChange={(checked) => set(key, checked)} />
    <Text>{label}</Text>
  </Space>
);

const sel = (
  obj: Obj,
  set: Setter,
  key: string,
  label: string,
  values: string[],
  opts: { dflt?: string; allowClear?: boolean; placeholder?: string; width?: number } = {}
) => (
  <Form.Item key={key} label={label} style={item}>
    <Select
      value={(obj[key] as string) ?? opts.dflt}
      onChange={(val) => set(key, val)}
      allowClear={opts.allowClear}
      placeholder={opts.placeholder}
      options={values.map((v) => ({ value: v, label: v }))}
      style={{ width: opts.width ?? 160 }}
    />
  </Form.Item>
);

// Which paginator params matter depends on the method; see HttpPaginatorConfig.
const PAGINATOR_FIELDS: Record<string, string[][]> = {
  offset: [
    ['limitParam', 'Limit Param'],
    ['offsetParam', 'Offset Param'],
  ],
  page: [
    ['pageParam', 'Page Param'],
    ['pageSizeParam', 'Page Size Param'],
  ],
  index: [
    ['limitParam', 'Limit Param'],
    ['indexParam', 'Index Param'],
    ['hasMoreParam', 'Has More Param'],
    ['nextPageParam', 'Next Page Param'],
  ],
  header_index: [
    ['limitParam', 'Limit Param'],
    ['indexParam', 'Index Param'],
    ['hasMoreParam', 'Has More Param'],
    ['nextPageParam', 'Next Page Param'],
  ],
};

export default function HttpForm({ config, onChange }: HttpFormProps) {
  const connections = (config.connections as Record<string, Obj>) || {};
  const credsProvider = (config.credentialsProvider as Obj) || {};
  const creds = (credsProvider.credentials as Record<string, string>) || {};
  const oauth = config.oAuthConfig as Obj | undefined;

  const emitChange = useCallback(
    (updates: Obj) => onChange({ ...config, ...updates }),
    [config, onChange]
  );

  const setTop: Setter = (field, value) => {
    const updated = { ...config, [field]: value };
    if (value === undefined || value === '') {
      delete updated[field];
    }
    onChange(updated);
  };

  const setCred: Setter = (field, value) => {
    const updated = { ...creds, [field]: value as string };
    if (!value) {
      delete updated[field];
    }
    emitChange({
      credentialsProvider: Object.keys(updated).length
        ? {
            credentialsProviderType: 'PlainCredentialsProvider',
            userCredentials: {},
            ...credsProvider,
            credentials: updated,
          }
        : undefined,
    });
  };

  const setOAuth: Setter = (field, value) => {
    const updated = { ...(oauth || {}), [field]: value };
    if (value === undefined || value === '') {
      delete updated[field];
    }
    emitChange({ oAuthConfig: updated });
  };

  const setConn = (name: string) => (field: string, value: unknown) => {
    const conn = { ...(connections[name] || { url: '' }), [field]: value };
    if (value === undefined || value === '') {
      delete conn[field];
    }
    emitChange({ connections: { ...connections, [name]: conn } });
  };

  // paginator / jsonOptions / xmlOptions / csvOptions are nested objects on a connection.
  const setNested = (name: string, group: string) => (field: string, value: unknown) => {
    const current = { ...((connections[name]?.[group] as Obj) || {}) };
    if (value === undefined || value === '') {
      delete current[field];
    } else {
      current[field] = value;
    }
    setConn(name)(group, current);
  };

  const addConnection = () => {
    const name = `connection_${Object.keys(connections).length + 1}`;
    emitChange({ connections: { ...connections, [name]: { url: '', method: 'GET' } } });
  };

  const removeConnection = (name: string) => {
    const updated = { ...connections };
    delete updated[name];
    emitChange({ connections: updated });
  };

  const renameConnection = (oldName: string, newName: string) => {
    if (!newName || newName === oldName || connections[newName]) {
      return;
    }
    const updated: Record<string, Obj> = {};
    Object.entries(connections).forEach(([k, v]) => {
      updated[k === oldName ? newName : k] = v;
    });
    emitChange({ connections: updated });
  };

  const renderFormatOptions = (name: string, conn: Obj, inputType: string) => {
    const set = setNested(name, `${inputType}Options`);
    const opts = (conn[`${inputType}Options`] as Obj) || {};

    if (inputType === 'json') {
      return (
        <Space direction="vertical">
          <Space size="large" wrap>
            {bool(opts, set, 'allTextMode', 'All Text Mode')}
            {bool(opts, set, 'allowNanInf', 'Allow NaN / Infinity')}
            {bool(opts, set, 'readNumbersAsDouble', 'Read Numbers as Double')}
          </Space>
          <Space size="large" wrap>
            {bool(opts, set, 'enableEscapeAnyChar', 'Escape Any Char')}
            {bool(opts, set, 'skipMalformedDocument', 'Skip Malformed Document')}
            {bool(opts, set, 'skipMalformedRecords', 'Skip Malformed Records')}
          </Space>
          <SchemaEditor value={opts.schema} onChange={(v) => set('schema', v)} />
        </Space>
      );
    }

    if (inputType === 'xml') {
      return (
        <>
          <Space align="start" wrap>
            {num(opts, set, 'dataLevel', 'Data Level', { min: 1 })}
            {bool(opts, set, 'allTextMode', 'All Text Mode')}
          </Space>
          <SchemaEditor value={opts.schema} onChange={(v) => set('schema', v)} />
        </>
      );
    }

    return (
      <>
        <Space align="start" wrap>
          {txt(opts, set, 'delimiter', 'Delimiter', { placeholder: ',', width: 140 })}
          {txt(opts, set, 'quote', 'Quote', { placeholder: '"', width: 140 })}
          {txt(opts, set, 'quoteEscape', 'Quote Escape', { placeholder: '"', width: 140 })}
          {txt(opts, set, 'lineSeparator', 'Line Separator', { placeholder: '\\n', width: 160 })}
          {txt(opts, set, 'nullValue', 'Null Value', { width: 160 })}
        </Space>
        <Space align="start" wrap>
          {num(opts, set, 'numberOfRowsToSkip', 'Rows to Skip', { min: 0 })}
          {num(opts, set, 'numberOfRecordsToRead', 'Records to Read', { placeholder: '-1' })}
          {num(opts, set, 'maxColumns', 'Max Columns', { min: 1, placeholder: '512' })}
          {num(opts, set, 'maxCharsPerColumn', 'Max Chars / Column', {
            min: 1,
            placeholder: '4096',
          })}
        </Space>
        <Space size="large" wrap>
          {bool(opts, set, 'headerExtractionEnabled', 'Extract Header', true)}
          {bool(opts, set, 'lineSeparatorDetectionEnabled', 'Detect Line Separator', true)}
          {bool(opts, set, 'skipEmptyLines', 'Skip Empty Lines', true)}
          {bool(opts, set, 'ignoreLeadingWhitespaces', 'Ignore Leading Whitespace', true)}
          {bool(opts, set, 'ignoreTrailingWhitespaces', 'Ignore Trailing Whitespace', true)}
        </Space>
      </>
    );
  };

  const renderPaginator = (name: string, conn: Obj) => {
    if (!conn.paginator) {
      return (
        <Button
          type="link"
          size="small"
          icon={<PlusOutlined />}
          onClick={() => setConn(name)('paginator', { method: 'offset' })}
        >
          Add Paginator
        </Button>
      );
    }
    const paginator = conn.paginator as Obj;
    const set = setNested(name, 'paginator');
    const method = (paginator.method as string) || 'offset';

    return (
      <Card
        size="small"
        title="Pagination"
        extra={
          <Button
            type="text"
            size="small"
            danger
            icon={<DeleteOutlined />}
            onClick={() => setConn(name)('paginator', undefined)}
          />
        }
      >
        <Form layout="vertical">
          {sel(paginator, set, 'method', 'Method', Object.keys(PAGINATOR_FIELDS), {
            dflt: 'offset',
            width: 180,
          })}
          {PAGINATOR_FIELDS[method].map(([field, label]) => txt(paginator, set, field, label))}
          <Space align="start" wrap>
            {num(paginator, set, 'pageSize', 'Page Size', { min: 1 })}
            {num(paginator, set, 'maxRecords', 'Max Records', { min: 1 })}
          </Space>
        </Form>
      </Card>
    );
  };

  const renderConnection = (name: string, conn: Obj) => {
    const set = setConn(name);
    const isPost = (conn.method as string) === 'POST';
    const inputType = (conn.inputType as string) || 'json';

    return (
      <Card
        key={name}
        size="small"
        title={
          <Input
            size="small"
            defaultValue={name}
            onBlur={(e) => renameConnection(name, e.target.value.trim())}
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
          {txt(conn, set, 'url', 'URL', { placeholder: 'https://api.example.com/data' })}
          <Space align="start" wrap>
            {sel(conn, set, 'method', 'Method', ['GET', 'POST'], { dflt: 'GET', width: 120 })}
            {sel(conn, set, 'inputType', 'Input Type', ['json', 'csv', 'xml'], {
              dflt: 'json',
              width: 120,
            })}
            {sel(conn, set, 'authType', 'Auth Type', ['none', 'basic'], {
              dflt: 'none',
              width: 120,
            })}
          </Space>
          {(conn.authType as string) === 'basic' && (
            <Space align="start" wrap>
              {txt(conn, set, 'userName', 'Username', { width: 220 })}
              {txt(conn, set, 'password', 'Password', { password: true, width: 220 })}
            </Space>
          )}
          {txt(conn, set, 'dataPath', 'Data Path', {
            placeholder: 'results/rows',
            help: 'Slash-delimited path to the data within the response',
          })}
          <Form.Item
            label="Parameters"
            style={item}
            help="Query parameters that may be used in the WHERE clause"
          >
            <Select
              mode="tags"
              value={(conn.params as string[]) || []}
              onChange={(vals) => set('params', vals.length ? vals : undefined)}
              placeholder="lat, lng, ..."
              tokenSeparators={[',']}
            />
          </Form.Item>
          <Form.Item label="Headers" style={item}>
            <KeyValueEditor
              value={(conn.headers as Record<string, string>) || {}}
              onChange={(h) => set('headers', Object.keys(h).length ? h : undefined)}
              keyPlaceholder="Content-Type"
              valuePlaceholder="application/json"
              addLabel="Add Header"
            />
          </Form.Item>
          {isPost && (
            <>
              <Form.Item
                label="POST Body"
                style={item}
                help="Static POST parameters, one key=value pair per line"
              >
                <Input.TextArea
                  rows={3}
                  value={(conn.postBody as string) || ''}
                  onChange={(e) => set('postBody', e.target.value)}
                />
              </Form.Item>
              {sel(
                conn,
                set,
                'postParameterLocation',
                'POST Parameter Location',
                ['post_body', 'query_string', 'json_body', 'xml_body'],
                { dflt: 'post_body', width: 200 }
              )}
            </>
          )}
          {txt(conn, set, 'limitQueryParam', 'Limit Query Parameter', {
            placeholder: 'maxRecords',
          })}
          <Space size="large" wrap style={{ marginBottom: 12 }}>
            {bool(conn, set, 'requireTail', 'Require Tail', true)}
            {bool(conn, set, 'verifySSLCert', 'Verify SSL Cert', true)}
            {bool(conn, set, 'errorOn400', 'Error on 400')}
            {bool(conn, set, 'caseSensitiveFilters', 'Case Sensitive Filters')}
          </Space>
          <Collapse
            ghost
            items={[
              {
                key: 'format',
                label: `${inputType.toUpperCase()} Options`,
                children: <Form layout="vertical">{renderFormatOptions(name, conn, inputType)}</Form>,
              },
            ]}
          />
          {renderPaginator(name, conn)}
        </Form>
      </Card>
    );
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
          {Object.entries(connections).map(([name, conn]) => renderConnection(name, conn))}
        </Space>
      </Form.Item>

      <Space align="start" wrap>
        {num(config, setTop, 'timeout', 'Timeout (seconds)', { min: 0 })}
        {num(config, setTop, 'retryDelay', 'Retry Delay (ms)', { min: 0 })}
        {sel(config, setTop, 'authMode', 'Auth Mode', ['SHARED_USER', 'USER_TRANSLATION'], {
          dflt: 'SHARED_USER',
          width: 180,
        })}
      </Space>
      <Space size="large" wrap style={{ marginBottom: 12 }}>
        {bool(config, setTop, 'cacheResults', 'Cache Results')}
        {bool(config, setTop, 'enhanced', 'Enhanced Parameter Syntax')}
      </Space>

      <Collapse
        ghost
        items={[
          {
            key: 'credentials',
            label: 'Credentials',
            children: (
              <Form layout="vertical">
                <Space align="start" wrap>
                  {txt(config, setTop, 'username', 'Username', { width: 240 })}
                  {txt(config, setTop, 'password', 'Password', { password: true, width: 240 })}
                </Space>
                <Space align="start" wrap>
                  {txt(creds, setCred, 'clientID', 'OAuth Client ID', { width: 240 })}
                  {txt(creds, setCred, 'clientSecret', 'OAuth Client Secret', {
                    password: true,
                    width: 240,
                  })}
                  {txt(creds, setCred, 'tokenURI', 'Token URI', { width: 320 })}
                </Space>
              </Form>
            ),
          },
          {
            key: 'oauth',
            label: 'OAuth 2.0',
            children: oauth ? (
              <Form layout="vertical">
                {txt(oauth, setOAuth, 'authorizationURL', 'Authorization URL', {
                  placeholder: 'https://example.com/oauth2/auth',
                })}
                {txt(oauth, setOAuth, 'callbackURL', 'Callback URL', {
                  placeholder: 'http://localhost:8047/credentials/<plugin>/update_oauth2_authtoken',
                })}
                <Space align="start" wrap>
                  {txt(oauth, setOAuth, 'scope', 'Scope', { width: 320 })}
                  {txt(oauth, setOAuth, 'tokenType', 'Token Type', {
                    placeholder: 'Bearer',
                    width: 200,
                  })}
                </Space>
                <Form.Item label="Authorization Params" style={item}>
                  <KeyValueEditor
                    value={(oauth.authorizationParams as Record<string, string>) || {}}
                    onChange={(p) =>
                      setOAuth('authorizationParams', Object.keys(p).length ? p : undefined)
                    }
                    keyPlaceholder="response_type"
                    valuePlaceholder="code"
                    addLabel="Add Param"
                  />
                </Form.Item>
                <Space size="large" wrap style={{ marginBottom: 12 }}>
                  {bool(oauth, setOAuth, 'generateCSRFToken', 'Generate CSRF Token')}
                  {bool(oauth, setOAuth, 'accessTokenInHeader', 'Access Token in Header')}
                </Space>
                <Button
                  danger
                  size="small"
                  icon={<DeleteOutlined />}
                  onClick={() => emitChange({ oAuthConfig: undefined })}
                >
                  Remove OAuth Config
                </Button>
              </Form>
            ) : (
              <Button
                type="link"
                size="small"
                icon={<PlusOutlined />}
                onClick={() => emitChange({ oAuthConfig: {} })}
              >
                Add OAuth Config
              </Button>
            ),
          },
          {
            key: 'proxy',
            label: 'Proxy Configuration',
            children: (
              <Form layout="vertical">
                {txt(config, setTop, 'proxyHost', 'Host', { placeholder: 'proxy.example.com' })}
                <Space align="start" wrap>
                  {num(config, setTop, 'proxyPort', 'Port', { min: 1, max: 65535 })}
                  {sel(config, setTop, 'proxyType', 'Type', ['direct', 'http', 'socks'], {
                    allowClear: true,
                    placeholder: 'direct',
                    width: 140,
                  })}
                </Space>
                <Space align="start" wrap>
                  {txt(config, setTop, 'proxyUsername', 'Username', { width: 240 })}
                  {txt(config, setTop, 'proxyPassword', 'Password', {
                    password: true,
                    width: 240,
                  })}
                </Space>
              </Form>
            ),
          },
        ]}
      />
    </Form>
  );
}
