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
import { useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Input,
  Button,
  Spin,
  Modal,
  Form,
  Select,
  Tooltip,
  Dropdown,
  message,
} from 'antd';
import type { MenuProps } from 'antd';
import {
  SearchOutlined,
  EditOutlined,
  DeleteOutlined,
  PlusOutlined,
  DatabaseOutlined,
  KeyOutlined,
  PoweroffOutlined,
  MoreOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getPlugins, deletePlugin, enablePlugin } from '../api/storage';
import { cleanupPluginDatasets } from '../api/projects';
import { getPluginLogoUrl, getPluginGradient, knownPluginTypes, getTemplate } from '../components/datasource';
import { usePageChrome } from '../contexts/AppChromeContext';
import type { StoragePluginDetail } from '../types';

type FilterMode = 'all' | 'enabled' | 'disabled';

function buildAuthorizationUrl(config: Record<string, unknown>): string {
  const oAuth = config.oAuthConfig as Record<string, unknown>;
  const credProvider = config.credentialsProvider as Record<string, unknown> | undefined;
  const creds = credProvider?.credentials as Record<string, unknown> | undefined;

  if (!oAuth?.authorizationURL || !creds?.clientID) {
    return '';
  }

  let url = `${oAuth.authorizationURL}?client_id=${encodeURIComponent(creds.clientID as string)}`
          + `&redirect_uri=${encodeURIComponent(oAuth.callbackURL as string)}`;

  const scope = oAuth.scope as string | undefined;
  if (scope) {
    url += `&scope=${encodeURIComponent(scope)}`;
  }

  const params = oAuth.authorizationParams as Record<string, string> | undefined;
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      url += `&${encodeURIComponent(k)}=${encodeURIComponent(v)}`;
    }
  }

  return url;
}

function isOAuthPlugin(config: Record<string, unknown>): boolean {
  const oAuth = config.oAuthConfig as Record<string, unknown> | undefined;
  const credProvider = config.credentialsProvider as Record<string, unknown> | undefined;
  const credentials = credProvider?.credentials as Record<string, unknown> | undefined;
  return !!(oAuth?.authorizationURL && credentials?.clientID);
}

function getPluginType(plugin: StoragePluginDetail): string {
  return (plugin.config?.type as string) || 'unknown';
}

function isEnabled(plugin: StoragePluginDetail): boolean {
  return (plugin.config?.enabled as boolean) || false;
}

function getConnectionSnippet(config: Record<string, unknown>): string {
  const conn = (config.connection as string) || (config.url as string) || '';
  if (conn.length > 64) {
    return conn.substring(0, 61) + '…';
  }
  return conn;
}

export default function DataSourcesPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [searchText, setSearchText] = useState('');
  const [filterMode, setFilterMode] = useState<FilterMode>('all');
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [createForm] = Form.useForm();

  const { data: plugins, isLoading, error } = useQuery({
    queryKey: ['storage-plugins'],
    queryFn: getPlugins,
  });

  const deleteMutation = useMutation({
    mutationFn: deletePlugin,
    onSuccess: (_data, pluginName) => {
      message.success('Plugin deleted');
      queryClient.invalidateQueries({ queryKey: ['storage-plugins'] });
      cleanupPluginDatasets(pluginName).then(() => {
        queryClient.invalidateQueries({ queryKey: ['projects'] });
      }).catch(() => {
        // Best-effort cleanup
      });
    },
    onError: (err: Error) => message.error(`Failed to delete plugin: ${err.message}`),
  });

  const enableMutation = useMutation({
    mutationFn: ({ name, enable }: { name: string; enable: boolean }) =>
      enablePlugin(name, enable),
    onSuccess: (_, { enable }) => {
      message.success(enable ? 'Plugin enabled' : 'Plugin disabled');
      queryClient.invalidateQueries({ queryKey: ['storage-plugins'] });
    },
    onError: (err: Error) => message.error(`Failed to toggle plugin: ${err.message}`),
  });

  const handleAuthorize = (config: Record<string, unknown>) => {
    const url = buildAuthorizationUrl(config);
    if (!url) {
      message.error('Cannot authorize: OAuth configuration is incomplete');
      return;
    }
    const popup = window.open(
      url,
      'Authorize Drill',
      'toolbar=no,menubar=no,scrollbars=yes,resizable=yes,top=500,left=500,width=450,height=600',
    );
    const timer = setInterval(() => {
      if (popup?.closed) {
        clearInterval(timer);
        queryClient.invalidateQueries({ queryKey: ['storage-plugins'] });
      }
    }, 1000);
  };

  const filteredPlugins = useMemo(() => {
    if (!plugins) {
      return [];
    }
    let result = [...plugins];
    if (filterMode === 'enabled') {
      result = result.filter((p) => isEnabled(p));
    } else if (filterMode === 'disabled') {
      result = result.filter((p) => !isEnabled(p));
    }
    if (searchText) {
      const lower = searchText.toLowerCase();
      result = result.filter(
        (p) =>
          p.name.toLowerCase().includes(lower) ||
          getPluginType(p).toLowerCase().includes(lower) ||
          ((p.config?.description as string) || '').toLowerCase().includes(lower),
      );
    }
    return result.sort((a, b) => {
      const aEnabled = isEnabled(a);
      const bEnabled = isEnabled(b);
      if (aEnabled !== bEnabled) {
        return aEnabled ? -1 : 1;
      }
      return a.name.localeCompare(b.name);
    });
  }, [plugins, searchText, filterMode]);

  const handleCreate = async () => {
    try {
      const values = await createForm.validateFields();
      const template = getTemplate(values.type);
      if (values.description) {
        template.description = values.description;
      }
      navigate(`/datasources/${encodeURIComponent(values.name)}`, {
        state: { config: template, isNew: true },
      });
      setCreateModalOpen(false);
      createForm.resetFields();
    } catch {
      // Form validation failed
    }
  };

  // Toolbar action registered in unified shell
  const toolbarActions = useMemo(
    () => (
      <Button type="primary" size="small" icon={<PlusOutlined />} onClick={() => setCreateModalOpen(true)}>
        New Data Source
      </Button>
    ),
    [],
  );
  usePageChrome({ toolbarActions });

  const total = plugins?.length ?? 0;
  const enabledCount = (plugins ?? []).filter(isEnabled).length;

  if (error) {
    return (
      <div className="page-datasources-error">
        <h2>Couldn't load data sources</h2>
        <p>{(error as Error).message}</p>
      </div>
    );
  }

  return (
    <div className="page-datasources">
      <header className="page-datasources-header">
        <div>
          <h1 className="page-datasources-title">Data Sources</h1>
          <p className="page-datasources-subtitle">
            {plugins === undefined
              ? 'Loading…'
              : total === 0
                ? 'No plugins configured'
                : `${enabledCount} enabled · ${total - enabledCount} disabled · ${total} total`}
          </p>
        </div>
      </header>

      <div className="page-datasources-toolbar">
        <Input
          placeholder="Search by name, type, or description…"
          prefix={<SearchOutlined style={{ color: 'var(--color-text-tertiary)' }} />}
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          allowClear
          className="page-datasources-search"
        />

        <div className="page-datasources-chips">
          {(['all', 'enabled', 'disabled'] as FilterMode[]).map((f) => (
            <button
              key={f}
              type="button"
              className={`page-datasources-chip${filterMode === f ? ' is-active' : ''}`}
              onClick={() => setFilterMode(f)}
            >
              {f === 'all' && 'All'}
              {f === 'enabled' && (
                <>
                  <span className="page-datasources-chip-dot" style={{ background: 'var(--color-success)' }} />
                  Enabled
                </>
              )}
              {f === 'disabled' && (
                <>
                  <span className="page-datasources-chip-dot" style={{ background: 'var(--color-text-quaternary)' }} />
                  Disabled
                </>
              )}
            </button>
          ))}
        </div>
      </div>

      {isLoading ? (
        <div className="page-datasources-loading">
          <Spin size="large" />
        </div>
      ) : (
        <div className="page-datasources-grid">
          <NewDataSourceTile onClick={() => setCreateModalOpen(true)} />

          {filteredPlugins.map((plugin) => {
            const type = getPluginType(plugin);
            const enabled = isEnabled(plugin);
            const logoUrl = getPluginLogoUrl(type, plugin.config?.connection as string);
            const oauth = isOAuthPlugin(plugin.config);
            const description = plugin.config?.description as string | undefined;
            const connSnippet = getConnectionSnippet(plugin.config);

            const moreItems: MenuProps['items'] = [
              {
                key: 'edit',
                icon: <EditOutlined />,
                label: 'Edit configuration',
                onClick: () => navigate(`/datasources/${encodeURIComponent(plugin.name)}`),
              },
              ...(oauth ? [{
                key: 'authorize',
                icon: <KeyOutlined />,
                label: 'Authorize…',
                onClick: () => handleAuthorize(plugin.config),
              }] : []),
              {
                key: 'toggle',
                icon: <PoweroffOutlined />,
                label: enabled ? 'Disable' : 'Enable',
                onClick: () => {
                  if (enabled) {
                    Modal.confirm({
                      title: `Disable "${plugin.name}"?`,
                      content: 'Queries using this plugin will fail while it is disabled.',
                      okText: 'Disable',
                      okButtonProps: { danger: true },
                      onOk: () => enableMutation.mutate({ name: plugin.name, enable: false }),
                    });
                  } else {
                    enableMutation.mutate({ name: plugin.name, enable: true });
                  }
                },
              },
              { type: 'divider' as const },
              {
                key: 'delete',
                icon: <DeleteOutlined />,
                label: 'Delete',
                danger: true,
                onClick: () => {
                  Modal.confirm({
                    title: 'Delete this plugin?',
                    content: 'This action cannot be undone.',
                    okText: 'Delete',
                    okButtonProps: { danger: true },
                    onOk: () => deleteMutation.mutate(plugin.name),
                  });
                },
              },
            ];

            return (
              <div
                key={plugin.name}
                className={`datasource-tile${enabled ? '' : ' is-disabled'}`}
                onClick={() => navigate(`/datasources/${encodeURIComponent(plugin.name)}`)}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    navigate(`/datasources/${encodeURIComponent(plugin.name)}`);
                  }
                }}
              >
                <div
                  className="datasource-tile-banner"
                  style={{ background: getPluginGradient(type) }}
                >
                  {logoUrl ? (
                    <img
                      src={logoUrl}
                      alt={type}
                      className="datasource-tile-logo"
                      onError={(e) => {
                        (e.target as HTMLImageElement).style.display = 'none';
                      }}
                    />
                  ) : (
                    <SettingOutlined className="datasource-tile-fallback-icon" />
                  )}

                  <Tooltip title={enabled ? 'Enabled' : 'Disabled'}>
                    <span className={`datasource-tile-status-dot${enabled ? ' is-on' : ''}`} aria-label={enabled ? 'Enabled' : 'Disabled'} />
                  </Tooltip>

                  <div className="datasource-tile-actions" onClick={(e) => e.stopPropagation()}>
                    {oauth && (
                      <Tooltip title="Authorize OAuth">
                        <button
                          type="button"
                          className="datasource-tile-action-btn"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleAuthorize(plugin.config);
                          }}
                          aria-label="Authorize"
                        >
                          <KeyOutlined />
                        </button>
                      </Tooltip>
                    )}
                    <Dropdown menu={{ items: moreItems }} placement="bottomRight" trigger={['click']}>
                      <button type="button" className="datasource-tile-action-btn" aria-label="More">
                        <MoreOutlined />
                      </button>
                    </Dropdown>
                  </div>
                </div>

                <div className="datasource-tile-meta">
                  <div className="datasource-tile-name-row">
                    <h3 className="datasource-tile-name" title={plugin.name}>{plugin.name}</h3>
                    <span className="datasource-tile-type">{type}</span>
                  </div>
                  {description && (
                    <p className="datasource-tile-description">{description}</p>
                  )}
                  {connSnippet && (
                    <p className="datasource-tile-connection">
                      <code>{connSnippet}</code>
                    </p>
                  )}
                </div>
              </div>
            );
          })}

          {filteredPlugins.length === 0 && total > 0 && (
            <div className="page-datasources-empty page-datasources-empty-inline">
              <SearchOutlined className="page-datasources-empty-glyph" />
              <h2>No matches</h2>
              <p>Try a different search term or filter.</p>
              <Button onClick={() => { setSearchText(''); setFilterMode('all'); }}>Clear filters</Button>
            </div>
          )}

          {total === 0 && !isLoading && (
            <div className="page-datasources-empty">
              <DatabaseOutlined className="page-datasources-empty-glyph" />
              <h2>No data sources yet</h2>
              <p>Connect Drill to a file system, JDBC database, REST API, or any other data source.</p>
            </div>
          )}
        </div>
      )}

      {/* Create Plugin Modal */}
      <Modal
        title="New Data Source"
        open={createModalOpen}
        onOk={handleCreate}
        onCancel={() => {
          setCreateModalOpen(false);
          createForm.resetFields();
        }}
        okText="Configure"
      >
        <Form form={createForm} layout="vertical">
          <Form.Item
            name="name"
            label="Plugin Name"
            rules={[
              { required: true, message: 'Please enter a plugin name' },
              { pattern: /^[a-zA-Z0-9_-]+$/, message: 'Only letters, numbers, hyphens, and underscores' },
            ]}
          >
            <Input placeholder="my_plugin" />
          </Form.Item>
          <Form.Item name="description" label="Description (optional)">
            <Input.TextArea
              placeholder="Brief description of this data source"
              autoSize={{ minRows: 1, maxRows: 3 }}
            />
          </Form.Item>
          <Form.Item
            name="type"
            label="Plugin Type"
            rules={[{ required: true, message: 'Please select a type' }]}
          >
            <Select
              placeholder="Select plugin type"
              options={[
                ...knownPluginTypes,
                { value: '__other', label: 'Other (JSON editor)' },
              ]}
            />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}

interface NewTileProps {
  onClick: () => void;
}

function NewDataSourceTile({ onClick }: NewTileProps) {
  return (
    <button type="button" className="datasource-tile datasource-tile-new" onClick={onClick}>
      <div className="datasource-tile-new-glyph">
        <PlusOutlined />
      </div>
      <div className="datasource-tile-new-label">New Data Source</div>
      <div className="datasource-tile-new-sublabel">Connect a database, file system, or API</div>
    </button>
  );
}
