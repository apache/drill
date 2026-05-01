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
import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import {
  Button,
  Input,
  Space,
  Tabs,
  Tag,
  Modal,
  message,
  Spin,
  Tooltip,
  Dropdown,
} from 'antd';
import type { MenuProps } from 'antd';
import {
  SaveOutlined,
  DeleteOutlined,
  PoweroffOutlined,
  DownloadOutlined,
  KeyOutlined,
  MoreOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import Editor from '@monaco-editor/react';
import { getPlugin, savePlugin, deletePlugin, enablePlugin, getExportUrl } from '../api/storage';
import { cleanupPluginDatasets } from '../api/projects';
import {
  FileSystemForm,
  JdbcForm,
  HttpForm,
  MongoForm,
  SplunkForm,
  CassandraForm,
  DruidForm,
  ElasticsearchForm,
  GoogleSheetsForm,
  HBaseForm,
  HiveForm,
  KafkaForm,
  KuduForm,
  OpenTSDBForm,
  PhoenixForm,
  getPluginLogoUrl,
} from '../components/datasource';
import { usePageChrome, type BreadcrumbSegment } from '../contexts/AppChromeContext';
import { useTheme } from '../hooks/useTheme';
import type { PluginType } from '../types';

const GUIDED_FORM_TYPES: PluginType[] = [
  'file', 'jdbc', 'http', 'mongo', 'splunk', 'cassandra', 'druid', 'elastic',
  'googlesheets', 'hbase', 'hive', 'kafka', 'kudu', 'openTSDB', 'phoenix',
];

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

export default function DataSourceEditPage() {
  const { name } = useParams<{ name: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const queryClient = useQueryClient();
  const { isDark } = useTheme();

  const navState = location.state as {
    config?: Record<string, unknown>;
    isNew?: boolean;
  } | null;

  const isNew = navState?.isNew || false;
  const [config, setConfig] = useState<Record<string, unknown>>(navState?.config || {});
  const [jsonText, setJsonText] = useState<string>('{}');
  const [activeTab, setActiveTab] = useState<string>('config');
  const [dirty, setDirty] = useState(isNew);
  const [isValid, setIsValid] = useState(true);
  const configRef = useRef(config);
  configRef.current = config;

  const pluginName = name ? decodeURIComponent(name) : '';

  const { data: pluginData, isLoading, error } = useQuery({
    queryKey: ['storage-plugin', pluginName],
    queryFn: () => getPlugin(pluginName),
    enabled: !!pluginName && !isNew,
  });

  useEffect(() => {
    if (pluginData && !isNew) {
      setConfig(pluginData.config);
      setJsonText(JSON.stringify(pluginData.config, null, 2));
    }
  }, [pluginData, isNew]);

  useEffect(() => {
    if (isNew && navState?.config) {
      setJsonText(JSON.stringify(navState.config, null, 2));
    }
  }, [isNew, navState?.config]);

  const pluginType = (config.type as string) || '';
  const enabled = (config.enabled as boolean) || false;
  const description = (config.description as string) || '';
  const hasGuidedForm = GUIDED_FORM_TYPES.includes(pluginType as PluginType);

  const saveMutation = useMutation({
    mutationFn: () => savePlugin(pluginName, configRef.current),
    onSuccess: () => {
      message.success('Plugin saved');
      setDirty(false);
      queryClient.invalidateQueries({ queryKey: ['storage-plugins'] });
      queryClient.invalidateQueries({ queryKey: ['storage-plugin', pluginName] });
      if (isNew) {
        window.history.replaceState({}, '');
      }
    },
    onError: (err: Error) => message.error(`Failed to save plugin: ${err.message}`),
  });

  const deleteMutation = useMutation({
    mutationFn: () => deletePlugin(pluginName),
    onSuccess: () => {
      message.success('Plugin deleted');
      queryClient.invalidateQueries({ queryKey: ['storage-plugins'] });
      cleanupPluginDatasets(pluginName).then(() => {
        queryClient.invalidateQueries({ queryKey: ['projects'] });
      }).catch(() => {
        // Best-effort cleanup
      });
      navigate('/datasources');
    },
    onError: (err: Error) => message.error(`Failed to delete plugin: ${err.message}`),
  });

  const enableMutation = useMutation({
    mutationFn: (enable: boolean) => enablePlugin(pluginName, enable),
    onSuccess: (_, enable) => {
      message.success(enable ? 'Plugin enabled' : 'Plugin disabled');
      setConfig((prev) => ({ ...prev, enabled: enable }));
      setJsonText((prev) => {
        try {
          const obj = JSON.parse(prev);
          obj.enabled = enable;
          return JSON.stringify(obj, null, 2);
        } catch {
          return prev;
        }
      });
      queryClient.invalidateQueries({ queryKey: ['storage-plugins'] });
      queryClient.invalidateQueries({ queryKey: ['storage-plugin', pluginName] });
    },
    onError: (err: Error) => message.error(`Failed to toggle plugin: ${err.message}`),
  });

  const oAuthConfig = config.oAuthConfig as Record<string, unknown> | undefined;
  const credProvider = config.credentialsProvider as Record<string, unknown> | undefined;
  const credentials = credProvider?.credentials as Record<string, unknown> | undefined;
  const isOAuth = !isNew && !!oAuthConfig?.authorizationURL && !!credentials?.clientID;

  const handleAuthorize = useCallback(() => {
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
        queryClient.invalidateQueries({ queryKey: ['storage-plugin', pluginName] });
      }
    }, 1000);
  }, [config, pluginName, queryClient]);

  const handleFormChange = useCallback((newConfig: Record<string, unknown>) => {
    setConfig(newConfig);
    setJsonText(JSON.stringify(newConfig, null, 2));
    setDirty(true);
  }, []);

  const handleJsonChange = useCallback((value: string | undefined) => {
    const val = value || '{}';
    setJsonText(val);
    try {
      const parsed = JSON.parse(val);
      setConfig(parsed);
      setDirty(true);
    } catch {
      setDirty(true);
    }
  }, []);

  const handleTabChange = (key: string) => {
    if (key === 'json') {
      setJsonText(JSON.stringify(config, null, 2));
    }
    setActiveTab(key);
  };

  const handleSave = useCallback(() => {
    if (activeTab === 'json') {
      try {
        const parsed = JSON.parse(jsonText);
        setConfig(parsed);
        configRef.current = parsed;
      } catch {
        message.error('Invalid JSON — please fix before saving');
        return;
      }
    }
    saveMutation.mutate();
  }, [activeTab, jsonText, saveMutation]);

  const handleToggleEnable = useCallback(() => {
    if (enabled) {
      Modal.confirm({
        title: `Disable "${pluginName}"?`,
        content: 'Queries using this plugin will fail while it is disabled.',
        okText: 'Disable',
        okButtonProps: { danger: true },
        onOk: () => enableMutation.mutate(false),
      });
    } else {
      enableMutation.mutate(true);
    }
  }, [enabled, pluginName, enableMutation]);

  const handleDelete = useCallback(() => {
    Modal.confirm({
      title: 'Delete this plugin?',
      content: 'This action cannot be undone.',
      okText: 'Delete',
      okButtonProps: { danger: true },
      onOk: () => deleteMutation.mutate(),
    });
  }, [deleteMutation]);

  const logoUrl = getPluginLogoUrl(pluginType, config.connection as string);

  // Breadcrumb in unified shell
  const breadcrumb = useMemo<BreadcrumbSegment[]>(
    () => [
      { key: 'datasources', label: 'Data Sources', to: '/datasources' },
      { key: 'plugin', label: pluginName || 'Plugin' },
    ],
    [pluginName],
  );

  // Toolbar actions
  const toolbarActions = useMemo(() => {
    const moreItems: MenuProps['items'] = !isNew ? [
      ...(isOAuth ? [{
        key: 'authorize',
        icon: <KeyOutlined />,
        label: 'Authorize…',
        onClick: handleAuthorize,
      }] : []),
      {
        key: 'export',
        icon: <DownloadOutlined />,
        label: 'Export config',
        onClick: () => {
          window.open(getExportUrl(pluginName), '_blank');
        },
      },
      { type: 'divider' as const },
      {
        key: 'delete',
        icon: <DeleteOutlined />,
        label: 'Delete plugin',
        danger: true,
        onClick: handleDelete,
      },
    ] : [];

    return (
      <Space size={4}>
        {!isNew && (
          <Tooltip title={enabled ? 'Disable plugin' : 'Enable plugin'}>
            <Button
              type="text"
              size="small"
              icon={<PoweroffOutlined style={{ color: enabled ? 'var(--color-success)' : undefined }} />}
              onClick={handleToggleEnable}
              loading={enableMutation.isPending}
            />
          </Tooltip>
        )}
        {isOAuth && (
          <Tooltip title="Authorize OAuth">
            <Button
              type="text"
              size="small"
              icon={<KeyOutlined />}
              onClick={handleAuthorize}
            />
          </Tooltip>
        )}
        <Tooltip title={!isValid ? 'Cannot save: at least one workspace is required' : !dirty ? 'No changes to save' : ''}>
          <Button
            type="primary"
            size="small"
            icon={<SaveOutlined />}
            onClick={handleSave}
            loading={saveMutation.isPending}
            disabled={!dirty || !isValid}
          >
            Save
          </Button>
        </Tooltip>
        {moreItems.length > 0 && (
          <Dropdown menu={{ items: moreItems }} placement="bottomRight" trigger={['click']}>
            <Button type="text" size="small" icon={<MoreOutlined />} />
          </Dropdown>
        )}
      </Space>
    );
  }, [
    isNew, isOAuth, enabled, isValid, dirty,
    pluginName, handleAuthorize, handleToggleEnable, handleSave, handleDelete,
    enableMutation.isPending, saveMutation.isPending,
  ]);

  usePageChrome({ breadcrumb, toolbarActions });

  if (isLoading) {
    return (
      <div className="page-datasource-edit-loading">
        <Spin size="large" />
      </div>
    );
  }

  if (error && !isNew) {
    return (
      <div className="page-datasource-edit-error">
        <h2>Couldn't load plugin</h2>
        <p>{(error as Error).message}</p>
        <Button onClick={() => navigate('/datasources')}>Back to Data Sources</Button>
      </div>
    );
  }

  const renderForm = () => {
    switch (pluginType) {
      case 'file':
        return <FileSystemForm config={config} onChange={handleFormChange} onValidationChange={setIsValid} pluginName={pluginName} />;
      case 'jdbc':
        return <JdbcForm config={config} onChange={handleFormChange} />;
      case 'http':
        return <HttpForm config={config} onChange={handleFormChange} />;
      case 'mongo':
        return <MongoForm config={config} onChange={handleFormChange} />;
      case 'splunk':
        return <SplunkForm config={config} onChange={handleFormChange} />;
      case 'cassandra':
        return <CassandraForm config={config} onChange={handleFormChange} />;
      case 'druid':
        return <DruidForm config={config} onChange={handleFormChange} />;
      case 'elastic':
        return <ElasticsearchForm config={config} onChange={handleFormChange} />;
      case 'googlesheets':
        return <GoogleSheetsForm config={config} onChange={handleFormChange} />;
      case 'hbase':
        return <HBaseForm config={config} onChange={handleFormChange} />;
      case 'hive':
        return <HiveForm config={config} onChange={handleFormChange} />;
      case 'kafka':
        return <KafkaForm config={config} onChange={handleFormChange} />;
      case 'kudu':
        return <KuduForm config={config} onChange={handleFormChange} />;
      case 'openTSDB':
        return <OpenTSDBForm config={config} onChange={handleFormChange} />;
      case 'phoenix':
        return <PhoenixForm config={config} onChange={handleFormChange} />;
      default:
        return null;
    }
  };

  const tabItems = [];
  if (hasGuidedForm) {
    tabItems.push({
      key: 'config',
      label: 'Configuration',
      children: (
        <div className="datasource-edit-form">
          {renderForm()}
        </div>
      ),
    });
  }
  tabItems.push({
    key: 'json',
    label: 'JSON',
    children: (
      <div className="datasource-edit-json">
        <Editor
          height="500px"
          language="json"
          theme={isDark ? 'vs-dark' : 'light'}
          value={jsonText}
          onChange={handleJsonChange}
          options={{
            minimap: { enabled: false },
            lineNumbers: 'on',
            scrollBeyondLastLine: false,
            fontSize: 13,
            fontFamily: "'SF Mono', SFMono-Regular, Consolas, monospace",
            formatOnPaste: true,
            renderLineHighlight: 'line',
            padding: { top: 12, bottom: 12 },
          }}
        />
      </div>
    ),
  });

  return (
    <div className="page-datasource-edit">
      <header className="datasource-edit-header">
        {logoUrl ? (
          <div className="datasource-edit-logo">
            <img src={logoUrl} alt={pluginType} onError={(e) => {
              (e.target as HTMLImageElement).style.display = 'none';
            }} />
          </div>
        ) : (
          <div className="datasource-edit-logo datasource-edit-logo-fallback">
            <SettingOutlined />
          </div>
        )}
        <div className="datasource-edit-title-block">
          <div className="datasource-edit-title-row">
            <h1 className="datasource-edit-title">{pluginName}</h1>
            {isNew && <Tag bordered={false} color="processing">New</Tag>}
            {!isNew && (
              <span className={`datasource-edit-status${enabled ? ' is-on' : ''}`}>
                <span className="datasource-edit-status-dot" />
                {enabled ? 'Enabled' : 'Disabled'}
              </span>
            )}
          </div>
          <div className="datasource-edit-subtitle">
            <span className="datasource-edit-type">{pluginType || 'unknown'}</span>
            {dirty && <span className="datasource-edit-dirty">· Unsaved changes</span>}
          </div>
        </div>
      </header>

      <div className="datasource-edit-description">
        <Input.TextArea
          placeholder="Describe what this data source connects to (optional)"
          value={description}
          onChange={(e) => {
            const val = e.target.value;
            const newConfig = { ...config, description: val || undefined };
            if (!val) {
              delete newConfig.description;
            }
            setConfig(newConfig);
            setJsonText(JSON.stringify(newConfig, null, 2));
            setDirty(true);
          }}
          autoSize={{ minRows: 1, maxRows: 3 }}
        />
      </div>

      <div className="datasource-edit-tabs">
        <Tabs
          activeKey={hasGuidedForm ? activeTab : 'json'}
          onChange={handleTabChange}
          items={tabItems}
        />
      </div>
    </div>
  );
}
