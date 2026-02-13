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
import { useState, useEffect, useCallback, useRef } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import {
  Card,
  Button,
  Input,
  Space,
  Tabs,
  Tag,
  Popconfirm,
  message,
  Typography,
  Spin,
  Empty,
  Badge,
  Tooltip,
} from 'antd';
import {
  ArrowLeftOutlined,
  SaveOutlined,
  DeleteOutlined,
  PoweroffOutlined,
  DownloadOutlined,
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
  getPluginLogoUrl,
} from '../components/datasource';
import type { PluginType } from '../types';

const { Title, Text } = Typography;

const GUIDED_FORM_TYPES: PluginType[] = ['file', 'jdbc', 'http', 'mongo'];

export default function DataSourceEditPage() {
  const { name } = useParams<{ name: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const queryClient = useQueryClient();

  // State from navigation (for new plugin creation)
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

  // Fetch existing plugin config (skip for new plugins)
  const { data: pluginData, isLoading, error } = useQuery({
    queryKey: ['storage-plugin', pluginName],
    queryFn: () => getPlugin(pluginName),
    enabled: !!pluginName && !isNew,
  });

  // Initialize config from fetched data
  useEffect(() => {
    if (pluginData && !isNew) {
      setConfig(pluginData.config);
      setJsonText(JSON.stringify(pluginData.config, null, 2));
    }
  }, [pluginData, isNew]);

  // Initialize JSON text from nav state config
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
      // Clear the nav state so refresh fetches from server
      if (isNew) {
        window.history.replaceState({}, '');
      }
    },
    onError: (err: Error) => {
      message.error(`Failed to save plugin: ${err.message}`);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: () => deletePlugin(pluginName),
    onSuccess: () => {
      message.success('Plugin deleted');
      queryClient.invalidateQueries({ queryKey: ['storage-plugins'] });
      // Remove dataset references for this plugin from all projects
      cleanupPluginDatasets(pluginName).then(() => {
        queryClient.invalidateQueries({ queryKey: ['projects'] });
      }).catch(() => {
        // Best-effort cleanup — ignore failures
      });
      navigate('/datasources');
    },
    onError: (err: Error) => {
      message.error(`Failed to delete plugin: ${err.message}`);
    },
  });

  const enableMutation = useMutation({
    mutationFn: (enable: boolean) => enablePlugin(pluginName, enable),
    onSuccess: (_, enable) => {
      message.success(enable ? 'Plugin enabled' : 'Plugin disabled');
      // Update local config state
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
    onError: (err: Error) => {
      message.error(`Failed to toggle plugin: ${err.message}`);
    },
  });

  const handleFormChange = useCallback(
    (newConfig: Record<string, unknown>) => {
      setConfig(newConfig);
      setJsonText(JSON.stringify(newConfig, null, 2));
      setDirty(true);
    },
    []
  );

  const handleJsonChange = useCallback(
    (value: string | undefined) => {
      const val = value || '{}';
      setJsonText(val);
      try {
        const parsed = JSON.parse(val);
        setConfig(parsed);
        setDirty(true);
      } catch {
        // Don't update config on invalid JSON — user is still typing
        setDirty(true);
      }
    },
    []
  );

  const handleTabChange = (key: string) => {
    if (key === 'json') {
      // Sync JSON from current config
      setJsonText(JSON.stringify(config, null, 2));
    }
    setActiveTab(key);
  };

  const handleSave = () => {
    // If we're on JSON tab, parse the JSON first
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
  };

  const logoUrl = getPluginLogoUrl(pluginType, config.connection as string);

  if (isLoading) {
    return (
      <div style={{ padding: 24, textAlign: 'center' }}>
        <Spin size="large" />
      </div>
    );
  }

  if (error && !isNew) {
    return (
      <div style={{ padding: 24 }}>
        <Card>
          <Empty
            description={
              <Text type="danger">
                Failed to load plugin: {(error as Error).message}
              </Text>
            }
          >
            <Button onClick={() => navigate('/datasources')}>Back to Data Sources</Button>
          </Empty>
        </Card>
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
        <div style={{ padding: '16px 0' }}>
          {renderForm()}
        </div>
      ),
    });
  }
  tabItems.push({
    key: 'json',
    label: 'JSON',
    children: (
      <div style={{ border: '1px solid var(--color-border)', borderRadius: 4 }}>
        <Editor
          height="500px"
          language="json"
          value={jsonText}
          onChange={handleJsonChange}
          options={{
            minimap: { enabled: false },
            lineNumbers: 'on',
            scrollBeyondLastLine: false,
            fontSize: 13,
            formatOnPaste: true,
          }}
        />
      </div>
    ),
  });

  return (
    <div style={{ padding: 24, overflow: 'auto', flex: 1 }}>
      <Card>
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          {/* Header */}
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <Space align="center" size="middle">
              <Button
                icon={<ArrowLeftOutlined />}
                onClick={() => navigate('/datasources')}
              />
              {logoUrl && (
                <img
                  src={logoUrl}
                  alt={pluginType}
                  style={{ height: 32, objectFit: 'contain' }}
                />
              )}
              <div>
                <Title level={3} style={{ margin: 0 }}>
                  {pluginName}
                  {isNew && (
                    <Tag color="blue" style={{ marginLeft: 8, verticalAlign: 'middle' }}>
                      New
                    </Tag>
                  )}
                </Title>
                <Space size={4} style={{ marginTop: 4 }}>
                  <Tag>{pluginType || 'unknown'}</Tag>
                  <Badge
                    status={enabled ? 'success' : 'default'}
                    text={enabled ? 'Enabled' : 'Disabled'}
                  />
                </Space>
              </div>
            </Space>
            <Space>
              <Tooltip title={!isValid ? 'Cannot save: at least one workspace is required' : !dirty ? 'No changes to save' : ''}>
                <Button
                  type="primary"
                  icon={<SaveOutlined />}
                  onClick={handleSave}
                  loading={saveMutation.isPending}
                  disabled={!dirty || !isValid}
                >
                  Save
                </Button>
              </Tooltip>
              {!isNew && (
                <>
                  <Button
                    icon={<PoweroffOutlined />}
                    onClick={() => enableMutation.mutate(!enabled)}
                    loading={enableMutation.isPending}
                  >
                    {enabled ? 'Disable' : 'Enable'}
                  </Button>
                  <Button
                    icon={<DownloadOutlined />}
                    href={getExportUrl(pluginName)}
                    target="_blank"
                  >
                    Export
                  </Button>
                  <Popconfirm
                    title="Delete this plugin?"
                    description="This action cannot be undone."
                    onConfirm={() => deleteMutation.mutate()}
                    okText="Delete"
                    cancelText="Cancel"
                    okButtonProps={{ danger: true }}
                  >
                    <Button
                      danger
                      icon={<DeleteOutlined />}
                      loading={deleteMutation.isPending}
                    >
                      Delete
                    </Button>
                  </Popconfirm>
                </>
              )}
            </Space>
          </div>

          {/* Description */}
          <Input.TextArea
            placeholder="Add a description for this data source (optional)"
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
            style={{ maxWidth: 600 }}
          />

          {/* Tabs */}
          <Tabs
            activeKey={hasGuidedForm ? activeTab : 'json'}
            onChange={handleTabChange}
            items={tabItems}
          />
        </Space>
      </Card>
    </div>
  );
}
