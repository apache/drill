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
import { Modal, Switch, Tabs, Button, Input, Space, Typography, Divider, Empty, Spin, message } from 'antd';
import { ApiOutlined, CopyOutlined } from '@ant-design/icons';
import { getSharedQueryApi, createSharedQueryApi, updateSharedQueryApi } from '../../api/sharedQueries';
import type { SharedQueryApi } from '../../types';

const { Text } = Typography;

interface ShareApiModalProps {
  open: boolean;
  onClose: () => void;
  sql: string;
  defaultSchema?: string;
  sharedQueryApiId?: string;
  onSharedQueryApiIdChange: (id: string | undefined) => void;
}

function generatePythonSnippet(apiUrl: string): string {
  return `import requests
import pandas as pd

url = "${apiUrl}"
response = requests.get(url)
response.raise_for_status()

data = response.json()
df = pd.DataFrame(data["rows"], columns=data["columns"])
print(f"Retrieved {len(df)} rows, {len(df.columns)} columns")
print(df.head())`;
}

function generateRSnippet(apiUrl: string): string {
  return `library(httr)
library(jsonlite)

url <- "${apiUrl}"
response <- GET(url)
stop_for_status(response)

data <- fromJSON(content(response, as = "text", encoding = "UTF-8"))
df <- as.data.frame(data$rows)
cat(sprintf("Retrieved %d rows, %d columns\\n", nrow(df), ncol(df)))
head(df)`;
}

function generateTypeScriptSnippet(apiUrl: string): string {
  return `interface DrillApiResponse {
  columns: string[];
  metadata: string[];
  rows: Record<string, unknown>[];
  queryId: string;
}

const response = await fetch("${apiUrl}");
if (!response.ok) {
  throw new Error(\`HTTP \${response.status}: \${response.statusText}\`);
}
const data: DrillApiResponse = await response.json();
console.log(\`Retrieved \${data.rows.length} rows, \${data.columns.length} columns\`);
console.table(data.rows.slice(0, 5));`;
}

function generateJavaScriptSnippet(apiUrl: string): string {
  return `fetch("${apiUrl}")
  .then((response) => {
    if (!response.ok) {
      throw new Error(\`HTTP \${response.status}: \${response.statusText}\`);
    }
    return response.json();
  })
  .then((data) => {
    console.log(\`Retrieved \${data.rows.length} rows, \${data.columns.length} columns\`);
    console.table(data.rows.slice(0, 5));
  })
  .catch((error) => console.error("Failed to fetch:", error));`;
}

function copyToClipboard(text: string) {
  navigator.clipboard.writeText(text).then(
    () => message.success('Copied to clipboard'),
    () => message.error('Failed to copy')
  );
}

export default function ShareApiModal({
  open,
  onClose,
  sql,
  defaultSchema,
  sharedQueryApiId,
  onSharedQueryApiIdChange,
}: ShareApiModalProps) {
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [apiEnabled, setApiEnabled] = useState(false);
  const [sharedQuery, setSharedQuery] = useState<SharedQueryApi | null>(null);
  const [activeTab, setActiveTab] = useState('python');

  // Load existing shared query when modal opens
  useEffect(() => {
    if (!open) {
      return;
    }

    if (sharedQueryApiId) {
      setLoading(true);
      getSharedQueryApi(sharedQueryApiId)
        .then((query) => {
          setSharedQuery(query);
          setApiEnabled(query.apiEnabled);
        })
        .catch(() => {
          setSharedQuery(null);
          setApiEnabled(false);
          onSharedQueryApiIdChange(undefined);
        })
        .finally(() => setLoading(false));
    } else {
      setSharedQuery(null);
      setApiEnabled(false);
    }
  }, [open, sharedQueryApiId, onSharedQueryApiIdChange]);

  const handleToggle = useCallback(async (enabled: boolean) => {
    setSaving(true);
    try {
      if (enabled && !sharedQuery) {
        // Create new shared query API
        const created = await createSharedQueryApi({
          name: `Shared API - ${new Date().toLocaleString()}`,
          sql,
          defaultSchema,
          apiEnabled: true,
        });
        setSharedQuery(created);
        setApiEnabled(true);
        onSharedQueryApiIdChange(created.id);
      } else if (enabled && sharedQuery) {
        // Enable existing
        const updated = await updateSharedQueryApi(sharedQuery.id, { apiEnabled: true });
        setSharedQuery(updated);
        setApiEnabled(true);
      } else if (!enabled && sharedQuery) {
        // Disable existing
        const updated = await updateSharedQueryApi(sharedQuery.id, { apiEnabled: false });
        setSharedQuery(updated);
        setApiEnabled(false);
      }
    } catch {
      message.error('Failed to update API access');
      // Revert toggle
      setApiEnabled(!enabled);
    } finally {
      setSaving(false);
    }
  }, [sharedQuery, sql, defaultSchema, onSharedQueryApiIdChange]);

  const apiUrl = sharedQuery
    ? `${window.location.origin}/api/v1/shared-queries/${sharedQuery.id}/data`
    : '';

  const tabItems = [
    { key: 'python', label: 'Python', snippet: generatePythonSnippet(apiUrl) },
    { key: 'r', label: 'R', snippet: generateRSnippet(apiUrl) },
    { key: 'typescript', label: 'TypeScript', snippet: generateTypeScriptSnippet(apiUrl) },
    { key: 'javascript', label: 'JavaScript', snippet: generateJavaScriptSnippet(apiUrl) },
  ];

  return (
    <Modal
      title="Share Query Results as API"
      open={open}
      onCancel={onClose}
      width={640}
      destroyOnClose
      footer={
        <Button onClick={onClose}>Close</Button>
      }
    >
      {loading ? (
        <div style={{ textAlign: 'center', padding: 40 }}>
          <Spin />
        </div>
      ) : (
        <>
          {/* Toggle row */}
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Space>
              <ApiOutlined style={{ fontSize: 18 }} />
              <div>
                <div><Text strong>Enable API Access</Text></div>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {apiEnabled
                    ? 'External applications can fetch live results'
                    : 'Toggle on to create a public API endpoint'}
                </Text>
              </div>
            </Space>
            <Switch
              checked={apiEnabled}
              onChange={handleToggle}
              loading={saving}
            />
          </div>

          <Divider />

          {apiEnabled && sharedQuery ? (
            <>
              {/* API URL */}
              <div style={{ marginBottom: 16 }}>
                <Text type="secondary" style={{ fontSize: 12, marginBottom: 4, display: 'block' }}>
                  API Endpoint URL
                </Text>
                <Input.Search
                  readOnly
                  value={apiUrl}
                  enterButton={<CopyOutlined />}
                  onSearch={() => copyToClipboard(apiUrl)}
                />
              </div>

              {/* Language tabs with code snippets */}
              <Tabs
                activeKey={activeTab}
                onChange={setActiveTab}
                items={tabItems.map((item) => ({
                  key: item.key,
                  label: (
                    <Space size={4}>
                      {item.label}
                      <ApiOutlined style={{ color: '#52c41a', fontSize: 12 }} />
                    </Space>
                  ),
                  children: (
                    <div style={{ position: 'relative' }}>
                      <Button
                        size="small"
                        icon={<CopyOutlined />}
                        onClick={() => copyToClipboard(item.snippet)}
                        style={{ position: 'absolute', top: 8, right: 8, zIndex: 1 }}
                      >
                        Copy
                      </Button>
                      <pre
                        style={{
                          backgroundColor: '#f5f5f5',
                          padding: 16,
                          borderRadius: 6,
                          fontSize: 12,
                          lineHeight: 1.5,
                          overflowX: 'auto',
                          maxHeight: 300,
                        }}
                      >
                        {item.snippet}
                      </pre>
                    </div>
                  ),
                }))}
              />
            </>
          ) : (
            <Empty description="Enable API access to generate code snippets" />
          )}
        </>
      )}
    </Modal>
  );
}
