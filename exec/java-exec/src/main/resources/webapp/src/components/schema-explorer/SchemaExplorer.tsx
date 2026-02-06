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
import { useState, useCallback, useMemo } from 'react';
import { Tree, Input, Spin, Empty, Tooltip, Typography, Tag } from 'antd';
import {
  DatabaseOutlined,
  TableOutlined,
  FieldStringOutlined,
  FieldNumberOutlined,
  FieldTimeOutlined,
  FieldBinaryOutlined,
  ReloadOutlined,
  SearchOutlined,
  CloudOutlined,
  ApiOutlined,
  WarningOutlined,
  FolderOutlined,
  ConsoleSqlOutlined,
  ClusterOutlined,
  FileOutlined,
  GoogleOutlined,
  AmazonOutlined,
} from '@ant-design/icons';
import type { DataNode, EventDataNode } from 'antd/es/tree';
import { useQuery } from '@tanstack/react-query';
import { getPlugins, getPluginSchemas, getTables, getColumns, getFiles, getFileColumns } from '../../api/metadata';
import type { SchemaInfo, TableInfo, ColumnInfo, FileInfo } from '../../types';

const { Search } = Input;
const { Text } = Typography;

interface SchemaExplorerProps {
  onInsertText?: (text: string) => void;
  onTableSelect?: (schema: string, table: string) => void;
}

// Map data types to icons
function getColumnIcon(dataType: string) {
  const type = dataType?.toUpperCase() || '';
  if (type.includes('INT') || type.includes('FLOAT') || type.includes('DOUBLE') || type.includes('DECIMAL') || type.includes('NUMERIC')) {
    return <FieldNumberOutlined style={{ color: '#52c41a' }} />;
  }
  if (type.includes('DATE') || type.includes('TIME') || type.includes('TIMESTAMP')) {
    return <FieldTimeOutlined style={{ color: '#722ed1' }} />;
  }
  if (type.includes('BINARY') || type.includes('BLOB') || type.includes('BYTES')) {
    return <FieldBinaryOutlined style={{ color: '#eb2f96' }} />;
  }
  return <FieldStringOutlined style={{ color: '#1890ff' }} />;
}

// Get icon for plugin type based on plugin type and name
function getPluginIcon(pluginType: string, pluginName: string) {
  const type = pluginType?.toLowerCase() || '';
  const name = pluginName?.toLowerCase() || '';

  // HTTP/REST API plugins
  if (type.includes('http') || name === 'http' || name.includes('api')) {
    return <ApiOutlined style={{ color: '#fa8c16' }} />;
  }

  // File system plugins (dfs, local)
  if (type.includes('file') || name === 'dfs' || name === 'local' || name.includes('tmp')) {
    return <FolderOutlined style={{ color: '#1890ff' }} />;
  }

  // Cloud storage - S3
  if (type.includes('s3') || name.includes('s3') || name.includes('minio')) {
    return <AmazonOutlined style={{ color: '#ff9900' }} />;
  }

  // Cloud storage - GCS (Google Cloud Storage)
  if (type.includes('gcs') || type.includes('google') || name.includes('gcs') || name.includes('google')) {
    return <GoogleOutlined style={{ color: '#4285f4' }} />;
  }

  // Cloud storage - Azure
  if (type.includes('azure') || name.includes('azure')) {
    return <CloudOutlined style={{ color: '#0078d4' }} />;
  }

  // Generic cloud storage
  if (type.includes('cloud')) {
    return <CloudOutlined style={{ color: '#13c2c2' }} />;
  }

  // JDBC/Database plugins
  if (type.includes('jdbc') || type.includes('rdbms')) {
    return <ConsoleSqlOutlined style={{ color: '#722ed1' }} />;
  }

  // Kafka/streaming plugins
  if (type.includes('kafka') || name.includes('kafka')) {
    return <ClusterOutlined style={{ color: '#231f20' }} />;
  }

  // MongoDB
  if (type.includes('mongo') || name.includes('mongo')) {
    return <DatabaseOutlined style={{ color: '#47a248' }} />;
  }

  // Elasticsearch
  if (type.includes('elastic') || name.includes('elastic')) {
    return <DatabaseOutlined style={{ color: '#f9b110' }} />;
  }

  // Hive/HBase
  if (type.includes('hive') || type.includes('hbase') || name.includes('hive') || name.includes('hbase')) {
    return <ClusterOutlined style={{ color: '#f09800' }} />;
  }

  // Parquet/Iceberg/Delta files
  if (type.includes('parquet') || type.includes('iceberg') || type.includes('delta')) {
    return <FileOutlined style={{ color: '#1890ff' }} />;
  }

  // Splunk
  if (type.includes('splunk') || name.includes('splunk')) {
    return <DatabaseOutlined style={{ color: '#65a637' }} />;
  }

  // Default database icon
  return <DatabaseOutlined style={{ color: '#595959' }} />;
}

// Check if a plugin is file-based (can browse files/folders)
function isFileBasedPlugin(pluginType: string, pluginName: string): boolean {
  const type = pluginType?.toLowerCase() || '';
  const name = pluginName?.toLowerCase() || '';
  return type.includes('file') || name === 'dfs' || name === 'local' || name.includes('tmp') ||
         type.includes('s3') || name.includes('s3') || name.includes('minio') ||
         type.includes('gcs') || type.includes('google') || name.includes('gcs') ||
         type.includes('azure') || name.includes('azure') ||
         type.includes('hdfs') || name.includes('hdfs');
}

export default function SchemaExplorer({ onInsertText, onTableSelect }: SchemaExplorerProps) {
  const [searchText, setSearchText] = useState('');
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);
  const [loadedKeys, setLoadedKeys] = useState<string[]>([]);
  const [schemasCache, setSchemasCache] = useState<Record<string, SchemaInfo[]>>({});
  const [tablesCache, setTablesCache] = useState<Record<string, TableInfo[]>>({});
  const [columnsCache, setColumnsCache] = useState<Record<string, ColumnInfo[]>>({});
  const [filesCache, setFilesCache] = useState<Record<string, FileInfo[]>>({});
  const [pluginTypesCache, setPluginTypesCache] = useState<Record<string, string>>({});

  // Fetch plugins at root level
  const { data: plugins, isLoading: pluginsLoading, refetch: refetchPlugins } = useQuery({
    queryKey: ['plugins'],
    queryFn: async () => {
      const pluginList = await getPlugins();
      // Cache plugin types for later use
      const types: Record<string, string> = {};
      pluginList.forEach((p) => {
        types[p.name] = p.type;
      });
      setPluginTypesCache(types);
      return pluginList;
    },
  });

  // Helper to build file/folder tree nodes recursively
  const buildFileNodes = useCallback((schema: string, files: FileInfo[], parentPath: string = ''): DataNode[] => {
    return files.map((file) => {
      const filePath = parentPath ? `${parentPath}/${file.name}` : file.name;
      const fileKey = `file:${schema}:${filePath}`;

      if (file.isDirectory) {
        // It's a folder - can be expanded to show more files
        const subFiles = filesCache[fileKey] || [];
        const subNodes = subFiles.length > 0 ? buildFileNodes(schema, subFiles, filePath) : undefined;

        return {
          key: fileKey,
          title: file.name,
          icon: <FolderOutlined style={{ color: '#1890ff' }} />,
          children: subNodes,
          isLeaf: false,
        };
      } else {
        // It's a file - can be expanded to show columns
        const columns = columnsCache[fileKey] || [];
        const columnNodes: DataNode[] = columns.map((col) => ({
          key: `column:${schema}:${filePath}:${col.name}`,
          title: (
            <Tooltip title={`${col.type}${col.nullable ? ' (nullable)' : ''}`}>
              <span>
                {col.name} <Text type="secondary" style={{ fontSize: 11 }}>{col.type}</Text>
              </span>
            </Tooltip>
          ),
          icon: getColumnIcon(col.type),
          isLeaf: true,
        }));

        return {
          key: fileKey,
          title: (
            <Tooltip title={`${file.length} bytes`}>
              <span>{file.name}</span>
            </Tooltip>
          ),
          icon: <FileOutlined style={{ color: '#52c41a' }} />,
          children: columnNodes.length > 0 ? columnNodes : undefined,
          isLeaf: false,
        };
      }
    });
  }, [filesCache, columnsCache]);

  // Build tree data from plugins, schemas, tables/files, and columns
  const treeData = useMemo(() => {
    if (!plugins) {
      return [];
    }

    const filteredPlugins = searchText
      ? plugins.filter((p) =>
          p.name.toLowerCase().includes(searchText.toLowerCase()) ||
          Object.keys(schemasCache[p.name] || []).some((s) =>
            s.toLowerCase().includes(searchText.toLowerCase())
          )
        )
      : plugins;

    return filteredPlugins.map((plugin): DataNode => {
      const pluginKey = `plugin:${plugin.name}`;
      const schemas = schemasCache[plugin.name] || [];
      const pluginIsFileBased = isFileBasedPlugin(plugin.type, plugin.name);

      const schemaNodes: DataNode[] = schemas.map((schema) => {
        const schemaKey = `schema:${schema.name}`;

        // For file-based plugins, show files instead of tables
        if (pluginIsFileBased) {
          const files = filesCache[schemaKey] || [];
          const fileNodes = files.length > 0 ? buildFileNodes(schema.name, files) : undefined;

          return {
            key: schemaKey,
            title: (
              <span>
                {schema.name.replace(`${plugin.name}.`, '')}
              </span>
            ),
            icon: <FolderOutlined style={{ color: '#1890ff' }} />,
            children: fileNodes,
            isLeaf: false,
          };
        }

        // For non-file plugins, show tables
        const tables = tablesCache[schema.name] || [];

        const tableNodes: DataNode[] = tables.map((table) => {
          const tableKey = `table:${schema.name}:${table.name}`;
          const columns = columnsCache[tableKey] || [];

          const columnNodes: DataNode[] = columns.map((col) => ({
            key: `column:${schema.name}:${table.name}:${col.name}`,
            title: (
              <Tooltip title={`${col.type}${col.nullable ? ' (nullable)' : ''}`}>
                <span>
                  {col.name} <Text type="secondary" style={{ fontSize: 11 }}>{col.type}</Text>
                </span>
              </Tooltip>
            ),
            icon: getColumnIcon(col.type),
            isLeaf: true,
          }));

          return {
            key: tableKey,
            title: table.name,
            icon: <TableOutlined style={{ color: '#faad14' }} />,
            children: columnNodes.length > 0 ? columnNodes : undefined,
            isLeaf: false,
          };
        });

        // For schemas, show warning if not browsable
        const schemaBrowsable = schema.browsable !== false;

        return {
          key: schemaKey,
          title: (
            <span>
              {schema.name.replace(`${plugin.name}.`, '')}
              {!schemaBrowsable && (
                <Tooltip title="Tables cannot be enumerated for this schema">
                  <WarningOutlined style={{ marginLeft: 4, color: '#faad14', fontSize: 11 }} />
                </Tooltip>
              )}
            </span>
          ),
          icon: <DatabaseOutlined style={{ color: '#52c41a' }} />,
          children: tableNodes.length > 0 ? tableNodes : undefined,
          isLeaf: !schemaBrowsable,
        };
      });

      // Plugin node with type badge
      const isHttpPlugin = plugin.type?.toLowerCase().includes('http') || plugin.name.toLowerCase() === 'http';

      return {
        key: pluginKey,
        title: (
          <span>
            {plugin.name}
            {!plugin.browsable && (
              <Tooltip title={isHttpPlugin
                ? "HTTP plugin - shows endpoints, but table schema cannot be browsed"
                : "Cannot enumerate tables - query directly"
              }>
                <Tag color={isHttpPlugin ? "blue" : "orange"} style={{ marginLeft: 8, fontSize: 10 }}>
                  {isHttpPlugin ? "API endpoints" : "non-browsable"}
                </Tag>
              </Tooltip>
            )}
          </span>
        ),
        icon: getPluginIcon(plugin.type, plugin.name),
        children: schemaNodes.length > 0 ? schemaNodes : undefined,
        isLeaf: false,
      };
    });
  }, [plugins, schemasCache, tablesCache, columnsCache, filesCache, searchText, buildFileNodes]);

  // Load schemas, tables/files, or columns on expand
  const loadData = useCallback(
    async (node: EventDataNode<DataNode>): Promise<void> => {
      const key = node.key as string;

      if (key.startsWith('plugin:')) {
        const pluginName = key.replace('plugin:', '');
        if (!schemasCache[pluginName]) {
          try {
            const schemas = await getPluginSchemas(pluginName);
            setSchemasCache((prev) => ({ ...prev, [pluginName]: schemas }));
          } catch (error) {
            console.error('Failed to load schemas:', error);
          }
        }
      } else if (key.startsWith('schema:')) {
        const schemaName = key.replace('schema:', '');

        // Check if this is a file-based plugin
        const pluginName = schemaName.split('.')[0];
        const pluginType = pluginTypesCache[pluginName] || '';
        const pluginIsFileBased = isFileBasedPlugin(pluginType, pluginName);

        if (pluginIsFileBased) {
          // Load files for file-based schemas
          if (!filesCache[key]) {
            try {
              const files = await getFiles(schemaName);
              setFilesCache((prev) => ({ ...prev, [key]: files }));
            } catch (error) {
              console.error('Failed to load files:', error);
            }
          }
        } else {
          // Load tables for non-file schemas
          if (!tablesCache[schemaName]) {
            try {
              const tables = await getTables(schemaName);
              setTablesCache((prev) => ({ ...prev, [schemaName]: tables }));
            } catch (error) {
              console.error('Failed to load tables:', error);
              // For non-browsable schemas, this is expected
            }
          }
        }
      } else if (key.startsWith('file:')) {
        // Handle file or folder expansion
        const parts = key.split(':');
        const schemaName = parts[1];
        const filePath = parts.slice(2).join(':'); // Handle paths with colons

        // Check if there's already data (it's a folder with children loaded)
        const cachedFiles = filesCache[key];
        if (cachedFiles !== undefined) {
          // Already loaded or it's a file, check for columns
          const cachedColumns = columnsCache[key];
          if (cachedColumns === undefined) {
            // Try to load columns for this file
            try {
              const columns = await getFileColumns(schemaName, filePath);
              setColumnsCache((prev) => ({ ...prev, [key]: columns }));
            } catch (error) {
              console.error('Failed to load file columns:', error);
              // Mark as empty to prevent repeated attempts
              setColumnsCache((prev) => ({ ...prev, [key]: [] }));
            }
          }
        } else {
          // Try to load as folder first
          try {
            const files = await getFiles(schemaName, filePath);
            if (files.length > 0) {
              setFilesCache((prev) => ({ ...prev, [key]: files }));
            } else {
              // Empty folder or it's actually a file, try to get columns
              setFilesCache((prev) => ({ ...prev, [key]: [] }));
              try {
                const columns = await getFileColumns(schemaName, filePath);
                setColumnsCache((prev) => ({ ...prev, [key]: columns }));
              } catch (colError) {
                console.error('Failed to load file columns:', colError);
                setColumnsCache((prev) => ({ ...prev, [key]: [] }));
              }
            }
          } catch (error) {
            // Folder listing failed, try to get columns (it's a file)
            console.debug('Not a folder, trying as file:', error);
            try {
              const columns = await getFileColumns(schemaName, filePath);
              setFilesCache((prev) => ({ ...prev, [key]: [] })); // Mark as not a folder
              setColumnsCache((prev) => ({ ...prev, [key]: columns }));
            } catch (colError) {
              console.error('Failed to load file columns:', colError);
              setFilesCache((prev) => ({ ...prev, [key]: [] }));
              setColumnsCache((prev) => ({ ...prev, [key]: [] }));
            }
          }
        }
      } else if (key.startsWith('table:')) {
        const [, schemaName, tableName] = key.split(':');
        const cacheKey = `table:${schemaName}:${tableName}`;
        if (!columnsCache[cacheKey]) {
          try {
            const columns = await getColumns(schemaName, tableName);
            setColumnsCache((prev) => ({ ...prev, [cacheKey]: columns }));
          } catch (error) {
            console.error('Failed to load columns:', error);
          }
        }
      }

      setLoadedKeys((prev) => [...prev, key]);
    },
    [schemasCache, tablesCache, columnsCache, filesCache, pluginTypesCache]
  );

  // Handle node selection (single-click on table or file)
  const handleSelect = useCallback(
    (_selectedKeys: React.Key[], info: { node: DataNode }) => {
      const key = info.node.key as string;

      if (key.startsWith('table:')) {
        const [, schemaName, tableName] = key.split(':');
        onTableSelect?.(schemaName, tableName);
      } else if (key.startsWith('file:')) {
        // For files: file:schema:path/to/file.parquet
        const parts = key.split(':');
        const schemaName = parts[1];
        const filePath = parts.slice(2).join(':');
        // Use file path as the "table" name
        onTableSelect?.(schemaName, filePath);
      }
    },
    [onTableSelect]
  );

  // Handle double-click to insert into editor
  const handleDoubleClick = useCallback(
    (_e: React.MouseEvent, node: DataNode) => {
      const key = node.key as string;
      let textToInsert = '';

      if (key.startsWith('plugin:')) {
        textToInsert = `\`${key.replace('plugin:', '')}\``;
      } else if (key.startsWith('schema:')) {
        textToInsert = `\`${key.replace('schema:', '')}\``;
      } else if (key.startsWith('table:')) {
        const [, schemaName, tableName] = key.split(':');
        textToInsert = `\`${schemaName}\`.\`${tableName}\``;
      } else if (key.startsWith('file:')) {
        // For files: file:schema:path/to/file.parquet
        const parts = key.split(':');
        const schemaName = parts[1];
        const filePath = parts.slice(2).join(':');
        textToInsert = `\`${schemaName}\`.\`${filePath}\``;
      } else if (key.startsWith('column:')) {
        const parts = key.split(':');
        // Column key format: column:schema:table_or_path:columnName
        // The column name is the last part
        textToInsert = `\`${parts[parts.length - 1]}\``;
      }

      if (textToInsert && onInsertText) {
        onInsertText(textToInsert);
      }
    },
    [onInsertText]
  );

  const handleRefresh = useCallback(() => {
    setSchemasCache({});
    setTablesCache({});
    setColumnsCache({});
    setFilesCache({});
    setPluginTypesCache({});
    setLoadedKeys([]);
    setExpandedKeys([]);
    refetchPlugins();
  }, [refetchPlugins]);

  if (pluginsLoading) {
    return (
      <div style={{ padding: 24, textAlign: 'center' }}>
        <Spin tip="Loading plugins..." />
      </div>
    );
  }

  return (
    <div className="schema-tree" style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <div style={{ padding: '8px 8px 0', display: 'flex', gap: 8 }}>
        <Search
          placeholder="Search plugins & schemas..."
          allowClear
          size="small"
          prefix={<SearchOutlined />}
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          style={{ flex: 1 }}
        />
        <Tooltip title="Refresh">
          <ReloadOutlined
            onClick={handleRefresh}
            style={{ cursor: 'pointer', padding: 4 }}
          />
        </Tooltip>
      </div>

      <div style={{ flex: 1, overflow: 'auto', marginTop: 8 }}>
        {treeData.length === 0 ? (
          <Empty description="No plugins found" style={{ marginTop: 40 }} />
        ) : (
          <Tree
            showIcon
            blockNode
            treeData={treeData}
            expandedKeys={expandedKeys}
            loadedKeys={loadedKeys}
            loadData={loadData}
            onExpand={(keys) => setExpandedKeys(keys as string[])}
            onSelect={handleSelect}
            onDoubleClick={handleDoubleClick}
            style={{ padding: '0 8px' }}
          />
        )}
      </div>

      <div style={{ padding: 8, borderTop: '1px solid #e8e8e8', fontSize: 11, color: '#999' }}>
        <div>Double-click to insert into query</div>
        <div style={{ marginTop: 2 }}>
          <WarningOutlined style={{ color: '#faad14' }} /> = Cannot browse tables
        </div>
      </div>
    </div>
  );
}
