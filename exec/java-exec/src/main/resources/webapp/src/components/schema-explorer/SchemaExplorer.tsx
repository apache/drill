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
import { useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { Tree, Input, Spin, Empty, Tooltip } from 'antd';
import {
  ReloadOutlined,
  SearchOutlined,
  WarningOutlined,
  StarFilled,
} from '@ant-design/icons';
import type { DataNode, EventDataNode } from 'antd/es/tree';
import { useQueryClient } from '@tanstack/react-query';
import { getPluginSchemas, getTables, getColumns, getFiles, getFileColumns, getNestedColumns } from '../../api/metadata';
import type { SchemaInfo, TableInfo, ColumnInfo, FileInfo, NestedFieldInfo } from '../../types';
import { isFileBasedPlugin } from './icons';
import { usePlugins } from './hooks';
import { buildPluginNode } from './TreeNodeBuilder';
import ContextMenu from './ContextMenu';
import type { NodeType } from './ContextMenu';
import { useFavorites } from './useFavorites';
import ColumnStats from './ColumnStats';

const { Search } = Input;

interface SchemaExplorerProps {
  onInsertText?: (text: string) => void;
  onTableSelect?: (schema: string, table: string) => void;
}

/**
 * Derive a backtick-quoted qualified name from a tree node key.
 * Handles prefixes: plugin, schema, table, dir, file, column.
 */
function getQualifiedName(key: string): string {
  if (key.startsWith('plugin:')) {
    return `\`${key.replace('plugin:', '')}\``;
  }
  if (key.startsWith('schema:')) {
    return `\`${key.replace('schema:', '')}\``;
  }
  if (key.startsWith('table:')) {
    const [, schemaName, tableName] = key.split(':');
    return `\`${schemaName}\`.\`${tableName}\``;
  }
  if (key.startsWith('dir:') || key.startsWith('file:')) {
    const parts = key.split(':');
    return `\`${parts[1]}\`.\`${parts.slice(2).join(':')}\``;
  }
  if (key.startsWith('column:')) {
    const parts = key.split(':');
    return `\`${parts[parts.length - 1]}\``;
  }
  if (key.startsWith('nested:')) {
    // nested:schema:parent:record.field1.subfield → `record`.`field1`.`subfield`
    const parts = key.split(':');
    const dotPath = parts.slice(3).join(':');
    return dotPath.split('.').map((p) => `\`${p}\``).join('.');
  }
  return key;
}

/** Derive the node type from a tree node key prefix. */
function getNodeType(key: string): NodeType {
  if (key.startsWith('plugin:')) { return 'plugin'; }
  if (key.startsWith('schema:')) { return 'schema'; }
  if (key.startsWith('table:')) { return 'table'; }
  if (key.startsWith('dir:') || key.startsWith('file:')) { return 'file'; }
  if (key.startsWith('nested:')) { return 'column'; }
  return 'column';
}

export default function SchemaExplorer({ onInsertText, onTableSelect }: SchemaExplorerProps) {
  const queryClient = useQueryClient();
  const [searchText, setSearchText] = useState('');
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);
  const [loadedKeys, setLoadedKeys] = useState<string[]>([]);
  const [schemasCache, setSchemasCache] = useState<Record<string, SchemaInfo[]>>({});
  const [tablesCache, setTablesCache] = useState<Record<string, TableInfo[]>>({});
  const [columnsCache, setColumnsCache] = useState<Record<string, ColumnInfo[]>>({});
  const [filesCache, setFilesCache] = useState<Record<string, FileInfo[]>>({});
  const [pluginTypesCache, setPluginTypesCache] = useState<Record<string, string>>({});
  const [nestedCache, setNestedCache] = useState<Record<string, NestedFieldInfo[]>>({});

  // Column statistics drawer
  const [statsTarget, setStatsTarget] = useState<{ schema: string; table: string } | null>(null);

  // Favorites
  const { favorites, toggleFavorite, isFavorite } = useFavorites();

  // React Query: plugins
  const { data: plugins, isLoading: pluginsLoading } = usePlugins();

  // Keep pluginTypesCache in sync with plugins data (useEffect, not useMemo)
  useEffect(() => {
    if (plugins) {
      const types: Record<string, string> = {};
      plugins.forEach((p) => { types[p.name] = p.type; });
      setPluginTypesCache(types);
    }
  }, [plugins]);

  // ---------- Refs for latest cache values (avoids stale closures in loadData) ----------
  const schemasCacheRef = useRef(schemasCache);
  schemasCacheRef.current = schemasCache;
  const tablesCacheRef = useRef(tablesCache);
  tablesCacheRef.current = tablesCache;
  const columnsCacheRef = useRef(columnsCache);
  columnsCacheRef.current = columnsCache;
  const filesCacheRef = useRef(filesCache);
  filesCacheRef.current = filesCache;
  const pluginTypesCacheRef = useRef(pluginTypesCache);
  pluginTypesCacheRef.current = pluginTypesCache;
  const nestedCacheRef = useRef(nestedCache);
  nestedCacheRef.current = nestedCache;

  // ---------- Tree data construction ----------

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

    return filteredPlugins.map((plugin) =>
      buildPluginNode(plugin, schemasCache, tablesCache, columnsCache, filesCache, nestedCache)
    );
  }, [plugins, schemasCache, tablesCache, columnsCache, filesCache, nestedCache, searchText]);

  // Favorites section: flat list of pinned nodes at the top
  const favoritesTreeData = useMemo((): DataNode[] => {
    if (favorites.length === 0) {
      return [];
    }
    const nodes: DataNode[] = favorites.map((key) => ({
      key: `fav:${key}`,
      title: (
        <span style={{ fontStyle: 'italic' }}>
          <StarFilled style={{ color: '#faad14', marginRight: 4, fontSize: 11 }} />
          {getQualifiedName(key)}
        </span>
      ),
      isLeaf: true,
    }));
    return [{
      key: '__favorites__',
      title: <span style={{ fontWeight: 600 }}>Favorites</span>,
      icon: <StarFilled style={{ color: '#faad14' }} />,
      children: nodes,
      selectable: false,
    }];
  }, [favorites]);

  const combinedTreeData = useMemo(
    () => [...favoritesTreeData, ...treeData],
    [favoritesTreeData, treeData],
  );

  // ---------- Lazy-load on expand ----------
  // Uses refs for cache reads so loadData never has stale closure values
  // and does not need to be recreated when caches change.

  const loadData = useCallback(
    async (node: EventDataNode<DataNode>): Promise<void> => {
      const key = node.key as string;
      let loaded = false;

      try {
        if (key.startsWith('plugin:')) {
          const pluginName = key.replace('plugin:', '');
          if (!schemasCacheRef.current[pluginName]) {
            const schemas = await getPluginSchemas(pluginName);
            setSchemasCache((prev) => ({ ...prev, [pluginName]: schemas }));
          }
          loaded = true;

        } else if (key.startsWith('schema:')) {
          const schemaName = key.replace('schema:', '');
          const pluginName = schemaName.split('.')[0];
          const pluginType = pluginTypesCacheRef.current[pluginName] || '';
          const pluginIsFileBased = isFileBasedPlugin(pluginType, pluginName);

          if (pluginIsFileBased) {
            if (!filesCacheRef.current[key]) {
              const files = await getFiles(schemaName);
              setFilesCache((prev) => ({ ...prev, [key]: files }));
            }
          } else {
            if (!tablesCacheRef.current[schemaName]) {
              const tables = await getTables(schemaName);
              setTablesCache((prev) => ({ ...prev, [schemaName]: tables }));
            }
          }
          loaded = true;

        } else if (key.startsWith('dir:')) {
          // ---- Directory node: load child files/folders ----
          const parts = key.split(':');
          const schemaName = parts[1];
          const dirPath = parts.slice(2).join(':');
          const files = await getFiles(schemaName, dirPath);
          setFilesCache((prev) => ({ ...prev, [key]: files }));
          loaded = true;

        } else if (key.startsWith('file:')) {
          // ---- File node: load columns ----
          const parts = key.split(':');
          const schemaName = parts[1];
          const filePath = parts.slice(2).join(':');
          if (!columnsCacheRef.current[key]) {
            try {
              const columns = await getFileColumns(schemaName, filePath);
              setColumnsCache((prev) => ({ ...prev, [key]: columns }));
            } catch {
              setColumnsCache((prev) => ({ ...prev, [key]: [] }));
            }
          }
          loaded = true;

        } else if (key.startsWith('table:')) {
          const [, schemaName, tableName] = key.split(':');
          const cacheKey = `table:${schemaName}:${tableName}`;
          if (!columnsCacheRef.current[cacheKey]) {
            const columns = await getColumns(schemaName, tableName);
            setColumnsCache((prev) => ({ ...prev, [cacheKey]: columns }));
          }
          loaded = true;

        } else if (key.startsWith('column:')) {
          // Complex column (MAP/STRUCT) — load nested sub-fields via getMapSchema()
          const parts = key.split(':');
          const schemaName = parts[1];
          const parentName = parts[2];
          const colName = parts.slice(3).join(':');
          if (!nestedCacheRef.current[key]) {
            try {
              const nestedFields = await getNestedColumns(schemaName, parentName, colName);
              setNestedCache((prev) => ({ ...prev, [key]: nestedFields }));
            } catch {
              // Column doesn't support getMapSchema — store empty
              setNestedCache((prev) => ({ ...prev, [key]: [] }));
            }
          }
          loaded = true;

        } else if (key.startsWith('nested:')) {
          // Deeper nested field (MAP within MAP) — load next level
          const parts = key.split(':');
          const schemaName = parts[1];
          const parentName = parts[2];
          const dotPath = parts.slice(3).join(':');
          if (!nestedCacheRef.current[key]) {
            try {
              const nestedFields = await getNestedColumns(schemaName, parentName, dotPath);
              setNestedCache((prev) => ({ ...prev, [key]: nestedFields }));
            } catch {
              setNestedCache((prev) => ({ ...prev, [key]: [] }));
            }
          }
          loaded = true;
        }
      } catch (error) {
        console.error('Failed to load data for node:', key, error);
      }

      // Only mark as loaded on success so failed nodes can be retried
      if (loaded) {
        setLoadedKeys((prev) => [...prev, key]);
      }
    },
    [], // No dependencies — uses refs for all cache reads
  );

  // ---------- Selection / double-click ----------

  const handleSelect = useCallback(
    (_selectedKeys: React.Key[], info: { node: DataNode }) => {
      const key = info.node.key as string;

      // Handle favorite clicks – insert the referenced name
      if (key.startsWith('fav:')) {
        const realKey = key.replace('fav:', '');
        const qn = getQualifiedName(realKey);
        onInsertText?.(qn);
        return;
      }

      if (key.startsWith('table:')) {
        const [, schemaName, tableName] = key.split(':');
        onTableSelect?.(schemaName, tableName);
      } else if (key.startsWith('file:') || key.startsWith('dir:')) {
        const parts = key.split(':');
        onTableSelect?.(parts[1], parts.slice(2).join(':'));
      }
    },
    [onTableSelect, onInsertText],
  );

  const handleDoubleClick = useCallback(
    (_e: React.MouseEvent, node: DataNode) => {
      const key = node.key as string;
      if (key.startsWith('fav:') || key === '__favorites__') {
        return;
      }
      const text = getQualifiedName(key);
      if (text && onInsertText) {
        onInsertText(text);
      }
    },
    [onInsertText],
  );

  // ---------- Drag-to-query ----------

  const handleDragStart = useCallback(
    (info: { event: React.DragEvent; node: EventDataNode<DataNode> }) => {
      const key = info.node.key as string;
      const qn = getQualifiedName(key);
      info.event.dataTransfer.setData('text/plain', qn);
      info.event.dataTransfer.effectAllowed = 'copy';
    },
    [],
  );

  // ---------- Context menu helpers ----------

  const handleRefreshNode = useCallback(
    (key: string) => {
      if (key.startsWith('plugin:')) {
        const pluginName = key.replace('plugin:', '');
        setSchemasCache((prev) => {
          const next = { ...prev };
          delete next[pluginName];
          return next;
        });
        setLoadedKeys((prev) => prev.filter((k) => k !== key));
      } else if (key.startsWith('schema:')) {
        const schemaName = key.replace('schema:', '');
        setTablesCache((prev) => {
          const next = { ...prev };
          delete next[schemaName];
          return next;
        });
        setFilesCache((prev) => {
          const next = { ...prev };
          delete next[key];
          return next;
        });
        setLoadedKeys((prev) => prev.filter((k) => k !== key));
      } else if (key.startsWith('table:')) {
        setColumnsCache((prev) => {
          const next = { ...prev };
          delete next[key];
          return next;
        });
        // Clear any nested field caches for columns under this table
        setNestedCache((prev) => {
          const next = { ...prev };
          const colPrefix = `column:${key.replace('table:', '')}:`;
          const nestedPrefix = `nested:${key.replace('table:', '')}:`;
          for (const k of Object.keys(next)) {
            if (k.startsWith(colPrefix) || k.startsWith(nestedPrefix)) {
              delete next[k];
            }
          }
          return next;
        });
        setLoadedKeys((prev) => prev.filter((k) => k !== key && !k.startsWith(`column:${key.replace('table:', '')}:`) && !k.startsWith(`nested:${key.replace('table:', '')}:`)));
      } else if (key.startsWith('dir:')) {
        // Clear directory listing + all nested children
        const prefix = key + '/';
        setFilesCache((prev) => {
          const next = { ...prev };
          for (const k of Object.keys(next)) {
            if (k === key || k.startsWith(prefix)) {
              delete next[k];
            }
          }
          return next;
        });
        setColumnsCache((prev) => {
          const next = { ...prev };
          for (const k of Object.keys(next)) {
            // Nested files under this dir use file:<schema>:<dirPath>/...
            const dirParts = key.split(':');
            const dirPath = dirParts.slice(2).join(':');
            const schema = dirParts[1];
            const filePrefix = `file:${schema}:${dirPath}/`;
            if (k.startsWith(filePrefix)) {
              delete next[k];
            }
          }
          return next;
        });
        setLoadedKeys((prev) => prev.filter((k) => {
          if (k === key) { return false; }
          // Also clear any nested dir: or file: keys
          const dirParts = key.split(':');
          const dirPath = dirParts.slice(2).join(':');
          const schema = dirParts[1];
          const nestedDirPrefix = `dir:${schema}:${dirPath}/`;
          const nestedFilePrefix = `file:${schema}:${dirPath}/`;
          return !k.startsWith(nestedDirPrefix) && !k.startsWith(nestedFilePrefix);
        }));
      } else if (key.startsWith('file:')) {
        const fileParts = key.split(':');
        const fileSchema = fileParts[1];
        const filePath = fileParts.slice(2).join(':');
        setColumnsCache((prev) => {
          const next = { ...prev };
          delete next[key];
          return next;
        });
        // Clear nested field caches for columns under this file
        setNestedCache((prev) => {
          const next = { ...prev };
          const colPrefix = `column:${fileSchema}:${filePath}:`;
          const nestedPrefix = `nested:${fileSchema}:${filePath}:`;
          for (const k of Object.keys(next)) {
            if (k.startsWith(colPrefix) || k.startsWith(nestedPrefix)) {
              delete next[k];
            }
          }
          return next;
        });
        setLoadedKeys((prev) => prev.filter((k) => k !== key && !k.startsWith(`column:${fileSchema}:${filePath}:`) && !k.startsWith(`nested:${fileSchema}:${filePath}:`)));
      }
    },
    [],
  );

  const handleShowStats = useCallback(
    (schemaName: string, tableName: string) => {
      setStatsTarget({ schema: schemaName, table: tableName });
    },
    [],
  );

  // ---------- Global refresh ----------

  const handleRefresh = useCallback(() => {
    setSchemasCache({});
    setTablesCache({});
    setColumnsCache({});
    setFilesCache({});
    setPluginTypesCache({});
    setNestedCache({});
    setLoadedKeys([]);
    setExpandedKeys([]);
    queryClient.invalidateQueries({ queryKey: ['plugins'] });
  }, [queryClient]);

  // ---------- Right-click handler for tree ----------

  const handleRightClick = useCallback(
    (info: { event: React.MouseEvent; node: EventDataNode<DataNode> }) => {
      info.event.preventDefault();
    },
    [],
  );

  // ---------- Title renderer that wraps each node in a ContextMenu ----------

  const titleRender = useCallback(
    (nodeData: DataNode) => {
      const key = nodeData.key as string;

      if (key === '__favorites__' || key.startsWith('fav:')) {
        return nodeData.title as React.ReactNode;
      }

      const nodeType = getNodeType(key);
      const qualifiedName = getQualifiedName(key);

      return (
        <ContextMenu
          nodeType={nodeType}
          nodeKey={key}
          qualifiedName={qualifiedName}
          onInsertText={onInsertText}
          onRefreshNode={handleRefreshNode}
          onShowStats={handleShowStats}
          isFavorite={isFavorite(key)}
          onToggleFavorite={toggleFavorite}
        >
          <span>{nodeData.title as React.ReactNode}</span>
        </ContextMenu>
      );
    },
    [onInsertText, handleRefreshNode, handleShowStats, isFavorite, toggleFavorite],
  );

  // ---------- Render ----------

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
        {combinedTreeData.length === 0 ? (
          <Empty description="No plugins found" style={{ marginTop: 40 }} />
        ) : (
          <Tree
            showIcon
            blockNode
            draggable={{ icon: false, nodeDraggable: () => true }}
            allowDrop={() => false}
            treeData={combinedTreeData}
            expandedKeys={expandedKeys}
            loadedKeys={loadedKeys}
            loadData={loadData}
            onExpand={(keys) => setExpandedKeys(keys as string[])}
            onSelect={handleSelect}
            onDoubleClick={handleDoubleClick}
            onDragStart={handleDragStart}
            onRightClick={handleRightClick}
            titleRender={titleRender}
            style={{ padding: '0 8px' }}
          />
        )}
      </div>

      <div style={{ padding: 8, borderTop: '1px solid #e8e8e8', fontSize: 11, color: '#999' }}>
        <div>Double-click or drag to insert into query</div>
        <div style={{ marginTop: 2 }}>Right-click for more options</div>
        <div style={{ marginTop: 2 }}>
          <WarningOutlined style={{ color: '#faad14' }} /> = Cannot browse tables
        </div>
      </div>

      {/* Column Statistics Drawer */}
      {statsTarget && (
        <ColumnStats
          open={!!statsTarget}
          onClose={() => setStatsTarget(null)}
          schemaName={statsTarget.schema}
          tableName={statsTarget.table}
        />
      )}
    </div>
  );
}
