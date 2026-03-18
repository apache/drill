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
import { Tree, Input, Spin, Empty, Tooltip, Button } from 'antd';
import {
  ReloadOutlined,
  SearchOutlined,
  WarningOutlined,
  StarFilled,
  SettingOutlined,
} from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import type { DataNode, EventDataNode } from 'antd/es/tree';
import { useQueryClient } from '@tanstack/react-query';
import { getPluginSchemas, getTables, getColumns, getFiles, getFileColumns, getNestedColumns, getSubTables, getSubTableColumns } from '../../api/metadata';
import type { SchemaInfo, TableInfo, ColumnInfo, FileInfo, NestedFieldInfo, SubTableInfo, DatasetRef } from '../../types';
import { isFileBasedPlugin, getMultiTableConfig, getHomogeneousDataFormat } from './icons';
import { usePlugins } from './hooks';
import { buildPluginNode } from './TreeNodeBuilder';
import ContextMenu from './ContextMenu';
import type { NodeType } from './ContextMenu';
import { useFavorites } from './useFavorites';
import ColumnStats from './ColumnStats';
import FileInfoModal from './FileInfoModal';
import { DataProfiler } from '../data-profiler';

const { Search } = Input;

export interface DatasetFilter {
  datasets: DatasetRef[];
}

interface SchemaExplorerProps {
  onInsertText?: (text: string) => void;
  onTableSelect?: (schema: string, table: string, columnNames?: string[]) => void;
  datasetFilter?: DatasetFilter;
}

/**
 * Format a compound schema name for SQL: plugin unquoted, workspace parts backtick-quoted.
 * e.g. "dfs.test" → "dfs.`test`", "dfs" → "dfs"
 */
function formatSchema(schema: string): string {
  const parts = schema.split('.');
  if (parts.length <= 1) {
    return schema;
  }
  return parts[0] + '.' + parts.slice(1).map((p) => `\`${p}\``).join('.');
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
    return `${formatSchema(schemaName)}.\`${tableName}\``;
  }
  if (key.startsWith('dir:') || key.startsWith('file:')) {
    const parts = key.split(':');
    return `${formatSchema(parts[1])}.\`${parts.slice(2).join(':')}\``;
  }
  if (key.startsWith('sheet:')) {
    // sheet:<schema>:<filePath>:<subTableName>
    const parts = key.split(':');
    const schemaName = parts[1];
    const filePath = parts[2];
    const subTableName = parts.slice(3).join(':');
    const fileName = filePath.split('/').pop() || filePath;
    const ext = fileName.split('.').pop()?.toLowerCase() || '';
    const formatConfigs: Record<string, { formatType: string; paramName: string }> = {
      xlsx: { formatType: 'excel', paramName: 'sheetName' },
      xls: { formatType: 'excel', paramName: 'sheetName' },
      h5: { formatType: 'hdf5', paramName: 'defaultPath' },
      hdf5: { formatType: 'hdf5', paramName: 'defaultPath' },
      mdb: { formatType: 'msaccess', paramName: 'tableName' },
      accdb: { formatType: 'msaccess', paramName: 'tableName' },
    };
    const cfg = formatConfigs[ext];
    if (cfg) {
      return `table( ${formatSchema(schemaName)}.\`${filePath}\` (type => '${cfg.formatType}', ${cfg.paramName} => '${subTableName}'))`;
    }
    return `${formatSchema(schemaName)}.\`${filePath}\``;
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
  if (key.startsWith('sheet:')) { return 'table'; }
  if (key.startsWith('dir:') || key.startsWith('file:')) { return 'file'; }
  if (key.startsWith('nested:')) { return 'column'; }
  return 'column';
}

/**
 * Lookup structure for dataset filtering, supporting plugin, schema, and table-level refs.
 */
interface DatasetAllowList {
  /** Plugins where everything is allowed */
  plugins: Set<string>;
  /** Schemas where everything is allowed */
  schemas: Set<string>;
  /** Specific table/file allowances: schema → set of table/file names */
  tables: Map<string, Set<string>>;
}

/**
 * Check whether a plugin is allowed at any level.
 */
function isPluginAllowed(pluginName: string, allow: DatasetAllowList): boolean {
  if (allow.plugins.has(pluginName)) {
    return true;
  }
  // Any schema-level ref under this plugin?
  for (const s of allow.schemas) {
    if (s === pluginName || s.startsWith(pluginName + '.')) {
      return true;
    }
  }
  // Any table-level ref under this plugin?
  for (const s of allow.tables.keys()) {
    if (s === pluginName || s.startsWith(pluginName + '.')) {
      return true;
    }
  }
  return false;
}

/**
 * Check whether a schema is allowed at any level.
 */
function isSchemaAllowed(schemaName: string, allow: DatasetAllowList): boolean {
  const pluginName = schemaName.split('.')[0];
  if (allow.plugins.has(pluginName)) {
    return true;
  }
  if (allow.schemas.has(schemaName)) {
    return true;
  }
  return allow.tables.has(schemaName);
}

/**
 * Filter tree nodes to only include datasets matching the provided allow list.
 * Supports plugin-level, schema-level, and table-level filtering.
 */
function filterTreeNodes(nodes: DataNode[], allow: DatasetAllowList): DataNode[] {
  return nodes.reduce<DataNode[]>((acc, node) => {
    const key = node.key as string;

    // Table leaf
    if (key.startsWith('table:')) {
      const [, schema, table] = key.split(':');
      const pluginName = schema.split('.')[0];
      // Allowed if plugin or schema is fully included, or specific table matches
      if (allow.plugins.has(pluginName) || allow.schemas.has(schema)) {
        acc.push(node);
      } else {
        const allowed = allow.tables.get(schema);
        if (allowed && allowed.has(table)) {
          acc.push(node);
        }
      }
      return acc;
    }

    // File leaf
    if (key.startsWith('file:')) {
      const parts = key.split(':');
      const schema = parts[1];
      const filePath = parts.slice(2).join(':');
      const pluginName = schema.split('.')[0];
      if (allow.plugins.has(pluginName) || allow.schemas.has(schema)) {
        acc.push(node);
      } else {
        const allowed = allow.tables.get(schema);
        if (allowed && allowed.has(filePath)) {
          acc.push(node);
        }
      }
      return acc;
    }

    // Column, nested, sheet nodes: always keep (children of approved tables)
    if (key.startsWith('column:') || key.startsWith('nested:') || key.startsWith('sheet:')) {
      acc.push(node);
      return acc;
    }

    // Directory nodes
    if (key.startsWith('dir:')) {
      const parts = key.split(':');
      const schema = parts[1];
      const pluginName = schema.split('.')[0];
      // If plugin or schema fully allowed, keep entire directory
      if (allow.plugins.has(pluginName) || allow.schemas.has(schema)) {
        acc.push(node);
        return acc;
      }
      if (node.children) {
        const filteredChildren = filterTreeNodes(node.children, allow);
        if (filteredChildren.length > 0) {
          acc.push({ ...node, children: filteredChildren });
        }
      } else {
        const dirPath = parts.slice(2).join(':');
        const allowed = allow.tables.get(schema);
        if (allowed) {
          const hasMatch = Array.from(allowed).some((t) => t.startsWith(dirPath + '/'));
          if (hasMatch) {
            acc.push(node);
          }
        }
      }
      return acc;
    }

    // Plugin / schema container nodes
    if (key.startsWith('plugin:') || key.startsWith('schema:')) {
      if (key.startsWith('plugin:')) {
        const pluginName = key.replace('plugin:', '');
        // Plugin fully allowed — keep everything
        if (allow.plugins.has(pluginName)) {
          acc.push(node);
          return acc;
        }
        // Check if any refs exist under this plugin
        if (!isPluginAllowed(pluginName, allow)) {
          return acc;
        }
      } else {
        const schemaName = key.replace('schema:', '');
        const pluginName = schemaName.split('.')[0];
        // Plugin or schema fully allowed — keep everything
        if (allow.plugins.has(pluginName) || allow.schemas.has(schemaName)) {
          acc.push(node);
          return acc;
        }
        if (!isSchemaAllowed(schemaName, allow)) {
          return acc;
        }
      }
      // Partially allowed — filter children
      if (node.children) {
        const filteredChildren = filterTreeNodes(node.children, allow);
        if (filteredChildren.length > 0) {
          acc.push({ ...node, children: filteredChildren });
        }
      } else {
        // Children not loaded yet — keep so user can expand
        acc.push(node);
      }
      return acc;
    }

    // Default: keep
    acc.push(node);
    return acc;
  }, []);
}

export default function SchemaExplorer({ onInsertText, onTableSelect, datasetFilter }: SchemaExplorerProps) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [searchText, setSearchText] = useState('');
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);
  const [loadedKeys, setLoadedKeys] = useState<string[]>([]);
  const [schemasCache, setSchemasCache] = useState<Record<string, SchemaInfo[]>>({});
  const [tablesCache, setTablesCache] = useState<Record<string, TableInfo[]>>({});
  const [columnsCache, setColumnsCache] = useState<Record<string, ColumnInfo[]>>({});
  const [filesCache, setFilesCache] = useState<Record<string, FileInfo[]>>({});
  const [pluginTypesCache, setPluginTypesCache] = useState<Record<string, string>>({});
  const [nestedCache, setNestedCache] = useState<Record<string, NestedFieldInfo[]>>({});
  const [subTablesCache, setSubTablesCache] = useState<Record<string, SubTableInfo[]>>({});
  const [columnFilter, setColumnFilter] = useState<Record<string, string>>({});

  // Column statistics drawer
  const [statsTarget, setStatsTarget] = useState<{ schema: string; table: string } | null>(null);

  // File info modal
  const [fileInfoState, setFileInfoState] = useState<{
    fileInfo: FileInfo; qualifiedName: string; columns: ColumnInfo[];
  } | null>(null);

  // Data profiler target
  const [profileTarget, setProfileTarget] = useState<{
    schemaName: string; tableName: string;
  } | null>(null);

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
  const subTablesCacheRef = useRef(subTablesCache);
  subTablesCacheRef.current = subTablesCache;
  const columnFilterRef = useRef(columnFilter);
  columnFilterRef.current = columnFilter;

  // ---------- Tree data construction ----------

  // Build a hierarchical allow list from datasetFilter
  const datasetAllowList = useMemo((): DatasetAllowList | null => {
    if (!datasetFilter || datasetFilter.datasets.length === 0) {
      return null;
    }
    const allow: DatasetAllowList = {
      plugins: new Set(),
      schemas: new Set(),
      tables: new Map(),
    };
    for (const ds of datasetFilter.datasets) {
      if (ds.type === 'plugin' && ds.schema) {
        allow.plugins.add(ds.schema);
      } else if (ds.type === 'schema' && ds.schema) {
        allow.schemas.add(ds.schema);
      } else if (ds.type === 'table' && ds.schema && ds.table) {
        if (!allow.tables.has(ds.schema)) {
          allow.tables.set(ds.schema, new Set());
        }
        allow.tables.get(ds.schema)!.add(ds.table);
      }
    }
    return allow;
  }, [datasetFilter]);

  const treeData = useMemo(() => {
    if (!plugins) {
      return [];
    }

    let nodes = plugins.map((plugin) =>
      buildPluginNode(
        plugin, schemasCache, tablesCache, columnsCache, filesCache, nestedCache, subTablesCache,
        columnFilter,
        (key, val) => setColumnFilter((prev) => ({ ...prev, [key]: val })),
      )
    );

    // Apply dataset filter if present
    if (datasetAllowList) {
      nodes = filterTreeNodes(nodes, datasetAllowList);
    }

    // Apply search filter across all levels (plugin, schema, table, column)
    if (searchText) {
      const lower = searchText.toLowerCase();
      const matchesSearch = (node: DataNode): boolean => {
        const title = typeof node.title === 'string'
          ? node.title
          : String(node.key).split(':').pop() || '';
        if (title.toLowerCase().includes(lower)) {
          return true;
        }
        if (node.children) {
          return node.children.some(matchesSearch);
        }
        return false;
      };
      const pruneTree = (nodeList: DataNode[]): DataNode[] => {
        return nodeList
          .filter(matchesSearch)
          .map((node) => {
            if (!node.children) {
              return node;
            }
            return { ...node, children: pruneTree(node.children) };
          });
      };
      nodes = pruneTree(nodes);
    }

    return nodes;
  }, [plugins, schemasCache, tablesCache, columnsCache, filesCache, nestedCache, subTablesCache, searchText, datasetAllowList, columnFilter]);

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

          if (!filesCacheRef.current[key]) {
            const files = await getFiles(schemaName, dirPath);
            setFilesCache((prev) => ({ ...prev, [key]: files }));

            // If the directory contains only files of a single data format,
            // treat it as a table and load columns instead of showing individual files.
            const homoFormat = getHomogeneousDataFormat(files);
            if (homoFormat && !columnsCacheRef.current[key]) {
              try {
                const columns = await getFileColumns(schemaName, dirPath);
                setColumnsCache((prev) => ({ ...prev, [key]: columns }));
              } catch {
                // Fall back to showing individual files
                setColumnsCache((prev) => ({ ...prev, [key]: [] }));
              }
            }
          } else {
            // Files already cached — check if we still need to load columns for a homogeneous dir
            const cachedFiles = filesCacheRef.current[key];
            const homoFormat = getHomogeneousDataFormat(cachedFiles);
            if (homoFormat && !columnsCacheRef.current[key]) {
              try {
                const columns = await getFileColumns(schemaName, dirPath);
                setColumnsCache((prev) => ({ ...prev, [key]: columns }));
              } catch {
                setColumnsCache((prev) => ({ ...prev, [key]: [] }));
              }
            }
          }
          loaded = true;

        } else if (key.startsWith('file:')) {
          // ---- File node: load columns or sub-tables ----
          const parts = key.split(':');
          const schemaName = parts[1];
          const filePath = parts.slice(2).join(':');
          const fileName = filePath.split('/').pop() || filePath;
          const mtConfig = getMultiTableConfig(fileName);

          if (mtConfig) {
            // Multi-table file: load sub-tables (sheets/datasets/tables)
            if (!subTablesCacheRef.current[key] && !columnsCacheRef.current[key]) {
              try {
                const subTables = await getSubTables(schemaName, filePath, mtConfig.formatType);
                if (subTables.length <= 1) {
                  // Single (or zero) sub-table: load columns directly like a regular file
                  setSubTablesCache((prev) => ({ ...prev, [key]: [] }));
                  if (subTables.length === 1) {
                    try {
                      const columns = await getSubTableColumns(
                        schemaName, filePath, mtConfig.formatType, mtConfig.paramName, subTables[0].name
                      );
                      setColumnsCache((prev) => ({ ...prev, [key]: columns }));
                    } catch {
                      setColumnsCache((prev) => ({ ...prev, [key]: [] }));
                    }
                  } else {
                    setColumnsCache((prev) => ({ ...prev, [key]: [] }));
                  }
                } else {
                  // Multiple sub-tables: show sub-table nodes
                  setSubTablesCache((prev) => ({ ...prev, [key]: subTables }));
                }
                loaded = true;
              } catch {
                console.error('Failed to load sub-tables for file:', schemaName, filePath);
                // Cache empty result so the node doesn't spin forever
                setSubTablesCache((prev) => ({ ...prev, [key]: [] }));
                setColumnsCache((prev) => ({ ...prev, [key]: [] }));
                loaded = true;
              }
            } else {
              loaded = true;
            }
          } else {
            // Regular file: load columns
            if (!columnsCacheRef.current[key]) {
              try {
                const columns = await getFileColumns(schemaName, filePath);
                setColumnsCache((prev) => ({ ...prev, [key]: columns }));
              } catch (err) {
                console.error('Failed to load columns for file:', schemaName, filePath, err);
                // Cache empty result so the node doesn't spin forever
                setColumnsCache((prev) => ({ ...prev, [key]: [] }));
              }
              loaded = true;
            } else {
              loaded = true;
            }
          }

        } else if (key.startsWith('sheet:')) {
          // ---- Sub-table node: load columns via table function ----
          const parts = key.split(':');
          const schemaName = parts[1];
          const filePath = parts[2];
          const subTableName = parts.slice(3).join(':');
          const fileName = filePath.split('/').pop() || filePath;
          const mtConfig = getMultiTableConfig(fileName);

          if (mtConfig && !columnsCacheRef.current[key]) {
            try {
              const columns = await getSubTableColumns(
                schemaName, filePath, mtConfig.formatType, mtConfig.paramName, subTableName
              );
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

      const cachedCols = columnsCacheRef.current[key];
      const colNames = cachedCols && cachedCols.length > 0
        ? cachedCols.map((c) => c.name)
        : undefined;

      if (key.startsWith('table:')) {
        const [, schemaName, tableName] = key.split(':');
        onTableSelect?.(schemaName, tableName, colNames);
      } else if (key.startsWith('sheet:')) {
        const qn = getQualifiedName(key);
        onTableSelect?.('', qn, colNames);
      } else if (key.startsWith('file:') || key.startsWith('dir:')) {
        const parts = key.split(':');
        onTableSelect?.(parts[1], parts.slice(2).join(':'), colNames);
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
        if (key.startsWith('sheet:') || key.startsWith('table:') || key.startsWith('file:') || key.startsWith('dir:')) {
          const cachedCols = columnsCacheRef.current[key];
          const cols = cachedCols && cachedCols.length > 0
            ? cachedCols.map((c) => `\`${c.name}\``).join(',\n       ')
            : '*';
          onInsertText(`SELECT ${cols}\nFROM ${text}\nLIMIT 100`);
        } else {
          onInsertText(text);
        }
      }
    },
    [onInsertText],
  );

  // ---------- Drag-to-query ----------

  const handleDragStart = useCallback(
    (info: { event: React.DragEvent; node: EventDataNode<DataNode> }) => {
      const key = info.node.key as string;
      const qn = getQualifiedName(key);
      let text = qn;
      if (key.startsWith('sheet:') || key.startsWith('table:') || key.startsWith('file:') || key.startsWith('dir:')) {
        const cachedCols = columnsCacheRef.current[key];
        const cols = cachedCols && cachedCols.length > 0
          ? cachedCols.map((c) => `\`${c.name}\``).join(',\n       ')
          : '*';
        text = `SELECT ${cols}\nFROM ${qn}\nLIMIT 100`;
      }
      info.event.dataTransfer.setData('text/plain', text);
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
          // Also clear columns cached under sheet: keys for this file
          const sheetPrefix = `sheet:${fileSchema}:${filePath}:`;
          for (const k of Object.keys(next)) {
            if (k.startsWith(sheetPrefix)) {
              delete next[k];
            }
          }
          return next;
        });
        // Clear sub-tables cache for this file
        setSubTablesCache((prev) => {
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
        const sheetKeyPrefix = `sheet:${fileSchema}:${filePath}:`;
        setLoadedKeys((prev) => prev.filter((k) => k !== key && !k.startsWith(`column:${fileSchema}:${filePath}:`) && !k.startsWith(`nested:${fileSchema}:${filePath}:`) && !k.startsWith(sheetKeyPrefix)));
      } else if (key.startsWith('sheet:')) {
        // Clear columns cached for this sub-table
        const sheetParts = key.split(':');
        const sheetSchema = sheetParts[1];
        const sheetFilePath = sheetParts[2];
        const sheetSubTable = sheetParts.slice(3).join(':');
        setColumnsCache((prev) => {
          const next = { ...prev };
          delete next[key];
          return next;
        });
        // Clear nested caches under this sub-table
        const parentName = `${sheetFilePath}/${sheetSubTable}`;
        setNestedCache((prev) => {
          const next = { ...prev };
          const colPrefix = `column:${sheetSchema}:${parentName}:`;
          const nestedPrefix = `nested:${sheetSchema}:${parentName}:`;
          for (const k of Object.keys(next)) {
            if (k.startsWith(colPrefix) || k.startsWith(nestedPrefix)) {
              delete next[k];
            }
          }
          return next;
        });
        setLoadedKeys((prev) => prev.filter((k) => k !== key && !k.startsWith(`column:${sheetSchema}:${parentName}:`) && !k.startsWith(`nested:${sheetSchema}:${parentName}:`)));
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
    setSubTablesCache({});
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

  // ---------- File info + plugin edit handlers ----------

  const handleShowFileInfo = useCallback(
    (fi: FileInfo, qn: string, cols: ColumnInfo[]) => {
      setFileInfoState({ fileInfo: fi, qualifiedName: qn, columns: cols });
    },
    [],
  );

  const handleEditPlugin = useCallback(
    (pluginName: string) => {
      navigate(`/datasources/${encodeURIComponent(pluginName)}`);
    },
    [navigate],
  );

  const handleProfileData = useCallback(
    (schemaName: string, tableName: string) => {
      setProfileTarget({ schemaName, tableName });
    },
    [],
  );

  /**
   * Look up a FileInfo from the files cache by a file/dir node key.
   * Files are stored under their parent directory's cache key.
   */
  const lookupFileInfo = useCallback(
    (key: string): FileInfo | undefined => {
      if (!key.startsWith('file:') && !key.startsWith('dir:')) {
        return undefined;
      }
      const parts = key.split(':');
      const schema = parts[1];
      const filePath = parts.slice(2).join(':');
      const fileName = filePath.includes('/') ? filePath.split('/').pop()! : filePath;

      // Parent key: either dir:schema:parentPath or schema:schema (for root files)
      const parentPath = filePath.includes('/') ? filePath.substring(0, filePath.lastIndexOf('/')) : '';
      const parentKey = parentPath ? `dir:${schema}:${parentPath}` : `schema:${schema}`;

      const siblings = filesCacheRef.current[parentKey];
      return siblings?.find((f) => f.name === fileName);
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

      // Look up cached column names for tables/files so Generate SELECT *
      // can enumerate columns instead of using *.
      const cachedColumns = columnsCacheRef.current[key];
      const columnNames = cachedColumns && cachedColumns.length > 0
        ? cachedColumns.map((c) => c.name)
        : undefined;

      // Look up FileInfo for file nodes (for Get Info)
      const fi = nodeType === 'file' ? lookupFileInfo(key) : undefined;

      return (
        <ContextMenu
          nodeType={nodeType}
          nodeKey={key}
          qualifiedName={qualifiedName}
          columnNames={columnNames}
          fileInfo={fi}
          columnInfos={cachedColumns}
          onInsertText={onInsertText}
          onRefreshNode={handleRefreshNode}
          onShowStats={handleShowStats}
          onShowFileInfo={handleShowFileInfo}
          onEditPlugin={handleEditPlugin}
          onProfileData={handleProfileData}
          isFavorite={isFavorite(key)}
          onToggleFavorite={toggleFavorite}
        >
          <span>{nodeData.title as React.ReactNode}</span>
        </ContextMenu>
      );
    },
    [onInsertText, handleRefreshNode, handleShowStats, handleShowFileInfo, handleEditPlugin, handleProfileData, lookupFileInfo, isFavorite, toggleFavorite],
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
          id="schema-search-input"
          placeholder="Search schemas, tables, columns... (Ctrl+Shift+F)"
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
        {datasetFilter && datasetFilter.datasets.length === 0 ? (
          <Empty
            description="No datasets in this project"
            style={{ marginTop: 40 }}
          >
            <Button
              type="link"
              icon={<SettingOutlined />}
              onClick={() => {
                // Navigate to project settings — extract project ID from URL if available
                const match = window.location.pathname.match(/\/projects\/([^/]+)/);
                if (match) {
                  navigate(`/projects/${match[1]}`);
                }
              }}
            >
              Add datasets in project settings
            </Button>
          </Empty>
        ) : combinedTreeData.length === 0 ? (
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

      <div style={{ padding: 8, borderTop: '1px solid var(--color-border)', fontSize: 11, color: 'var(--color-text-tertiary)' }}>
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

      {/* File Info Modal */}
      <FileInfoModal
        open={!!fileInfoState}
        onClose={() => setFileInfoState(null)}
        fileInfo={fileInfoState?.fileInfo ?? null}
        qualifiedName={fileInfoState?.qualifiedName ?? ''}
        columns={fileInfoState?.columns ?? []}
      />

      {/* Data Profiler Modal */}
      {profileTarget && (
        <DataProfiler
          open={!!profileTarget}
          onClose={() => setProfileTarget(null)}
          schemaName={profileTarget.schemaName}
          tableName={profileTarget.tableName}
          sql={`SELECT * FROM ${formatSchema(profileTarget.schemaName)}.\`${profileTarget.tableName}\``}
        />
      )}
    </div>
  );
}
