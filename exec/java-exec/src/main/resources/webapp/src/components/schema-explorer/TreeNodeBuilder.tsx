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
import { Tooltip, Typography, Tag } from 'antd';
import { FolderOutlined, TableOutlined, WarningOutlined } from '@ant-design/icons';
import { DatabaseOutlined } from '@ant-design/icons';
import type { DataNode } from 'antd/es/tree';
import type { PluginInfo, SchemaInfo, TableInfo, ColumnInfo, FileInfo, NestedFieldInfo, SubTableInfo } from '../../types';
import { getPluginIcon, getColumnIcon, getFileIcon, isFileBasedPlugin, isComplexColumnType, isMultiTableFile, getSubTableIcon } from './icons';

const { Text } = Typography;

/**
 * Key-prefix conventions:
 *   plugin:<name>                          – storage plugin
 *   schema:<schemaName>                    – schema / workspace
 *   table:<schema>:<table>                 – table inside a non-file schema
 *   dir:<schema>:<path>                    – directory inside a file-based schema
 *   file:<schema>:<path>                   – file inside a file-based schema
 *   sheet:<schema>:<filePath>:<subTable>   – sub-table inside a multi-table file (Excel/HDF5/Access)
 *   column:<schema>:<parent>:<col>         – column
 *   nested:<schema>:<parent>:<dotPath>     – nested field inside a MAP/STRUCT column
 */

/** Build nested field nodes for a MAP/STRUCT column (recursive). */
export function buildNestedFieldNodes(
  fields: NestedFieldInfo[],
  parentSchema: string,
  parentName: string,
  parentPath: string,
  nestedCache: Record<string, NestedFieldInfo[]>,
): DataNode[] {
  return fields.map((field) => {
    const fieldPath = `${parentPath}.${field.name}`;
    const nodeKey = `nested:${parentSchema}:${parentName}:${fieldPath}`;
    const complex = isComplexColumnType(field.type);

    const children = complex && nestedCache[nodeKey]
      ? buildNestedFieldNodes(nestedCache[nodeKey], parentSchema, parentName, fieldPath, nestedCache)
      : undefined;

    return {
      key: nodeKey,
      title: (
        <Tooltip title={field.type}>
          <span>
            {field.name} <Text type="secondary" style={{ fontSize: 11 }}>{field.type}</Text>
          </span>
        </Tooltip>
      ),
      icon: getColumnIcon(field.type),
      isLeaf: !complex,
      children,
    };
  });
}

/** Build column tree nodes from a list of ColumnInfo. */
export function buildColumnNodes(
  columns: ColumnInfo[],
  parentSchema: string,
  parentName: string,
  nestedCache: Record<string, NestedFieldInfo[]>,
): DataNode[] {
  return columns.map((col) => {
    const colKey = `column:${parentSchema}:${parentName}:${col.name}`;
    const complex = isComplexColumnType(col.type);

    const children = complex && nestedCache[colKey]
      ? buildNestedFieldNodes(nestedCache[colKey], parentSchema, parentName, col.name, nestedCache)
      : undefined;

    return {
      key: colKey,
      title: (
        <Tooltip title={`${col.type}${col.nullable ? ' (nullable)' : ''}`}>
          <span>
            {col.name} <Text type="secondary" style={{ fontSize: 11 }}>{col.type}</Text>
          </span>
        </Tooltip>
      ),
      icon: getColumnIcon(col.type),
      isLeaf: !complex,
      children,
    };
  });
}

/** Build sub-table nodes (sheets/datasets/tables) for a multi-table file. */
export function buildSubTableNodes(
  schema: string,
  filePath: string,
  subTables: SubTableInfo[],
  columnsCache: Record<string, ColumnInfo[]>,
  nestedCache: Record<string, NestedFieldInfo[]>,
): DataNode[] {
  return subTables.map((sub) => {
    const sheetKey = `sheet:${schema}:${filePath}:${sub.name}`;
    const columns = columnsCache[sheetKey] || [];
    const columnNodes = columns.length > 0
      ? buildColumnNodes(columns, schema, `${filePath}/${sub.name}`, nestedCache)
      : undefined;

    const subtitle = sub.dataType ? ` (${sub.dataType})` : '';

    return {
      key: sheetKey,
      title: (
        <Tooltip title={`Sub-table: ${sub.name}${subtitle}`}>
          <span>{sub.name}{subtitle && <Text type="secondary" style={{ fontSize: 11 }}>{subtitle}</Text>}</span>
        </Tooltip>
      ),
      icon: getSubTableIcon(),
      children: columnNodes,
      isLeaf: false,
    };
  });
}

/** Build file/folder tree nodes recursively. */
export function buildFileNodes(
  schema: string,
  files: FileInfo[],
  filesCache: Record<string, FileInfo[]>,
  columnsCache: Record<string, ColumnInfo[]>,
  nestedCache: Record<string, NestedFieldInfo[]>,
  subTablesCache: Record<string, SubTableInfo[]>,
  parentPath: string = '',
): DataNode[] {
  return files.map((file) => {
    const filePath = parentPath ? `${parentPath}/${file.name}` : file.name;

    if (file.isDirectory) {
      // Key prefix is "dir:" for directories
      const dirKey = `dir:${schema}:${filePath}`;
      const subFiles = filesCache[dirKey] || [];
      const subNodes = subFiles.length > 0
        ? buildFileNodes(schema, subFiles, filesCache, columnsCache, nestedCache, subTablesCache, filePath)
        : undefined;

      return {
        key: dirKey,
        title: file.name,
        icon: <FolderOutlined style={{ color: '#3b82f6' }} />,
        children: subNodes,
        isLeaf: false,
      };
    }

    // Key prefix is "file:" for files
    const fileKey = `file:${schema}:${filePath}`;

    // Multi-table files with multiple sub-tables show sub-table nodes
    if (isMultiTableFile(file.name)) {
      const subTables = subTablesCache[fileKey] || [];
      if (subTables.length > 0) {
        const subTableNodes = buildSubTableNodes(schema, filePath, subTables, columnsCache, nestedCache);
        return {
          key: fileKey,
          title: (
            <Tooltip title={`${file.length} bytes`}>
              <span>{file.name}</span>
            </Tooltip>
          ),
          icon: getFileIcon(file.name),
          children: subTableNodes,
          isLeaf: false,
        };
      }
      // Single sub-table or not yet loaded — fall through to show columns directly
    }

    const columns = columnsCache[fileKey] || [];
    const columnNodes = columns.length > 0
      ? buildColumnNodes(columns, schema, filePath, nestedCache)
      : undefined;

    return {
      key: fileKey,
      title: (
        <Tooltip title={`${file.length} bytes`}>
          <span>{file.name}</span>
        </Tooltip>
      ),
      icon: getFileIcon(file.name),
      children: columnNodes,
      isLeaf: false,
    };
  });
}

/** Build table tree nodes for a non-file schema. */
export function buildTableNodes(
  schemaName: string,
  tables: TableInfo[],
  columnsCache: Record<string, ColumnInfo[]>,
  nestedCache: Record<string, NestedFieldInfo[]>,
): DataNode[] {
  return tables.map((table) => {
    const tableKey = `table:${schemaName}:${table.name}`;
    const columns = columnsCache[tableKey] || [];
    const columnNodes = columns.length > 0
      ? buildColumnNodes(columns, schemaName, table.name, nestedCache)
      : undefined;

    return {
      key: tableKey,
      title: table.name,
      icon: <TableOutlined style={{ color: '#faad14' }} />,
      children: columnNodes,
      isLeaf: false,
    };
  });
}

/** Build schema tree nodes for a plugin. */
export function buildSchemaNodes(
  plugin: PluginInfo,
  schemas: SchemaInfo[],
  tablesCache: Record<string, TableInfo[]>,
  columnsCache: Record<string, ColumnInfo[]>,
  filesCache: Record<string, FileInfo[]>,
  nestedCache: Record<string, NestedFieldInfo[]>,
  subTablesCache: Record<string, SubTableInfo[]>,
): DataNode[] {
  const pluginIsFileBased = isFileBasedPlugin(plugin.type, plugin.name);

  return schemas.map((schema) => {
    const schemaKey = `schema:${schema.name}`;

    if (pluginIsFileBased) {
      const files = filesCache[schemaKey] || [];
      const fileNodes = files.length > 0
        ? buildFileNodes(schema.name, files, filesCache, columnsCache, nestedCache, subTablesCache)
        : undefined;

      return {
        key: schemaKey,
        title: <span>{schema.name.replace(`${plugin.name}.`, '')}</span>,
        icon: <FolderOutlined style={{ color: '#3b82f6' }} />,
        children: fileNodes,
        isLeaf: false,
      };
    }

    // Non-file schema
    const tables = tablesCache[schema.name] || [];
    const tableNodes = tables.length > 0
      ? buildTableNodes(schema.name, tables, columnsCache, nestedCache)
      : undefined;

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
      children: tableNodes,
      isLeaf: !schemaBrowsable,
    };
  });
}

/** Build a full plugin tree node. */
export function buildPluginNode(
  plugin: PluginInfo,
  schemasCache: Record<string, SchemaInfo[]>,
  tablesCache: Record<string, TableInfo[]>,
  columnsCache: Record<string, ColumnInfo[]>,
  filesCache: Record<string, FileInfo[]>,
  nestedCache: Record<string, NestedFieldInfo[]>,
  subTablesCache: Record<string, SubTableInfo[]>,
): DataNode {
  const pluginKey = `plugin:${plugin.name}`;
  const schemas = schemasCache[plugin.name] || [];
  const schemaNodes = schemas.length > 0
    ? buildSchemaNodes(plugin, schemas, tablesCache, columnsCache, filesCache, nestedCache, subTablesCache)
    : undefined;

  const isHttpPlugin = plugin.type?.toLowerCase().includes('http') || plugin.name.toLowerCase() === 'http';

  return {
    key: pluginKey,
    title: (
      <span>
        {plugin.name}
        {!plugin.browsable && (
          <Tooltip title={isHttpPlugin
            ? 'HTTP plugin - shows endpoints, but table schema cannot be browsed'
            : 'Cannot enumerate tables - query directly'
          }>
            <Tag color={isHttpPlugin ? 'blue' : 'orange'} style={{ marginLeft: 8, fontSize: 10 }}>
              {isHttpPlugin ? 'API endpoints' : 'non-browsable'}
            </Tag>
          </Tooltip>
        )}
      </span>
    ),
    icon: getPluginIcon(plugin.type, plugin.name),
    children: schemaNodes,
    isLeaf: false,
  };
}
