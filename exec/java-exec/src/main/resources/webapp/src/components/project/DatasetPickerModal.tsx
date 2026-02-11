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
import { Modal, Tree, Spin, Empty, message, Typography } from 'antd';
import type { DataNode, EventDataNode } from 'antd/es/tree';
import { getPluginSchemas, getTables, getFiles } from '../../api/metadata';
import { addDataset } from '../../api/projects';
import { usePlugins } from '../schema-explorer/hooks';
import { isFileBasedPlugin, getPluginIcon, getFileIcon } from '../schema-explorer/icons';
import type { DatasetRef, SchemaInfo, TableInfo, FileInfo } from '../../types';
import {
  DatabaseOutlined,
  FolderOutlined,
  TableOutlined,
  FileOutlined,
} from '@ant-design/icons';

const { Text } = Typography;

interface DatasetPickerModalProps {
  open: boolean;
  projectId: string;
  existingDatasets: DatasetRef[];
  onClose: () => void;
  onSuccess: () => void;
}

/**
 * Check whether a tree key is covered by an existing dataset ref.
 * For example, if plugin "dfs" is already added, then schema "dfs.tmp"
 * and table "dfs.tmp:myfile" are all covered.
 */
function isCoveredByExisting(
  key: string,
  existingPlugins: Set<string>,
  existingSchemas: Set<string>,
  existingTables: Set<string>,
): boolean {
  if (key.startsWith('plugin:')) {
    const pluginName = key.replace('plugin:', '');
    return existingPlugins.has(pluginName);
  }
  if (key.startsWith('schema:')) {
    const schemaName = key.replace('schema:', '');
    const pluginName = schemaName.split('.')[0];
    return existingPlugins.has(pluginName) || existingSchemas.has(schemaName);
  }
  if (key.startsWith('table:') || key.startsWith('file:')) {
    const parts = key.split(':');
    const schema = parts[1];
    const pluginName = schema.split('.')[0];
    return existingPlugins.has(pluginName) || existingSchemas.has(schema) || existingTables.has(key);
  }
  if (key.startsWith('dir:')) {
    const parts = key.split(':');
    const schema = parts[1];
    const pluginName = schema.split('.')[0];
    return existingPlugins.has(pluginName) || existingSchemas.has(schema);
  }
  return false;
}

export default function DatasetPickerModal({
  open,
  projectId,
  existingDatasets,
  onClose,
  onSuccess,
}: DatasetPickerModalProps) {
  const { data: plugins, isLoading: pluginsLoading } = usePlugins();

  const [schemasCache, setSchemasCache] = useState<Record<string, SchemaInfo[]>>({});
  const [tablesCache, setTablesCache] = useState<Record<string, TableInfo[]>>({});
  const [filesCache, setFilesCache] = useState<Record<string, FileInfo[]>>({});
  const [loadedKeys, setLoadedKeys] = useState<string[]>([]);
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);
  const [checkedKeys, setCheckedKeys] = useState<string[]>([]);
  const [saving, setSaving] = useState(false);

  // Build sets of already-added datasets at each level for disabling
  const { existingPlugins, existingSchemas, existingTables } = useMemo(() => {
    const pPlugins = new Set<string>();
    const pSchemas = new Set<string>();
    const pTables = new Set<string>();
    for (const ds of existingDatasets) {
      if (ds.type === 'plugin' && ds.schema) {
        pPlugins.add(ds.schema);
      } else if (ds.type === 'schema' && ds.schema) {
        pSchemas.add(ds.schema);
      } else if (ds.type === 'table' && ds.schema && ds.table) {
        pTables.add(`table:${ds.schema}:${ds.table}`);
        pTables.add(`file:${ds.schema}:${ds.table}`);
      }
    }
    return { existingPlugins: pPlugins, existingSchemas: pSchemas, existingTables: pTables };
  }, [existingDatasets]);

  /** Returns true if the key is already covered by an existing dataset ref */
  const isExisting = useCallback(
    (key: string) => isCoveredByExisting(key, existingPlugins, existingSchemas, existingTables),
    [existingPlugins, existingSchemas, existingTables],
  );

  // Build a file/directory tree node
  const buildFileNode = useCallback(
    (file: FileInfo, schema: string, parentPath: string): DataNode => {
      const fullPath = parentPath ? `${parentPath}/${file.name}` : file.name;

      if (file.isDirectory) {
        const dirKey = `dir:${schema}:${fullPath}`;
        const children = filesCache[dirKey];
        const covered = isExisting(dirKey);
        return {
          key: dirKey,
          title: covered ? <Text type="secondary">{file.name} (added)</Text> : file.name,
          icon: <FolderOutlined />,
          children: children
            ? children.map((f) => buildFileNode(f, schema, fullPath))
            : undefined,
          isLeaf: false,
          disableCheckbox: covered,
          disabled: covered,
        };
      }

      const key = `file:${schema}:${fullPath}`;
      const covered = isExisting(key);
      return {
        key,
        title: covered ? <Text type="secondary">{file.name} (added)</Text> : file.name,
        icon: getFileIcon(file.name) || <FileOutlined />,
        isLeaf: true,
        disableCheckbox: covered,
        disabled: covered,
      };
    },
    [filesCache, isExisting],
  );

  // Build tree data from caches
  const treeData = useMemo((): DataNode[] => {
    if (!plugins) {
      return [];
    }

    return plugins.map((plugin) => {
      const schemas = schemasCache[plugin.name] || [];
      const pluginCovered = isExisting(`plugin:${plugin.name}`);

      const children: DataNode[] = schemas.map((schema) => {
        const pluginType = plugin.type;
        const fileBased = isFileBasedPlugin(pluginType, plugin.name);
        const schemaCovered = isExisting(`schema:${schema.name}`);

        let schemaChildren: DataNode[] | undefined;

        if (fileBased) {
          const cacheKey = `schema:${schema.name}`;
          const files = filesCache[cacheKey];
          if (files) {
            schemaChildren = files.map((f) => buildFileNode(f, schema.name, ''));
          }
        } else {
          const tables = tablesCache[schema.name];
          if (tables) {
            schemaChildren = tables.map((t) => {
              const key = `table:${schema.name}:${t.name}`;
              const covered = isExisting(key);
              return {
                key,
                title: covered ? <Text type="secondary">{t.name} (added)</Text> : t.name,
                icon: <TableOutlined />,
                isLeaf: true,
                disableCheckbox: covered,
                disabled: covered,
              };
            });
          }
        }

        return {
          key: `schema:${schema.name}`,
          title: schemaCovered
            ? <Text type="secondary">{schema.name.includes('.') ? schema.name.split('.').slice(1).join('.') : schema.name} (added)</Text>
            : (schema.name.includes('.') ? schema.name.split('.').slice(1).join('.') : schema.name),
          icon: <DatabaseOutlined />,
          children: schemaChildren,
          isLeaf: false,
          disableCheckbox: schemaCovered,
          disabled: schemaCovered,
        };
      });

      return {
        key: `plugin:${plugin.name}`,
        title: pluginCovered
          ? <Text type="secondary">{plugin.name} (added)</Text>
          : plugin.name,
        icon: getPluginIcon(plugin.type, plugin.name),
        children: schemas.length > 0 ? children : undefined,
        isLeaf: false,
        disableCheckbox: pluginCovered,
        disabled: pluginCovered,
      };
    });
  }, [plugins, schemasCache, tablesCache, filesCache, isExisting, buildFileNode]);

  // Lazy-load on expand
  const loadData = useCallback(
    async (node: EventDataNode<DataNode>): Promise<void> => {
      const key = node.key as string;

      if (key.startsWith('plugin:')) {
        const pluginName = key.replace('plugin:', '');
        if (!schemasCache[pluginName]) {
          const schemas = await getPluginSchemas(pluginName);
          setSchemasCache((prev) => ({ ...prev, [pluginName]: schemas }));
        }
      } else if (key.startsWith('schema:')) {
        const schemaName = key.replace('schema:', '');
        const pluginName = schemaName.split('.')[0];
        const plugin = plugins?.find((p) => p.name === pluginName);
        const pluginType = plugin?.type || '';
        const fileBased = isFileBasedPlugin(pluginType, pluginName);

        if (fileBased) {
          if (!filesCache[key]) {
            const files = await getFiles(schemaName);
            setFilesCache((prev) => ({ ...prev, [key]: files }));
          }
        } else {
          if (!tablesCache[schemaName]) {
            const tables = await getTables(schemaName);
            setTablesCache((prev) => ({ ...prev, [schemaName]: tables }));
          }
        }
      } else if (key.startsWith('dir:')) {
        const parts = key.split(':');
        const schemaName = parts[1];
        const dirPath = parts.slice(2).join(':');
        if (!filesCache[key]) {
          const files = await getFiles(schemaName, dirPath);
          setFilesCache((prev) => ({ ...prev, [key]: files }));
        }
      }

      setLoadedKeys((prev) => [...prev, key]);
    },
    [plugins, schemasCache, tablesCache, filesCache],
  );

  // Handle check — with checkStrictly, we get { checked, halfChecked }
  const handleCheck = useCallback(
    (checked: React.Key[] | { checked: React.Key[]; halfChecked: React.Key[] }) => {
      const keys = Array.isArray(checked) ? checked : checked.checked;
      setCheckedKeys(keys as string[]);
    },
    [],
  );

  // Compute the effective new selections (not covered by existing)
  const newSelections = useMemo(
    () => checkedKeys.filter((k) => !isExisting(k)),
    [checkedKeys, isExisting],
  );

  // Prune redundant selections: if a plugin is checked, remove its schemas/tables.
  // If a schema is checked, remove its tables.
  const prunedSelections = useMemo(() => {
    const selectedPlugins = new Set<string>();
    const selectedSchemas = new Set<string>();
    const result: string[] = [];

    // First pass: collect plugins and schemas
    for (const key of newSelections) {
      if (key.startsWith('plugin:')) {
        selectedPlugins.add(key.replace('plugin:', ''));
      } else if (key.startsWith('schema:')) {
        selectedSchemas.add(key.replace('schema:', ''));
      }
    }

    // Second pass: prune children covered by a parent selection
    for (const key of newSelections) {
      if (key.startsWith('plugin:')) {
        result.push(key);
      } else if (key.startsWith('schema:')) {
        const schemaName = key.replace('schema:', '');
        const pluginName = schemaName.split('.')[0];
        if (!selectedPlugins.has(pluginName)) {
          result.push(key);
        }
      } else if (key.startsWith('table:') || key.startsWith('file:')) {
        const parts = key.split(':');
        const schema = parts[1];
        const pluginName = schema.split('.')[0];
        if (!selectedPlugins.has(pluginName) && !selectedSchemas.has(schema)) {
          result.push(key);
        }
      } else if (key.startsWith('dir:')) {
        const parts = key.split(':');
        const schema = parts[1];
        const pluginName = schema.split('.')[0];
        if (!selectedPlugins.has(pluginName) && !selectedSchemas.has(schema)) {
          result.push(key);
        }
      }
    }

    return result;
  }, [newSelections]);

  const selectedCount = prunedSelections.length;

  // Handle add — sequential to avoid backend race conditions on the same project record
  const handleAdd = useCallback(async () => {
    if (prunedSelections.length === 0) {
      return;
    }

    setSaving(true);
    try {
      const datasets: DatasetRef[] = prunedSelections.map((key) => {
        if (key.startsWith('plugin:')) {
          const pluginName = key.replace('plugin:', '');
          return {
            id: '',
            type: 'plugin' as const,
            schema: pluginName,
            label: pluginName,
          };
        }
        if (key.startsWith('schema:')) {
          const schemaName = key.replace('schema:', '');
          return {
            id: '',
            type: 'schema' as const,
            schema: schemaName,
            label: schemaName,
          };
        }
        if (key.startsWith('table:')) {
          const [, schema, table] = key.split(':');
          return {
            id: '',
            type: 'table' as const,
            schema,
            table,
            label: `${schema}.${table}`,
          };
        }
        // file: or dir: key
        const parts = key.split(':');
        const schema = parts[1];
        const filePath = parts.slice(2).join(':');
        return {
          id: '',
          type: 'table' as const,
          schema,
          table: filePath,
          label: `${schema}/${filePath}`,
        };
      });

      for (const ds of datasets) {
        await addDataset(projectId, ds);
      }
      message.success(`Added ${datasets.length} dataset${datasets.length > 1 ? 's' : ''}`);
      setCheckedKeys([]);
      onSuccess();
    } catch (err) {
      message.error(`Failed to add datasets: ${(err as Error).message}`);
    } finally {
      setSaving(false);
    }
  }, [prunedSelections, projectId, onSuccess]);

  const handleClose = useCallback(() => {
    setCheckedKeys([]);
    onClose();
  }, [onClose]);

  // Describe what will be added
  const selectionSummary = useMemo(() => {
    if (selectedCount === 0) {
      return '';
    }
    const pluginCount = prunedSelections.filter((k) => k.startsWith('plugin:')).length;
    const schemaCount = prunedSelections.filter((k) => k.startsWith('schema:')).length;
    const tableCount = prunedSelections.filter(
      (k) => k.startsWith('table:') || k.startsWith('file:') || k.startsWith('dir:')
    ).length;

    const parts: string[] = [];
    if (pluginCount > 0) {
      parts.push(`${pluginCount} plugin${pluginCount > 1 ? 's' : ''}`);
    }
    if (schemaCount > 0) {
      parts.push(`${schemaCount} schema${schemaCount > 1 ? 's' : ''}`);
    }
    if (tableCount > 0) {
      parts.push(`${tableCount} table${tableCount > 1 ? 's' : ''}/file${tableCount > 1 ? 's' : ''}`);
    }
    return parts.join(', ');
  }, [selectedCount, prunedSelections]);

  return (
    <Modal
      title="Add Datasets"
      open={open}
      onOk={handleAdd}
      onCancel={handleClose}
      confirmLoading={saving}
      okText={selectedCount > 0 ? `Add ${selectedCount} item${selectedCount > 1 ? 's' : ''}` : 'Add'}
      okButtonProps={{ disabled: selectedCount === 0 }}
      width={600}
      styles={{ body: { maxHeight: 500, overflow: 'auto' } }}
    >
      {pluginsLoading ? (
        <div style={{ textAlign: 'center', padding: 40 }}>
          <Spin />
        </div>
      ) : !plugins || plugins.length === 0 ? (
        <Empty description="No storage plugins available" />
      ) : (
        <Tree
          showIcon
          checkable
          checkStrictly
          blockNode
          treeData={treeData}
          expandedKeys={expandedKeys}
          loadedKeys={loadedKeys}
          loadData={loadData}
          onExpand={(keys) => setExpandedKeys(keys as string[])}
          checkedKeys={checkedKeys}
          onCheck={handleCheck}
        />
      )}
      {selectionSummary && (
        <div style={{ marginTop: 8, color: '#888', fontSize: 12 }}>
          Selected: {selectionSummary}
        </div>
      )}
    </Modal>
  );
}
