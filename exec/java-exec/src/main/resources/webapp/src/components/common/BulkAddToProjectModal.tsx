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
import { Modal, Input, List, Typography, Space, message } from 'antd';
import { SearchOutlined, FolderOutlined } from '@ant-design/icons';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getProjects, addSavedQuery, addVisualization, addDashboard } from '../../api/projects';

const { Text } = Typography;

interface Props {
  open: boolean;
  onClose: () => void;
  itemIds: string[];
  itemType: 'savedQuery' | 'visualization' | 'dashboard';
}

export default function BulkAddToProjectModal({ open, onClose, itemIds, itemType }: Props) {
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(false);
  const queryClient = useQueryClient();

  const { data: projects } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
    enabled: open,
  });

  const filtered = useMemo(() => {
    if (!projects) {
      return [];
    }
    if (!search) {
      return projects;
    }
    const lower = search.toLowerCase();
    return projects.filter(p => p.name.toLowerCase().includes(lower));
  }, [projects, search]);

  const handleAdd = async (projectId: string) => {
    setLoading(true);
    try {
      const addFn = itemType === 'savedQuery' ? addSavedQuery
        : itemType === 'visualization' ? addVisualization
        : addDashboard;
      for (const id of itemIds) {
        await addFn(projectId, id);
      }
      message.success(`Added ${itemIds.length} item${itemIds.length > 1 ? 's' : ''} to project`);
      queryClient.invalidateQueries({ queryKey: ['project', projectId] });
      onClose();
    } catch (err) {
      message.error(`Failed: ${(err as Error).message}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Modal
      title={`Add ${itemIds.length} item${itemIds.length > 1 ? 's' : ''} to Project`}
      open={open}
      onCancel={onClose}
      footer={null}
      width={480}
    >
      <Input
        placeholder="Search projects..."
        prefix={<SearchOutlined />}
        value={search}
        onChange={e => setSearch(e.target.value)}
        allowClear
        style={{ marginBottom: 12 }}
      />
      <List
        dataSource={filtered}
        loading={loading || !projects}
        style={{ maxHeight: 320, overflow: 'auto' }}
        renderItem={project => (
          <List.Item
            style={{ cursor: loading ? 'wait' : 'pointer', padding: '8px 12px' }}
            onClick={() => !loading && handleAdd(project.id)}
          >
            <Space>
              <FolderOutlined />
              <Text strong>{project.name}</Text>
            </Space>
          </List.Item>
        )}
      />
    </Modal>
  );
}
