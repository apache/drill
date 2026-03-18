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
import { useState, useEffect, useMemo, useRef } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { Modal, Input, List, Typography, Space } from 'antd';
import { SearchOutlined, FolderOutlined, CheckOutlined } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { getProjects } from '../../api/projects';

const { Text } = Typography;

export default function CommandPalette() {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const navigate = useNavigate();
  const location = useLocation();
  const inputRef = useRef<any>(null);

  // Extract current project ID from URL
  const currentProjectId = location.pathname.match(/\/projects\/([^/]+)/)?.[1];

  const { data: projects } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
    enabled: open,
  });

  // Filter projects
  const filtered = useMemo(() => {
    if (!projects) {
      return [];
    }
    if (!search) {
      return projects;
    }
    const lower = search.toLowerCase();
    return projects.filter(p =>
      p.name.toLowerCase().includes(lower) ||
      (p.description && p.description.toLowerCase().includes(lower)) ||
      p.tags?.some(t => t.toLowerCase().includes(lower))
    );
  }, [projects, search]);

  // Keyboard shortcut
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        setOpen(prev => !prev);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, []);

  // Focus input when modal opens
  useEffect(() => {
    if (open) {
      setSearch('');
      setTimeout(() => inputRef.current?.focus(), 100);
    }
  }, [open]);

  const selectProject = (id: string) => {
    setOpen(false);
    navigate(`/projects/${id}/query`);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && filtered.length > 0) {
      selectProject(filtered[0].id);
    }
  };

  return (
    <Modal
      open={open}
      onCancel={() => setOpen(false)}
      footer={null}
      closable={false}
      width={500}
      styles={{ body: { padding: 0 } }}
    >
      <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--color-border-secondary)' }}>
        <Input
          ref={inputRef}
          placeholder="Search projects..."
          prefix={<SearchOutlined />}
          value={search}
          onChange={e => setSearch(e.target.value)}
          onKeyDown={handleKeyDown}
          allowClear
          variant="borderless"
          size="large"
        />
      </div>
      <List
        dataSource={filtered}
        style={{ maxHeight: 350, overflow: 'auto' }}
        locale={{ emptyText: search ? 'No matching projects' : 'No projects' }}
        renderItem={project => (
          <List.Item
            style={{ cursor: 'pointer', padding: '10px 16px' }}
            onClick={() => selectProject(project.id)}
          >
            <Space style={{ width: '100%' }}>
              {project.id === currentProjectId
                ? <CheckOutlined style={{ color: 'var(--color-primary)' }} />
                : <FolderOutlined />
              }
              <div>
                <Text strong>{project.name}</Text>
                {project.description && (
                  <Text type="secondary" style={{ display: 'block', fontSize: 12 }} ellipsis>
                    {project.description}
                  </Text>
                )}
              </div>
            </Space>
          </List.Item>
        )}
      />
      <div style={{
        padding: '8px 16px',
        borderTop: '1px solid var(--color-border-secondary)',
        display: 'flex',
        gap: 16,
      }}>
        <Text type="secondary" style={{ fontSize: 11 }}>
          <kbd style={{ padding: '1px 4px', border: '1px solid var(--color-border)', borderRadius: 3, fontSize: 10 }}>Enter</kbd> to select
        </Text>
        <Text type="secondary" style={{ fontSize: 11 }}>
          <kbd style={{ padding: '1px 4px', border: '1px solid var(--color-border)', borderRadius: 3, fontSize: 10 }}>Esc</kbd> to close
        </Text>
      </div>
    </Modal>
  );
}
