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
import { useState } from 'react';
import { Modal, Switch, Input, Button, Space, Tag, Typography, message, Divider } from 'antd';
import { GlobalOutlined, LockOutlined, UserAddOutlined, CloseOutlined } from '@ant-design/icons';
import { updateProject } from '../../api/projects';
import type { Project } from '../../types';

const { Text } = Typography;

interface ShareModalProps {
  open: boolean;
  project: Project;
  onClose: () => void;
  onSuccess: () => void;
}

export default function ShareModal({ open, project, onClose, onSuccess }: ShareModalProps) {
  const [isPublic, setIsPublic] = useState(project.isPublic);
  const [sharedWith, setSharedWith] = useState<string[]>(project.sharedWith || []);
  const [newUser, setNewUser] = useState('');
  const [saving, setSaving] = useState(false);

  const handleAddUser = () => {
    const username = newUser.trim();
    if (!username) {
      return;
    }
    if (sharedWith.includes(username)) {
      message.warning('User already added');
      return;
    }
    setSharedWith([...sharedWith, username]);
    setNewUser('');
  };

  const handleRemoveUser = (user: string) => {
    setSharedWith(sharedWith.filter((u) => u !== user));
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      await updateProject(project.id, { isPublic, sharedWith });
      message.success('Sharing settings updated');
      onSuccess();
    } catch (err) {
      message.error(`Failed to update sharing: ${(err as Error).message}`);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal
      title="Share Project"
      open={open}
      onOk={handleSave}
      onCancel={onClose}
      confirmLoading={saving}
      okText="Save"
    >
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        {/* Public toggle */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Space>
            {isPublic ? (
              <GlobalOutlined style={{ color: '#52c41a', fontSize: 18 }} />
            ) : (
              <LockOutlined style={{ color: '#faad14', fontSize: 18 }} />
            )}
            <div>
              <Text strong>{isPublic ? 'Public' : 'Private'}</Text>
              <br />
              <Text type="secondary" style={{ fontSize: 12 }}>
                {isPublic
                  ? 'Anyone can view this project'
                  : 'Only you and shared users can view this project'}
              </Text>
            </div>
          </Space>
          <Switch checked={isPublic} onChange={setIsPublic} />
        </div>

        <Divider style={{ margin: '8px 0' }} />

        {/* Share with specific users */}
        <div>
          <Text strong>Share with users</Text>
          <Space.Compact style={{ width: '100%', marginTop: 8 }}>
            <Input
              placeholder="Enter username..."
              value={newUser}
              onChange={(e) => setNewUser(e.target.value)}
              onPressEnter={handleAddUser}
              prefix={<UserAddOutlined />}
            />
            <Button type="primary" onClick={handleAddUser}>
              Add
            </Button>
          </Space.Compact>
        </div>

        {/* Shared users list */}
        {sharedWith.length > 0 && (
          <div>
            {sharedWith.map((user) => (
              <Tag
                key={user}
                closable
                closeIcon={<CloseOutlined />}
                onClose={() => handleRemoveUser(user)}
                style={{ marginBottom: 4 }}
              >
                {user}
              </Tag>
            ))}
          </div>
        )}
      </Space>
    </Modal>
  );
}
