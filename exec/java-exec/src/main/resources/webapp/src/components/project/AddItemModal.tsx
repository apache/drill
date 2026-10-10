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
import { Modal, Input, List, Button, Empty, message, Tag } from 'antd';
import { SearchOutlined, PlusOutlined, CheckOutlined } from '@ant-design/icons';
import { addSavedQuery, addVisualization, addDashboard } from '../../api/projects';

interface AddItemModalProps {
  open: boolean;
  type: 'savedQuery' | 'visualization' | 'dashboard';
  projectId: string;
  existingIds: string[];
  items: { id: string; name: string; description?: string }[];
  onClose: () => void;
  onSuccess: () => void;
}

const typeLabels = {
  savedQuery: 'Saved Query',
  visualization: 'Visualization',
  dashboard: 'Dashboard',
};

export default function AddItemModal({
  open,
  type,
  projectId,
  existingIds,
  items,
  onClose,
  onSuccess,
}: AddItemModalProps) {
  const [searchText, setSearchText] = useState('');
  const [loading, setLoading] = useState<string | null>(null);

  const filteredItems = useMemo(() => {
    if (!searchText) {
      return items;
    }
    const lower = searchText.toLowerCase();
    return items.filter(
      (item) =>
        item.name.toLowerCase().includes(lower) ||
        (item.description && item.description.toLowerCase().includes(lower))
    );
  }, [items, searchText]);

  const handleAdd = async (itemId: string) => {
    setLoading(itemId);
    try {
      if (type === 'savedQuery') {
        await addSavedQuery(projectId, itemId);
      } else if (type === 'visualization') {
        await addVisualization(projectId, itemId);
      } else {
        await addDashboard(projectId, itemId);
      }
      message.success(`${typeLabels[type]} added to project`);
      onSuccess();
    } catch (err) {
      message.error(`Failed to add ${typeLabels[type].toLowerCase()}: ${(err as Error).message}`);
    } finally {
      setLoading(null);
    }
  };

  return (
    <Modal
      title={`Add ${typeLabels[type]}`}
      open={open}
      onCancel={() => {
        setSearchText('');
        onClose();
      }}
      footer={null}
      width={600}
    >
      <Input
        placeholder={`Search ${typeLabels[type].toLowerCase()}s...`}
        prefix={<SearchOutlined />}
        value={searchText}
        onChange={(e) => setSearchText(e.target.value)}
        allowClear
        style={{ marginBottom: 16 }}
      />
      {filteredItems.length === 0 ? (
        <Empty description={`No ${typeLabels[type].toLowerCase()}s available`} />
      ) : (
        <List
          dataSource={filteredItems}
          style={{ maxHeight: 400, overflow: 'auto' }}
          renderItem={(item) => {
            const alreadyAdded = existingIds.includes(item.id);
            return (
              <List.Item
                actions={[
                  alreadyAdded ? (
                    <Tag key="added" color="green" icon={<CheckOutlined />}>Added</Tag>
                  ) : (
                    <Button
                      key="add"
                      type="primary"
                      size="small"
                      icon={<PlusOutlined />}
                      loading={loading === item.id}
                      onClick={() => handleAdd(item.id)}
                    >
                      Add
                    </Button>
                  ),
                ]}
              >
                <List.Item.Meta
                  title={item.name}
                  description={item.description}
                />
              </List.Item>
            );
          }}
        />
      )}
    </Modal>
  );
}
