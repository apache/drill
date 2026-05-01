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
import { useNavigate } from 'react-router-dom';
import {
  Card,
  List,
  Button,
  Tag,
  Empty,
  Space,
  Typography,
  Tooltip,
  Popconfirm,
  message,
} from 'antd';
import {
  DatabaseOutlined,
  PlusOutlined,
  DeleteOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { removeDataset } from '../api/projects';
import { useProjectContext } from '../contexts/ProjectContext';
import { DatasetPickerModal } from '../components/project';

const { Title } = Typography;

export default function ProjectDataSourcesPage() {
  const { project, projectId } = useProjectContext();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [pickerOpen, setPickerOpen] = useState(false);

  const removeDatasetMutation = useMutation({
    mutationFn: (datasetId: string) => removeDataset(projectId!, datasetId),
    onSuccess: () => {
      message.success('Dataset removed');
      queryClient.invalidateQueries({ queryKey: ['project', projectId] });
    },
  });

  const datasets = project?.datasets || [];

  return (
    <div style={{ padding: 24 }}>
      <Card>
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Title level={3} style={{ margin: 0 }}>
              Data Sources
            </Title>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setPickerOpen(true)}
            >
              Add Dataset
            </Button>
          </div>

          {datasets.length === 0 ? (
            <Empty description="No datasets added to this project yet" />
          ) : (
            <List
              dataSource={datasets}
              renderItem={(dataset) => (
                <List.Item
                  actions={[
                    dataset.type !== 'saved_query' && dataset.schema ? (
                      <Tooltip title="Data source settings" key="settings">
                        <Button
                          type="text"
                          icon={<SettingOutlined />}
                          size="small"
                          onClick={() => navigate(`/datasources/${encodeURIComponent(dataset.schema!.split('.')[0])}`)}
                        />
                      </Tooltip>
                    ) : null,
                    <Popconfirm
                      key="remove"
                      title="Remove this dataset?"
                      onConfirm={() => removeDatasetMutation.mutate(dataset.id)}
                      okText="Remove"
                      cancelText="Cancel"
                    >
                      <Button type="text" danger icon={<DeleteOutlined />} size="small" />
                    </Popconfirm>,
                  ].filter(Boolean)}
                >
                  <List.Item.Meta
                    avatar={<DatabaseOutlined style={{ fontSize: 20 }} />}
                    title={dataset.label || dataset.schema || '(unnamed)'}
                    description={
                      dataset.type === 'plugin'
                        ? `All schemas in ${dataset.schema}`
                        : dataset.type === 'schema'
                        ? `All tables in ${dataset.schema}`
                        : dataset.type === 'saved_query'
                        ? `Saved Query: ${dataset.savedQueryId}`
                        : `${dataset.schema}.${dataset.table}`
                    }
                  />
                  <Tag>
                    {dataset.type === 'plugin' ? 'Plugin' :
                     dataset.type === 'schema' ? 'Schema' :
                     dataset.type === 'saved_query' ? 'Saved Query' : 'Table'}
                  </Tag>
                </List.Item>
              )}
            />
          )}
        </Space>
      </Card>

      <DatasetPickerModal
        open={pickerOpen}
        projectId={projectId!}
        existingDatasets={datasets}
        onClose={() => setPickerOpen(false)}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
          setPickerOpen(false);
        }}
      />
    </div>
  );
}
