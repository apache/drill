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
import { Input, Select, Space, Typography } from 'antd';

const { Title, Text } = Typography;

interface TitlePanelProps {
  content: string;
  config?: Record<string, string>;
  editMode: boolean;
  onContentChange: (content: string) => void;
  onConfigChange: (config: Record<string, string>) => void;
}

export default function TitlePanel({
  content,
  config,
  editMode,
  onContentChange,
  onConfigChange,
}: TitlePanelProps) {
  const subtitle = config?.subtitle || '';
  const textAlign = (config?.textAlign || 'center') as 'left' | 'center' | 'right';
  const backgroundColor = config?.backgroundColor || 'transparent';

  if (editMode) {
    return (
      <div style={{ padding: 12 }}>
        <Space direction="vertical" style={{ width: '100%' }}>
          <Input
            value={content}
            onChange={(e) => onContentChange(e.target.value)}
            placeholder="Title text..."
            addonBefore="Title"
          />
          <Input
            value={subtitle}
            onChange={(e) => onConfigChange({ ...config, subtitle: e.target.value })}
            placeholder="Subtitle (optional)"
            addonBefore="Subtitle"
          />
          <Space>
            <Select
              value={textAlign}
              onChange={(value) => onConfigChange({ ...config, textAlign: value })}
              options={[
                { value: 'left', label: 'Left' },
                { value: 'center', label: 'Center' },
                { value: 'right', label: 'Right' },
              ]}
              style={{ width: 100 }}
            />
            <Input
              value={backgroundColor !== 'transparent' ? backgroundColor : ''}
              onChange={(e) => onConfigChange({ ...config, backgroundColor: e.target.value || 'transparent' })}
              placeholder="#hex or color name"
              addonBefore="BG"
              style={{ width: 200 }}
            />
          </Space>
        </Space>
      </div>
    );
  }

  return (
    <div
      className="title-panel"
      style={{ textAlign, backgroundColor }}
    >
      <Title level={2} style={{ margin: 0 }}>
        {content || 'Untitled'}
      </Title>
      {subtitle && (
        <Text type="secondary" style={{ fontSize: 16, marginTop: 4 }}>
          {subtitle}
        </Text>
      )}
    </div>
  );
}
