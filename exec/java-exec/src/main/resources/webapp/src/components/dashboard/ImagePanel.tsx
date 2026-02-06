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
import { Input, Space } from 'antd';
import { PictureOutlined } from '@ant-design/icons';

interface ImagePanelProps {
  content: string;
  config?: Record<string, string>;
  editMode: boolean;
  onContentChange: (content: string) => void;
  onConfigChange: (config: Record<string, string>) => void;
}

export default function ImagePanel({
  content,
  config,
  editMode,
  onContentChange,
  onConfigChange,
}: ImagePanelProps) {
  const [imgError, setImgError] = useState(false);
  const altText = config?.imageAlt || '';

  if (editMode) {
    return (
      <div style={{ padding: 12 }}>
        <Space direction="vertical" style={{ width: '100%' }}>
          <Input
            value={content}
            onChange={(e) => {
              onContentChange(e.target.value);
              setImgError(false);
            }}
            placeholder="Image URL..."
            addonBefore="URL"
          />
          <Input
            value={altText}
            onChange={(e) => onConfigChange({ ...config, imageAlt: e.target.value })}
            placeholder="Alt text (optional)"
            addonBefore="Alt"
          />
          {content && !imgError && (
            <div className="image-panel">
              <img
                src={content}
                alt={altText}
                onError={() => setImgError(true)}
              />
            </div>
          )}
        </Space>
      </div>
    );
  }

  if (!content || imgError) {
    return (
      <div className="image-panel">
        <PictureOutlined style={{ fontSize: 48, color: '#d9d9d9' }} />
      </div>
    );
  }

  return (
    <div className="image-panel">
      <img
        src={content}
        alt={altText}
        onError={() => setImgError(true)}
      />
    </div>
  );
}
