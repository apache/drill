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
import { useState, useCallback } from 'react';
import { Input, Space, Tabs, Upload, message, Spin } from 'antd';
import { PictureOutlined, InboxOutlined } from '@ant-design/icons';
import { uploadImage } from '../../api/dashboards';
import { sanitizeImageUrl } from '../../utils/sanitize';
import type { RcFile } from 'antd/es/upload';

const ALLOWED_TYPES = ['image/jpeg', 'image/png', 'image/gif', 'image/svg+xml', 'image/webp'];
const MAX_FILE_SIZE = 5 * 1024 * 1024; // 5 MB
const UPLOADED_IMAGE_PREFIX = '/api/v1/dashboards/images/';

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
  const [uploading, setUploading] = useState(false);
  const altText = config?.imageAlt || '';

  const defaultTab = content?.startsWith(UPLOADED_IMAGE_PREFIX) ? 'upload' : 'url';

  const handleBeforeUpload = useCallback((file: RcFile) => {
    if (!ALLOWED_TYPES.includes(file.type)) {
      message.error('Invalid file type. Allowed: JPG, PNG, GIF, SVG, WebP');
      return false;
    }
    if (file.size > MAX_FILE_SIZE) {
      message.error('File exceeds maximum size of 5 MB');
      return false;
    }

    setUploading(true);
    uploadImage(file)
      .then((result) => {
        onContentChange(result.url);
        setImgError(false);
        message.success(`Uploaded ${result.filename}`);
      })
      .catch(() => {
        message.error('Failed to upload image');
      })
      .finally(() => {
        setUploading(false);
      });

    return false; // Prevent antd default upload behavior
  }, [onContentChange]);

  if (editMode) {
    return (
      <div style={{ padding: 12 }}>
        <Space direction="vertical" style={{ width: '100%' }}>
          <Tabs
            defaultActiveKey={defaultTab}
            items={[
              {
                key: 'url',
                label: 'URL',
                children: (
                  <Input
                    value={content}
                    onChange={(e) => {
                      onContentChange(e.target.value);
                      setImgError(false);
                    }}
                    placeholder="Image URL..."
                    addonBefore="URL"
                  />
                ),
              },
              {
                key: 'upload',
                label: 'Upload',
                children: (
                  <Spin spinning={uploading} tip="Uploading...">
                    <Upload.Dragger
                      accept=".jpg,.jpeg,.png,.gif,.svg,.webp"
                      showUploadList={false}
                      beforeUpload={handleBeforeUpload}
                    >
                      <p className="ant-upload-drag-icon">
                        <InboxOutlined />
                      </p>
                      <p className="ant-upload-text">Click or drag an image file here</p>
                      <p className="ant-upload-hint">
                        JPG, PNG, GIF, SVG, or WebP — max 5 MB
                      </p>
                    </Upload.Dragger>
                  </Spin>
                ),
              },
            ]}
          />
          <Input
            value={altText}
            onChange={(e) => onConfigChange({ ...config, imageAlt: e.target.value })}
            placeholder="Alt text (optional)"
            addonBefore="Alt"
          />
          {content && !imgError && sanitizeImageUrl(content) && (
            <div className="image-panel">
              <img
                src={sanitizeImageUrl(content)}
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

  const safeUrl = sanitizeImageUrl(content);
  if (!safeUrl) {
    return (
      <div className="image-panel">
        <PictureOutlined style={{ fontSize: 48, color: '#d9d9d9' }} />
      </div>
    );
  }

  return (
    <div className="image-panel">
      <img
        src={safeUrl}
        alt={altText}
        onError={() => setImgError(true)}
      />
    </div>
  );
}
