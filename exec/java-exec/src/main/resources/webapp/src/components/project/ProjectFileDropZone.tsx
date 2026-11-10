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
import { useCallback, useRef, useState } from 'react';
import type { DragEvent, ReactNode } from 'react';
import { Modal, Typography, message } from 'antd';
import { InboxOutlined } from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import {
  uploadProjectFile,
  getUploadStatus,
  UploadCollisionError,
  UploadNotConfiguredError,
} from '../../api/uploads';
import { useProjectContext } from '../../contexts/ProjectContext';

const NOT_CONFIGURED_MSG =
  'File uploads are not set up on this cluster. Ask an administrator to configure an upload target.';

/** True only for drags that carry files from outside the browser, not in-app drags such as tree nodes. */
function carriesFiles(e: DragEvent): boolean {
  return Array.from(e.dataTransfer?.types ?? []).includes('Files');
}

/** Returns a function that uploads a file into the project and reports the outcome to the user. */
export function useProjectFileUpload(projectId: string): (file: File) => Promise<void> {
  const queryClient = useQueryClient();

  return useCallback(
    async function upload(file: File): Promise<void> {
      const attempt = async (overwrite: boolean): Promise<void> => {
        try {
          const result = await uploadProjectFile(projectId, file, overwrite ? { overwrite: true } : undefined);
          message.success(`Added ${file.name} as ${result.path}`);
          queryClient.invalidateQueries({ queryKey: ['schemaCache', projectId] });
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
        } catch (error) {
          if (error instanceof UploadCollisionError) {
            Modal.confirm({
              title: 'Replace existing file?',
              content: `A file named ${file.name} is already in this project. Replace it with the one you dropped?`,
              okText: 'Replace',
              cancelText: 'Cancel',
              onOk: () => attempt(true),
            });
          } else if (error instanceof UploadNotConfiguredError) {
            message.error(NOT_CONFIGURED_MSG);
          } else {
            message.error(`Could not upload ${file.name}`);
          }
        }
      };

      try {
        const status = await getUploadStatus();
        if (!status.configured) {
          message.error(NOT_CONFIGURED_MSG);
          return;
        }
      } catch {
        // Status is only a pre-check; the upload itself reports a real problem.
      }
      await attempt(false);
    },
    [projectId, queryClient],
  );
}

interface Props {
  projectId: string;
  children: ReactNode;
}

export default function ProjectFileDropZone({ projectId, children }: Props) {
  const { project } = useProjectContext();
  const upload = useProjectFileUpload(projectId);
  const [dragging, setDragging] = useState(false);
  // dragenter/dragleave fire for every child crossed; count them so the overlay does not flicker.
  const depth = useRef(0);

  const onDragEnter = (e: DragEvent) => {
    if (!carriesFiles(e)) {
      return;
    }
    depth.current += 1;
    setDragging(true);
  };
  const onDragLeave = (e: DragEvent) => {
    if (!carriesFiles(e)) {
      return;
    }
    depth.current = Math.max(0, depth.current - 1);
    if (depth.current === 0) {
      setDragging(false);
    }
  };
  const onDragOver = (e: DragEvent) => {
    if (carriesFiles(e)) {
      e.preventDefault(); // required for the browser to allow a drop
    }
  };
  const onDrop = (e: DragEvent) => {
    if (!carriesFiles(e)) {
      return;
    }
    e.preventDefault();
    depth.current = 0;
    setDragging(false);
    Array.from(e.dataTransfer.files).forEach((f) => {
      void upload(f);
    });
  };

  return (
    <div
      style={{ flex: 1, position: 'relative', display: 'flex', flexDirection: 'column' }}
      onDragEnter={onDragEnter}
      onDragLeave={onDragLeave}
      onDragOver={onDragOver}
      onDrop={onDrop}
    >
      {children}
      {dragging && (
        <div
          style={{
            position: 'absolute',
            inset: 0,
            zIndex: 1000,
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            background: 'rgba(22, 119, 255, 0.12)',
            border: '2px dashed #1677ff',
            pointerEvents: 'none',
          }}
        >
          <InboxOutlined style={{ fontSize: 48, color: '#1677ff' }} />
          <Typography.Title level={4} style={{ margin: 8 }}>
            Drop to add to {project?.name}
          </Typography.Title>
        </div>
      )}
    </div>
  );
}
