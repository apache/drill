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
import { useRef, useState } from 'react';
import type { DragEvent, ReactNode } from 'react';
import { Typography } from 'antd';
import { InboxOutlined } from '@ant-design/icons';
import { useProjectContext } from '../../contexts/ProjectContext';
import { useProjectFileUpload } from '../../hooks/useProjectFileUpload';
import { isDropHandled } from '../../utils/dropHandled';

/** True only for drags that carry files from outside the browser, not in-app drags such as tree nodes. */
function carriesFiles(e: DragEvent): boolean {
  return Array.from(e.dataTransfer?.types ?? []).includes('Files');
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
    if (isDropHandled(e)) {
      return; // a drop target inside the zone took these files
    }
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
            background: 'rgba(22, 119, 255, 0.12)',
            border: '2px dashed #1677ff',
            pointerEvents: 'none',
          }}
        >
          {/* Sticky so the message stays in view however far the page is scrolled. */}
          <div
            style={{
              position: 'sticky',
              top: 0,
              height: '100vh',
              maxHeight: '100%',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <InboxOutlined style={{ fontSize: 48, color: '#1677ff' }} />
            <Typography.Title level={4} style={{ margin: 8 }}>
              Drop to add to {project?.name}
            </Typography.Title>
          </div>
        </div>
      )}
    </div>
  );
}
