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
import { useCallback } from 'react';
import { message, Modal } from 'antd';
import { useQueryClient } from '@tanstack/react-query';
import {
  uploadProjectFile,
  getUploadStatus,
  UploadCollisionError,
  UploadNotConfiguredError,
} from '../api/uploads';

const NOT_CONFIGURED_MSG =
  'File uploads are not set up on this cluster. Ask an administrator to configure an upload target.';

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
            const reason = error instanceof Error && error.message ? `: ${error.message}` : '';
            message.error(`Could not upload ${file.name}${reason}`);
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

