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
import { getErrorMessage } from '../api/client';
import {
  uploadProjectFile,
  getUploadStatus,
  type UploadedFile,
  UploadCollisionError,
  UploadNotConfiguredError,
} from '../api/uploads';

const NOT_CONFIGURED_MSG =
  'File uploads are not set up on this cluster. Ask an administrator to configure an upload target.';

/**
 * Returns a function that uploads a file into the project and reports the outcome to the user.
 * It resolves with the stored file, or undefined when nothing was uploaded (a failure, or a
 * replace the user declined).
 */
export function useProjectFileUpload(projectId: string): (file: File) => Promise<UploadedFile | undefined> {
  const queryClient = useQueryClient();

  return useCallback(
    async function upload(file: File): Promise<UploadedFile | undefined> {
      const attempt = async (overwrite: boolean): Promise<UploadedFile | undefined> => {
        try {
          const result = await uploadProjectFile(projectId, file, overwrite ? { overwrite: true } : undefined);
          // tablePath, not path: path is the bare file name, and the point of the toast
          // is to hand the user something they can paste into a query.
          message.success(`Added ${file.name} as ${result.tablePath}`);
          queryClient.invalidateQueries({ queryKey: ['schemaCache', projectId] });
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
          return result;
        } catch (error) {
          if (error instanceof UploadCollisionError) {
            return new Promise<UploadedFile | undefined>((resolve) => {
              Modal.confirm({
                title: 'Replace existing file?',
                content: `A file named ${file.name} is already in this project. Replace it with the one you dropped?`,
                okText: 'Replace',
                cancelText: 'Cancel',
                onOk: async () => resolve(await attempt(true)),
                onCancel: () => resolve(undefined),
              });
            });
          } else if (error instanceof UploadNotConfiguredError) {
            message.error(NOT_CONFIGURED_MSG);
          } else {
            // getErrorMessage, not error.message: axios reports "Request failed with
            // status code 403" and buries the server's reason in response.data.message.
            const reason = getErrorMessage(error);
            message.error(`Could not upload ${file.name}${reason ? `: ${reason}` : ''}`);
          }
        }
        return undefined;
      };

      try {
        const status = await getUploadStatus();
        if (!status.configured) {
          message.error(NOT_CONFIGURED_MSG);
          return undefined;
        }
      } catch {
        // Status is only a pre-check; the upload itself reports a real problem.
      }
      return attempt(false);
    },
    [projectId, queryClient],
  );
}

