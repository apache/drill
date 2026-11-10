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
import { useProjectFileUpload } from './useProjectFileUpload';

/**
 * Returns a handler for a file dropped on a conversation: it uploads the file into the project,
 * then sends an ordinary user message naming the file and its table path so Prospector can query
 * it with the tools it already has.
 */
export function useConversationFileDrop(
  projectId: string,
  sendMessage: (text: string) => void,
): (file: File) => Promise<void> {
  const upload = useProjectFileUpload(projectId);

  return useCallback(
    async (file: File) => {
      const uploaded = await upload(file);
      if (uploaded) {
        sendMessage(`I added the file ${uploaded.name} to this project. You can query it as ${uploaded.schema}. Take a look at it.`);
      }
    },
    [upload, sendMessage],
  );
}
