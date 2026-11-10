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
import { useCallback, useRef } from 'react';
import type { UploadedFile } from '../api/uploads';
import { useProjectFileUpload } from './useProjectFileUpload';

function describe(files: UploadedFile[]): string {
  if (files.length === 1) {
    return `I added the file ${files[0].name} to this project. You can query it as ${files[0].schema}. Take a look at it.`;
  }
  const list = files.map((f) => `${f.name} (query it as ${f.schema})`).join(', ');
  return `I added these files to this project: ${list}. Take a look at them.`;
}

/**
 * Returns a handler for a file dropped on a conversation: it uploads the file into the project,
 * then sends an ordinary user message naming the file and its table path so Prospector can query
 * it with the tools it already has. Files dropped together are announced in one message, so a
 * multi-file drop does not fire several sends at once.
 */
export function useConversationFileDrop(
  projectId: string,
  sendMessage: (text: string) => void,
): (file: File) => Promise<void> {
  const upload = useProjectFileUpload(projectId);
  // Uploads started in the same burst (one drop event) share a single message.
  const batch = useRef<Promise<UploadedFile | undefined>[] | null>(null);

  return useCallback(
    async (file: File) => {
      const pending = upload(file);
      if (batch.current) {
        batch.current.push(pending);
        return;
      }
      const mine: Promise<UploadedFile | undefined>[] = [pending];
      batch.current = mine;
      await Promise.resolve(); // let the rest of the same drop join the batch
      batch.current = null;
      const uploaded = (await Promise.all(mine)).filter((f): f is UploadedFile => !!f);
      if (uploaded.length > 0) {
        sendMessage(describe(uploaded));
      }
    },
    [upload, sendMessage],
  );
}
