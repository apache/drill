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
import { useEffect, useMemo, useState } from 'react';
import { Alert, Button, message } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { getPlugins } from '../../api/storage';
import {
  getUploadConfig,
  getUploadStatus,
  setUploadConfig,
} from '../../api/uploads';
import { getErrorMessage } from '../../api/client';
import type { WorkspaceConfig } from '../../types';

/** Admin card: choose the filesystem workspace that project file uploads are written to. */
export default function UploadTargetCard() {
  const queryClient = useQueryClient();
  const [selected, setSelected] = useState('');
  const [warning, setWarning] = useState<string | null>(null);

  const { data: plugins } = useQuery({ queryKey: ['plugins'], queryFn: getPlugins });
  const { data: status } = useQuery({ queryKey: ['uploadStatus'], queryFn: getUploadStatus });
  const { data: config } = useQuery({ queryKey: ['uploadConfig'], queryFn: getUploadConfig });

  // Only writable workspaces of enabled filesystem plugins are valid targets.
  const targets = useMemo(() => {
    const out: { plugin: string; workspace: string }[] = [];
    for (const p of plugins ?? []) {
      if (p.config?.type !== 'file' || p.config?.enabled === false) {
        continue;
      }
      const ws = (p.config.workspaces ?? {}) as Record<string, WorkspaceConfig>;
      for (const [name, w] of Object.entries(ws)) {
        if (w?.writable) {
          out.push({ plugin: p.name, workspace: name });
        }
      }
    }
    return out;
  }, [plugins]);

  useEffect(() => {
    if (config?.plugin && config?.workspace) {
      setSelected(`${config.plugin}.${config.workspace}`);
    }
  }, [config]);

  const save = useMutation({
    mutationFn: () => {
      const t = targets.find((x) => `${x.plugin}.${x.workspace}` === selected);
      return setUploadConfig({ ...t, maxSizeBytes: config?.maxSizeBytes });
    },
    onSuccess: (result) => {
      setWarning(result.warning ?? null);
      message.success('Upload target saved');
      queryClient.invalidateQueries({ queryKey: ['uploadStatus'] });
      queryClient.invalidateQueries({ queryKey: ['uploadConfig'] });
    },
    onError: (e) => message.error(getErrorMessage(e)),
  });

  const configured = status?.configured && status.plugin && status.workspace;

  return (
    <div className="upload-target-card" style={{ marginBottom: 16 }}>
      <h3>File uploads</h3>
      <p>
        {configured ? (
          <>Uploads are stored in <strong>{`${status?.plugin}.${status?.workspace}`}</strong></>
        ) : (
          <>
            <strong>Not configured</strong>. Pick a workspace to let users upload files into projects.
          </>
        )}
      </p>
      <select
        aria-label="Upload workspace"
        value={selected}
        onChange={(e) => setSelected(e.target.value)}
      >
        <option value="">Select a workspace</option>
        {targets.map((t) => (
          <option key={`${t.plugin}.${t.workspace}`} value={`${t.plugin}.${t.workspace}`}>
            {`${t.plugin}.${t.workspace}`}
          </option>
        ))}
      </select>{' '}
      <Button
        type="primary"
        disabled={!selected}
        loading={save.isPending}
        onClick={() => save.mutate()}
      >
        Save
      </Button>
      {warning && <Alert style={{ marginTop: 12 }} type="warning" showIcon message={warning} />}
    </div>
  );
}
