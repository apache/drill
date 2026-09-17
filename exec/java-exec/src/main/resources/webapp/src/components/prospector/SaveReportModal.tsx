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
import { useEffect, useState } from 'react';
import { Modal, Form, Input, Select, Radio, Spin, message } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { getProjects, createWikiPage, updateWikiPage, getProject } from '../../api/projects';
import { buildReportMarkdown, reportTitle, REPORTS_FOLDER } from '../../utils/report';
import type { ChatMessage } from '../../types/ai';

interface SaveReportModalProps {
  open: boolean;
  /** Markdown of the assistant message being saved. */
  content: string;
  /** Conversation so far, used to build the query appendix. */
  messages: ChatMessage[];
  defaultProjectId?: string;
  /** Recorded in the provenance footer so a saved report points back at its thread. */
  conversationId?: string;
  onClose: () => void;
}

export default function SaveReportModal({
  open,
  content,
  messages,
  defaultProjectId,
  conversationId,
  onClose,
}: SaveReportModalProps) {
  const [projectId, setProjectId] = useState<string | undefined>(defaultProjectId);
  const [title, setTitle] = useState('');
  const [mode, setMode] = useState<'new' | 'update'>('new');
  const [saving, setSaving] = useState(false);

  const { data: projects } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
    enabled: open,
  });

  // The chosen project's existing reports decide whether an update is on offer.
  const { data: project, isLoading: projectLoading } = useQuery({
    queryKey: ['project', projectId],
    queryFn: () => getProject(projectId as string),
    enabled: open && Boolean(projectId),
  });

  useEffect(() => {
    if (open) {
      setProjectId(defaultProjectId);
      setTitle(reportTitle(content));
      setMode('new');
    }
  }, [open, content, defaultProjectId]);

  // While the project's pages are still loading, there is no way to know whether
  // the title collides, so treat the collision check itself as pending rather
  // than assuming there is none.
  const checkingCollision = Boolean(projectId) && projectLoading;

  const existing = (project?.wikiPages ?? []).find(
    (page) => page.folder === REPORTS_FOLDER
      && page.title.trim().toLowerCase() === title.trim().toLowerCase()
  );

  // A title that no longer collides must not keep an Update selection alive.
  useEffect(() => {
    if (!existing) {
      setMode('new');
    }
  }, [existing]);

  const handleSave = async () => {
    if (!projectId || !title.trim() || checkingCollision) {
      return;
    }
    setSaving(true);
    try {
      // model / provider are left unset here: the authenticated-user-facing AI
      // status endpoint does not carry them (only the admin-only config does),
      // and provenanceFooter already degrades to "an unspecified model".
      const markdown = buildReportMarkdown(content, messages, {
        generatedAt: Date.now(),
        conversationId,
      });
      if (mode === 'update' && existing) {
        await updateWikiPage(projectId, existing.id, { title: title.trim(), content: markdown });
        message.success('Report updated');
      } else {
        await createWikiPage(projectId, {
          title: title.trim(),
          content: markdown,
          folder: REPORTS_FOLDER,
        });
        message.success('Report saved');
      }
      onClose();
    } catch (e) {
      // Leave the modal open with the chosen project and typed title intact
      // so a failed save does not lose the user's work.
      message.error(`Could not save the report: ${(e as Error).message}`);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal
      open={open}
      title="Save as report"
      onCancel={onClose}
      onOk={handleSave}
      okText="Save"
      okButtonProps={{
        loading: saving,
        disabled: !projectId || !title.trim() || checkingCollision,
      }}
    >
      <Form layout="vertical">
        <Form.Item label="Project">
          <Select
            value={projectId}
            onChange={setProjectId}
            placeholder="Choose a project"
            options={(projects ?? []).map((p) => ({ value: p.id, label: p.name }))}
            showSearch
            optionFilterProp="label"
          />
        </Form.Item>

        <Form.Item label="Title">
          <Input value={title} onChange={(e) => setTitle(e.target.value)} />
        </Form.Item>

        {checkingCollision && (
          <Form.Item label="Checking for an existing report with this title">
            <Spin size="small" />
          </Form.Item>
        )}

        {!checkingCollision && existing && (
          <Form.Item label="A report with this title already exists">
            <Radio.Group value={mode} onChange={(e) => setMode(e.target.value)}>
              <Radio value="update">Update it</Radio>
              <Radio value="new">Save as a new page</Radio>
            </Radio.Group>
          </Form.Item>
        )}
      </Form>
    </Modal>
  );
}
