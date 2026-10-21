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
import { useQuery } from '@tanstack/react-query';
import { Alert, Table, Tag } from 'antd';
import dayjs from 'dayjs';
import type { WorkflowRun } from '../../types/workflow';
import { listRuns } from '../../api/workflows';
import { getErrorMessage } from '../../api/client';
import OutcomeList from './OutcomeList';

function parseJson<T>(s: string | undefined, fallback: T): T {
  try {
    return s ? (JSON.parse(s) as T) : fallback;
  } catch {
    return fallback;
  }
}

export function isForbidden(err: unknown): boolean {
  return (err as { response?: { status?: number } })?.response?.status === 403;
}

export default function RunsTab({ workflowId }: { workflowId: string }) {
  const { data = [], isLoading, error } = useQuery({
    queryKey: ['workflow-runs', workflowId],
    queryFn: () => listRuns(workflowId),
    retry: false,
  });
  if (error) {
    return isForbidden(error)
      ? <Alert type="info" showIcon message="Run history is visible to project members" />
      : <Alert type="error" showIcon message={getErrorMessage(error)} />;
  }
  const count = (r: WorkflowRun, o: string) => (r.actions ?? []).filter((a) => a.outcome === o).length;
  return (
    <Table<WorkflowRun> rowKey="id" loading={isLoading} dataSource={data} size="small"
      columns={[
        { title: 'When', dataIndex: 'executedAt', render: (v: string) => dayjs(v).format('YYYY-MM-DD HH:mm') },
        { title: 'Trigger', dataIndex: 'trigger' },
        { title: 'Status', dataIndex: 'status',
          render: (s: string) => <Tag color={s === 'success' ? 'green' : s === 'error' ? 'red' : 'default'}>{s}</Tag> },
        { title: 'Rows', dataIndex: 'rowCount' },
        { title: 'Duration', dataIndex: 'duration', render: (d?: number) => (d == null ? '' : `${d} ms`) },
        { title: 'Alerts', render: (_, r) => (r.actions ?? [])
          .filter((o) => o.type === 'record_alert' && o.outcome === 'fired')
          .map((o) => <Tag key={o.id} color="orange">{String(o.outputs?.message ?? o.id)}</Tag>) },
        { title: 'Actions', render: (_, r) => `${count(r, 'fired')} fired · ${count(r, 'skipped')} skipped · ${count(r, 'errored')} errored` },
      ]}
      expandable={{
        expandedRowRender: (r) => {
          const columns = parseJson<string[]>(r.previewColumns, []);
          const rows = parseJson<Record<string, string>[]>(r.previewRows, []);
          return (
            <>
              {r.errorMessage && <Tag color="red">{r.errorMessage}</Tag>}
              <Table size="small" pagination={false} rowKey={(_, i) => String(i)} dataSource={rows}
                columns={columns.map((c) => ({ title: c, dataIndex: c }))} scroll={{ x: true }} />
              <OutcomeList outcomes={r.actions ?? []} />
            </>
          );
        },
      }} />
  );
}
