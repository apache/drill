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
import { Alert, Spin, Table } from 'antd';
import ReactECharts from 'echarts-for-react';
import { getWorkflowHealth } from '../../api/workflows';
import { getErrorMessage } from '../../api/client';
import { isForbidden } from './RunsTab';

export default function HealthTab({ workflowId }: { workflowId: string }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ['workflow-health', workflowId],
    queryFn: () => getWorkflowHealth(workflowId),
    retry: false,
  });
  if (isLoading) {
    return <Spin />;
  }
  if (error) {
    return isForbidden(error)
      ? <Alert type="info" showIcon message="Run history is visible to project members" />
      : <Alert type="error" showIcon message={getErrorMessage(error)} />;
  }
  if (!data) {
    return null;
  }
  if (data.setupRequired) {
    return <Alert type="info" showIcon message="Run history is not set up"
      description="An admin can enable it from the Workflows page under Administration." />;
  }
  const days = data.daily.map((d) => d.day);
  const option = {
    tooltip: { trigger: 'axis' },
    legend: { data: ['Success rate %', 'Avg duration (ms)'] },
    xAxis: { type: 'category', data: days },
    yAxis: [{ type: 'value', max: 100, name: '%' }, { type: 'value', name: 'ms' }],
    series: [
      { name: 'Success rate %', type: 'line',
        data: data.daily.map((d) => Math.round((100 * Number(d.successes)) / Math.max(1, Number(d.runs)))) },
      { name: 'Avg duration (ms)', type: 'bar', yAxisIndex: 1, data: data.daily.map((d) => Math.round(Number(d.avgDurationMs))) },
    ],
  };
  return (
    <>
      {data.error && <Alert type="warning" showIcon style={{ marginBottom: 12 }} message="Some run history could not be loaded"
        description={data.error} />}
      <ReactECharts option={option} style={{ height: 280 }} />
      <Table size="small" rowKey={(r) => `${r.id}-${r.outcome}`} dataSource={data.actions} pagination={false}
        columns={[
          { title: 'Action', dataIndex: 'id' }, { title: 'Type', dataIndex: 'type' },
          { title: 'Outcome', dataIndex: 'outcome' }, { title: 'Count (30 d)', dataIndex: 'cnt' },
        ]} />
    </>
  );
}
