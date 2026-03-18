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
import { useMemo } from 'react';
import { Card, Typography, Space, Empty, Tag, Row, Col } from 'antd';
import {
  CodeOutlined,
  BarChartOutlined,
  DashboardOutlined,
  ArrowRightOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { getSavedQueries } from '../../api/savedQueries';
import { getVisualizations } from '../../api/visualizations';
import { getDashboards } from '../../api/dashboards';
import type { Project } from '../../types';

const { Text } = Typography;

interface Props {
  project: Project;
}

export default function ProjectLineage({ project }: Props) {
  const { data: allQueries } = useQuery({ queryKey: ['savedQueries'], queryFn: getSavedQueries });
  const { data: allVizs } = useQuery({ queryKey: ['visualizations'], queryFn: getVisualizations });
  const { data: allDashboards } = useQuery({ queryKey: ['dashboards'], queryFn: getDashboards });

  const lineage = useMemo(() => {
    const queryIdSet = new Set(project.savedQueryIds || []);
    const vizIdSet = new Set(project.visualizationIds || []);
    const dashIdSet = new Set(project.dashboardIds || []);

    const queries = (allQueries || []).filter(q => queryIdSet.has(q.id));
    const vizs = (allVizs || []).filter(v => vizIdSet.has(v.id));
    const dashboards = (allDashboards || []).filter(d => dashIdSet.has(d.id));

    // Build viz -> dashboards map
    const vizToDash: Record<string, string[]> = {};
    dashboards.forEach(d => {
      (d.panels || []).forEach(p => {
        if (p.visualizationId && vizIdSet.has(p.visualizationId)) {
          if (!vizToDash[p.visualizationId]) {
            vizToDash[p.visualizationId] = [];
          }
          if (!vizToDash[p.visualizationId].includes(d.id)) {
            vizToDash[p.visualizationId].push(d.id);
          }
        }
      });
    });

    // Build query -> viz map (via savedQueryId on viz, if available)
    const queryToViz: Record<string, string[]> = {};
    vizs.forEach(v => {
      if ((v as any).savedQueryId && queryIdSet.has((v as any).savedQueryId)) {
        const qid = (v as any).savedQueryId;
        if (!queryToViz[qid]) {
          queryToViz[qid] = [];
        }
        queryToViz[qid].push(v.id);
      }
    });

    // Orphaned vizs (not linked to a query)
    const linkedVizIds = new Set(Object.values(queryToViz).flat());
    const orphanVizs = vizs.filter(v => !linkedVizIds.has(v.id));

    // Orphaned dashboards (no viz panels in project)
    const linkedDashIds = new Set(Object.values(vizToDash).flat());
    const orphanDashboards = dashboards.filter(d => !linkedDashIds.has(d.id));

    return { queries, vizs, dashboards, queryToViz, vizToDash, orphanVizs, orphanDashboards };
  }, [project, allQueries, allVizs, allDashboards]);

  const { queries, vizs, dashboards, queryToViz, vizToDash, orphanVizs, orphanDashboards } = lineage;

  if (queries.length === 0 && vizs.length === 0 && dashboards.length === 0) {
    return <Empty description="No items in this project to show lineage for" />;
  }

  const getDashName = (id: string) => dashboards.find(d => d.id === id)?.name || id;
  const getVizName = (id: string) => vizs.find(v => v.id === id)?.name || id;

  return (
    <Space direction="vertical" style={{ width: '100%' }} size="middle">
      <Text type="secondary">
        Showing how queries, visualizations, and dashboards are connected in this project.
      </Text>

      {/* Query chains */}
      {queries.map(q => {
        const vizIds = queryToViz[q.id] || [];
        return (
          <Card key={q.id} size="small">
            <Row align="middle" gutter={16} wrap>
              <Col>
                <Space>
                  <Tag color="green" icon={<CodeOutlined />}>Query</Tag>
                  <Text strong>{q.name}</Text>
                </Space>
              </Col>
              {vizIds.length > 0 && (
                <>
                  <Col><ArrowRightOutlined style={{ color: 'var(--color-text-tertiary)' }} /></Col>
                  <Col>
                    <Space direction="vertical" size={4}>
                      {vizIds.map(vid => (
                        <Space key={vid}>
                          <Tag color="blue" icon={<BarChartOutlined />}>Viz</Tag>
                          <Text>{getVizName(vid)}</Text>
                          {(vizToDash[vid] || []).length > 0 && (
                            <>
                              <ArrowRightOutlined style={{ color: 'var(--color-text-tertiary)', fontSize: 11 }} />
                              {(vizToDash[vid] || []).map(did => (
                                <Tag key={did} color="purple" icon={<DashboardOutlined />}>
                                  {getDashName(did)}
                                </Tag>
                              ))}
                            </>
                          )}
                        </Space>
                      ))}
                    </Space>
                  </Col>
                </>
              )}
            </Row>
          </Card>
        );
      })}

      {/* Orphan visualizations */}
      {orphanVizs.length > 0 && (
        <Card size="small" title={<Text type="secondary">Standalone Visualizations</Text>}>
          <Space wrap>
            {orphanVizs.map(v => (
              <Space key={v.id}>
                <Tag color="blue" icon={<BarChartOutlined />}>Viz</Tag>
                <Text>{v.name}</Text>
                {(vizToDash[v.id] || []).length > 0 && (
                  <>
                    <ArrowRightOutlined style={{ color: 'var(--color-text-tertiary)', fontSize: 11 }} />
                    {(vizToDash[v.id] || []).map(did => (
                      <Tag key={did} color="purple" icon={<DashboardOutlined />}>{getDashName(did)}</Tag>
                    ))}
                  </>
                )}
              </Space>
            ))}
          </Space>
        </Card>
      )}

      {/* Orphan dashboards */}
      {orphanDashboards.length > 0 && (
        <Card size="small" title={<Text type="secondary">Standalone Dashboards</Text>}>
          <Space wrap>
            {orphanDashboards.map(d => (
              <Tag key={d.id} color="purple" icon={<DashboardOutlined />}>{d.name}</Tag>
            ))}
          </Space>
        </Card>
      )}
    </Space>
  );
}
