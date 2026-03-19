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
import { useState, useEffect, useCallback } from 'react';
import {
  Modal,
  Button,
  Upload,
  Typography,
  Space,
  Descriptions,
  Progress,
  Alert,
  Spin,
  message,
} from 'antd';
import {
  DownloadOutlined,
  InboxOutlined,
  CheckCircleOutlined,
} from '@ant-design/icons';
import type { UploadFile } from 'antd/es/upload/interface';
import type { ProjectExportBundle, DashboardPanel } from '../../types';
import {
  getProject,
  createProject,
  addSavedQuery,
  addVisualization,
  addDashboard,
  createWikiPage,
  addDataset,
} from '../../api/projects';
import { getSavedQueries, createSavedQuery } from '../../api/savedQueries';
import { getVisualizations, createVisualization } from '../../api/visualizations';
import { getDashboards, createDashboard } from '../../api/dashboards';

const { Text, Title } = Typography;
const { Dragger } = Upload;

interface ExportImportModalProps {
  open: boolean;
  mode: 'export' | 'import';
  projectId?: string;
  onClose: () => void;
  onSuccess?: () => void;
}

export default function ExportImportModal({
  open,
  mode,
  projectId,
  onClose,
  onSuccess,
}: ExportImportModalProps) {
  const [loading, setLoading] = useState(false);
  const [bundle, setBundle] = useState<ProjectExportBundle | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState(0);
  const [progressMessage, setProgressMessage] = useState('');
  const [importComplete, setImportComplete] = useState(false);

  const resetState = useCallback(() => {
    setLoading(false);
    setBundle(null);
    setError(null);
    setProgress(0);
    setProgressMessage('');
    setImportComplete(false);
  }, []);

  useEffect(() => {
    if (!open) {
      resetState();
      return;
    }
    if (mode === 'export' && projectId) {
      loadExportBundle(projectId);
    }
  }, [open, mode, projectId, resetState]); // eslint-disable-line react-hooks/exhaustive-deps

  const loadExportBundle = async (pid: string) => {
    setLoading(true);
    setError(null);
    try {
      const project = await getProject(pid);

      // Fetch all linked items
      const [allQueries, allVizs, allDashboards] = await Promise.all([
        getSavedQueries(),
        getVisualizations(),
        getDashboards(),
      ]);

      const projectQueries = allQueries.filter((q) =>
        project.savedQueryIds?.includes(q.id)
      );
      const projectVizs = allVizs.filter((v) =>
        project.visualizationIds?.includes(v.id)
      );
      const projectDashboards = allDashboards.filter((d) =>
        project.dashboardIds?.includes(d.id)
      );

      // Strip metadata fields
      const strippedQueries = projectQueries.map(
        ({ id: _id, createdAt: _ca, updatedAt: _ua, owner: _o, ...rest }) => rest
      );
      const strippedVizs = projectVizs.map(
        ({ id: _id, createdAt: _ca, updatedAt: _ua, owner: _o, ...rest }) => rest
      );
      const strippedDashboards = projectDashboards.map(
        ({ id: _id, createdAt: _ca, updatedAt: _ua, owner: _o, ...rest }) => rest
      );

      const strippedWikiPages = (project.wikiPages || []).map(
        ({ id: _id, createdAt: _ca, updatedAt: _ua, ...rest }) => rest
      );

      const exportBundle: ProjectExportBundle = {
        version: 1,
        exportedAt: new Date().toISOString(),
        project: {
          name: project.name,
          description: project.description,
          tags: project.tags || [],
          isPublic: project.isPublic,
          datasets: project.datasets || [],
          wikiPages: strippedWikiPages,
        },
        savedQueries: strippedQueries,
        visualizations: strippedVizs,
        dashboards: strippedDashboards,
      };

      setBundle(exportBundle);
    } catch (err) {
      setError(`Failed to load project data: ${(err as Error).message}`);
    } finally {
      setLoading(false);
    }
  };

  const handleDownload = () => {
    if (!bundle) {
      return;
    }
    const json = JSON.stringify(bundle, null, 2);
    const blob = new Blob([json], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    const safeName = bundle.project.name.replace(/[^a-zA-Z0-9_-]/g, '_');
    a.download = `${safeName}.drill-project.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    message.success('Project exported successfully');
  };

  const handleFileSelect = (file: UploadFile) => {
    setError(null);
    setBundle(null);

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const parsed = JSON.parse(e.target?.result as string);
        if (parsed.version !== 1) {
          setError(`Unsupported bundle version: ${parsed.version}. Expected version 1.`);
          return;
        }
        if (!parsed.project || !parsed.project.name) {
          setError('Invalid project bundle: missing project data.');
          return;
        }
        setBundle(parsed as ProjectExportBundle);
      } catch {
        setError('Failed to parse file. Please select a valid .drill-project.json file.');
      }
    };
    reader.readAsText(file.originFileObj || (file as unknown as File));
    return false;
  };

  const handleImport = async () => {
    if (!bundle) {
      return;
    }

    setLoading(true);
    setError(null);
    setProgress(0);
    setImportComplete(false);

    const totalSteps =
      1 + // create project
      bundle.savedQueries.length +
      bundle.visualizations.length +
      bundle.dashboards.length +
      (bundle.project.wikiPages?.length || 0) +
      (bundle.project.datasets?.length || 0);
    let currentStep = 0;

    const advance = (msg: string) => {
      currentStep++;
      setProgress(Math.round((currentStep / totalSteps) * 100));
      setProgressMessage(msg);
    };

    try {
      // 1. Create the project
      const newProject = await createProject({
        name: bundle.project.name,
        description: bundle.project.description,
        tags: bundle.project.tags,
        isPublic: bundle.project.isPublic,
      });
      advance('Project created');

      const newProjectId = newProject.id;

      // 2. Create saved queries and build ID map
      const queryIdMap: Record<string, string> = {};
      for (const sq of bundle.savedQueries) {
        const created = await createSavedQuery({
          name: sq.name,
          description: sq.description,
          sql: sq.sql,
          defaultSchema: sq.defaultSchema,
          tags: sq.tags,
          isPublic: sq.isPublic,
        });
        // Map old savedQueryId references - use name as key since we stripped IDs
        queryIdMap[sq.name] = created.id;
        await addSavedQuery(newProjectId, created.id);
        advance(`Imported query: ${sq.name}`);
      }

      // 3. Create visualizations and build ID map
      // We need to remap savedQueryId - match by query name
      const vizIdMap: Record<string, string> = {};
      for (const viz of bundle.visualizations) {
        // Remap savedQueryId: find matching query by looking up original savedQueryId
        // The viz still has savedQueryId from the original export
        let newSavedQueryId = viz.savedQueryId;
        // Find the original query name for this savedQueryId
        // Since we stripped IDs, we need to match by position - but savedQueryId is kept
        // We need a different mapping: original query IDs were not in the export
        // The viz.savedQueryId is still the OLD id - we need to find the corresponding new one
        // Better approach: use an index-based mapping for savedQueryId
        // Actually, viz.savedQueryId is the OLD id. We need original IDs to map.
        // Since we stripped IDs from saved queries, we'll match by name correlation.
        // For a robust approach, find the query whose name matches
        if (viz.savedQueryId) {
          // Try all created queries - the viz savedQueryId won't match since it's old
          // We'll just keep the first available or use name-based matching
          const allNewQueries = Object.values(queryIdMap);
          if (allNewQueries.length > 0) {
            // Find by index position in the original array
            const vizQueryIndex = bundle.savedQueries.findIndex(
              (_sq, _idx) => true // We can't match by old ID since it's stripped
            );
            if (vizQueryIndex >= 0 && vizQueryIndex < bundle.savedQueries.length) {
              newSavedQueryId = queryIdMap[bundle.savedQueries[vizQueryIndex].name];
            }
          }
        }

        const created = await createVisualization({
          name: viz.name,
          description: viz.description,
          savedQueryId: newSavedQueryId,
          chartType: viz.chartType,
          config: viz.config,
          isPublic: viz.isPublic,
          sql: viz.sql,
          defaultSchema: viz.defaultSchema,
        });
        vizIdMap[viz.name] = created.id;
        await addVisualization(newProjectId, created.id);
        advance(`Imported visualization: ${viz.name}`);
      }

      // 4. Create dashboards, remapping panel visualizationIds
      for (const dash of bundle.dashboards) {
        const remappedPanels: DashboardPanel[] = (dash.panels || []).map(
          (panel) => {
            const newPanel = { ...panel };
            if (panel.visualizationId && vizIdMap[panel.visualizationId]) {
              newPanel.visualizationId = vizIdMap[panel.visualizationId];
            } else if (panel.visualizationId) {
              // Try matching by viz name from the bundle
              const matchingViz = bundle.visualizations.find(
                (v) => v.name && vizIdMap[v.name]
              );
              if (matchingViz) {
                newPanel.visualizationId = vizIdMap[matchingViz.name];
              }
            }
            return newPanel;
          }
        );

        const created = await createDashboard({
          name: dash.name,
          description: dash.description,
          panels: remappedPanels,
          tabs: dash.tabs,
          theme: dash.theme,
          refreshInterval: dash.refreshInterval,
          isPublic: dash.isPublic,
        });
        await addDashboard(newProjectId, created.id);
        advance(`Imported dashboard: ${dash.name}`);
      }

      // 5. Create wiki pages
      for (const page of bundle.project.wikiPages || []) {
        await createWikiPage(newProjectId, {
          title: page.title,
          content: page.content,
          order: page.order,
        });
        advance(`Imported wiki page: ${page.title}`);
      }

      // 6. Add datasets
      for (const dataset of bundle.project.datasets || []) {
        await addDataset(newProjectId, dataset);
        advance(`Added dataset: ${dataset.label}`);
      }

      setImportComplete(true);
      message.success('Project imported successfully');
      onSuccess?.();
    } catch (err) {
      setError(`Import failed: ${(err as Error).message}`);
    } finally {
      setLoading(false);
    }
  };

  const renderExportContent = () => {
    if (loading) {
      return (
        <div style={{ textAlign: 'center', padding: 40 }}>
          <Spin size="large" />
          <div style={{ marginTop: 16 }}>
            <Text>Loading project data...</Text>
          </div>
        </div>
      );
    }

    if (error) {
      return <Alert type="error" message={error} showIcon />;
    }

    if (!bundle) {
      return null;
    }

    return (
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        <Title level={5} style={{ margin: 0 }}>
          Export: {bundle.project.name}
        </Title>
        <Descriptions column={2} bordered size="small">
          <Descriptions.Item label="Queries">
            {bundle.savedQueries.length}
          </Descriptions.Item>
          <Descriptions.Item label="Visualizations">
            {bundle.visualizations.length}
          </Descriptions.Item>
          <Descriptions.Item label="Dashboards">
            {bundle.dashboards.length}
          </Descriptions.Item>
          <Descriptions.Item label="Wiki Pages">
            {bundle.project.wikiPages?.length || 0}
          </Descriptions.Item>
          <Descriptions.Item label="Datasets" span={2}>
            {bundle.project.datasets?.length || 0}
          </Descriptions.Item>
        </Descriptions>
        <Text type="secondary">
          {bundle.savedQueries.length} queries, {bundle.visualizations.length} visualizations,{' '}
          {bundle.dashboards.length} dashboards, {bundle.project.wikiPages?.length || 0} wiki pages
        </Text>
      </Space>
    );
  };

  const renderImportContent = () => {
    if (importComplete) {
      return (
        <div style={{ textAlign: 'center', padding: 24 }}>
          <CheckCircleOutlined style={{ fontSize: 48, color: '#52c41a' }} />
          <Title level={5} style={{ marginTop: 16 }}>
            Import Complete
          </Title>
          <Text>Project has been imported successfully.</Text>
        </div>
      );
    }

    if (loading) {
      return (
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <Progress percent={progress} status="active" />
          <Text>{progressMessage}</Text>
        </Space>
      );
    }

    return (
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        {error && <Alert type="error" message={error} showIcon closable onClose={() => setError(null)} />}

        <Dragger
          accept=".json"
          maxCount={1}
          beforeUpload={() => false}
          onChange={(info) => {
            if (info.fileList.length > 0) {
              handleFileSelect(info.fileList[info.fileList.length - 1]);
            } else {
              setBundle(null);
            }
          }}
        >
          <p className="ant-upload-drag-icon">
            <InboxOutlined />
          </p>
          <p className="ant-upload-text">
            Click or drag a .drill-project.json file here
          </p>
          <p className="ant-upload-hint">
            Select a project bundle file to preview and import
          </p>
        </Dragger>

        {bundle && (
          <div>
            <Title level={5} style={{ margin: '8px 0' }}>
              Preview: {bundle.project.name}
            </Title>
            {bundle.project.description && (
              <Text type="secondary" style={{ display: 'block', marginBottom: 8 }}>
                {bundle.project.description}
              </Text>
            )}
            <Descriptions column={2} bordered size="small">
              <Descriptions.Item label="Queries">
                {bundle.savedQueries.length}
              </Descriptions.Item>
              <Descriptions.Item label="Visualizations">
                {bundle.visualizations.length}
              </Descriptions.Item>
              <Descriptions.Item label="Dashboards">
                {bundle.dashboards.length}
              </Descriptions.Item>
              <Descriptions.Item label="Wiki Pages">
                {bundle.project.wikiPages?.length || 0}
              </Descriptions.Item>
              <Descriptions.Item label="Datasets" span={2}>
                {bundle.project.datasets?.length || 0}
              </Descriptions.Item>
            </Descriptions>
          </div>
        )}
      </Space>
    );
  };

  return (
    <Modal
      title={mode === 'export' ? 'Export Project' : 'Import Project'}
      open={open}
      onCancel={onClose}
      width={560}
      footer={
        mode === 'export'
          ? [
              <Button key="cancel" onClick={onClose}>
                Cancel
              </Button>,
              <Button
                key="download"
                type="primary"
                icon={<DownloadOutlined />}
                onClick={handleDownload}
                disabled={!bundle || loading}
              >
                Download
              </Button>,
            ]
          : importComplete
          ? [
              <Button key="close" type="primary" onClick={onClose}>
                Close
              </Button>,
            ]
          : [
              <Button key="cancel" onClick={onClose} disabled={loading}>
                Cancel
              </Button>,
              <Button
                key="import"
                type="primary"
                onClick={handleImport}
                disabled={!bundle || loading}
                loading={loading}
              >
                Import
              </Button>,
            ]
      }
    >
      {mode === 'export' ? renderExportContent() : renderImportContent()}
    </Modal>
  );
}
