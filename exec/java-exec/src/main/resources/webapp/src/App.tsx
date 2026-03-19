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
import { Routes, Route, Navigate } from 'react-router-dom';
import { Layout } from 'antd';
import Navbar from './components/common/Navbar';
import CommandPalette from './components/common/CommandPalette';
import ProjectsPage from './pages/ProjectsPage';
import ProjectDetailPage from './pages/ProjectDetailPage';
import DataSourcesPage from './pages/DataSourcesPage';
import DataSourceEditPage from './pages/DataSourceEditPage';
import SqlLabPage from './pages/SqlLabPage';
import ProjectQueryPage from './pages/ProjectQueryPage';
import ProjectSavedQueriesPage from './pages/ProjectSavedQueriesPage';
import ProjectVisualizationsPage from './pages/ProjectVisualizationsPage';
import ProjectDashboardsPage from './pages/ProjectDashboardsPage';
import ProjectDataSourcesPage from './pages/ProjectDataSourcesPage';
import ProjectWikiPage from './pages/ProjectWikiPage';
import SavedQueriesPage from './pages/SavedQueriesPage';
import ProfilesPage from './pages/ProfilesPage';
import ProfileDetailPage from './pages/ProfileDetailPage';
import VisualizationsPage from './pages/VisualizationsPage';
import DashboardsPage from './pages/DashboardsPage';
import DashboardViewPage from './pages/DashboardViewPage';
import MetricsPage from './pages/MetricsPage';
import OptionsPage from './pages/OptionsPage';
import LogsPage from './pages/LogsPage';
import WorkflowsPage from './pages/WorkflowsPage';
import { ProjectLayout } from './components/project';

const { Content } = Layout;

function App() {
  return (
    <Layout className="sqllab-container">
      <CommandPalette />
      <Navbar />
      <Content style={{ flex: 1, overflow: 'auto', display: 'flex', flexDirection: 'column' }}>
        <Routes>
          <Route path="/" element={<Navigate to="/projects" replace />} />
          <Route path="/projects" element={<ProjectsPage />} />
          <Route path="/projects/:id" element={<ProjectLayout />}>
            <Route index element={<Navigate to="query" replace />} />
            <Route path="query" element={<ProjectQueryPage />} />
            <Route path="queries" element={<ProjectSavedQueriesPage />} />
            <Route path="visualizations" element={<ProjectVisualizationsPage />} />
            <Route path="dashboards" element={<ProjectDashboardsPage />} />
            <Route path="datasources" element={<ProjectDataSourcesPage />} />
            <Route path="wiki" element={<ProjectWikiPage />} />
            <Route path="wiki/:pageId" element={<ProjectWikiPage />} />
            <Route path="settings" element={<ProjectDetailPage />} />
          </Route>
          <Route path="/datasources" element={<DataSourcesPage />} />
          <Route path="/datasources/:name" element={<DataSourceEditPage />} />
          <Route path="/query" element={<SqlLabPage />} />
          <Route path="/saved-queries" element={<SavedQueriesPage />} />
          <Route path="/workflows" element={<WorkflowsPage />} />
          <Route path="/profiles" element={<ProfilesPage />} />
          <Route path="/profiles/:queryId" element={<ProfileDetailPage />} />
          <Route path="/visualizations" element={<VisualizationsPage />} />
          <Route path="/dashboards" element={<DashboardsPage />} />
          <Route path="/dashboards/:id" element={<DashboardViewPage />} />
          <Route path="/metrics" element={<MetricsPage />} />
          <Route path="/options" element={<OptionsPage />} />
          <Route path="/logs" element={<LogsPage />} />
          <Route path="*" element={<Navigate to="/projects" replace />} />
        </Routes>
      </Content>
    </Layout>
  );
}

export default App;
