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
import { lazy, Suspense } from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { Spin } from 'antd';
import { AiModalProvider } from './contexts/AiModalContext';
import { AppChromeProvider } from './contexts/AppChromeContext';
import AppShell from './components/shell/AppShell';

// Lazy-loaded routes — each page becomes its own chunk so the initial
// bundle drops well below the 500kB warning. Only the AppShell + sidebar
// + toolbar load up front; pages download on first navigation.
const ProjectsPage = lazy(() => import('./pages/ProjectsPage'));
const ProjectDetailPage = lazy(() => import('./pages/ProjectDetailPage'));
const DataSourcesPage = lazy(() => import('./pages/DataSourcesPage'));
const DataSourceEditPage = lazy(() => import('./pages/DataSourceEditPage'));
const SqlLabPage = lazy(() => import('./pages/SqlLabPage'));
const ProjectQueryPage = lazy(() => import('./pages/ProjectQueryPage'));
const ProjectSavedQueriesPage = lazy(() => import('./pages/ProjectSavedQueriesPage'));
const ProjectVisualizationsPage = lazy(() => import('./pages/ProjectVisualizationsPage'));
const ProjectDashboardsPage = lazy(() => import('./pages/ProjectDashboardsPage'));
const ProjectDataSourcesPage = lazy(() => import('./pages/ProjectDataSourcesPage'));
const ProjectWikiPage = lazy(() => import('./pages/ProjectWikiPage'));
const ProjectWorkflowsPage = lazy(() => import('./pages/ProjectWorkflowsPage'));
const SavedQueriesPage = lazy(() => import('./pages/SavedQueriesPage'));
const ProfilesPage = lazy(() => import('./pages/ProfilesPage'));
const ProfileDetailPage = lazy(() => import('./pages/ProfileDetailPage'));
const VisualizationsPage = lazy(() => import('./pages/VisualizationsPage'));
const VisualizationDetailPage = lazy(() => import('./pages/VisualizationDetailPage'));
const ProjectVisualizationDetailPage = lazy(() => import('./pages/ProjectVisualizationDetailPage'));
const DashboardsPage = lazy(() => import('./pages/DashboardsPage'));
const DashboardViewPage = lazy(() => import('./pages/DashboardViewPage'));
const MetricsPage = lazy(() => import('./pages/MetricsPage'));
const OptionsPage = lazy(() => import('./pages/OptionsPage'));
const LogsPage = lazy(() => import('./pages/LogsPage'));
const WorkflowsPage = lazy(() => import('./pages/WorkflowsPage'));
const ProjectLayout = lazy(() => import('./components/project').then((m) => ({ default: m.ProjectLayout })));

/** Subtle fallback used while a route chunk is loading. */
function RouteFallback() {
  return (
    <div className="route-fallback">
      <Spin size="large" />
    </div>
  );
}

function App() {
  return (
    <AppChromeProvider>
      <AiModalProvider>
        <AppShell>
          <Suspense fallback={<RouteFallback />}>
            <Routes>
              <Route path="/" element={<Navigate to="/projects" replace />} />
              <Route path="/projects" element={<ProjectsPage />} />
              <Route path="/projects/:id" element={<ProjectLayout />}>
                <Route index element={<Navigate to="query" replace />} />
                <Route path="query" element={<ProjectQueryPage />} />
                <Route path="queries" element={<ProjectSavedQueriesPage />} />
                <Route path="visualizations" element={<ProjectVisualizationsPage />} />
                <Route path="visualizations/:vizId" element={<ProjectVisualizationDetailPage />} />
                <Route path="dashboards" element={<ProjectDashboardsPage />} />
                <Route path="dashboards/:dashboardId" element={<DashboardViewPage />} />
                <Route path="datasources" element={<ProjectDataSourcesPage />} />
                <Route path="workflows" element={<ProjectWorkflowsPage />} />
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
              <Route path="/visualizations/:vizId" element={<VisualizationDetailPage />} />
              <Route path="/dashboards" element={<DashboardsPage />} />
              <Route path="/dashboards/:id" element={<DashboardViewPage />} />
              <Route path="/metrics" element={<MetricsPage />} />
              <Route path="/options" element={<OptionsPage />} />
              <Route path="/logs" element={<LogsPage />} />
              <Route path="*" element={<Navigate to="/projects" replace />} />
            </Routes>
          </Suspense>
        </AppShell>
      </AiModalProvider>
    </AppChromeProvider>
  );
}

export default App;
