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

// Schema/Metadata types
export interface PluginInfo {
  name: string;
  type: string;
  enabled: boolean;
  browsable: boolean;
}

export interface SchemaInfo {
  name: string;
  type: 'schema';
  plugin?: string;
  browsable?: boolean;
}

export interface TableInfo {
  name: string;
  schema: string;
  type: 'TABLE' | 'VIEW' | 'SYSTEM TABLE';
}

export interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
  schema: string;
  table: string;
}

export interface FileInfo {
  name: string;
  isDirectory: boolean;
  isFile: boolean;
  length: number;
  owner?: string;
  group?: string;
  permissions?: string;
  modificationTime?: string;
}

// Query types
export interface QueryRequest {
  query: string;
  queryType: 'SQL' | 'PHYSICAL' | 'LOGICAL';
  autoLimitRowCount?: number;
  userName?: string;
  defaultSchema?: string;
  options?: Record<string, string>;
}

export interface QueryResult {
  columns: string[];
  metadata: string[];
  rows: Record<string, unknown>[];
  queryId: string;
  queryState?: string;
}

export interface QueryError {
  message: string;
  errorType?: string;
  stackTrace?: string;
}

// Saved Query types
export interface SavedQuery {
  id: string;
  name: string;
  description?: string;
  sql: string;
  defaultSchema?: string;
  owner: string;
  createdAt: string;
  updatedAt: string;
  tags?: Record<string, string>;
  isPublic: boolean;
}

export interface SavedQueryCreate {
  name: string;
  description?: string;
  sql: string;
  defaultSchema?: string;
  tags?: Record<string, string>;
  isPublic?: boolean;
}

// Visualization types
export type ChartType =
  | 'bar'
  | 'line'
  | 'pie'
  | 'scatter'
  | 'table'
  | 'heatmap'
  | 'treemap'
  | 'gauge'
  | 'funnel'
  | 'map';

export interface VisualizationConfig {
  xAxis?: string;
  yAxis?: string;
  metrics?: string[];
  dimensions?: string[];
  chartOptions?: Record<string, unknown>;
  colorScheme?: string;
}

export interface Visualization {
  id: string;
  name: string;
  description?: string;
  savedQueryId: string;
  chartType: ChartType;
  config: VisualizationConfig;
  owner: string;
  createdAt: string;
  updatedAt: string;
  isPublic: boolean;
  sql?: string;
  defaultSchema?: string;
}

export interface VisualizationCreate {
  name: string;
  description?: string;
  savedQueryId?: string;
  chartType: ChartType;
  config: VisualizationConfig;
  isPublic?: boolean;
  sql?: string;
  defaultSchema?: string;
}

// Dashboard types
export type DashboardPanelType = 'visualization' | 'markdown' | 'image' | 'title';

export interface DashboardPanel {
  id: string;
  type: DashboardPanelType;
  visualizationId?: string;
  content?: string;
  config?: Record<string, string>;
  tabId?: string;
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface DashboardTab {
  id: string;
  name: string;
  order: number;
}

export interface DashboardTheme {
  mode: 'light' | 'dark';
  fontFamily: string;
  backgroundColor: string;
  fontColor: string;
  panelBackground: string;
  panelBorderColor: string;
  panelBorderRadius: string;
  accentColor: string;
  headerColor: string;
}

export interface Dashboard {
  id: string;
  name: string;
  description?: string;
  panels: DashboardPanel[];
  tabs?: DashboardTab[];
  theme?: DashboardTheme;
  owner: string;
  createdAt: string;
  updatedAt: string;
  refreshInterval: number;
  isPublic: boolean;
}

export interface DashboardCreate {
  name: string;
  description?: string;
  panels?: DashboardPanel[];
  tabs?: DashboardTab[];
  theme?: DashboardTheme;
  refreshInterval?: number;
  isPublic?: boolean;
}

// Tree node types for schema explorer
export interface TreeNodeData {
  key: string;
  title: string;
  icon?: React.ReactNode;
  isLeaf?: boolean;
  children?: TreeNodeData[];
  data?: SchemaInfo | TableInfo | ColumnInfo;
}

// Storage plugin types
export interface StoragePlugin {
  name: string;
  config: Record<string, unknown>;
  enabled: boolean;
}
