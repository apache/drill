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
import apiClient from './client';
import type { SchemaInfo, TableInfo, ColumnInfo, PluginInfo } from '../types';

const METADATA_BASE = '/api/v1/metadata';

export interface PluginsResponse {
  plugins: PluginInfo[];
}

export interface SchemasResponse {
  schemas: SchemaInfo[];
}

export interface TablesResponse {
  tables: TableInfo[];
}

export interface ColumnsResponse {
  columns: ColumnInfo[];
}

export interface TablePreviewResponse {
  columns: string[];
  rows: Record<string, unknown>[];
}

export interface FunctionsResponse {
  functions: string[];
}

/**
 * Fetch all enabled storage plugins
 */
export async function getPlugins(): Promise<PluginInfo[]> {
  const response = await apiClient.get<PluginsResponse>(`${METADATA_BASE}/plugins`);
  return response.data.plugins;
}

/**
 * Fetch schemas for a specific plugin
 */
export async function getPluginSchemas(plugin: string): Promise<SchemaInfo[]> {
  const response = await apiClient.get<SchemasResponse>(
    `${METADATA_BASE}/plugins/${encodeURIComponent(plugin)}/schemas`
  );
  return response.data.schemas;
}

/**
 * Fetch all available schemas
 */
export async function getSchemas(): Promise<SchemaInfo[]> {
  const response = await apiClient.get<SchemasResponse>(`${METADATA_BASE}/schemas`);
  return response.data.schemas;
}

/**
 * Fetch tables in a specific schema
 */
export async function getTables(schema: string): Promise<TableInfo[]> {
  const response = await apiClient.get<TablesResponse>(
    `${METADATA_BASE}/schemas/${encodeURIComponent(schema)}/tables`
  );
  return response.data.tables;
}

/**
 * Fetch columns for a specific table
 */
export async function getColumns(schema: string, table: string): Promise<ColumnInfo[]> {
  const response = await apiClient.get<ColumnsResponse>(
    `${METADATA_BASE}/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/columns`
  );
  return response.data.columns;
}

/**
 * Preview table data (limited rows)
 */
export async function previewTable(
  schema: string,
  table: string,
  limit: number = 100
): Promise<TablePreviewResponse> {
  const response = await apiClient.get<TablePreviewResponse>(
    `${METADATA_BASE}/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/preview`,
    { params: { limit } }
  );
  return response.data;
}

/**
 * Fetch available SQL functions for autocomplete
 */
export async function getFunctions(): Promise<string[]> {
  const response = await apiClient.get<FunctionsResponse>(`${METADATA_BASE}/functions`);
  return response.data.functions;
}

/**
 * File info from SHOW FILES command
 */
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

export interface FilesResponse {
  files: FileInfo[];
  path: string;
}

/**
 * Fetch files in a schema/workspace (for file-based plugins like dfs)
 */
export async function getFiles(schema: string, subPath?: string): Promise<FileInfo[]> {
  const params = subPath ? { path: subPath } : {};
  const response = await apiClient.get<FilesResponse>(
    `${METADATA_BASE}/schemas/${encodeURIComponent(schema)}/files`,
    { params }
  );
  return response.data.files;
}

/**
 * Fetch columns from a file by executing SELECT * LIMIT 1
 */
export async function getFileColumns(schema: string, filePath: string): Promise<ColumnInfo[]> {
  const response = await apiClient.get<ColumnsResponse>(
    `${METADATA_BASE}/schemas/${encodeURIComponent(schema)}/files/columns`,
    { params: { path: filePath } }
  );
  return response.data.columns;
}
