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
import { getPlugins, getPluginSchemas, getSchemas, getTables, getColumns, getFunctions, previewTable } from '../api/metadata';

export function usePlugins() {
  return useQuery({
    queryKey: ['plugins'],
    queryFn: getPlugins,
    staleTime: 5 * 60 * 1000, // Cache for 5 minutes
  });
}

export function usePluginSchemas(plugin: string | undefined) {
  return useQuery({
    queryKey: ['pluginSchemas', plugin],
    queryFn: () => (plugin ? getPluginSchemas(plugin) : Promise.resolve([])),
    enabled: !!plugin,
    staleTime: 5 * 60 * 1000,
  });
}

export function useSchemas() {
  return useQuery({
    queryKey: ['schemas'],
    queryFn: getSchemas,
    staleTime: 5 * 60 * 1000, // Cache for 5 minutes
  });
}

export function useTables(schema: string | undefined) {
  return useQuery({
    queryKey: ['tables', schema],
    queryFn: () => (schema ? getTables(schema) : Promise.resolve([])),
    enabled: !!schema,
    staleTime: 5 * 60 * 1000,
  });
}

export function useColumns(schema: string | undefined, table: string | undefined) {
  return useQuery({
    queryKey: ['columns', schema, table],
    queryFn: () =>
      schema && table ? getColumns(schema, table) : Promise.resolve([]),
    enabled: !!schema && !!table,
    staleTime: 5 * 60 * 1000,
  });
}

export function useFunctions() {
  return useQuery({
    queryKey: ['functions'],
    queryFn: getFunctions,
    staleTime: 30 * 60 * 1000, // Cache functions for 30 minutes
  });
}

export function useTablePreview(
  schema: string | undefined,
  table: string | undefined,
  enabled: boolean = false
) {
  return useQuery({
    queryKey: ['tablePreview', schema, table],
    queryFn: () =>
      schema && table ? previewTable(schema, table, 100) : Promise.resolve({ columns: [], rows: [] }),
    enabled: enabled && !!schema && !!table,
    staleTime: 1 * 60 * 1000, // Cache preview for 1 minute
  });
}
