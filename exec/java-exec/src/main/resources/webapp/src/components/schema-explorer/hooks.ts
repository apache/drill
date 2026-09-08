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
import {
  getPlugins,
  getPluginSchemas,
  getTables,
  getColumns,
  getFiles,
  getFileColumns,
} from '../../api/metadata';

const STALE_TIME = 5 * 60 * 1000;   // 5 minutes
const GC_TIME   = 30 * 60 * 1000;   // 30 minutes

export function usePlugins() {
  return useQuery({
    queryKey: ['plugins'],
    queryFn: getPlugins,
    staleTime: STALE_TIME,
    gcTime: GC_TIME,
  });
}

export function useSchemasByPlugin(pluginName: string | undefined) {
  return useQuery({
    queryKey: ['schemas', pluginName],
    queryFn: () => getPluginSchemas(pluginName!),
    enabled: !!pluginName,
    staleTime: STALE_TIME,
    gcTime: GC_TIME,
  });
}

export function useTables(schemaName: string | undefined) {
  return useQuery({
    queryKey: ['tables', schemaName],
    queryFn: () => getTables(schemaName!),
    enabled: !!schemaName,
    staleTime: STALE_TIME,
    gcTime: GC_TIME,
  });
}

export function useColumns(schemaName: string | undefined, tableName: string | undefined) {
  return useQuery({
    queryKey: ['columns', schemaName, tableName],
    queryFn: () => getColumns(schemaName!, tableName!),
    enabled: !!schemaName && !!tableName,
    staleTime: STALE_TIME,
    gcTime: GC_TIME,
  });
}

export function useFiles(schemaName: string | undefined, subPath?: string) {
  return useQuery({
    queryKey: ['files', schemaName, subPath ?? ''],
    queryFn: () => getFiles(schemaName!, subPath),
    enabled: !!schemaName,
    staleTime: STALE_TIME,
    gcTime: GC_TIME,
  });
}

export function useFileColumns(schemaName: string | undefined, filePath: string | undefined) {
  return useQuery({
    queryKey: ['fileColumns', schemaName, filePath],
    queryFn: () => getFileColumns(schemaName!, filePath!),
    enabled: !!schemaName && !!filePath,
    staleTime: STALE_TIME,
    gcTime: GC_TIME,
  });
}
