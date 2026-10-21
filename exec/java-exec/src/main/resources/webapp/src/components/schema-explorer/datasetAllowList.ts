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
import type { DatasetRef } from '../../types';

/**
 * Lookup structure for dataset filtering, supporting plugin, schema, and table-level refs.
 */
export interface DatasetAllowList {
  /** Plugins where everything is allowed */
  plugins: Set<string>;
  /** Schemas where everything is allowed */
  schemas: Set<string>;
  /** Specific table/file allowances: schema → set of table/file names */
  tables: Map<string, Set<string>>;
}

/**
 * Build a hierarchical allow list from a project's dataset refs. Tables, views, and
 * materialized views are all treated as table-level refs so a project-scoped schema
 * tree shows them; plugin and schema refs allow everything beneath them.
 */
export function buildDatasetAllowList(datasets: DatasetRef[]): DatasetAllowList {
  const allow: DatasetAllowList = {
    plugins: new Set(),
    schemas: new Set(),
    tables: new Map(),
  };
  for (const ds of datasets) {
    if (ds.type === 'plugin' && ds.schema) {
      allow.plugins.add(ds.schema);
    } else if (ds.type === 'schema' && ds.schema) {
      allow.schemas.add(ds.schema);
    } else if (
      (ds.type === 'table' || ds.type === 'view' || ds.type === 'materialized_view')
      && ds.schema && ds.table
    ) {
      if (!allow.tables.has(ds.schema)) {
        allow.tables.set(ds.schema, new Set());
      }
      allow.tables.get(ds.schema)!.add(ds.table);
    }
  }
  return allow;
}
