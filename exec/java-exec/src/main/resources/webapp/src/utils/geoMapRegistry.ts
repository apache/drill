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

import { resolveLocationName } from './isoCountryCodes';
import { resolveUsStateName, resolveCanadianProvinceName } from './usStatesCaProvinces';

export interface GeoMapDefinition {
  id: string;           // ECharts map ID (also the filename without .json)
  label: string;        // Display label in the scope selector
  group: string;        // Grouping label (e.g. 'North America', 'Europe')
  center: [number, number];
  zoom: number;
  resolve: (code: string) => string;  // code/name → GeoJSON feature name
}

// Resolver for regions that don't need transformation (full names already match)
const passthrough = (code: string) => code;

export const GEO_MAP_REGISTRY: GeoMapDefinition[] = [
  // World-level maps
  { id: 'world',          label: 'World',                      group: 'World',         center: [0, 20],    zoom: 1.2, resolve: resolveLocationName },
  // World regions (all use 'world' map with different center/zoom)
  { id: 'world-europe',      label: 'Europe',                     group: 'World Regions', center: [15, 52],   zoom: 4.5, resolve: resolveLocationName },
  { id: 'world-na',          label: 'North America',              group: 'World Regions', center: [-96, 50],  zoom: 2.5, resolve: resolveLocationName },
  { id: 'world-sa',          label: 'South America',              group: 'World Regions', center: [-60, -15], zoom: 2.2, resolve: resolveLocationName },
  { id: 'world-africa',      label: 'Africa',                     group: 'World Regions', center: [20, 0],    zoom: 2.0, resolve: resolveLocationName },
  { id: 'world-asia',        label: 'Asia',                       group: 'World Regions', center: [90, 45],   zoom: 2.0, resolve: resolveLocationName },
  { id: 'world-oceania',     label: 'Oceania',                    group: 'World Regions', center: [140, -20],  zoom: 2.5, resolve: resolveLocationName },
  // Sub-national: North America
  { id: 'us-states',      label: 'US States',                  group: 'North America', center: [-98, 39],  zoom: 3.5, resolve: resolveUsStateName },
  { id: 'us-counties',    label: 'US Counties',                group: 'North America', center: [-98, 39],  zoom: 3.5, resolve: passthrough },
  { id: 'us-zipcodes',    label: 'US ZIP Codes',               group: 'North America', center: [-98, 39],  zoom: 4.0, resolve: passthrough },
  { id: 'ca-provinces',   label: 'Canadian Provinces',         group: 'North America', center: [-106, 56], zoom: 2.5, resolve: resolveCanadianProvinceName },
  { id: 'mx-states',      label: 'Mexican States',             group: 'North America', center: [-102, 24], zoom: 3.5, resolve: passthrough },
  // Sub-national: South America
  { id: 'br-states',      label: 'Brazilian States',           group: 'South America', center: [-55, -12], zoom: 2.5, resolve: passthrough },
  // Sub-national: Europe
  { id: 'gb-regions',     label: 'UK Regions',                 group: 'Europe',        center: [-2, 54],   zoom: 4.5, resolve: passthrough },
  { id: 'de-states',      label: 'German States',              group: 'Europe',        center: [10, 51],   zoom: 4.5, resolve: passthrough },
  { id: 'fr-departments', label: 'French Departments',         group: 'Europe',        center: [2.5, 46],  zoom: 3.5, resolve: passthrough },
  // Sub-national: Oceania
  { id: 'au-states',      label: 'Australian States',          group: 'Oceania',       center: [134, -28], zoom: 2.5, resolve: passthrough },
  // Sub-national: Asia
  { id: 'in-states',      label: 'Indian States',              group: 'Asia',          center: [78, 22],   zoom: 3.0, resolve: passthrough },
  { id: 'cn-provinces',   label: 'Chinese Provinces',          group: 'Asia',          center: [105, 36],  zoom: 2.5, resolve: passthrough },
  { id: 'jp-prefectures', label: 'Japanese Prefectures',       group: 'Asia',          center: [138, 37],  zoom: 4.5, resolve: passthrough },
];

/**
 * Get a map definition by ID.
 */
export function getMapDef(mapId: string): GeoMapDefinition | undefined {
  return GEO_MAP_REGISTRY.find((m) => m.id === mapId);
}

/**
 * Build grouped options for Ant Design Select with optGroup support.
 * Format: [{ value, label }, { label: "Group", options: [...] }, ...]
 */
export function buildGroupedOptions(
  maps: GeoMapDefinition[]
): Array<{ label: string; value?: string; options?: Array<{ label: string; value: string }> }> {
  const grouped = new Map<string, GeoMapDefinition[]>();

  // Group maps by their group field
  for (const map of maps) {
    if (!grouped.has(map.group)) {
      grouped.set(map.group, []);
    }
    grouped.get(map.group)!.push(map);
  }

  // Build option array: maintain insertion order via Map
  const result: Array<{ label: string; value?: string; options?: Array<{ label: string; value: string }> }> = [];

  // Add "World" as a simple option (not grouped)
  const worldMap = maps.find((m) => m.id === 'world');
  if (worldMap) {
    result.push({ value: worldMap.id, label: worldMap.label });
  }

  // Add World Regions group
  const worldRegions = grouped.get('World Regions');
  if (worldRegions && worldRegions.length > 0) {
    result.push({
      label: 'World Regions',
      options: worldRegions.map((m) => ({ label: m.label, value: m.id })),
    });
  }

  // Add sub-national groups (in order: NA, SA, Europe, Oceania, Asia)
  const groups = ['North America', 'South America', 'Europe', 'Oceania', 'Asia'];
  for (const group of groups) {
    const maps_in_group = grouped.get(group);
    if (maps_in_group && maps_in_group.length > 0) {
      result.push({
        label: group,
        options: maps_in_group.map((m) => ({ label: m.label, value: m.id })),
      });
    }
  }

  return result;
}

// Pre-built grouped options for export
export const GEO_SCOPE_OPTIONS = buildGroupedOptions(GEO_MAP_REGISTRY);

/**
 * Get the actual map ID for world region shortcuts.
 * World regions like 'europe' use 'world' map with different center/zoom.
 */
export function getActualMapId(scopeKey: string): string {
  const mapDef = getMapDef(scopeKey);
  if (mapDef?.id.startsWith('world-')) {
    return 'world';  // world-europe, world-asia, etc. all use 'world' map
  }
  return mapDef?.id || 'world';
}
