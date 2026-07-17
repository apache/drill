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
  multiSelectAllowed?: boolean;  // Whether this scope allows multi-select (for state-level ZIP codes)
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
  { id: 'us-zipcodes',    label: 'US ZIP Codes (All)',         group: 'North America', center: [-98, 39],  zoom: 4.0, resolve: passthrough },
  // State-level ZIP codes (support multi-select)
  { id: 'us-al-zipcodes', label: 'Alabama ZIP Codes',          group: 'US ZIP Codes (by State)', center: [-86.9023, 32.8067],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ak-zipcodes', label: 'Alaska ZIP Codes',           group: 'US ZIP Codes (by State)', center: [-152.404, 63.5885],   zoom: 3.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-az-zipcodes', label: 'Arizona ZIP Codes',          group: 'US ZIP Codes (by State)', center: [-111.4312, 33.7298],  zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ar-zipcodes', label: 'Arkansas ZIP Codes',         group: 'US ZIP Codes (by State)', center: [-92.3731, 34.9697],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ca-zipcodes', label: 'California ZIP Codes',       group: 'US ZIP Codes (by State)', center: [-119.4179, 36.1162],  zoom: 4.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-co-zipcodes', label: 'Colorado ZIP Codes',         group: 'US ZIP Codes (by State)', center: [-105.3111, 39.0598],  zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ct-zipcodes', label: 'Connecticut ZIP Codes',      group: 'US ZIP Codes (by State)', center: [-72.7554, 41.5978],   zoom: 6.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-de-zipcodes', label: 'Delaware ZIP Codes',         group: 'US ZIP Codes (by State)', center: [-75.5277, 39.3185],   zoom: 6.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-fl-zipcodes', label: 'Florida ZIP Codes',          group: 'US ZIP Codes (by State)', center: [-81.5158, 27.6648],   zoom: 4.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ga-zipcodes', label: 'Georgia ZIP Codes',          group: 'US ZIP Codes (by State)', center: [-83.6431, 33.0406],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-hi-zipcodes', label: 'Hawaii ZIP Codes',           group: 'US ZIP Codes (by State)', center: [-157.5, 20.7967],     zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-id-zipcodes', label: 'Idaho ZIP Codes',            group: 'US ZIP Codes (by State)', center: [-113.9626, 44.2405],  zoom: 4.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-il-zipcodes', label: 'Illinois ZIP Codes',         group: 'US ZIP Codes (by State)', center: [-88.9937, 40.3495],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-in-zipcodes', label: 'Indiana ZIP Codes',          group: 'US ZIP Codes (by State)', center: [-85.2604, 39.8494],   zoom: 5.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ia-zipcodes', label: 'Iowa ZIP Codes',             group: 'US ZIP Codes (by State)', center: [-93.0977, 42.0115],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ks-zipcodes', label: 'Kansas ZIP Codes',           group: 'US ZIP Codes (by State)', center: [-96.7265, 38.5266],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ky-zipcodes', label: 'Kentucky ZIP Codes',         group: 'US ZIP Codes (by State)', center: [-84.6701, 37.6681],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-la-zipcodes', label: 'Louisiana ZIP Codes',        group: 'US ZIP Codes (by State)', center: [-91.8749, 30.9843],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-me-zipcodes', label: 'Maine ZIP Codes',            group: 'US ZIP Codes (by State)', center: [-69.6619, 44.6939],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-md-zipcodes', label: 'Maryland ZIP Codes',         group: 'US ZIP Codes (by State)', center: [-76.7519, 39.0639],   zoom: 6.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ma-zipcodes', label: 'Massachusetts ZIP Codes',    group: 'US ZIP Codes (by State)', center: [-71.5724, 42.2302],   zoom: 6.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-mi-zipcodes', label: 'Michigan ZIP Codes',         group: 'US ZIP Codes (by State)', center: [-84.5361, 43.3266],   zoom: 4.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-mn-zipcodes', label: 'Minnesota ZIP Codes',        group: 'US ZIP Codes (by State)', center: [-93.9196, 45.6945],   zoom: 4.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ms-zipcodes', label: 'Mississippi ZIP Codes',      group: 'US ZIP Codes (by State)', center: [-89.6787, 32.7416],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-mo-zipcodes', label: 'Missouri ZIP Codes',         group: 'US ZIP Codes (by State)', center: [-91.8318, 38.4561],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-mt-zipcodes', label: 'Montana ZIP Codes',          group: 'US ZIP Codes (by State)', center: [-109.6333, 47.0527],  zoom: 4.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ne-zipcodes', label: 'Nebraska ZIP Codes',         group: 'US ZIP Codes (by State)', center: [-99.9018, 41.4925],   zoom: 4.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-nv-zipcodes', label: 'Nevada ZIP Codes',           group: 'US ZIP Codes (by State)', center: [-117.0554, 38.3135],  zoom: 4.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-nh-zipcodes', label: 'New Hampshire ZIP Codes',    group: 'US ZIP Codes (by State)', center: [-71.5653, 43.4525],   zoom: 6.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-nj-zipcodes', label: 'New Jersey ZIP Codes',       group: 'US ZIP Codes (by State)', center: [-74.5210, 40.0583],   zoom: 6.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-nm-zipcodes', label: 'New Mexico ZIP Codes',       group: 'US ZIP Codes (by State)', center: [-106.3700, 34.5199],  zoom: 4.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ny-zipcodes', label: 'New York ZIP Codes',         group: 'US ZIP Codes (by State)', center: [-74.9481, 42.1657],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-nc-zipcodes', label: 'North Carolina ZIP Codes',   group: 'US ZIP Codes (by State)', center: [-79.8064, 35.6301],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-nd-zipcodes', label: 'North Dakota ZIP Codes',     group: 'US ZIP Codes (by State)', center: [-99.6635, 47.5289],   zoom: 4.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-oh-zipcodes', label: 'Ohio ZIP Codes',             group: 'US ZIP Codes (by State)', center: [-82.7649, 40.3888],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ok-zipcodes', label: 'Oklahoma ZIP Codes',         group: 'US ZIP Codes (by State)', center: [-96.9289, 35.5653],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-or-zipcodes', label: 'Oregon ZIP Codes',           group: 'US ZIP Codes (by State)', center: [-122.0709, 43.8041],  zoom: 4.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-pa-zipcodes', label: 'Pennsylvania ZIP Codes',     group: 'US ZIP Codes (by State)', center: [-77.2098, 40.5908],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ri-zipcodes', label: 'Rhode Island ZIP Codes',     group: 'US ZIP Codes (by State)', center: [-71.5118, 41.6812],   zoom: 7.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-sc-zipcodes', label: 'South Carolina ZIP Codes',   group: 'US ZIP Codes (by State)', center: [-80.9066, 34.0007],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-sd-zipcodes', label: 'South Dakota ZIP Codes',     group: 'US ZIP Codes (by State)', center: [-99.4388, 44.2998],   zoom: 4.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-tn-zipcodes', label: 'Tennessee ZIP Codes',        group: 'US ZIP Codes (by State)', center: [-86.6923, 35.7478],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-tx-zipcodes', label: 'Texas ZIP Codes',            group: 'US ZIP Codes (by State)', center: [-99.9018, 31.9686],   zoom: 4.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-ut-zipcodes', label: 'Utah ZIP Codes',             group: 'US ZIP Codes (by State)', center: [-111.0937, 39.3210],  zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-vt-zipcodes', label: 'Vermont ZIP Codes',          group: 'US ZIP Codes (by State)', center: [-72.7107, 43.9695],   zoom: 6.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-va-zipcodes', label: 'Virginia ZIP Codes',         group: 'US ZIP Codes (by State)', center: [-78.1694, 37.7693],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-wa-zipcodes', label: 'Washington ZIP Codes',       group: 'US ZIP Codes (by State)', center: [-120.7401, 47.4009],  zoom: 4.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-wv-zipcodes', label: 'West Virginia ZIP Codes',    group: 'US ZIP Codes (by State)', center: [-80.4549, 38.5976],   zoom: 5.5, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-wi-zipcodes', label: 'Wisconsin ZIP Codes',        group: 'US ZIP Codes (by State)', center: [-89.0165, 44.2685],   zoom: 5.0, resolve: passthrough, multiSelectAllowed: true },
  { id: 'us-wy-zipcodes', label: 'Wyoming ZIP Codes',          group: 'US ZIP Codes (by State)', center: [-107.3025, 42.7559],  zoom: 4.0, resolve: passthrough, multiSelectAllowed: true },
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

  // Add sub-national groups (in order: NA with ZIP codes, SA, Europe, Oceania, Asia)
  const groups = ['North America', 'US ZIP Codes (by State)', 'South America', 'Europe', 'Oceania', 'Asia'];
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
