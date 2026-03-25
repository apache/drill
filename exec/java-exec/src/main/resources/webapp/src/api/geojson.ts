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

import * as topojson from 'topojson-client';

/**
 * Fetch GeoJSON for a map by ID from the public/geojson/ directory.
 * Supports both .geojson and .topojson formats.
 * TopoJSON is automatically converted to GeoJSON for ECharts compatibility.
 * The base URL is relative to the page location to work in both dev and prod.
 */
export async function fetchGeoJson(mapId: string): Promise<object> {
  try {
    // Try TopoJSON first (more compact), fall back to GeoJSON
    let data = await tryFetchMap(mapId, '.topojson');
    if (!data) {
      data = await tryFetchMap(mapId, '.json');
    }

    if (!data || typeof data !== 'object') {
      throw new Error(`Invalid GeoJSON for map ${mapId}`);
    }

    // If it's TopoJSON, convert to GeoJSON
    if ('objects' in data) {
      console.debug(`[GeoJSON] Converting TopoJSON to GeoJSON for ${mapId}`);
      return convertTopoJsonToGeoJson(data as topojson.Topology);
    }

    return data;
  } catch (error) {
    console.error(`[GeoJSON] Error loading ${mapId}:`, error);
    throw error;
  }
}

async function tryFetchMap(mapId: string, ext: string): Promise<unknown> {
  try {
    const url = `/sqllab/geojson/${mapId}${ext}`;
    console.debug(`[GeoJSON] Fetching from: ${url}`);

    const response = await fetch(url);
    if (!response.ok) {
      // Try alternative paths
      const altPaths = [
        `/geojson/${mapId}${ext}`,
        `./geojson/${mapId}${ext}`,
      ];

      for (const altPath of altPaths) {
        try {
          const altResponse = await fetch(altPath);
          if (altResponse.ok) {
            console.log(`[GeoJSON] Found at fallback path: ${altPath}`);
            return altResponse.json();
          }
        } catch {
          // Continue to next fallback
        }
      }
      return null;
    }

    return response.json();
  } catch (error) {
    console.debug(`[GeoJSON] Error trying ${ext}:`, error);
    return null;
  }
}

function convertTopoJsonToGeoJson(
  topoData: topojson.Topology
): Record<string, unknown> {
  try {
    // Get the first object from the topology (usually 'features' or a similar key)
    const objectKey = Object.keys(topoData.objects)[0];
    if (!objectKey) {
      throw new Error('TopoJSON has no objects');
    }

    // Convert the topology object to GeoJSON FeatureCollection
    const geoJson = topojson.feature(topoData, topoData.objects[objectKey]);
    console.debug(`[GeoJSON] Converted TopoJSON with key "${objectKey}"`);
    return geoJson;
  } catch (error) {
    console.error(`[GeoJSON] TopoJSON conversion failed:`, error);
    throw new Error(`Failed to convert TopoJSON: ${error}`);
  }
}
