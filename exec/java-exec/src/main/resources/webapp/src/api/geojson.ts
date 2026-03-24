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

/**
 * Fetch GeoJSON for a map by ID from the public/geojson/ directory.
 * The base URL is relative to the page location to work in both dev and prod.
 */
export async function fetchGeoJson(mapId: string): Promise<object> {
  try {
    // Use relative path that works regardless of deployment location
    // In dev: served by Vite from public/
    // In prod: served from webapp resources
    const url = `/sqllab/geojson/${mapId}.json`;

    console.debug(`[GeoJSON] Fetching from: ${url}`);

    const response = await fetch(url);

    if (!response.ok) {
      console.error(`[GeoJSON] 404 for ${url} - trying fallback paths`);

      // Try alternative paths
      const altPaths = [
        `/geojson/${mapId}.json`,
        `./geojson/${mapId}.json`,
      ];

      for (const altPath of altPaths) {
        try {
          const altResponse = await fetch(altPath);
          if (altResponse.ok) {
            console.log(`[GeoJSON] Found at fallback path: ${altPath}`);
            const data = await altResponse.json();
            return data;
          }
        } catch {
          // Continue to next fallback
        }
      }

      throw new Error(`HTTP ${response.status} - map file not found at ${url}`);
    }

    const data = await response.json();

    if (!data || typeof data !== 'object') {
      throw new Error(`Invalid GeoJSON for map ${mapId}`);
    }

    return data;
  } catch (error) {
    console.error(`[GeoJSON] Error loading ${mapId}:`, error);
    throw error;
  }
}
