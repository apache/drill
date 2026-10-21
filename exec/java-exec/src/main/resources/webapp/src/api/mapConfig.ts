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

export interface MapConfig {
  /** XYZ tile template. Empty means bundled vector basemaps only. */
  tileUrl: string;
  /** Credit line for the tile provider. Never empty when tileUrl is set. */
  attribution: string;
}

const NO_TILES: MapConfig = { tileUrl: '', attribution: '' };

/**
 * Basemap configuration.
 *
 * Degrades to the bundled vector basemap on any failure: a map with no tiles is
 * perfectly usable for drawing, so an unreachable config should not break the feature.
 *
 * A tile URL arriving without an attribution is discarded. The server rejects that
 * combination, so it means something bypassed validation — and rendering tiles while
 * crediting nobody is the situation the attribution field exists to prevent.
 */
export async function getMapConfig(): Promise<MapConfig> {
  try {
    const response = await apiClient.get<Partial<MapConfig>>('/api/v1/map/config');
    const tileUrl = response.data?.tileUrl ?? '';
    const attribution = response.data?.attribution ?? '';
    if (!tileUrl || !attribution) {
      return NO_TILES;
    }
    return { tileUrl, attribution };
  } catch {
    return NO_TILES;
  }
}
