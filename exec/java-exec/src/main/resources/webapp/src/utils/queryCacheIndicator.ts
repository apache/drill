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

import { lookupCacheForQuery } from './queryExecutionHistory';

export interface CacheIndicator {
  hasCached: boolean;
  tooltip: string;
  icon: string; // Emoji or icon key
}

/**
 * Get cache status indicator for a query.
 * Shows whether results are cached and how old the cache is.
 */
export function getCacheIndicator(sql: string): CacheIndicator {
  const cached = lookupCacheForQuery(sql);

  if (!cached) {
    return {
      hasCached: false,
      tooltip: 'No cached results',
      icon: '',
    };
  }

  const ageMs = Date.now() - cached.executedAt;
  const ageMinutes = Math.floor(ageMs / 60000);
  const ageHours = Math.floor(ageMs / 3600000);

  let ageText = '';
  if (ageMinutes < 1) {
    ageText = 'just now';
  } else if (ageMinutes < 60) {
    ageText = `${ageMinutes}m ago`;
  } else if (ageHours < 24) {
    ageText = `${ageHours}h ago`;
  } else {
    ageText = `${Math.floor(ageHours / 24)}d ago`;
  }

  return {
    hasCached: true,
    tooltip: `Cached ${ageText} • ${cached.rowCount.toLocaleString()} rows`,
    icon: '⚡', // Lightning bolt to indicate fast/cached
  };
}
