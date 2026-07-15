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
 * Human-readable label for every `feature` slug the server can record on an AI event.
 *
 * The AI analytics dashboard uses this both to render the feature column and to build
 * the feature filter dropdown, so a slug missing here is unselectable in the UI. Every
 * client-side slug is passed as `context.feature` on a Prospector chat request; the
 * server-side slugs come from AiConfigResources (config_test), TranspileResources
 * (transpile), and ProspectorResources' default (prospector_chat).
 *
 * Keep this in sync with the table in docs/dev/ui/pages/ai-analytics.md. The companion
 * test scans the source tree for emitted slugs and fails when one has no label here.
 */
export const FEATURE_LABEL: Record<string, string> = {
  // SQL Lab
  sql_lab_chat: 'SQL Lab chat',
  sql_lab_optimize: 'SQL Lab optimize',
  // Standalone query actions
  query_suggestions: 'Query suggestions',
  explain_query: 'Explain query',
  optimize_query: 'Optimize query',
  // Dashboards
  dashboard_qna: 'Dashboard Q&A',
  executive_summary: 'Executive summary',
  nl_filter: 'Natural-language filter',
  ai_alerts: 'AI alerts',
  // Other pages
  log_analysis: 'Log analysis',
  wiki_generation: 'Wiki generation',
  profile_analysis: 'Profile analysis',
  global_chat: 'Global Prospector chat',
  filesystem_form: 'Filesystem plugin form',
  // Server-originated
  config_test: 'AI config test',
  transpile: 'SQL transpile',
  prospector_chat: 'Prospector chat',
};

/** Label for a feature slug, falling back to the raw slug for unknown values. */
export function featureLabel(feature: string): string {
  return FEATURE_LABEL[feature] ?? feature;
}
