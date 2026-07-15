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
import { useMemo } from 'react';
import { useMatch } from 'react-router-dom';
import { useProspector } from '../../hooks/useProspector';
import { ProspectorPanel } from '../prospector';
import type { ChatContext } from '../../types/ai';

/**
 * The always-available Prospector tab inside the right inspector.
 * Pages can override or augment with richer context by registering
 * their own inspector tabs via usePageChrome.
 */
export default function GlobalProspectorTab() {
  const prospector = useProspector();
  // RightInspector (which hosts this tab) renders above the router outlet in
  // AppShell, so ProjectContextProvider (mounted only inside the /projects/:id
  // route element) is not an ancestor here and useProjectContext() would throw.
  // Read the active project from the route instead. The trailing "/*" matches
  // both the bare project route ("/projects/:id") and nested ones
  // ("/projects/:id/sql", etc).
  const match = useMatch('/projects/:id/*');
  const projectId = match?.params.id;
  const context: ChatContext = useMemo(
    () => ({ feature: 'global_chat', projectId }),
    [projectId]
  );
  return <ProspectorPanel prospector={prospector} context={context} />;
}
