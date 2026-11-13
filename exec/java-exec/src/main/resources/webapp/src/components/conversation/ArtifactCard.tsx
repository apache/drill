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
import {
  AppstoreOutlined,
  BarChartOutlined,
  CodeOutlined,
  DashboardOutlined,
  DatabaseOutlined,
  FileTextOutlined,
} from '@ant-design/icons';
import type { ReactNode } from 'react';
import type { ArtifactKind } from './ArtifactCanvas';

export interface ArtifactCardProps {
  kind: ArtifactKind;
  id: string;
  /** The name the artifact had when Prospector made it. */
  name: string;
  /** Whether this artifact is the one on the canvas now. */
  live: boolean;
  onOpen: () => void;
}

const KINDS: Record<ArtifactKind, { label: string; icon: ReactNode }> = {
  chart: { label: 'Chart', icon: <BarChartOutlined /> },
  dashboard: { label: 'Dashboard', icon: <DashboardOutlined /> },
  query: { label: 'Saved query', icon: <CodeOutlined /> },
  workflow: { label: 'Workflow', icon: <AppstoreOutlined /> },
  wiki: { label: 'Wiki page', icon: <FileTextOutlined /> },
  datasource: { label: 'Data source', icon: <DatabaseOutlined /> },
};

/**
 * A record in the thread that Prospector made something. It is a record of the moment: the
 * name is the one the artifact was created with, and the card stays if the artifact is later
 * renamed or deleted elsewhere. Opening it puts the artifact on the canvas, whose viewer says
 * so if it no longer exists.
 */
export default function ArtifactCard({ kind, id, name, live, onOpen }: ArtifactCardProps) {
  const { label, icon } = KINDS[kind];
  return (
    <button
      type="button"
      data-artifact-id={id}
      aria-current={live ? 'true' : undefined}
      onClick={onOpen}
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: 8,
        width: '100%',
        marginTop: 8,
        padding: '6px 10px',
        textAlign: 'left',
        cursor: 'pointer',
        color: 'inherit',
        font: 'inherit',
        background: 'var(--color-bg-container)',
        border: `1px solid ${live ? 'var(--color-primary)' : 'var(--color-border-secondary)'}`,
        borderRadius: 6,
      }}
    >
      <span aria-hidden="true">{icon}</span>
      <span style={{ fontSize: 11, opacity: 0.65, whiteSpace: 'nowrap' }}>{label}</span>
      <span style={{ flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {name}
      </span>
      {live && <span style={{ fontSize: 11, fontWeight: 600 }}>Live</span>}
    </button>
  );
}
