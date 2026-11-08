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
import { useState } from 'react';
import type { ReactNode } from 'react';
import { Button, Empty, Tabs, Tooltip } from 'antd';
import {
  FullscreenExitOutlined,
  FullscreenOutlined,
  PushpinFilled,
  PushpinOutlined,
} from '@ant-design/icons';
import type { QueryResult } from '../../types';
import ResultsCanvas, { RESULTS_TABS } from './ResultsCanvas';

export type CanvasTarget =
  | { kind: 'results'; sql: string; result: QueryResult }
  | { kind: 'chart' | 'dashboard' | 'query' | 'workflow' | 'wiki' | 'datasource'; id: string };

export interface CanvasHostProps {
  /** What Prospector is working on, or null when there is nothing yet. */
  target: CanvasTarget | null;
  /** While pinned the canvas keeps showing what it shows now, whatever target arrives. */
  pinned: boolean;
  onTogglePin: () => void;
  onFullScreen: () => void;
  /** Whether the page has the canvas full screen, so the control can offer the way back. */
  isFullScreen?: boolean;
}

interface Viewer {
  /** The secondary tabs this kind supports. A single tab shows no strip. */
  tabs: { key: string; label: string }[];
  render: (tab: string) => ReactNode;
}

const STUB_LABELS = {
  chart: 'chart',
  dashboard: 'dashboard',
  query: 'saved query',
  workflow: 'workflow',
  wiki: 'wiki page',
  datasource: 'data source',
} as const;

/** The viewer for a target. Task 8 replaces each stub with the real component. */
function viewerFor(target: CanvasTarget): Viewer {
  if (target.kind === 'results') {
    return {
      tabs: RESULTS_TABS,
      render: (tab) => <ResultsCanvas sql={target.sql} result={target.result} tab={tab} />,
    };
  }
  return {
    tabs: [{ key: 'view', label: 'View' }],
    render: () => (
      <Empty
        image={Empty.PRESENTED_IMAGE_SIMPLE}
        description={`The ${STUB_LABELS[target.kind]} viewer for ${target.id} is not available yet.`}
        style={{ marginTop: 48 }}
      />
    ),
  };
}

/**
 * The slot beside the chat that shows whatever Prospector is working on. It owns the
 * secondary tab strip and the pin and full-screen controls; each viewer says which tabs
 * it supports and renders the body for the active one.
 */
export default function CanvasHost({
  target,
  pinned,
  onTogglePin,
  onFullScreen,
  isFullScreen = false,
}: CanvasHostProps) {
  // What is on screen. Follows target until pinned, then holds. Adjusting state during
  // render (rather than in an effect) means a pinned canvas never flashes the new target.
  const [shown, setShown] = useState<CanvasTarget | null>(target);
  if (!pinned && shown !== target) {
    setShown(target);
  }
  const [tab, setTab] = useState('');

  const viewer = shown ? viewerFor(shown) : null;
  const activeTab = viewer?.tabs.some((t) => t.key === tab) ? tab : viewer?.tabs[0].key;

  return (
    <aside
      aria-label="Canvas"
      style={{
        width: isFullScreen ? undefined : 440,
        flexShrink: 0,
        borderLeft: '1px solid var(--color-border-secondary)',
        display: 'flex',
        flexDirection: 'column',
        minHeight: 0,
        background: 'var(--color-bg-container)',
        ...(isFullScreen
          ? { position: 'fixed', inset: 0, zIndex: 1000, borderLeft: 'none' }
          : {}),
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 4,
          padding: '0 8px',
          borderBottom: '1px solid var(--color-border-secondary)',
        }}
      >
        <div style={{ flex: 1, minWidth: 0 }}>
          {viewer && viewer.tabs.length > 1 && (
            <Tabs
              size="small"
              activeKey={activeTab}
              onChange={setTab}
              items={viewer.tabs}
              tabBarStyle={{ margin: 0 }}
            />
          )}
        </div>
        <Tooltip title={pinned ? 'Unpin: follow new results' : 'Pin: keep showing this'}>
          <Button
            type="text"
            size="small"
            aria-label={pinned ? 'Unpin canvas' : 'Pin canvas'}
            icon={pinned ? <PushpinFilled /> : <PushpinOutlined />}
            disabled={!shown && !pinned}
            onClick={onTogglePin}
          />
        </Tooltip>
        <Tooltip title={isFullScreen ? 'Exit full screen' : 'Full screen'}>
          <Button
            type="text"
            size="small"
            aria-label={isFullScreen ? 'Exit full screen' : 'Full screen'}
            icon={isFullScreen ? <FullscreenExitOutlined /> : <FullscreenOutlined />}
            disabled={!shown && !isFullScreen}
            onClick={onFullScreen}
          />
        </Tooltip>
      </div>
      <div style={{ flex: 1, minHeight: 0 }}>
        {viewer && activeTab ? (
          viewer.render(activeTab)
        ) : (
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description="Nothing on the canvas yet. Charts and tables from this conversation will appear here."
            style={{ marginTop: 48 }}
          />
        )}
      </div>
    </aside>
  );
}
