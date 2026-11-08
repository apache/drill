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
import { useEffect, useRef, useState } from 'react';
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

// Things that handle Escape themselves and are drawn outside the canvas. While one is open
// the key belongs to it, not to full screen. Modals and select/dropdown/popover panels
// from antd, and the context menus ag-grid opens.
const OPEN_OVERLAY_SELECTOR = [
  '.ant-modal-wrap:not([style*="display: none"])',
  '.ant-dropdown:not(.ant-dropdown-hidden)',
  '.ant-select-dropdown:not(.ant-select-dropdown-hidden)',
  '.ant-popover:not(.ant-popover-hidden)',
  '.ag-popup-child',
].join(',');

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
  // The chosen tab is remembered per kind, so one viewer's selection can never be applied to
  // another that happens to share a tab name.
  const [tabByKind, setTabByKind] = useState<Record<string, string>>({});

  const viewer = shown ? viewerFor(shown) : null;
  const activeTab = shown && viewer ? tabByKind[shown.kind] ?? viewer.tabs[0].key : undefined;

  const asideRef = useRef<HTMLElement>(null);
  const fullScreenButtonRef = useRef<HTMLButtonElement>(null);

  // Full screen is a modal dialog: Escape leaves it, focus goes to the exit control and
  // comes back to the full-screen control, and nothing behind it can be tabbed to.
  const onFullScreenRef = useRef(onFullScreen);
  onFullScreenRef.current = onFullScreen;
  const wasFullScreen = useRef(false);
  useEffect(() => {
    if (isFullScreen !== wasFullScreen.current) {
      wasFullScreen.current = isFullScreen;
      fullScreenButtonRef.current?.focus();
    }
    if (!isFullScreen) {
      return undefined;
    }
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key !== 'Escape' || e.defaultPrevented || document.querySelector(OPEN_OVERLAY_SELECTOR)) {
        return;
      }
      onFullScreenRef.current();
    };
    document.addEventListener('keydown', onKeyDown);

    // Siblings of the canvas and of each of its ancestors, up to the body: the chat, the
    // conversation list and the app shell. Only those that are not already inert, so exit
    // leaves anything else's inert alone.
    const inerted: Element[] = [];
    for (let node: Element | null = asideRef.current; node && node !== document.body; node = node.parentElement) {
      Array.from(node.parentElement?.children ?? []).forEach((sibling) => {
        if (sibling !== node && !sibling.hasAttribute('inert')) {
          sibling.setAttribute('inert', '');
          inerted.push(sibling);
        }
      });
    }
    return () => {
      document.removeEventListener('keydown', onKeyDown);
      inerted.forEach((el) => el.removeAttribute('inert'));
    };
  }, [isFullScreen]);

  return (
    <aside
      ref={asideRef}
      aria-label="Canvas"
      role={isFullScreen ? 'dialog' : undefined}
      aria-modal={isFullScreen ? true : undefined}
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
              onChange={(key) => shown && setTabByKind((prev) => ({ ...prev, [shown.kind]: key }))}
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
            ref={fullScreenButtonRef}
            type="text"
            size="small"
            aria-label={isFullScreen ? 'Exit full screen' : 'Full screen'}
            icon={isFullScreen ? <FullscreenExitOutlined /> : <FullscreenOutlined />}
            disabled={!shown && !isFullScreen}
            onClick={onFullScreen}
          />
        </Tooltip>
      </div>
      <div data-testid="canvas-body" data-active-tab={activeTab} style={{ flex: 1, minHeight: 0 }}>
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
