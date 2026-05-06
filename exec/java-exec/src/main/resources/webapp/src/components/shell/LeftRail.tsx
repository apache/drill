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
import { useCallback, useEffect, useRef, useState, type CSSProperties } from 'react';
import { Tooltip } from 'antd';
import { LeftOutlined } from '@ant-design/icons';
import { useAppChrome } from '../../contexts/AppChromeContext';

const WIDTH_KEY = 'drill-shell-left-rail-width';
const MIN_WIDTH = 220;
const MAX_WIDTH = 480;
const DEFAULT_WIDTH = 280;

function readWidth(): number {
  try {
    const v = localStorage.getItem(WIDTH_KEY);
    if (v) {
      const n = parseInt(v, 10);
      if (!Number.isNaN(n)) {
        return Math.max(MIN_WIDTH, Math.min(MAX_WIDTH, n));
      }
    }
  } catch {
    // Ignore
  }
  return DEFAULT_WIDTH;
}

/**
 * Single-slot rail that sits between the global Sidebar and the main content
 * area. Pages opt in by registering `chrome.leftRail` via usePageChrome; when
 * no rail is registered the component renders nothing (no layout impact).
 *
 * Mirrors RightInspector but anchored to the opposite edge and intentionally
 * single-slot — the use case (a schema browser, etc.) doesn't need tabs.
 */
export default function LeftRail() {
  const { chrome, leftRailCollapsed, toggleLeftRail } = useAppChrome();
  const rail = chrome.leftRail;

  const [width, setWidth] = useState<number>(readWidth);
  const [dragging, setDragging] = useState(false);
  const widthRef = useRef(width);
  widthRef.current = width;

  const onResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setDragging(true);
    const prevCursor = document.body.style.cursor;
    const prevSelect = document.body.style.userSelect;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    // Where the rail's left edge actually is on screen — depends on the global
    // Sidebar width, which we can read from getBoundingClientRect.
    const railEl = (e.currentTarget as HTMLElement).parentElement;
    const railLeft = railEl?.getBoundingClientRect().left ?? 0;

    const onMouseMove = (ev: MouseEvent) => {
      const next = Math.max(MIN_WIDTH, Math.min(MAX_WIDTH, ev.clientX - railLeft));
      setWidth(next);
    };
    const onMouseUp = () => {
      setDragging(false);
      document.body.style.cursor = prevCursor;
      document.body.style.userSelect = prevSelect;
      try {
        localStorage.setItem(WIDTH_KEY, String(widthRef.current));
      } catch {
        // Ignore
      }
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }, []);

  // No registered rail → render nothing so this leaves no layout footprint.
  if (!rail) {
    return null;
  }

  if (leftRailCollapsed) {
    // Collapsed strip with a single chevron to expand. Keeps the UI honest:
    // when the rail is hidden, the user still has a visible affordance to
    // bring it back without learning a keyboard shortcut.
    return (
      <Tooltip title={`Show ${typeof rail.title === 'string' ? rail.title : 'left panel'}`} placement="right">
        <button
          type="button"
          className="shell-left-rail-collapsed"
          onClick={toggleLeftRail}
          aria-label="Expand left panel"
        >
          <LeftOutlined style={{ transform: 'rotate(180deg)' }} />
        </button>
      </Tooltip>
    );
  }

  const inlineStyle: CSSProperties = { width };

  return (
    <aside
      className={`shell-left-rail${dragging ? ' is-dragging' : ''}`}
      style={inlineStyle}
    >
      {rail.title && (
        <div className="shell-left-rail-header">
          <span className="shell-left-rail-title">{rail.title}</span>
          <Tooltip title="Hide" placement="right">
            <button
              type="button"
              className="shell-left-rail-collapse-btn"
              onClick={toggleLeftRail}
              aria-label="Hide left panel"
            >
              <LeftOutlined />
            </button>
          </Tooltip>
        </div>
      )}
      <div className="shell-left-rail-body">{rail.content}</div>
      <div
        className={`shell-left-rail-resize-handle${dragging ? ' is-dragging' : ''}`}
        onMouseDown={onResizeStart}
        role="separator"
        aria-orientation="vertical"
        aria-label="Resize left panel"
      />
    </aside>
  );
}

/**
 * Auto-collapse the left rail when the viewport is narrower than the
 * threshold and re-evaluates on resize. Uses the user's persisted state
 * elsewhere, so once you manually expand on a wide screen and then resize
 * down, you stay collapsed; if you resize back up, you stay collapsed too
 * (the auto-collapse only fires on initial mount or when crossing the
 * threshold downward).
 */
export function useAutoCollapseLeftRail() {
  const { setLeftRailCollapsed } = useAppChrome();

  useEffect(() => {
    if (typeof window === 'undefined' || !window.matchMedia) {
      return;
    }
    const mq = window.matchMedia('(max-width: 1099px)');
    const handler = (ev: MediaQueryListEvent | MediaQueryList) => {
      if (ev.matches) {
        setLeftRailCollapsed(true);
      }
    };
    handler(mq);
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, [setLeftRailCollapsed]);
}
