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
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Tooltip } from 'antd';
import { CloseOutlined, RobotOutlined } from '@ant-design/icons';
import { useAppChrome, type InspectorTab } from '../../contexts/AppChromeContext';
import GlobalProspectorTab from './GlobalProspectorTab';

const PROSPECTOR_TAB_KEY = 'prospector';
const WIDTH_KEY = 'drill-shell-inspector-width';
const MIN_WIDTH = 280;
const MAX_WIDTH = 720;
const DEFAULT_WIDTH = 360;

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

export default function RightInspector() {
  const {
    chrome,
    inspectorOpen,
    setInspectorOpen,
    inspectorActiveTab,
    setInspectorActiveTab,
  } = useAppChrome();

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

    const onMouseMove = (ev: MouseEvent) => {
      // Inspector is anchored to the right edge.
      const next = Math.max(MIN_WIDTH, Math.min(MAX_WIDTH, window.innerWidth - ev.clientX));
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

  // Always include the global Prospector tab so the inspector is meaningful
  // even on pages that don't register their own tabs. Pages can register
  // their own Prospector replacement (with richer context) by using the
  // same key, and it will take precedence.
  const tabs = useMemo<InspectorTab[]>(() => {
    const pageTabs = chrome.inspectorTabs ?? [];
    const hasProspector = pageTabs.some((t) => t.key === PROSPECTOR_TAB_KEY);
    if (hasProspector) {
      return pageTabs;
    }
    return [
      ...pageTabs,
      {
        key: PROSPECTOR_TAB_KEY,
        title: 'Prospector',
        icon: <RobotOutlined />,
        content: <GlobalProspectorTab />,
      },
    ];
  }, [chrome.inspectorTabs]);

  // Pick a default active tab when tabs change
  useEffect(() => {
    if (tabs.length === 0) {
      if (inspectorActiveTab !== null) {
        setInspectorActiveTab(null);
      }
      return;
    }
    if (!inspectorActiveTab || !tabs.find((t) => t.key === inspectorActiveTab)) {
      setInspectorActiveTab(tabs[0].key);
    }
  }, [tabs, inspectorActiveTab, setInspectorActiveTab]);

  if (!inspectorOpen) {
    return null;
  }

  const active = tabs.find((t) => t.key === inspectorActiveTab) ?? tabs[0];

  return (
    <aside className="shell-inspector" style={{ width }}>
      <div
        className={`shell-inspector-resize-handle${dragging ? ' is-dragging' : ''}`}
        onMouseDown={onResizeStart}
        role="separator"
        aria-orientation="vertical"
        aria-label="Resize inspector"
      />
      <div className="shell-inspector-header">
        <div className="shell-inspector-tabs" role="tablist">
          {tabs.map((t) => (
            <button
              key={t.key}
              type="button"
              role="tab"
              aria-selected={t.key === active.key}
              className={`shell-inspector-tab${t.key === active.key ? ' is-active' : ''}`}
              onClick={() => setInspectorActiveTab(t.key)}
            >
              {t.icon && <span className="shell-inspector-tab-icon">{t.icon}</span>}
              <span>{t.title}</span>
            </button>
          ))}
        </div>
        <Tooltip title="Hide Inspector (⌘⌥0)">
          <button
            type="button"
            className="shell-inspector-close"
            onClick={() => setInspectorOpen(false)}
            aria-label="Close inspector"
          >
            <CloseOutlined />
          </button>
        </Tooltip>
      </div>
      <div className="shell-inspector-body">{active.content}</div>
    </aside>
  );
}
