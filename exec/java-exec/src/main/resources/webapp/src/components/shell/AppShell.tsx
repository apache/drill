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
import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react';
import { useLocation } from 'react-router-dom';
import { useAppChrome } from '../../contexts/AppChromeContext';
import Sidebar from './Sidebar';
import Toolbar from './Toolbar';
import RightInspector from './RightInspector';
import LeftRail, { useAutoCollapseLeftRail } from './LeftRail';
import BrowseDataDrawer from './BrowseDataDrawer';
import CommandPalette from '../common/CommandPalette';
import KeyboardShortcutsModal from '../query-editor/KeyboardShortcutsModal';

interface AppShellProps {
  children: ReactNode;
}

function isMobileViewport(): boolean {
  if (typeof window === 'undefined' || !window.matchMedia) {
    return false;
  }
  return window.matchMedia('(max-width: 768px)').matches;
}

export default function AppShell({ children }: AppShellProps) {
  const {
    chrome,
    sidebarCollapsed,
    setSidebarCollapsed,
    toggleSidebar,
    inspectorOpen,
    setInspectorOpen,
    toggleInspector,
    leftRailCollapsed,
    toggleLeftRail,
  } = useAppChrome();
  const location = useLocation();
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [shortcutsOpen, setShortcutsOpen] = useState(false);
  const [browseOpen, setBrowseOpen] = useState(false);
  const isFirstLocation = useRef(true);

  // Auto-collapse the left rail on narrow viewports so it doesn't crowd the
  // editor. The hook stays a no-op on wide screens.
  useAutoCollapseLeftRail();

  const hasLeftRail = !!chrome.leftRail;

  // ⌘B routes to whichever surface fits context: a page that registered a
  // left rail (e.g. SQL Lab schema browser) gets toggled directly; everywhere
  // else opens the global Browse Data drawer so the user can still reach
  // schemas without leaving the page.
  const handleBrowseShortcut = useCallback(() => {
    if (hasLeftRail) {
      toggleLeftRail();
    } else {
      setBrowseOpen((v) => !v);
    }
  }, [hasLeftRail, toggleLeftRail]);

  // Global keyboard shortcuts
  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      // `?` opens the shortcuts cheatsheet (when not typing in an input)
      if (e.key === '?' && !e.metaKey && !e.ctrlKey && !e.altKey) {
        const target = e.target as HTMLElement | null;
        const isEditable =
          target?.tagName === 'INPUT' ||
          target?.tagName === 'TEXTAREA' ||
          target?.isContentEditable ||
          target?.classList.contains('monaco-editor');
        if (!isEditable) {
          e.preventDefault();
          setShortcutsOpen(true);
          return;
        }
      }

      const cmd = e.metaKey || e.ctrlKey;
      if (!cmd) {
        return;
      }
      // ⌘0 toggles sidebar
      if (e.key === '0' && !e.altKey && !e.shiftKey) {
        e.preventDefault();
        toggleSidebar();
      }
      // ⌘⌥0 toggles inspector
      if (e.key === '0' && e.altKey && !e.shiftKey) {
        e.preventDefault();
        toggleInspector();
      }
      // ⌘B toggles the contextual "Browse data" surface.
      if ((e.key === 'b' || e.key === 'B') && !e.altKey && !e.shiftKey) {
        e.preventDefault();
        handleBrowseShortcut();
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [toggleSidebar, toggleInspector, handleBrowseShortcut]);

  // On mobile, auto-close the sidebar/inspector overlays on navigation so
  // the user can see the page content. Skip the very first render.
  useEffect(() => {
    if (isFirstLocation.current) {
      isFirstLocation.current = false;
      return;
    }
    if (isMobileViewport()) {
      setSidebarCollapsed(true);
      setInspectorOpen(false);
    }
  }, [location.pathname, setSidebarCollapsed, setInspectorOpen]);

  // Translucent backdrop that appears on mobile when an overlay is up.
  // Tapping it closes both panels.
  const showBackdrop = !sidebarCollapsed || inspectorOpen;
  const handleBackdropClick = () => {
    setSidebarCollapsed(true);
    setInspectorOpen(false);
  };

  return (
    <div
      className={[
        'shell-root',
        sidebarCollapsed ? 'is-sidebar-collapsed' : '',
        inspectorOpen ? 'is-inspector-open' : '',
        hasLeftRail ? 'has-left-rail' : '',
        hasLeftRail && leftRailCollapsed ? 'is-left-rail-collapsed' : '',
      ].filter(Boolean).join(' ')}
    >
      <Sidebar />
      <div className="shell-main">
        <Toolbar
          onOpenCommand={() => setPaletteOpen(true)}
          onBrowseData={handleBrowseShortcut}
        />
        <div className="shell-content-wrap">
          <LeftRail />
          <main className="shell-content">{children}</main>
          <RightInspector />
        </div>
      </div>
      <BrowseDataDrawer open={browseOpen} onClose={() => setBrowseOpen(false)} />
      {showBackdrop && (
        <div
          className="shell-mobile-backdrop"
          onClick={handleBackdropClick}
          aria-hidden="true"
        />
      )}
      <CommandPalette
        externalOpen={paletteOpen}
        onExternalOpenChange={setPaletteOpen}
      />
      <KeyboardShortcutsModal
        open={shortcutsOpen}
        onClose={() => setShortcutsOpen(false)}
      />
    </div>
  );
}
