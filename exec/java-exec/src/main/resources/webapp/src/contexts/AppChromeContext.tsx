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
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';

export interface BreadcrumbSegment {
  key: string;
  label: ReactNode;
  to?: string;
  /** Render as project switcher dropdown trigger */
  onClick?: () => void;
}

export interface InspectorTab {
  key: string;
  title: string;
  icon?: ReactNode;
  content: ReactNode;
}

export interface PageChrome {
  breadcrumb?: BreadcrumbSegment[];
  toolbarActions?: ReactNode;
  inspectorTabs?: InspectorTab[];
}

interface AppChromeValue {
  chrome: PageChrome;
  setChrome: (chrome: PageChrome) => void;
  sidebarCollapsed: boolean;
  toggleSidebar: () => void;
  setSidebarCollapsed: (v: boolean) => void;
  inspectorOpen: boolean;
  toggleInspector: () => void;
  setInspectorOpen: (v: boolean) => void;
  inspectorActiveTab: string | null;
  setInspectorActiveTab: (key: string | null) => void;
}

const SIDEBAR_KEY = 'drill-shell-sidebar-collapsed';
const INSPECTOR_KEY = 'drill-shell-inspector-open';

function readBool(key: string, fallback: boolean): boolean {
  try {
    const v = localStorage.getItem(key);
    if (v === 'true') {
      return true;
    }
    if (v === 'false') {
      return false;
    }
  } catch {
    // Ignore storage errors
  }
  return fallback;
}

function isMobileViewport(): boolean {
  if (typeof window === 'undefined' || !window.matchMedia) {
    return false;
  }
  return window.matchMedia('(max-width: 768px)').matches;
}

function writeBool(key: string, value: boolean): void {
  try {
    localStorage.setItem(key, String(value));
  } catch {
    // Ignore storage errors
  }
}

const AppChromeContext = createContext<AppChromeValue>({
  chrome: {},
  setChrome: () => {},
  sidebarCollapsed: false,
  toggleSidebar: () => {},
  setSidebarCollapsed: () => {},
  inspectorOpen: false,
  toggleInspector: () => {},
  setInspectorOpen: () => {},
  inspectorActiveTab: null,
  setInspectorActiveTab: () => {},
});

export function AppChromeProvider({ children }: { children: ReactNode }) {
  const [chrome, setChromeState] = useState<PageChrome>({});
  // On first paint on a mobile viewport, default sidebar/inspector to closed
  // so the user sees content first; on desktop, restore the user's preference.
  const [sidebarCollapsed, setSidebarState] = useState<boolean>(() => {
    if (isMobileViewport()) {
      return true;
    }
    return readBool(SIDEBAR_KEY, false);
  });
  const [inspectorOpen, setInspectorState] = useState<boolean>(() => {
    if (isMobileViewport()) {
      return false;
    }
    return readBool(INSPECTOR_KEY, false);
  });
  const [inspectorActiveTab, setInspectorActiveTab] = useState<string | null>(null);

  const setChrome = useCallback((next: PageChrome) => {
    setChromeState(next);
  }, []);

  const setSidebarCollapsed = useCallback((v: boolean) => {
    setSidebarState(v);
    writeBool(SIDEBAR_KEY, v);
  }, []);

  const toggleSidebar = useCallback(() => {
    setSidebarState((prev) => {
      const next = !prev;
      writeBool(SIDEBAR_KEY, next);
      return next;
    });
  }, []);

  const setInspectorOpen = useCallback((v: boolean) => {
    setInspectorState(v);
    writeBool(INSPECTOR_KEY, v);
  }, []);

  const toggleInspector = useCallback(() => {
    setInspectorState((prev) => {
      const next = !prev;
      writeBool(INSPECTOR_KEY, next);
      return next;
    });
  }, []);

  const value = useMemo<AppChromeValue>(
    () => ({
      chrome,
      setChrome,
      sidebarCollapsed,
      toggleSidebar,
      setSidebarCollapsed,
      inspectorOpen,
      toggleInspector,
      setInspectorOpen,
      inspectorActiveTab,
      setInspectorActiveTab,
    }),
    [
      chrome,
      setChrome,
      sidebarCollapsed,
      toggleSidebar,
      setSidebarCollapsed,
      inspectorOpen,
      toggleInspector,
      setInspectorOpen,
      inspectorActiveTab,
    ],
  );

  return <AppChromeContext.Provider value={value}>{children}</AppChromeContext.Provider>;
}

export function useAppChrome(): AppChromeValue {
  return useContext(AppChromeContext);
}

/**
 * Pages call this to register their chrome (breadcrumb / actions / inspector tabs).
 * The registration is cleared on unmount.
 */
export function usePageChrome(chrome: PageChrome): void {
  const ctx = useContext(AppChromeContext);
  const ref = useRef<PageChrome>(chrome);
  ref.current = chrome;

  useEffect(() => {
    ctx.setChrome(ref.current);
    return () => {
      ctx.setChrome({});
    };
    // We re-register whenever the page mounts; the ref keeps content fresh.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Update chrome when the value changes during the page's lifetime
  useEffect(() => {
    ctx.setChrome(chrome);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [chrome.breadcrumb, chrome.toolbarActions, chrome.inspectorTabs]);
}
