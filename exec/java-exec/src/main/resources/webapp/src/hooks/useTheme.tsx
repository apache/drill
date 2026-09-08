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
  useContext,
  useState,
  useEffect,
  useCallback,
  useMemo,
} from 'react';
import { ConfigProvider, theme } from 'antd';

export type ThemeMode = 'light' | 'dark' | 'system';

const STORAGE_KEY = 'drill-sqllab-theme-mode';

interface ThemeContextValue {
  mode: ThemeMode;
  isDark: boolean;
  setMode: (mode: ThemeMode) => void;
  toggle: () => void;
}

const ThemeContext = createContext<ThemeContextValue>({
  mode: 'system',
  isDark: false,
  setMode: () => {},
  toggle: () => {},
});

function getSystemPrefersDark(): boolean {
  return window.matchMedia('(prefers-color-scheme: dark)').matches;
}

function getStoredMode(): ThemeMode {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored === 'light' || stored === 'dark' || stored === 'system') {
      return stored;
    }
  } catch {
    // Ignore storage errors
  }
  return 'system';
}

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [mode, setModeState] = useState<ThemeMode>(getStoredMode);
  const [systemDark, setSystemDark] = useState(getSystemPrefersDark);

  // Listen for system preference changes
  useEffect(() => {
    const mql = window.matchMedia('(prefers-color-scheme: dark)');
    const handler = (e: MediaQueryListEvent) => setSystemDark(e.matches);
    mql.addEventListener('change', handler);
    return () => mql.removeEventListener('change', handler);
  }, []);

  const isDark = mode === 'dark' || (mode === 'system' && systemDark);

  // Toggle dark class on <html>
  useEffect(() => {
    if (isDark) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }, [isDark]);

  const setMode = useCallback((newMode: ThemeMode) => {
    setModeState(newMode);
    try {
      localStorage.setItem(STORAGE_KEY, newMode);
    } catch {
      // Ignore storage errors
    }
  }, []);

  const toggle = useCallback(() => {
    setMode(isDark ? 'light' : 'dark');
  }, [isDark, setMode]);

  const ctx = useMemo<ThemeContextValue>(
    () => ({ mode, isDark, setMode, toggle }),
    [mode, isDark, setMode, toggle],
  );

  // Apple Sonoma/Tahoe-flavored antd theme — System Blue accent,
  // SF type stack, continuous-style radii, controls sized for 13px body type.
  const appleFontFamily =
    "-apple-system, BlinkMacSystemFont, 'SF Pro Text', 'SF Pro Display', 'Inter', 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif";
  const accent = isDark ? '#0a84ff' : '#007aff';
  const accentHover = isDark ? '#409cff' : '#0064d2';

  return (
    <ThemeContext.Provider value={ctx}>
      <ConfigProvider
        theme={{
          algorithm: isDark ? theme.darkAlgorithm : theme.defaultAlgorithm,
          token: {
            colorPrimary: accent,
            colorLink: accent,
            colorLinkHover: accentHover,
            colorInfo: accent,
            colorSuccess: isDark ? '#32d74b' : '#30d158',
            colorWarning: isDark ? '#ffd60a' : '#ff9f0a',
            colorError: isDark ? '#ff453a' : '#ff3b30',
            fontFamily: appleFontFamily,
            fontSize: 13,
            fontSizeLG: 15,
            fontSizeSM: 12,
            fontSizeHeading1: 28,
            fontSizeHeading2: 22,
            fontSizeHeading3: 17,
            lineHeight: 1.45,
            borderRadius: 8,
            borderRadiusLG: 12,
            borderRadiusSM: 6,
            borderRadiusXS: 4,
            controlHeight: 28,
            controlHeightLG: 36,
            controlHeightSM: 22,
            wireframe: false,
            motionDurationFast: '0.14s',
            motionDurationMid: '0.22s',
            motionDurationSlow: '0.36s',
            motionEaseInOut: 'cubic-bezier(0.32, 0.72, 0, 1)',
            motionEaseOut: 'cubic-bezier(0.2, 0.9, 0.1, 1)',
            ...(isDark
              ? {
                  colorBgLayout: '#1c1c1e',
                  colorBgContainer: '#1e1e20',
                  colorBgElevated: '#2c2c2e',
                  colorBorder: 'rgba(255, 255, 255, 0.12)',
                  colorBorderSecondary: 'rgba(255, 255, 255, 0.06)',
                  colorText: 'rgba(255, 255, 255, 0.92)',
                  colorTextSecondary: 'rgba(235, 235, 245, 0.62)',
                  colorTextTertiary: 'rgba(235, 235, 245, 0.40)',
                  colorTextQuaternary: 'rgba(235, 235, 245, 0.24)',
                  boxShadow:
                    '0 0 0 0.5px rgba(0, 0, 0, 0.6), 0 2px 6px rgba(0, 0, 0, 0.4), 0 12px 32px rgba(0, 0, 0, 0.5)',
                  boxShadowSecondary:
                    '0 0 0 0.5px rgba(0, 0, 0, 0.7), 0 18px 48px rgba(0, 0, 0, 0.55)',
                }
              : {
                  colorBgLayout: '#f5f5f7',
                  colorBgContainer: '#ffffff',
                  colorBgElevated: '#ffffff',
                  colorBorder: 'rgba(60, 60, 67, 0.18)',
                  colorBorderSecondary: 'rgba(60, 60, 67, 0.10)',
                  colorText: 'rgba(0, 0, 0, 0.88)',
                  colorTextSecondary: 'rgba(60, 60, 67, 0.65)',
                  colorTextTertiary: 'rgba(60, 60, 67, 0.42)',
                  colorTextQuaternary: 'rgba(60, 60, 67, 0.28)',
                  boxShadow:
                    '0 0 0 0.5px rgba(0, 0, 0, 0.06), 0 1px 2px rgba(0, 0, 0, 0.05), 0 8px 24px rgba(0, 0, 0, 0.08)',
                  boxShadowSecondary:
                    '0 0 0 0.5px rgba(0, 0, 0, 0.08), 0 12px 32px rgba(0, 0, 0, 0.16)',
                }),
          },
          components: {
            Button: {
              borderRadius: 8,
              borderRadiusLG: 10,
              borderRadiusSM: 6,
              controlHeight: 28,
              controlHeightLG: 36,
              controlHeightSM: 22,
              fontWeight: 500,
              primaryShadow: 'none',
              defaultShadow: 'none',
              dangerShadow: 'none',
            },
            Input: {
              borderRadius: 8,
              controlHeight: 28,
              activeShadow: '0 0 0 3px rgba(0, 122, 255, 0.35)',
              hoverBorderColor: accent,
              activeBorderColor: accent,
            },
            Select: {
              borderRadius: 8,
              controlHeight: 28,
              optionSelectedBg: 'var(--color-primary-soft)',
              optionActiveBg: 'var(--color-bg-hover)',
            },
            Tabs: {
              horizontalItemPadding: '8px 14px',
              horizontalMargin: '0',
              cardBg: 'transparent',
              itemColor: 'var(--color-text-secondary)',
              itemHoverColor: 'var(--color-text)',
              itemSelectedColor: 'var(--color-text)',
              itemActiveColor: 'var(--color-text)',
              inkBarColor: accent,
              titleFontSize: 13,
            },
            Menu: {
              borderRadius: 8,
              borderRadiusLG: 10,
              itemBorderRadius: 6,
              itemHeight: 28,
              itemHoverBg: 'var(--color-bg-hover)',
              itemSelectedBg: 'var(--color-primary-soft)',
              itemSelectedColor: accent,
              itemColor: 'var(--color-text)',
              itemActiveBg: 'var(--color-bg-hover)',
              activeBarHeight: 0,
              horizontalItemSelectedColor: accent,
              horizontalItemHoverColor: 'var(--color-text)',
              fontSize: 13,
              iconSize: 14,
            },
            Modal: {
              borderRadiusLG: 14,
              paddingContentHorizontalLG: 24,
            },
            Drawer: {
              colorBgElevated: 'var(--color-bg-popover)',
            },
            Tree: {
              borderRadius: 6,
              nodeSelectedBg: 'var(--color-primary-soft)',
              nodeHoverBg: 'var(--color-bg-hover)',
              titleHeight: 26,
              directoryNodeSelectedBg: 'var(--color-primary-soft)',
              directoryNodeSelectedColor: accent,
            },
            Card: {
              borderRadiusLG: 14,
              boxShadowTertiary:
                '0 0 0 0.5px rgba(0, 0, 0, 0.06), 0 1px 2px rgba(0, 0, 0, 0.05)',
            },
            Tooltip: {
              colorBgSpotlight: isDark
                ? 'rgba(56, 56, 60, 0.95)'
                : 'rgba(40, 40, 42, 0.92)',
              borderRadius: 6,
              fontSize: 12,
            },
            Dropdown: {
              borderRadiusLG: 10,
              controlPaddingHorizontal: 12,
              paddingBlock: 6,
            },
            Switch: {
              trackHeight: 22,
              trackMinWidth: 36,
              handleSize: 18,
            },
            Tag: {
              borderRadiusSM: 6,
              defaultBg: 'var(--color-bg-elevated)',
              defaultColor: 'var(--color-text-secondary)',
            },
            Segmented: {
              itemSelectedBg: 'var(--color-bg-container)',
              itemSelectedColor: 'var(--color-text)',
              itemHoverBg: 'var(--color-bg-hover)',
              borderRadius: 8,
              borderRadiusLG: 10,
              borderRadiusSM: 6,
              trackBg: 'var(--color-bg-elevated)',
              trackPadding: 2,
            },
          },
        }}
      >
        {children}
      </ConfigProvider>
    </ThemeContext.Provider>
  );
}

export function useTheme(): ThemeContextValue {
  return useContext(ThemeContext);
}
