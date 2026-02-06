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
import { useState, useEffect } from 'react';
import {
  Drawer,
  Button,
  Select,
  Radio,
  Typography,
  Divider,
  ColorPicker,
  Space,
} from 'antd';
import { SunOutlined, MoonOutlined, UndoOutlined } from '@ant-design/icons';
import type { DashboardTheme } from '../../types';
import type { Color } from 'antd/es/color-picker';

const { Text, Title } = Typography;

export const DEFAULT_THEME: DashboardTheme = {
  mode: 'light',
  fontFamily: 'system-ui',
  backgroundColor: '#f0f2f5',
  fontColor: '#333333',
  panelBackground: '#ffffff',
  panelBorderColor: '#e8e8e8',
  panelBorderRadius: '8px',
  accentColor: '#1890ff',
  headerColor: '#000000',
};

export const DARK_THEME: DashboardTheme = {
  mode: 'dark',
  fontFamily: 'system-ui',
  backgroundColor: '#1a1a2e',
  fontColor: '#e0e0e0',
  panelBackground: '#16213e',
  panelBorderColor: '#2a2a4a',
  panelBorderRadius: '8px',
  accentColor: '#4fc3f7',
  headerColor: '#ffffff',
};

const BLUE_THEME: DashboardTheme = {
  mode: 'light',
  fontFamily: 'system-ui',
  backgroundColor: '#e6f7ff',
  fontColor: '#003a8c',
  panelBackground: '#ffffff',
  panelBorderColor: '#91d5ff',
  panelBorderRadius: '8px',
  accentColor: '#1890ff',
  headerColor: '#003a8c',
};

const WARM_THEME: DashboardTheme = {
  mode: 'light',
  fontFamily: 'system-ui',
  backgroundColor: '#fff7e6',
  fontColor: '#613400',
  panelBackground: '#ffffff',
  panelBorderColor: '#ffd591',
  panelBorderRadius: '8px',
  accentColor: '#fa8c16',
  headerColor: '#613400',
};

const FONT_OPTIONS = [
  { value: 'system-ui', label: 'System Default' },
  { value: "'Inter', sans-serif", label: 'Inter' },
  { value: "'Roboto', sans-serif", label: 'Roboto' },
  { value: "'Open Sans', sans-serif", label: 'Open Sans' },
  { value: "'Lato', sans-serif", label: 'Lato' },
  { value: "'Merriweather', serif", label: 'Merriweather' },
  { value: "'Source Code Pro', monospace", label: 'Source Code Pro' },
];

const RADIUS_OPTIONS = [
  { value: '0px', label: 'None (0px)' },
  { value: '4px', label: 'Small (4px)' },
  { value: '8px', label: 'Medium (8px)' },
  { value: '16px', label: 'Large (16px)' },
];

interface DashboardSettingsDrawerProps {
  open: boolean;
  theme: DashboardTheme;
  onClose: () => void;
  onThemeChange: (theme: DashboardTheme) => void;
}

function toHex(color: Color | string): string {
  if (typeof color === 'string') {
    return color;
  }
  return color.toHexString();
}

export default function DashboardSettingsDrawer({
  open,
  theme,
  onClose,
  onThemeChange,
}: DashboardSettingsDrawerProps) {
  const [localTheme, setLocalTheme] = useState<DashboardTheme>(theme);

  useEffect(() => {
    if (open) {
      setLocalTheme(theme);
    }
  }, [open, theme]);

  const updateField = <K extends keyof DashboardTheme>(field: K, value: DashboardTheme[K]) => {
    const updated = { ...localTheme, [field]: value };
    setLocalTheme(updated);
    // Live preview: push changes immediately
    onThemeChange(updated);
  };

  const applyPreset = (preset: DashboardTheme) => {
    setLocalTheme(preset);
    onThemeChange(preset);
  };

  const handleReset = () => {
    setLocalTheme(DEFAULT_THEME);
    onThemeChange(DEFAULT_THEME);
  };

  return (
    <Drawer
      title="Dashboard Settings"
      open={open}
      onClose={onClose}
      width={340}
      extra={
        <Button icon={<UndoOutlined />} onClick={handleReset} size="small">
          Reset
        </Button>
      }
    >
      {/* Preset Themes */}
      <Title level={5} style={{ marginTop: 0 }}>Presets</Title>
      <div style={{ marginBottom: 16 }}>
        <Button className="theme-preset-btn" onClick={() => applyPreset(DEFAULT_THEME)}>
          Light
        </Button>
        <Button className="theme-preset-btn" onClick={() => applyPreset(DARK_THEME)}>
          Dark
        </Button>
        <Button className="theme-preset-btn" onClick={() => applyPreset(BLUE_THEME)}>
          Blue
        </Button>
        <Button className="theme-preset-btn" onClick={() => applyPreset(WARM_THEME)}>
          Warm
        </Button>
      </div>

      <Divider />

      {/* Mode */}
      <Title level={5}>Mode</Title>
      <Radio.Group
        value={localTheme.mode}
        onChange={(e) => {
          const newMode = e.target.value;
          if (newMode === 'dark' && localTheme.mode === 'light') {
            applyPreset({ ...DARK_THEME, fontFamily: localTheme.fontFamily, panelBorderRadius: localTheme.panelBorderRadius });
          } else if (newMode === 'light' && localTheme.mode === 'dark') {
            applyPreset({ ...DEFAULT_THEME, fontFamily: localTheme.fontFamily, panelBorderRadius: localTheme.panelBorderRadius });
          }
        }}
        style={{ marginBottom: 16 }}
      >
        <Radio.Button value="light"><SunOutlined /> Light</Radio.Button>
        <Radio.Button value="dark"><MoonOutlined /> Dark</Radio.Button>
      </Radio.Group>

      <Divider />

      {/* Font */}
      <Title level={5}>Font</Title>
      <Select
        value={localTheme.fontFamily}
        onChange={(value) => updateField('fontFamily', value)}
        options={FONT_OPTIONS}
        style={{ width: '100%', marginBottom: 16 }}
      />

      {/* Border Radius */}
      <Title level={5}>Border Radius</Title>
      <Select
        value={localTheme.panelBorderRadius}
        onChange={(value) => updateField('panelBorderRadius', value)}
        options={RADIUS_OPTIONS}
        style={{ width: '100%', marginBottom: 16 }}
      />

      <Divider />

      {/* Colors */}
      <Title level={5}>Colors</Title>
      <Space direction="vertical" style={{ width: '100%' }}>
        <div className="theme-color-row">
          <Text>Background</Text>
          <ColorPicker
            value={localTheme.backgroundColor}
            onChange={(color) => updateField('backgroundColor', toHex(color))}
            size="small"
          />
        </div>
        <div className="theme-color-row">
          <Text>Font Color</Text>
          <ColorPicker
            value={localTheme.fontColor}
            onChange={(color) => updateField('fontColor', toHex(color))}
            size="small"
          />
        </div>
        <div className="theme-color-row">
          <Text>Panel Background</Text>
          <ColorPicker
            value={localTheme.panelBackground}
            onChange={(color) => updateField('panelBackground', toHex(color))}
            size="small"
          />
        </div>
        <div className="theme-color-row">
          <Text>Panel Border</Text>
          <ColorPicker
            value={localTheme.panelBorderColor}
            onChange={(color) => updateField('panelBorderColor', toHex(color))}
            size="small"
          />
        </div>
        <div className="theme-color-row">
          <Text>Accent Color</Text>
          <ColorPicker
            value={localTheme.accentColor}
            onChange={(color) => updateField('accentColor', toHex(color))}
            size="small"
          />
        </div>
        <div className="theme-color-row">
          <Text>Header Color</Text>
          <ColorPicker
            value={localTheme.headerColor}
            onChange={(color) => updateField('headerColor', toHex(color))}
            size="small"
          />
        </div>
      </Space>
    </Drawer>
  );
}
