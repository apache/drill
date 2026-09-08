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
import { useMemo, useState, useCallback, type ReactNode } from 'react';
import {
  Button,
  Empty,
  Input,
  InputNumber,
  Spin,
  Switch,
  Tag,
  Tooltip,
  Typography,
  message,
} from 'antd';
import {
  SearchOutlined,
  UndoOutlined,
  CheckOutlined,
  AppstoreOutlined,
  StarOutlined,
  EyeInvisibleOutlined,
  EyeOutlined,
} from '@ant-design/icons';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  getOptions,
  getInternalOptions,
  getOptionDescriptions,
  updateOption,
} from '../api/options';
import type { DrillOption } from '../api/options';

const { Text } = Typography;

interface OptionRow extends DrillOption {
  description: string;
  isModified: boolean;
  category: string;
}

/** Pseudo-categories that aren't tied to a name prefix. */
const ALL_KEY = '__all__';
const MODIFIED_KEY = '__modified__';

/** Friendly labels for known top-level option namespaces. */
const CATEGORY_LABELS: Record<string, string> = {
  planner: 'Query Planner',
  exec: 'Execution',
  store: 'Storage',
  drill: 'Drill Core',
  security: 'Security',
  format: 'Formats',
  web: 'Web',
  alter: 'Session',
};

function categoryFor(name: string): string {
  const segment = name.split('.')[0] ?? 'other';
  return segment || 'other';
}

function categoryLabel(key: string): string {
  if (CATEGORY_LABELS[key]) {
    return CATEGORY_LABELS[key];
  }
  return key.charAt(0).toUpperCase() + key.slice(1);
}

interface OptionValueEditorProps {
  option: OptionRow;
  onSave: (name: string, value: string, kind: string) => Promise<void>;
}

function OptionValueEditor({ option, onSave }: OptionValueEditorProps) {
  const [value, setValue] = useState<string>(String(option.value));
  const [saving, setSaving] = useState(false);
  const isChanged = value !== String(option.value);

  const handleSave = async (next: string) => {
    setSaving(true);
    try {
      await onSave(option.name, next, option.kind);
      setValue(next);
    } finally {
      setSaving(false);
    }
  };

  const handleReset = async () => {
    setSaving(true);
    try {
      await onSave(option.name, option.defaultValue, option.kind);
      setValue(option.defaultValue);
    } finally {
      setSaving(false);
    }
  };

  const resetButton = option.isModified ? (
    <Tooltip title={`Reset to default (${option.defaultValue})`}>
      <Button
        type="text"
        size="small"
        icon={<UndoOutlined />}
        onClick={handleReset}
        loading={saving}
      />
    </Tooltip>
  ) : null;

  if (option.kind === 'BOOLEAN') {
    return (
      <div className="options-row-control">
        <Switch
          checked={value === 'true'}
          loading={saving}
          onChange={(checked) => handleSave(String(checked))}
        />
        {resetButton}
      </div>
    );
  }

  if (option.kind === 'LONG' || option.kind === 'DOUBLE') {
    return (
      <div className="options-row-control">
        <InputNumber
          value={Number(value)}
          onChange={(v) => setValue(String(v ?? option.defaultValue))}
          onPressEnter={() => isChanged && handleSave(value)}
          style={{ width: 140 }}
          size="small"
          step={option.kind === 'DOUBLE' ? 0.1 : 1}
        />
        {isChanged && (
          <Button
            type="primary"
            size="small"
            icon={<CheckOutlined />}
            onClick={() => handleSave(value)}
            loading={saving}
          />
        )}
        {resetButton}
      </div>
    );
  }

  return (
    <div className="options-row-control">
      <Input
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onPressEnter={() => isChanged && handleSave(value)}
        style={{ width: 200 }}
        size="small"
      />
      {isChanged && (
        <Button
          type="primary"
          size="small"
          icon={<CheckOutlined />}
          onClick={() => handleSave(value)}
          loading={saving}
        />
      )}
      {resetButton}
    </div>
  );
}

interface SidebarItemProps {
  icon: ReactNode;
  title: string;
  count: number;
  active: boolean;
  onClick: () => void;
}

function SidebarItem({ icon, title, count, active, onClick }: SidebarItemProps) {
  return (
    <button
      type="button"
      className={`prefs-section${active ? ' is-active' : ''}`}
      onClick={onClick}
    >
      <span className="prefs-section-icon">{icon}</span>
      <span className="prefs-section-text">
        <span className="prefs-section-title">{title}</span>
      </span>
      <span className="options-sidebar-count">{count}</span>
    </button>
  );
}

export default function OptionsPage() {
  const [activeCategory, setActiveCategory] = useState<string>(ALL_KEY);
  const [search, setSearch] = useState('');
  const [showInternal, setShowInternal] = useState(false);
  const queryClient = useQueryClient();

  const { data: publicOptions, isLoading: loadingPublic } = useQuery({
    queryKey: ['options', 'public'],
    queryFn: getOptions,
  });

  const { data: internalOptions, isLoading: loadingInternal } = useQuery({
    queryKey: ['options', 'internal'],
    queryFn: getInternalOptions,
    enabled: showInternal,
  });

  const { data: descriptions } = useQuery({
    queryKey: ['option-descriptions'],
    queryFn: getOptionDescriptions,
    staleTime: Infinity,
  });

  const handleSave = useCallback(
    async (name: string, value: string, kind: string) => {
      try {
        await updateOption(name, value, kind);
        message.success(`Updated ${name}`);
        queryClient.invalidateQueries({ queryKey: ['options'] });
      } catch (err) {
        message.error(
          `Failed to update ${name}: ${err instanceof Error ? err.message : 'Unknown error'}`,
        );
        throw err;
      }
    },
    [queryClient],
  );

  const options = useMemo((): OptionRow[] => {
    const raw = showInternal
      ? [...(publicOptions || []), ...(internalOptions || [])]
      : publicOptions || [];

    return raw.map((opt) => ({
      ...opt,
      description: descriptions?.[opt.name] || '',
      isModified: String(opt.value) !== opt.defaultValue,
      category: categoryFor(opt.name),
    }));
  }, [publicOptions, internalOptions, descriptions, showInternal]);

  const categories = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const o of options) {
      counts[o.category] = (counts[o.category] || 0) + 1;
    }
    return Object.entries(counts)
      .map(([key, count]) => ({ key, count, label: categoryLabel(key) }))
      .sort((a, b) => a.label.localeCompare(b.label));
  }, [options]);

  const modifiedCount = useMemo(
    () => options.filter((o) => o.isModified).length,
    [options],
  );

  const visible = useMemo(() => {
    let result = options;

    if (activeCategory === MODIFIED_KEY) {
      result = result.filter((o) => o.isModified);
    } else if (activeCategory !== ALL_KEY) {
      result = result.filter((o) => o.category === activeCategory);
    }

    if (search) {
      const q = search.toLowerCase();
      result = result.filter(
        (o) =>
          o.name.toLowerCase().includes(q) ||
          o.description.toLowerCase().includes(q),
      );
    }

    return result.sort((a, b) => a.name.localeCompare(b.name));
  }, [options, activeCategory, search]);

  const paneTitle =
    activeCategory === ALL_KEY
      ? 'All Options'
      : activeCategory === MODIFIED_KEY
        ? 'Modified'
        : categoryLabel(activeCategory);

  const paneSubtitle = (() => {
    const total = visible.length;
    if (search) {
      return `${total} matching ${total === 1 ? 'option' : 'options'}`;
    }
    return `${total} ${total === 1 ? 'option' : 'options'}`;
  })();

  const isLoading = loadingPublic || (showInternal && loadingInternal);

  return (
    <div className="options-shell">
      <aside className="prefs-sidebar options-sidebar">
        <div className="prefs-sidebar-title">System Options</div>

        <ul className="prefs-section-list">
          <li>
            <SidebarItem
              icon={<AppstoreOutlined />}
              title="All Options"
              count={options.length}
              active={activeCategory === ALL_KEY}
              onClick={() => setActiveCategory(ALL_KEY)}
            />
          </li>
          <li>
            <SidebarItem
              icon={<StarOutlined />}
              title="Modified"
              count={modifiedCount}
              active={activeCategory === MODIFIED_KEY}
              onClick={() => setActiveCategory(MODIFIED_KEY)}
            />
          </li>
        </ul>

        <div className="options-sidebar-divider" />
        <div className="prefs-sidebar-title">Categories</div>

        <ul className="prefs-section-list">
          {categories.map((c) => (
            <li key={c.key}>
              <SidebarItem
                icon={<span className="options-sidebar-glyph">{c.label.charAt(0)}</span>}
                title={c.label}
                count={c.count}
                active={activeCategory === c.key}
                onClick={() => setActiveCategory(c.key)}
              />
            </li>
          ))}
        </ul>

        <div className="options-sidebar-footer">
          <button
            type="button"
            className="options-sidebar-toggle"
            onClick={() => setShowInternal((v) => !v)}
          >
            <span className="prefs-section-icon">
              {showInternal ? <EyeOutlined /> : <EyeInvisibleOutlined />}
            </span>
            <span className="prefs-section-text">
              <span className="prefs-section-title">Show internal</span>
              <span className="prefs-section-subtitle">
                {showInternal ? 'Visible' : 'Hidden'}
              </span>
            </span>
            <Switch checked={showInternal} size="small" />
          </button>
        </div>
      </aside>

      <main className="prefs-pane">
        <header className="prefs-pane-header options-pane-header">
          <div>
            <h2 className="prefs-pane-title">{paneTitle}</h2>
            <p className="prefs-pane-subtitle">{paneSubtitle}</p>
          </div>
          <Input
            allowClear
            size="middle"
            placeholder="Search options…"
            prefix={<SearchOutlined />}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ width: 280 }}
          />
        </header>

        <div className="prefs-pane-body options-pane-body">
          {isLoading ? (
            <div className="options-loading">
              <Spin tip="Loading options..." />
            </div>
          ) : visible.length === 0 ? (
            <Empty description={search ? 'No matching options' : 'No options in this category'} />
          ) : (
            <div className="options-list">
              {visible.map((opt) => (
                <div key={opt.name} className="options-row">
                  <div className="options-row-meta">
                    <div className="options-row-name-line">
                      <Text
                        copyable={{ text: opt.name }}
                        className="options-row-name"
                      >
                        {opt.name}
                      </Text>
                      {opt.isModified && (
                        <Tag color="blue" className="options-row-modified-pill">
                          modified
                        </Tag>
                      )}
                    </div>
                    {opt.description && (
                      <div className="options-row-description">{opt.description}</div>
                    )}
                    <div className="options-row-tags">
                      <span className="options-row-tag is-kind">{opt.kind.toLowerCase()}</span>
                      <span className="options-row-tag is-scope">{opt.optionScope.toLowerCase()}</span>
                      <span className="options-row-tag is-default">
                        default <code>{opt.defaultValue}</code>
                      </span>
                    </div>
                  </div>
                  <div className="options-row-edit">
                    <OptionValueEditor option={opt} onSave={handleSave} />
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
