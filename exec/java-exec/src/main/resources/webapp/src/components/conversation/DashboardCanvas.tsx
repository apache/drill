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
import { useMemo, useState } from 'react';
import { Button, Empty, Typography } from 'antd';
import { DashboardPanelCard } from '../dashboard';
import { useTheme } from '../../hooks/useTheme';
import type { Dashboard, DashboardPanel } from '../../types';

/**
 * How many panels mount before the rest are asked for. Every visualization panel runs its
 * own query on mount, so a dashboard with dozens would fire them all at once beside the chat.
 */
export const INITIAL_PANELS = 6;

// A panel's grid height is in rows of 120px on the page; beside a chat each gets a
// single-column slot, scaled down but kept within a range a chart can be read in. The
// floor stays low enough that a short panel -- a single number, a title -- still reads as
// a short panel here, instead of every panel in the stack being the same tall box.
const panelHeight = (p: DashboardPanel) => Math.min(Math.max(p.height * 55, 170), 380);

/** The card's own chrome around the chart: title row, footer and padding. */
const CARD_CHROME = 80;

const byPosition = (a: DashboardPanel, b: DashboardPanel) => a.y - b.y || a.x - b.x;

/**
 * A dashboard as a read-only, single-column stack. The panel cards are the dashboard page's
 * own, mounted with editMode off and none of the mutation callbacks, so there is nothing
 * here to drag, remove or edit, and each card fetches and refreshes its own data. The grid,
 * filter bar and settings drawer stay on the dashboard page, reached through full screen.
 */
export default function DashboardCanvas({ dashboard }: { dashboard: Dashboard }) {
  const { isDark } = useTheme();
  const [showAll, setShowAll] = useState(false);

  // Tabs flatten into sections in tab order, root panels first, so nothing is hidden
  // just because the canvas has no tab strip for it.
  const ordered = useMemo(() => {
    const tabs = [...(dashboard.tabs ?? [])].sort((a, b) => a.order - b.order);
    const known = new Set(tabs.map((t) => t.id));
    const root = dashboard.panels.filter((p) => !p.tabId || !known.has(p.tabId)).sort(byPosition);
    const sections = tabs.map((t) => ({
      name: t.name,
      panels: dashboard.panels.filter((p) => p.tabId === t.id).sort(byPosition),
    }));
    return [{ name: '', panels: root }, ...sections].filter((s) => s.panels.length > 0);
  }, [dashboard.panels, dashboard.tabs]);

  const total = dashboard.panels.length;
  if (total === 0) {
    return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="This dashboard has no panels." style={{ marginTop: 48 }} />;
  }

  const darkMode = dashboard.theme ? dashboard.theme.mode === 'dark' : isDark;
  let budget = showAll ? Infinity : INITIAL_PANELS;
  return (
    <div style={{ padding: 12, height: '100%', overflow: 'auto' }}>
      <Typography.Title level={5} style={{ marginTop: 0 }}>{dashboard.name}</Typography.Title>
      {ordered.map((section) => {
        const shown = section.panels.slice(0, Math.max(budget, 0));
        budget -= shown.length;
        return shown.length === 0 ? null : (
          <div key={section.name || 'root'}>
            {section.name && <Typography.Title level={5}>{section.name}</Typography.Title>}
            {shown.map((panel) => (
              <div key={panel.id} style={{ height: panelHeight(panel), marginBottom: 12 }}>
                <DashboardPanelCard
                  panel={panel}
                  editMode={false}
                  refreshInterval={dashboard.refreshInterval}
                  dashboardUpdatedAt={dashboard.updatedAt}
                  darkMode={darkMode}
                  allPanels={dashboard.panels}
                  // The card sizes its chart from the dashboard grid, which is taller than
                  // this slot: without this the x axis and legend are clipped off the bottom.
                  chartHeight={panelHeight(panel) - CARD_CHROME}
                />
              </div>
            ))}
          </div>
        );
      })}
      {!showAll && total > INITIAL_PANELS && (
        <Button block onClick={() => setShowAll(true)}>Show all {total} panels</Button>
      )}
    </div>
  );
}
