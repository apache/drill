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
import type { ReactNode } from 'react';
import { Button, Empty, Typography } from 'antd';

const { Text, Title } = Typography;

/**
 * Shimmering placeholder block. Width can be a px number or any valid CSS length.
 * Use this as a primitive — most pages should reach for the shape-aware skeletons
 * below, not raw blocks.
 */
export function SkeletonBlock({
  width,
  height = 14,
  radius = 6,
  style,
}: {
  width?: number | string;
  height?: number | string;
  radius?: number;
  style?: React.CSSProperties;
}) {
  return (
    <span
      className="skeleton-block"
      style={{
        width: typeof width === 'number' ? `${width}px` : width ?? '100%',
        height: typeof height === 'number' ? `${height}px` : height,
        borderRadius: radius,
        ...style,
      }}
    />
  );
}

/**
 * Loading shape for a grid of cards (Projects, Dashboards, Visualizations).
 * Renders `count` placeholders matching the eventual gallery layout so the
 * page doesn't reflow when data lands.
 */
export function CardGridSkeleton({ count = 6 }: { count?: number }) {
  return (
    <div className="skeleton-grid">
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="skeleton-card">
          <SkeletonBlock height={120} radius={8} />
          <div style={{ marginTop: 12 }}>
            <SkeletonBlock width="70%" height={14} />
            <div style={{ marginTop: 8 }}>
              <SkeletonBlock width="45%" height={11} />
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

/**
 * Loading shape for a list (Saved Queries, Profiles, Logs file table).
 * Each row mimics the eventual row's geometry — primary line + secondary line.
 */
export function ListSkeleton({ rows = 8 }: { rows?: number }) {
  return (
    <div className="skeleton-list">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="skeleton-list-row">
          <SkeletonBlock width={36} height={36} radius={8} />
          <div className="skeleton-list-row-text">
            <SkeletonBlock width={`${40 + ((i * 11) % 30)}%`} height={13} />
            <SkeletonBlock width={`${30 + ((i * 7) % 30)}%`} height={11} style={{ marginTop: 6 }} />
          </div>
          <SkeletonBlock width={60} height={11} />
        </div>
      ))}
    </div>
  );
}

/**
 * Loading shape for a stat-card row (AI Analytics, Metrics top stats).
 */
export function StatRowSkeleton({ count = 6 }: { count?: number }) {
  return (
    <div className="skeleton-stat-row">
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="skeleton-stat-tile">
          <SkeletonBlock width="55%" height={11} />
          <SkeletonBlock width="40%" height={22} style={{ marginTop: 10 }} />
        </div>
      ))}
    </div>
  );
}

interface EmptyStateProps {
  icon?: ReactNode;
  title: string;
  description?: ReactNode;
  cta?: {
    label: string;
    onClick: () => void;
    type?: 'primary' | 'default';
  };
  /** Optional secondary action shown alongside the primary CTA. */
  secondary?: {
    label: string;
    onClick: () => void;
  };
}

/**
 * Drop-in replacement for `<Empty>` that follows the Apple-inspired language:
 * generous whitespace, a single icon, a clear title, optional muted body, and a
 * named CTA button. Use this whenever the user lands on a page with no data and
 * there's a sensible next action.
 */
export function EmptyState({ icon, title, description, cta, secondary }: EmptyStateProps) {
  return (
    <div className="empty-state">
      {icon && <div className="empty-state-icon">{icon}</div>}
      <Title level={5} className="empty-state-title">{title}</Title>
      {description && (
        <Text type="secondary" className="empty-state-description">
          {description}
        </Text>
      )}
      {(cta || secondary) && (
        <div className="empty-state-actions">
          {cta && (
            <Button type={cta.type ?? 'primary'} onClick={cta.onClick}>
              {cta.label}
            </Button>
          )}
          {secondary && (
            <Button type="text" onClick={secondary.onClick}>
              {secondary.label}
            </Button>
          )}
        </div>
      )}
    </div>
  );
}

/** Thin wrapper around Ant's Empty for cases where there's truly no data and no action. */
export function EmptyHint({ description }: { description: string }) {
  return <Empty description={description} image={Empty.PRESENTED_IMAGE_SIMPLE} />;
}
