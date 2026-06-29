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
import { useEffect } from 'react';
import { Modal, Tabs, Progress, Alert, Typography, Space } from 'antd';
import { useDataProfile } from '../../hooks/useDataProfile';
import type { ProfilePhase } from '../../hooks/useDataProfile';
import ProfileOverview from './ProfileOverview';
import ColumnProfile from './ColumnProfile';
import CorrelationMatrix from './CorrelationMatrix';

const PHASE_LABELS: Record<ProfilePhase, string> = {
  idle: '',
  aggregates: 'Computing aggregate statistics...',
  sample: 'Fetching sample data...',
  computing: 'Computing distributions and correlations...',
  done: 'Complete',
  error: 'Failed',
};

const PHASE_PROGRESS: Record<ProfilePhase, number> = {
  idle: 0,
  aggregates: 25,
  sample: 55,
  computing: 80,
  done: 100,
  error: 0,
};

interface DataProfilerProps {
  open: boolean;
  onClose: () => void;
  cacheId?: string;
  columns?: string[];
  metadata?: string[];
  totalRows?: number;
  sql?: string;
  schemaName?: string;
  tableName?: string;
}

export default function DataProfiler({
  open,
  onClose,
  cacheId,
  columns,
  metadata,
  totalRows,
  sql,
  schemaName,
  tableName,
}: DataProfilerProps) {
  const { profile, isLoading, phase, error, startProfile, cancel } = useDataProfile({
    cacheId,
    columns,
    metadata,
    totalRows,
    sql,
    schemaName,
    tableName,
  });

  // Auto-start profiling when modal opens
  useEffect(() => {
    if (open && phase === 'idle' && !profile) {
      startProfile();
    }
  }, [open, phase, profile, startProfile]);

  const handleClose = () => {
    if (isLoading) {
      cancel();
    }
    onClose();
  };

  const title = profile
    ? `Data Profile: ${profile.tableName}`
    : tableName
      ? `Data Profile: ${tableName}`
      : 'Data Profile';

  const tabItems = profile ? [
    {
      key: 'overview',
      label: 'Overview',
      children: <ProfileOverview profile={profile} />,
    },
    {
      key: 'columns',
      label: `Columns (${profile.columnCount})`,
      children: <ColumnProfile columns={profile.columns} />,
    },
    {
      key: 'correlations',
      label: 'Correlations',
      children: <CorrelationMatrix matrix={profile.correlationMatrix} />,
    },
  ] : [];

  return (
    <Modal
      title={title}
      open={open}
      onCancel={handleClose}
      footer={null}
      width="90vw"
      style={{ top: 24 }}
      styles={{ body: { minHeight: 400, maxHeight: 'calc(100vh - 120px)', overflow: 'auto' } }}
      destroyOnClose
    >
      {isLoading && (
        <div style={{ textAlign: 'center', padding: '40px 0' }}>
          <Progress
            percent={PHASE_PROGRESS[phase]}
            status="active"
            style={{ maxWidth: 400, margin: '0 auto' }}
          />
          <Space style={{ marginTop: 16 }}>
            <Typography.Text type="secondary">{PHASE_LABELS[phase]}</Typography.Text>
          </Space>
        </div>
      )}

      {error && (
        <Alert type="error" message="Profile Error" description={error} showIcon style={{ marginBottom: 16 }} />
      )}

      {profile && (
        <Tabs defaultActiveKey="overview" items={tabItems} />
      )}
    </Modal>
  );
}
