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
import { useState, useCallback } from 'react';
import { Tag, Button, Popover, Input, Space, DatePicker, Select, InputNumber } from 'antd';
import { FilterOutlined, ClearOutlined, EditOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import type { Dayjs } from 'dayjs';
import type { DashboardFilter, NumericOperator } from '../../types';

interface DashboardFilterBarProps {
  filters: DashboardFilter[];
  onRemoveFilter: (filterId: string) => void;
  onUpdateFilter: (filterId: string, update: Partial<DashboardFilter>) => void;
  onClearAll: () => void;
}

const NUMERIC_OPS: { value: NumericOperator; label: string }[] = [
  { value: '=', label: '=' },
  { value: '!=', label: '!=' },
  { value: '>', label: '>' },
  { value: '>=', label: '>=' },
  { value: '<', label: '<' },
  { value: '<=', label: '<=' },
  { value: 'between', label: 'between' },
];

/** Editable tag for a plain text (VARCHAR) filter. */
function TextFilterTag({
  filter,
  onRemove,
  onUpdate,
}: {
  filter: DashboardFilter;
  onRemove: () => void;
  onUpdate: (update: Partial<DashboardFilter>) => void;
}) {
  const [open, setOpen] = useState(false);
  const [editValue, setEditValue] = useState(filter.value);

  const handleOpen = useCallback((visible: boolean) => {
    if (visible) {
      setEditValue(filter.value);
    }
    setOpen(visible);
  }, [filter.value]);

  const handleApply = useCallback(() => {
    const trimmed = editValue.trim();
    if (trimmed && trimmed !== filter.value) {
      onUpdate({ value: trimmed });
    }
    setOpen(false);
  }, [editValue, filter.value, onUpdate]);

  return (
    <Popover
      open={open}
      onOpenChange={handleOpen}
      trigger="click"
      placement="bottom"
      content={
        <Space.Compact size="small">
          <Input
            value={editValue}
            onChange={(e) => setEditValue(e.target.value)}
            onPressEnter={handleApply}
            style={{ width: 160 }}
            autoFocus
            prefix={<span style={{ color: 'var(--color-text-tertiary)', fontSize: 12 }}>{filter.column} =</span>}
          />
          <Button type="primary" size="small" onClick={handleApply}>
            Apply
          </Button>
        </Space.Compact>
      }
    >
      <Tag
        closable
        onClose={(e) => { e.stopPropagation(); onRemove(); }}
        color="blue"
        style={{ cursor: 'pointer' }}
      >
        {filter.label || `${filter.column} = ${filter.value}`}
        <EditOutlined style={{ marginLeft: 4, fontSize: 10, opacity: 0.7 }} />
      </Tag>
    </Popover>
  );
}

/** Editable tag for a temporal (date range) filter. */
function TemporalFilterTag({
  filter,
  onRemove,
  onUpdate,
}: {
  filter: DashboardFilter;
  onRemove: () => void;
  onUpdate: (update: Partial<DashboardFilter>) => void;
}) {
  const [open, setOpen] = useState(false);

  const rangeValue: [Dayjs, Dayjs] = [
    dayjs(filter.rangeStart || filter.value),
    dayjs(filter.rangeEnd || filter.value),
  ];

  const handleRangeChange = useCallback((dates: [Dayjs | null, Dayjs | null] | null) => {
    if (dates && dates[0] && dates[1]) {
      onUpdate({
        rangeStart: dates[0].format('YYYY-MM-DD'),
        rangeEnd: dates[1].format('YYYY-MM-DD'),
      });
      setOpen(false);
    }
  }, [onUpdate]);

  return (
    <Popover
      open={open}
      onOpenChange={setOpen}
      trigger="click"
      placement="bottom"
      content={
        <div>
          <div style={{ marginBottom: 4, fontSize: 12, color: 'var(--color-text-secondary)' }}>
            {filter.column}
          </div>
          <DatePicker.RangePicker
            size="small"
            value={rangeValue}
            onChange={handleRangeChange}
            allowClear={false}
            autoFocus
          />
        </div>
      }
    >
      <Tag
        closable
        onClose={(e) => { e.stopPropagation(); onRemove(); }}
        color="blue"
        style={{ cursor: 'pointer' }}
      >
        {filter.label}
        <EditOutlined style={{ marginLeft: 4, fontSize: 10, opacity: 0.7 }} />
      </Tag>
    </Popover>
  );
}

/** Editable tag for a numeric filter with operator selection. */
function NumericFilterTag({
  filter,
  onRemove,
  onUpdate,
}: {
  filter: DashboardFilter;
  onRemove: () => void;
  onUpdate: (update: Partial<DashboardFilter>) => void;
}) {
  const [open, setOpen] = useState(false);
  const [op, setOp] = useState<NumericOperator>(filter.numericOp || '=');
  const [val, setVal] = useState<string>(filter.value);
  const [valEnd, setValEnd] = useState<string>(filter.numericEnd || filter.value);

  const handleOpen = useCallback((visible: boolean) => {
    if (visible) {
      setOp(filter.numericOp || '=');
      setVal(filter.value);
      setValEnd(filter.numericEnd || filter.value);
    }
    setOpen(visible);
  }, [filter]);

  const handleApply = useCallback(() => {
    if (val === '') {
      return;
    }
    const update: Partial<DashboardFilter> = { numericOp: op, value: val };
    if (op === 'between') {
      update.numericEnd = valEnd;
    } else {
      update.numericEnd = undefined;
    }
    onUpdate(update);
    setOpen(false);
  }, [op, val, valEnd, onUpdate]);

  return (
    <Popover
      open={open}
      onOpenChange={handleOpen}
      trigger="click"
      placement="bottom"
      content={
        <Space direction="vertical" size={8} style={{ width: 220 }}>
          <div style={{ fontSize: 12, color: 'var(--color-text-secondary)' }}>
            {filter.column}
          </div>
          <Space.Compact size="small" style={{ width: '100%' }}>
            <Select
              value={op}
              onChange={setOp}
              options={NUMERIC_OPS}
              style={{ width: 90 }}
              size="small"
            />
            <InputNumber
              value={val !== '' ? Number(val) : undefined}
              onChange={(v) => setVal(v != null ? String(v) : '')}
              style={{ flex: 1 }}
              size="small"
              autoFocus
              onPressEnter={op !== 'between' ? handleApply : undefined}
            />
          </Space.Compact>
          {op === 'between' && (
            <Space size={4} align="center" style={{ width: '100%' }}>
              <span style={{ fontSize: 12, color: 'var(--color-text-tertiary)' }}>and</span>
              <InputNumber
                value={valEnd !== '' ? Number(valEnd) : undefined}
                onChange={(v) => setValEnd(v != null ? String(v) : '')}
                style={{ flex: 1 }}
                size="small"
                onPressEnter={handleApply}
              />
            </Space>
          )}
          <Button type="primary" size="small" block onClick={handleApply}>
            Apply
          </Button>
        </Space>
      }
    >
      <Tag
        closable
        onClose={(e) => { e.stopPropagation(); onRemove(); }}
        color="blue"
        style={{ cursor: 'pointer' }}
      >
        {filter.label || `${filter.column} = ${filter.value}`}
        <EditOutlined style={{ marginLeft: 4, fontSize: 10, opacity: 0.7 }} />
      </Tag>
    </Popover>
  );
}

export default function DashboardFilterBar({
  filters,
  onRemoveFilter,
  onUpdateFilter,
  onClearAll,
}: DashboardFilterBarProps) {
  if (filters.length === 0) {
    return null;
  }

  return (
    <div className="dashboard-filter-bar">
      <FilterOutlined style={{ marginRight: 8, opacity: 0.6 }} />
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, flex: 1 }}>
        {filters.map((filter) => {
          if (filter.isTemporal) {
            return (
              <TemporalFilterTag
                key={filter.id}
                filter={filter}
                onRemove={() => onRemoveFilter(filter.id)}
                onUpdate={(update) => onUpdateFilter(filter.id, update)}
              />
            );
          }
          if (filter.isNumeric) {
            return (
              <NumericFilterTag
                key={filter.id}
                filter={filter}
                onRemove={() => onRemoveFilter(filter.id)}
                onUpdate={(update) => onUpdateFilter(filter.id, update)}
              />
            );
          }
          return (
            <TextFilterTag
              key={filter.id}
              filter={filter}
              onRemove={() => onRemoveFilter(filter.id)}
              onUpdate={(update) => onUpdateFilter(filter.id, update)}
            />
          );
        })}
      </div>
      {filters.length > 1 && (
        <Button
          type="text"
          size="small"
          icon={<ClearOutlined />}
          onClick={onClearAll}
        >
          Clear all
        </Button>
      )}
    </div>
  );
}
