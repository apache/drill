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
import { useEffect, useState } from 'react';
import { AutoComplete, Button, Input, Segmented, Select, Space, Typography, message } from 'antd';
import { DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import type { Condition } from '../../types/workflow';
import { parseCondition } from '../../api/workflows';
import { OPS, coerceValue, fromBuilderModel, toBuilderModel, treeToExpr } from '../../utils/conditionTree';

interface Props {
  value: Condition;
  onChange: (c: Condition) => void;
  refs: string[];
  error?: string;
}

/** Keeps the typed text locally so partial numbers such as "0." are never rewritten under the cursor. */
function LeafValueInput({ value, onChange }: { value: unknown; onChange: (v: unknown) => void }) {
  const display = value === null ? 'null' : String(value ?? '');
  const [text, setText] = useState(display);
  useEffect(() => {
    setText((t) => (Object.is(coerceValue(t), value) ? t : display));
  }, [value, display]);
  return (
    <Input aria-label="Condition value" value={text}
      onChange={(e) => { setText(e.target.value); onChange(coerceValue(e.target.value)); }} />
  );
}

type Mode = 'always' | 'builder' | 'expr';

const modeOf = (c: Condition): Mode => (c.always ? 'always' : c.expr !== undefined ? 'expr' : 'builder');

export default function ConditionEditor({ value, onChange, refs, error }: Props) {
  const mode = modeOf(value);
  const [parseError, setParseError] = useState<string | null>(null);

  useEffect(() => {
    if (mode !== 'expr' || !value.expr?.trim()) {
      setParseError(null);
      return undefined;
    }
    let cancelled = false;
    const t = setTimeout(() => {
      parseCondition(value.expr!)
        .then((r) => { if (!cancelled) { setParseError(r.error ?? null); } })
        .catch(() => { if (!cancelled) { setParseError(null); } });
    }, 400);
    return () => { cancelled = true; clearTimeout(t); };
  }, [mode, value.expr]);

  const switchMode = async (next: Mode) => {
    try {
      await applyMode(next);
    } catch (e) {
      message.error(e instanceof Error ? e.message : 'Could not switch mode');
    }
  };

  const applyMode = async (next: Mode) => {
    if (next === 'always') {
      onChange({ always: true });
    } else if (next === 'expr') {
      onChange({ expr: value.tree ? treeToExpr(value.tree) : '' });
    } else if (mode === 'expr' && value.expr?.trim()) {
      const r = await parseCondition(value.expr);
      if (r.tree && toBuilderModel(r.tree)) {
        onChange({ tree: r.tree });
      } else {
        message.warning(r.error ?? 'This expression is too complex for the builder');
      }
    } else {
      onChange({ tree: { all: [] } });
    }
  };

  const model = mode === 'builder' ? toBuilderModel(value.tree) : null;
  const setModel = (m: NonNullable<typeof model>) => onChange({ tree: fromBuilderModel(m) });

  return (
    <Space direction="vertical" style={{ width: '100%' }}>
      <Segmented<Mode> value={mode} onChange={switchMode}
        options={[{ label: 'Always', value: 'always' }, { label: 'Builder', value: 'builder' }, { label: 'Expression', value: 'expr' }]} />
      {mode === 'expr' && (
        <>
          <Input aria-label="Condition expression" value={value.expr} status={parseError || error ? 'error' : undefined}
            placeholder="rowCount > 50 && actions.judge.verdict" onChange={(e) => onChange({ expr: e.target.value })}
            style={{ fontFamily: 'monospace' }} />
          {(parseError || error) && <Typography.Text type="danger">{parseError ?? error}</Typography.Text>}
        </>
      )}
      {mode === 'builder' && model && (
        <>
          <Select aria-label="Match mode" value={model.mode} style={{ width: 200 }} onChange={(m) => setModel({ ...model, mode: m })}
            options={[{ value: 'all', label: 'All of these (AND)' }, { value: 'any', label: 'Any of these (OR)' }]} />
          {model.leaves.map((leaf, i) => {
            const update = (patch: Partial<typeof leaf>) =>
              setModel({ ...model, leaves: model.leaves.map((l, j) => (j === i ? { ...l, ...patch } : l)) });
            return (
              <Space.Compact key={i} style={{ width: '100%' }}>
                <AutoComplete aria-label="Condition field" value={leaf.field} style={{ width: '40%' }}
                  onChange={(f) => update({ field: f })} options={[...refs, 'rows[*].'].map((r) => ({ value: r }))} />
                <Select aria-label="Condition operator" value={leaf.op} style={{ width: 130 }} onChange={(op) => update({ op })}
                  options={OPS.map((o) => ({ value: o, label: o }))} />
                <LeafValueInput value={leaf.value} onChange={(v) => update({ value: v })} />
                <Button icon={<DeleteOutlined />} aria-label="Remove condition"
                  onClick={() => setModel({ ...model, leaves: model.leaves.filter((_, j) => j !== i) })} />
              </Space.Compact>
            );
          })}
          <Button icon={<PlusOutlined />} size="small"
            onClick={() => setModel({ ...model, leaves: [...model.leaves, { field: 'rowCount', op: '>', value: 0 }] })}>
            Add condition
          </Button>
          {error && <Typography.Text type="danger">{error}</Typography.Text>}
        </>
      )}
      {mode === 'builder' && !model && (
        <Typography.Text type="warning">Too complex for the builder; switch to Expression to edit.</Typography.Text>
      )}
    </Space>
  );
}
