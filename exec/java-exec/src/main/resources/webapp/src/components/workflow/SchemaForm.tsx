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
import { Form, Input, InputNumber, Select, Switch } from 'antd';
import type { ActionTypeInfo, JsonSchemaProp } from '../../types/workflow';
import TemplateInput from './TemplateInput';

interface Props {
  schema: ActionTypeInfo['configSchema'];
  value: Record<string, unknown>;
  onChange: (v: Record<string, unknown>) => void;
  refs: string[];
  errors: Record<string, string>;
}

/** Renders an action's config form from its JSON schema subset. */
export default function SchemaForm({ schema, value, onChange, refs, errors }: Props) {
  const set = (key: string, v: unknown) => onChange({ ...value, [key]: v });
  const required = new Set(schema.required ?? []);

  const control = (key: string, p: JsonSchemaProp) => {
    const v = value[key] ?? p.default;
    const label = p.title ?? key;
    if (p.enum) {
      return <Select aria-label={label} value={v as string} options={p.enum.map((o) => ({ value: o, label: o }))}
        onChange={(x) => set(key, x)} />;
    }
    switch (p.type) {
      case 'boolean':
        return <Switch aria-label={label} checked={Boolean(v)} onChange={(x) => set(key, x)} />;
      case 'number':
      case 'integer':
        return <InputNumber aria-label={label} value={v as number} onChange={(x) => set(key, x)} style={{ width: '100%' }} />;
      case 'array':
        return <Select aria-label={label} mode="tags" value={(v as string[]) ?? []} onChange={(x) => set(key, x)}
          tokenSeparators={[',', ' ']} open={false} placeholder="Type and press Enter" />;
      default:
        return p['x-template']
          ? <TemplateInput label={label} value={v as string} onChange={(x) => set(key, x)} refs={refs}
            multiline={p['x-multiline']} />
          : <Input aria-label={label} value={v as string} onChange={(e) => set(key, e.target.value)} />;
    }
  };

  return (
    <Form layout="vertical" component="div">
      {Object.entries(schema.properties ?? {}).map(([key, p]) => (
        <Form.Item key={key} label={p.title ?? key} required={required.has(key)} tooltip={p.description}
          validateStatus={errors[key] ? 'error' : undefined} help={errors[key]}>
          {control(key, p)}
        </Form.Item>
      ))}
    </Form>
  );
}
