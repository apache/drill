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
import { useRef } from 'react';
import { Button, Dropdown, Input, Space } from 'antd';
import type { InputRef } from 'antd';
import type { TextAreaRef } from 'antd/es/input/TextArea';

interface Props {
  value?: string;
  onChange: (v: string) => void;
  refs: string[];
  multiline?: boolean;
  placeholder?: string;
  label?: string;
}

/** Text field with an "Insert field" menu that drops {{path}} at the cursor. */
export default function TemplateInput({ value = '', onChange, refs, multiline, placeholder, label }: Props) {
  const inputRef = useRef<InputRef>(null);
  const areaRef = useRef<TextAreaRef>(null);

  const insert = (path: string) => {
    const el = multiline ? areaRef.current?.resizableTextArea?.textArea : inputRef.current?.input;
    const pos = el?.selectionStart ?? value.length;
    onChange(`${value.slice(0, pos)}{{${path}}}${value.slice(pos)}`);
  };

  const menu = { items: refs.map((r) => ({ key: r, label: r })), onClick: ({ key }: { key: string }) => insert(key) };

  return (
    <Space direction="vertical" style={{ width: '100%' }} size={4}>
      {multiline ? (
        <Input.TextArea ref={areaRef} rows={4} value={value} placeholder={placeholder} aria-label={label}
          onChange={(e) => onChange(e.target.value)} />
      ) : (
        <Input ref={inputRef} value={value} placeholder={placeholder} aria-label={label}
          onChange={(e) => onChange(e.target.value)} />
      )}
      <Dropdown menu={menu} trigger={['click']}>
        <Button size="small" type="link" style={{ padding: 0 }}>Insert field</Button>
      </Dropdown>
    </Space>
  );
}
