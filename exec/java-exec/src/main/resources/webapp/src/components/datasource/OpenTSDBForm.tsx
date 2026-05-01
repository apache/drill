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
import { useState, useEffect, useCallback } from 'react';
import { Form, Input, Tooltip } from 'antd';
import { QuestionCircleOutlined } from '@ant-design/icons';

const helpIcon = { color: '#999', cursor: 'help' as const };

function label(text: string, tip: string) {
  return (
    <span>
      {text}{' '}
      <Tooltip title={tip}>
        <QuestionCircleOutlined style={helpIcon} />
      </Tooltip>
    </span>
  );
}

interface OpenTSDBFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

export default function OpenTSDBForm({ config, onChange }: OpenTSDBFormProps) {
  const [connection, setConnection] = useState<string>(
    (config.connection as string) || ''
  );

  useEffect(() => {
    setConnection((config.connection as string) || '');
  }, [config]);

  const emitChange = useCallback(
    (updates: Partial<Record<string, unknown>>) => {
      onChange({ ...config, ...updates });
    },
    [config, onChange]
  );

  return (
    <Form layout="vertical">
      <Form.Item label={label('Connection', 'The OpenTSDB HTTP API endpoint URL (e.g. "http://localhost:4242").')}>
        <Input
          value={connection}
          onChange={(e) => {
            setConnection(e.target.value);
            emitChange({ connection: e.target.value });
          }}
          placeholder="http://localhost:4242"
        />
      </Form.Item>
    </Form>
  );
}
