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
import { Form, Input, InputNumber, Collapse, Tooltip, Typography } from 'antd';
import { QuestionCircleOutlined } from '@ant-design/icons';

const { Text } = Typography;

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

interface DruidFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

export default function DruidForm({ config, onChange }: DruidFormProps) {
  const [brokerAddress, setBrokerAddress] = useState<string>(
    (config.brokerAddress as string) || 'http://localhost:8082'
  );
  const [coordinatorAddress, setCoordinatorAddress] = useState<string>(
    (config.coordinatorAddress as string) || 'http://localhost:8081'
  );
  const [averageRowSizeBytes, setAverageRowSizeBytes] = useState<number>(
    (config.averageRowSizeBytes as number) || 100
  );

  useEffect(() => {
    setBrokerAddress((config.brokerAddress as string) || 'http://localhost:8082');
    setCoordinatorAddress((config.coordinatorAddress as string) || 'http://localhost:8081');
    setAverageRowSizeBytes((config.averageRowSizeBytes as number) || 100);
  }, [config]);

  const emitChange = useCallback(
    (updates: Partial<Record<string, unknown>>) => {
      onChange({ ...config, ...updates });
    },
    [config, onChange]
  );

  return (
    <Form layout="vertical">
      <Text strong style={{ display: 'block', marginBottom: 12 }}>
        Connection
      </Text>

      <Form.Item label={label('Broker Address', 'The URL of the Druid Broker node. The Broker handles queries from external clients.')}>
        <Input
          value={brokerAddress}
          onChange={(e) => {
            setBrokerAddress(e.target.value);
            emitChange({ brokerAddress: e.target.value });
          }}
          placeholder="http://localhost:8082"
        />
      </Form.Item>

      <Form.Item label={label('Coordinator Address', 'The URL of the Druid Coordinator node. The Coordinator manages data availability and segment loading.')}>
        <Input
          value={coordinatorAddress}
          onChange={(e) => {
            setCoordinatorAddress(e.target.value);
            emitChange({ coordinatorAddress: e.target.value });
          }}
          placeholder="http://localhost:8081"
        />
      </Form.Item>

      <Collapse
        ghost
        style={{ marginTop: 16 }}
        items={[
          {
            key: 'advanced',
            label: <Text strong>Advanced Options</Text>,
            children: (
              <Form.Item label={label('Average Row Size (bytes)', 'Estimated average row size in bytes, used by the query planner to calculate scan batch sizes. Default is 100.')}>
                <InputNumber
                  value={averageRowSizeBytes}
                  onChange={(val) => {
                    const v = val || 100;
                    setAverageRowSizeBytes(v);
                    emitChange({ averageRowSizeBytes: v });
                  }}
                  min={1}
                  style={{ width: 200 }}
                />
              </Form.Item>
            ),
          },
        ]}
      />
    </Form>
  );
}
