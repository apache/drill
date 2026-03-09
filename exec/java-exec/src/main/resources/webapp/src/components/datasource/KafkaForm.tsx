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
import { Form, Input, Button, Collapse, Tooltip, Typography } from 'antd';
import { QuestionCircleOutlined, PlusOutlined, DeleteOutlined } from '@ant-design/icons';

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

const RESERVED_KEYS = new Set(['bootstrap.servers']);

interface KafkaFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

export default function KafkaForm({ config, onChange }: KafkaFormProps) {
  const consumerProps = (config.kafkaConsumerProps as Record<string, string>) || {};

  const [bootstrapServers, setBootstrapServers] = useState<string>(
    consumerProps['bootstrap.servers'] || 'localhost:9092'
  );
  const [extraProps, setExtraProps] = useState<{ key: string; value: string }[]>(() =>
    Object.entries(consumerProps)
      .filter(([k]) => !RESERVED_KEYS.has(k))
      .map(([key, value]) => ({ key, value }))
  );

  useEffect(() => {
    const cp = (config.kafkaConsumerProps as Record<string, string>) || {};
    setBootstrapServers(cp['bootstrap.servers'] || 'localhost:9092');
    setExtraProps(
      Object.entries(cp)
        .filter(([k]) => !RESERVED_KEYS.has(k))
        .map(([key, value]) => ({ key, value }))
    );
  }, [config]);

  const buildProps = useCallback(
    (servers: string, extras: { key: string; value: string }[]): Record<string, string> => {
      const m: Record<string, string> = {
        'bootstrap.servers': servers,
      };
      for (const { key, value } of extras) {
        if (key) {
          m[key] = value;
        }
      }
      return m;
    },
    []
  );

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

      <Form.Item label={label('Bootstrap Servers', 'Comma-separated list of Kafka broker addresses used for initial cluster connection (e.g. "broker1:9092,broker2:9092").')}>
        <Input
          value={bootstrapServers}
          onChange={(e) => {
            setBootstrapServers(e.target.value);
            emitChange({
              kafkaConsumerProps: buildProps(e.target.value, extraProps),
            });
          }}
          placeholder="localhost:9092"
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
              <>
                <Text strong style={{ display: 'block', marginBottom: 12 }}>
                  Additional Kafka Consumer Properties
                </Text>
                <Text type="secondary" style={{ display: 'block', marginBottom: 12 }}>
                  Any Kafka consumer configuration property can be added here (e.g. security.protocol, sasl.mechanism, group.id).
                </Text>

                {extraProps.map((prop, index) => (
                  <div key={index} style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
                    <Input
                      value={prop.key}
                      onChange={(e) => {
                        const updated = [...extraProps];
                        updated[index] = { ...updated[index], key: e.target.value };
                        setExtraProps(updated);
                        emitChange({
                          kafkaConsumerProps: buildProps(bootstrapServers, updated),
                        });
                      }}
                      placeholder="Property name"
                      style={{ flex: 1 }}
                    />
                    <Input
                      value={prop.value}
                      onChange={(e) => {
                        const updated = [...extraProps];
                        updated[index] = { ...updated[index], value: e.target.value };
                        setExtraProps(updated);
                        emitChange({
                          kafkaConsumerProps: buildProps(bootstrapServers, updated),
                        });
                      }}
                      placeholder="Value"
                      style={{ flex: 1 }}
                    />
                    <Button
                      icon={<DeleteOutlined />}
                      onClick={() => {
                        const updated = extraProps.filter((_, i) => i !== index);
                        setExtraProps(updated);
                        emitChange({
                          kafkaConsumerProps: buildProps(bootstrapServers, updated),
                        });
                      }}
                      danger
                    />
                  </div>
                ))}
                <Button
                  type="dashed"
                  onClick={() => {
                    setExtraProps([...extraProps, { key: '', value: '' }]);
                  }}
                  icon={<PlusOutlined />}
                  style={{ width: '100%' }}
                >
                  Add Property
                </Button>
              </>
            ),
          },
        ]}
      />
    </Form>
  );
}
