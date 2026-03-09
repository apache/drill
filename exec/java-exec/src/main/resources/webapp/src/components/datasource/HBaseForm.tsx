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
import { Form, Input, Switch, Button, Collapse, Tooltip, Typography } from 'antd';
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

interface HBaseFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

export default function HBaseForm({ config, onChange }: HBaseFormProps) {
  const configMap = (config.config as Record<string, string>) || {};

  const [zkQuorum, setZkQuorum] = useState<string>(
    configMap['hbase.zookeeper.quorum'] || 'localhost'
  );
  const [zkPort, setZkPort] = useState<string>(
    configMap['hbase.zookeeper.property.clientPort'] || '2181'
  );
  const [sizeCalculatorEnabled, setSizeCalculatorEnabled] = useState<boolean>(
    (config['size.calculator.enabled'] as boolean) || false
  );
  const [extraProps, setExtraProps] = useState<{ key: string; value: string }[]>(() => {
    const reserved = new Set([
      'hbase.zookeeper.quorum',
      'hbase.zookeeper.property.clientPort',
    ]);
    return Object.entries(configMap)
      .filter(([k]) => !reserved.has(k))
      .map(([key, value]) => ({ key, value }));
  });

  useEffect(() => {
    const cm = (config.config as Record<string, string>) || {};
    setZkQuorum(cm['hbase.zookeeper.quorum'] || 'localhost');
    setZkPort(cm['hbase.zookeeper.property.clientPort'] || '2181');
    setSizeCalculatorEnabled((config['size.calculator.enabled'] as boolean) || false);
    const reserved = new Set([
      'hbase.zookeeper.quorum',
      'hbase.zookeeper.property.clientPort',
    ]);
    setExtraProps(
      Object.entries(cm)
        .filter(([k]) => !reserved.has(k))
        .map(([key, value]) => ({ key, value }))
    );
  }, [config]);

  const buildConfigMap = useCallback(
    (
      quorum: string,
      port: string,
      extras: { key: string; value: string }[]
    ): Record<string, string> => {
      const m: Record<string, string> = {
        'hbase.zookeeper.quorum': quorum,
        'hbase.zookeeper.property.clientPort': port,
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

      <Form.Item label={label('ZooKeeper Quorum', 'Comma-separated list of ZooKeeper hosts that HBase uses for coordination (e.g. "zk1,zk2,zk3").')}>
        <Input
          value={zkQuorum}
          onChange={(e) => {
            setZkQuorum(e.target.value);
            emitChange({
              config: buildConfigMap(e.target.value, zkPort, extraProps),
            });
          }}
          placeholder="localhost"
        />
      </Form.Item>

      <Form.Item label={label('ZooKeeper Port', 'The client port for ZooKeeper. Default is 2181.')}>
        <Input
          value={zkPort}
          onChange={(e) => {
            setZkPort(e.target.value);
            emitChange({
              config: buildConfigMap(zkQuorum, e.target.value, extraProps),
            });
          }}
          placeholder="2181"
          style={{ width: 200 }}
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
                <Form.Item label={label('Size Calculator', 'When enabled, Drill uses HBase region size statistics to optimize query planning. May add overhead for large clusters.')}>
                  <Switch
                    checked={sizeCalculatorEnabled}
                    onChange={(checked) => {
                      setSizeCalculatorEnabled(checked);
                      emitChange({ 'size.calculator.enabled': checked });
                    }}
                  />
                </Form.Item>

                <Text strong style={{ display: 'block', marginTop: 16, marginBottom: 12 }}>
                  Additional HBase Properties
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
                          config: buildConfigMap(zkQuorum, zkPort, updated),
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
                          config: buildConfigMap(zkQuorum, zkPort, updated),
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
                          config: buildConfigMap(zkQuorum, zkPort, updated),
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
