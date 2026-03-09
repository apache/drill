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

interface HiveFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

export default function HiveForm({ config, onChange }: HiveFormProps) {
  const configProps = (config.configProps as Record<string, string>) || {};

  const [metastoreUri, setMetastoreUri] = useState<string>(
    configProps['hive.metastore.uris'] || ''
  );
  const [extraProps, setExtraProps] = useState<{ key: string; value: string }[]>(() => {
    const reserved = new Set(['hive.metastore.uris']);
    return Object.entries(configProps)
      .filter(([k]) => !reserved.has(k))
      .map(([key, value]) => ({ key, value }));
  });

  useEffect(() => {
    const cp = (config.configProps as Record<string, string>) || {};
    setMetastoreUri(cp['hive.metastore.uris'] || '');
    const reserved = new Set(['hive.metastore.uris']);
    setExtraProps(
      Object.entries(cp)
        .filter(([k]) => !reserved.has(k))
        .map(([key, value]) => ({ key, value }))
    );
  }, [config]);

  const buildConfigProps = useCallback(
    (
      uri: string,
      extras: { key: string; value: string }[]
    ): Record<string, string> => {
      const m: Record<string, string> = {};
      if (uri) {
        m['hive.metastore.uris'] = uri;
      }
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

      <Form.Item label={label('Hive Metastore URI', 'Thrift URI of the Hive Metastore service (e.g. "thrift://metastore-host:9083"). Leave empty to use an embedded metastore.')}>
        <Input
          value={metastoreUri}
          onChange={(e) => {
            setMetastoreUri(e.target.value);
            emitChange({
              configProps: buildConfigProps(e.target.value, extraProps),
            });
          }}
          placeholder="thrift://localhost:9083"
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
                  Additional Hive Properties
                </Text>
                <Text type="secondary" style={{ display: 'block', marginBottom: 12 }}>
                  Any Hadoop or Hive configuration property can be added here (e.g. hive.metastore.warehouse.dir, fs.defaultFS).
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
                          configProps: buildConfigProps(metastoreUri, updated),
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
                          configProps: buildConfigProps(metastoreUri, updated),
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
                          configProps: buildConfigProps(metastoreUri, updated),
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
