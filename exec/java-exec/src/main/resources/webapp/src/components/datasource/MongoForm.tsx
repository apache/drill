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
import { Form, Input, Switch, InputNumber, Collapse, Tooltip, Typography } from 'antd';
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

interface MongoFormProps {
  config: Record<string, unknown>;
  onChange: (config: Record<string, unknown>) => void;
}

export default function MongoForm({ config, onChange }: MongoFormProps) {
  const [connection, setConnection] = useState<string>(
    (config.connection as string) || 'mongodb://localhost:27017'
  );
  const [batchSize, setBatchSize] = useState<number>((config.batchSize as number) || 100);
  const [allowDiskUse, setAllowDiskUse] = useState<boolean>(
    (config.allowDiskUse as boolean) || false
  );
  const [projectPushdown, setProjectPushdown] = useState<boolean>(
    config.mongoPushdownProjections !== false
  );
  const [filterPushdown, setFilterPushdown] = useState<boolean>(
    config.mongoPushdownFilters !== false
  );
  const [aggregatePushdown, setAggregatePushdown] = useState<boolean>(
    config.mongoPushdownAggregations !== false
  );
  const [sortPushdown, setSortPushdown] = useState<boolean>(
    config.mongoPushdownSort !== false
  );
  const [unionPushdown, setUnionPushdown] = useState<boolean>(
    config.mongoPushdownUnion !== false
  );
  const [limitPushdown, setLimitPushdown] = useState<boolean>(
    config.mongoPushdownLimit !== false
  );

  useEffect(() => {
    setConnection((config.connection as string) || 'mongodb://localhost:27017');
    setBatchSize((config.batchSize as number) || 100);
    setAllowDiskUse((config.allowDiskUse as boolean) || false);
    setProjectPushdown(config.mongoPushdownProjections !== false);
    setFilterPushdown(config.mongoPushdownFilters !== false);
    setAggregatePushdown(config.mongoPushdownAggregations !== false);
    setSortPushdown(config.mongoPushdownSort !== false);
    setUnionPushdown(config.mongoPushdownUnion !== false);
    setLimitPushdown(config.mongoPushdownLimit !== false);
  }, [config]);

  const emitChange = useCallback(
    (updates: Partial<Record<string, unknown>>) => {
      onChange({ ...config, ...updates });
    },
    [config, onChange]
  );

  return (
    <Form layout="vertical">
      <Form.Item label={label('Connection', 'MongoDB connection string. Supports standalone, replica set, and sharded cluster URIs (e.g. mongodb://host:27017, mongodb+srv://cluster).')}>
        <Input
          value={connection}
          onChange={(e) => {
            setConnection(e.target.value);
            emitChange({ connection: e.target.value });
          }}
          placeholder="mongodb://host:27017"
        />
      </Form.Item>

      <Form.Item label={label('Batch Size', 'Number of documents fetched per batch from MongoDB. Larger values may improve throughput but use more memory.')}>
        <InputNumber
          value={batchSize}
          onChange={(val) => {
            const v = val || 100;
            setBatchSize(v);
            emitChange({ batchSize: v });
          }}
          min={1}
          style={{ width: 200 }}
        />
      </Form.Item>

      <Form.Item label={label('Allow Disk Use', 'When enabled, MongoDB operations that require more memory than allowed can write temporary data to disk. Useful for large aggregations.')}>
        <Switch
          checked={allowDiskUse}
          onChange={(checked) => {
            setAllowDiskUse(checked);
            emitChange({ allowDiskUse: checked });
          }}
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
                  Query Pushdown
                </Text>

                <Form.Item label={label('Project Pushdown', 'When enabled, Drill pushes column projections down to MongoDB so only the required fields are returned, reducing network traffic.')} style={{ marginBottom: 8 }}>
                  <Switch
                    checked={projectPushdown}
                    onChange={(checked) => {
                      setProjectPushdown(checked);
                      emitChange({ mongoPushdownProjections: checked });
                    }}
                  />
                </Form.Item>

                <Form.Item label={label('Filter Pushdown', 'When enabled, Drill pushes WHERE clause filters down to MongoDB so filtering happens server-side.')} style={{ marginBottom: 8 }}>
                  <Switch
                    checked={filterPushdown}
                    onChange={(checked) => {
                      setFilterPushdown(checked);
                      emitChange({ mongoPushdownFilters: checked });
                    }}
                  />
                </Form.Item>

                <Form.Item label={label('Aggregate Pushdown', 'When enabled, Drill pushes aggregation operations (GROUP BY, COUNT, SUM, etc.) down to MongoDB.')} style={{ marginBottom: 8 }}>
                  <Switch
                    checked={aggregatePushdown}
                    onChange={(checked) => {
                      setAggregatePushdown(checked);
                      emitChange({ mongoPushdownAggregations: checked });
                    }}
                  />
                </Form.Item>

                <Form.Item label={label('Sort Pushdown', 'When enabled, Drill pushes ORDER BY operations down to MongoDB so sorting happens server-side.')} style={{ marginBottom: 8 }}>
                  <Switch
                    checked={sortPushdown}
                    onChange={(checked) => {
                      setSortPushdown(checked);
                      emitChange({ mongoPushdownSort: checked });
                    }}
                  />
                </Form.Item>

                <Form.Item label={label('Union Pushdown', 'When enabled, Drill pushes UNION operations down to MongoDB.')} style={{ marginBottom: 8 }}>
                  <Switch
                    checked={unionPushdown}
                    onChange={(checked) => {
                      setUnionPushdown(checked);
                      emitChange({ mongoPushdownUnion: checked });
                    }}
                  />
                </Form.Item>

                <Form.Item label={label('Limit Pushdown', 'When enabled, Drill pushes LIMIT operations down to MongoDB so only the requested number of documents are returned.')} style={{ marginBottom: 8 }}>
                  <Switch
                    checked={limitPushdown}
                    onChange={(checked) => {
                      setLimitPushdown(checked);
                      emitChange({ mongoPushdownLimit: checked });
                    }}
                  />
                </Form.Item>
              </>
            ),
          },
        ]}
      />
    </Form>
  );
}
