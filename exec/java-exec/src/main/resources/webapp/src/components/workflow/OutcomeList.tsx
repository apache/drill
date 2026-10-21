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
import { List, Tag, Typography } from 'antd';
import type { ActionOutcome } from '../../types/workflow';

const COLORS: Record<ActionOutcome['outcome'], string> = { fired: 'green', skipped: 'default', errored: 'red' };

export default function OutcomeList({ outcomes }: { outcomes: ActionOutcome[] }) {
  return (
    <List size="small" dataSource={outcomes} renderItem={(o) => (
      <List.Item>
        <List.Item.Meta
          title={<>{o.id} <Tag>{o.type}</Tag><Tag color={COLORS[o.outcome]}>{o.outcome}</Tag>
            <Typography.Text type="secondary">{o.durationMs} ms</Typography.Text></>}
          description={<>
            {o.reason && <div>{o.reason}</div>}
            {o.error && <Typography.Text type="danger">{o.error}</Typography.Text>}
            {o.outputs && <pre style={{ whiteSpace: 'pre-wrap', margin: 0 }}>{JSON.stringify(o.outputs, null, 2)}</pre>}
          </>} />
      </List.Item>
    )} />
  );
}
