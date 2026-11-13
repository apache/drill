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
import { Typography } from 'antd';

/** SQL as monospace text that wraps and scrolls inside whatever gives it a height. */
export default function SqlBlock({ sql }: { sql: string | undefined }) {
  return (
    <pre
      style={{
        margin: 0,
        padding: 12,
        height: '100%',
        overflow: 'auto',
        whiteSpace: 'pre-wrap',
        wordBreak: 'break-word',
        fontFamily: 'var(--font-mono, monospace)',
        fontSize: 12,
      }}
    >
      {sql?.trim() ? sql : <Typography.Text type="secondary">No SQL recorded.</Typography.Text>}
    </pre>
  );
}
