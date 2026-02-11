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
import { Collapse, Tag } from 'antd';
import {
  CodeOutlined,
  DatabaseOutlined,
  BarChartOutlined,
  DashboardOutlined,
  SaveOutlined,
  FunctionOutlined,
} from '@ant-design/icons';
import type { ToolCall } from '../../types/ai';
import type { ChatMessage } from '../../types/ai';

const TOOL_ICONS: Record<string, React.ReactNode> = {
  execute_sql: <CodeOutlined />,
  get_schema_info: <DatabaseOutlined />,
  create_visualization: <BarChartOutlined />,
  create_dashboard: <DashboardOutlined />,
  save_query: <SaveOutlined />,
  get_available_functions: <FunctionOutlined />,
};

const TOOL_LABELS: Record<string, string> = {
  execute_sql: 'Execute SQL',
  get_schema_info: 'Schema Info',
  create_visualization: 'Create Chart',
  create_dashboard: 'Create Dashboard',
  save_query: 'Save Query',
  get_available_functions: 'List Functions',
};

interface ToolCallDisplayProps {
  toolCalls: ToolCall[];
  toolResults: ChatMessage[];
}

function formatJson(str: string): string {
  try {
    return JSON.stringify(JSON.parse(str), null, 2);
  } catch {
    return str;
  }
}

export default function ToolCallDisplay({ toolCalls, toolResults }: ToolCallDisplayProps) {
  const items = toolCalls.map((tc) => {
    const result = toolResults.find((r) => r.toolCallId === tc.id);
    const icon = TOOL_ICONS[tc.name] || <CodeOutlined />;
    const label = TOOL_LABELS[tc.name] || tc.name;

    return {
      key: tc.id,
      label: (
        <span>
          {icon}
          <span style={{ marginLeft: 6 }}>{label}</span>
          {result && (
            <Tag
              color="green"
              style={{ marginLeft: 8, fontSize: 10 }}
            >
              Done
            </Tag>
          )}
        </span>
      ),
      children: (
        <div style={{ fontSize: 12 }}>
          <div style={{ marginBottom: 8 }}>
            <strong>Arguments:</strong>
            <pre className="prospector-tool-json">{formatJson(tc.arguments)}</pre>
          </div>
          {result && (
            <div>
              <strong>Result:</strong>
              <pre className="prospector-tool-json">
                {formatJson(result.content || '')}
              </pre>
            </div>
          )}
        </div>
      ),
    };
  });

  return (
    <Collapse
      size="small"
      items={items}
      style={{ marginTop: 8 }}
    />
  );
}
