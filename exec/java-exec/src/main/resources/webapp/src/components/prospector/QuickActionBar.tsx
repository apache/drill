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
import { Button, Space } from 'antd';
import {
  CodeOutlined,
  QuestionCircleOutlined,
  BugOutlined,
  BarChartOutlined,
} from '@ant-design/icons';

interface QuickActionBarProps {
  onAction: (prompt: string) => void;
  hasError: boolean;
  hasResults: boolean;
  hasSql: boolean;
  disabled: boolean;
}

export default function QuickActionBar({
  onAction,
  hasError,
  hasResults,
  hasSql,
  disabled,
}: QuickActionBarProps) {
  return (
    <div className="prospector-quick-actions">
      <Space size={[4, 4]} wrap>
        <Button
          size="small"
          icon={<CodeOutlined />}
          onClick={() => onAction('Generate a SQL query for this data')}
          disabled={disabled}
        >
          Generate SQL
        </Button>
        {hasSql && (
          <Button
            size="small"
            icon={<QuestionCircleOutlined />}
            onClick={() => onAction('Explain the current SQL query in the editor')}
            disabled={disabled}
          >
            Explain Query
          </Button>
        )}
        {hasError && (
          <Button
            size="small"
            icon={<BugOutlined />}
            onClick={() => onAction('Fix the current error in my SQL query')}
            disabled={disabled}
            danger
          >
            Fix Error
          </Button>
        )}
        {hasResults && (
          <Button
            size="small"
            icon={<BarChartOutlined />}
            onClick={() => onAction('Suggest the best chart type for the current query results and create a visualization')}
            disabled={disabled}
          >
            Suggest Chart
          </Button>
        )}
      </Space>
    </div>
  );
}
