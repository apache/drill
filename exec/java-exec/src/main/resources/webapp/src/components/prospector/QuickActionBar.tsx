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
  ExperimentOutlined,
  LineChartOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';

interface QuickActionBarProps {
  onAction: (prompt: string) => void;
  hasError: boolean;
  hasResults: boolean;
  hasSql: boolean;
  disabled: boolean;
  notebookMode?: boolean;
  notebookCellError?: boolean;
  notebookDfName?: string;
  logAnalysisMode?: boolean;
}

export default function QuickActionBar({
  onAction,
  hasError,
  hasResults,
  hasSql,
  disabled,
  notebookMode,
  notebookCellError,
  notebookDfName = 'df',
  logAnalysisMode,
}: QuickActionBarProps) {
  if (logAnalysisMode) {
    return (
      <div className="prospector-quick-actions">
        <Space size={[4, 4]} wrap>
          <Button
            size="small"
            icon={<BugOutlined />}
            onClick={() => onAction('Summarize the errors in the log. Identify the most common errors, their frequency, and potential root causes.')}
            disabled={disabled}
          >
            Summarize Errors
          </Button>
          <Button
            size="small"
            icon={<ExperimentOutlined />}
            onClick={() => onAction('Analyze the log for performance issues. Look for slow operations, thread contention, timeouts, or memory warnings.')}
            disabled={disabled}
          >
            Find Performance Issues
          </Button>
          <Button
            size="small"
            icon={<QuestionCircleOutlined />}
            onClick={() => onAction('Explain the most recent errors in the log. What went wrong and what should I do to fix them?')}
            disabled={disabled}
          >
            Explain Errors
          </Button>
          <Button
            size="small"
            icon={<CodeOutlined />}
            onClick={() => onAction('Write a SQL query using the dfs.logs workspace to analyze these log files. Suggest useful analytical queries for monitoring and troubleshooting.')}
            disabled={disabled}
          >
            Suggest SQL
          </Button>
        </Space>
      </div>
    );
  }

  if (notebookMode) {
    return (
      <div className="prospector-quick-actions">
        <Space size={[4, 4]} wrap>
          <Button
            size="small"
            icon={<ExperimentOutlined />}
            onClick={() => onAction(`Write Python code to analyze the DataFrame '${notebookDfName}'. Suggest interesting patterns, statistics, or insights to explore. Return the code in a single python code block.`)}
            disabled={disabled}
          >
            Analyze Data
          </Button>
          <Button
            size="small"
            icon={<LineChartOutlined />}
            onClick={() => onAction(`Write Python code using matplotlib to create a meaningful visualization of the DataFrame '${notebookDfName}'. Choose the best chart type for the data. Return the code in a single python code block.`)}
            disabled={disabled}
          >
            Suggest Plot
          </Button>
          <Button
            size="small"
            icon={<ThunderboltOutlined />}
            onClick={() => onAction(`Write Python code to build a machine learning model using scikit-learn on the DataFrame '${notebookDfName}'. Choose an appropriate model based on the data types and suggest preprocessing steps. Return the code in a single python code block.`)}
            disabled={disabled}
          >
            Build Model
          </Button>
          {notebookCellError && (
            <Button
              size="small"
              icon={<BugOutlined />}
              onClick={() => onAction('Fix the error in my notebook cell. Explain what went wrong and provide corrected code in a single python code block.')}
              disabled={disabled}
              danger
            >
              Fix Cell Error
            </Button>
          )}
        </Space>
      </div>
    );
  }

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
