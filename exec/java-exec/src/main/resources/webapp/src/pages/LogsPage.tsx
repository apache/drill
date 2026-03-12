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
import { useState, useMemo, useRef, useEffect, useCallback } from 'react';
import {
  Table,
  Button,
  Input,
  Typography,
  Space,
  Spin,
  Tag,
  Drawer,
  Empty,
  Tooltip,
  Tabs,
  Alert,
  message,
} from 'antd';
import {
  FileTextOutlined,
  DownloadOutlined,
  SearchOutlined,
  ReloadOutlined,
  ArrowUpOutlined,
  ArrowDownOutlined,
  DatabaseOutlined,
  PlayCircleOutlined,
  CheckCircleOutlined,
  SettingOutlined,
  RobotOutlined,
  FolderOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import type { ColumnsType } from 'antd/es/table';
import {
  getLogFiles,
  getLogContent,
  getLogDownloadUrl,
  getLogSqlStatus,
  setupLogSql,
} from '../api/logs';
import { executeQuery } from '../api/queries';
import { getAiStatus } from '../api/ai';
import { useProspector } from '../hooks/useProspector';
import { ProspectorPanel } from '../components/prospector/index';
import type { LogFile } from '../api/logs';
import type { ChatContext } from '../types/ai';

const { Text } = Typography;
const { TextArea } = Input;

const EXAMPLE_QUERIES = [
  {
    label: 'Recent errors',
    sql: "SELECT log_timestamp, logger, message\nFROM dfs.logs.`*.drilllog`\nWHERE level = 'ERROR'\nORDER BY log_timestamp DESC\nLIMIT 100",
  },
  {
    label: 'Error frequency by hour',
    sql: "SELECT SUBSTR(log_timestamp, 1, 13) AS hour, COUNT(*) AS error_count\nFROM dfs.logs.`*.drilllog`\nWHERE level = 'ERROR'\nGROUP BY SUBSTR(log_timestamp, 1, 13)\nORDER BY hour DESC\nLIMIT 48",
  },
  {
    label: 'Log level distribution',
    sql: "SELECT level, COUNT(*) AS cnt\nFROM dfs.logs.`*.drilllog`\nGROUP BY level\nORDER BY cnt DESC",
  },
  {
    label: 'Top error loggers',
    sql: "SELECT logger, COUNT(*) AS error_count\nFROM dfs.logs.`*.drilllog`\nWHERE level = 'ERROR'\nGROUP BY logger\nORDER BY error_count DESC\nLIMIT 20",
  },
  {
    label: 'Browse logs (sample)',
    sql: "SELECT *\nFROM dfs.logs.`*.drilllog`\nLIMIT 50",
  },
];

interface LogFilesTabProps {
  onAnalyzeWithAi?: (prompt: string, lines: string[], fileName: string) => void;
  aiAvailable: boolean;
}

function LogFilesTab({ onAnalyzeWithAi, aiAvailable }: LogFilesTabProps) {
  const [search, setSearch] = useState('');
  const [selectedLog, setSelectedLog] = useState<string | null>(null);
  const [logSearch, setLogSearch] = useState('');
  const logContentRef = useRef<HTMLPreElement>(null);

  const { data: logFiles, isLoading, error: logFilesError, refetch } = useQuery({
    queryKey: ['logs'],
    queryFn: getLogFiles,
  });

  const { data: logContent, isLoading: loadingContent } = useQuery({
    queryKey: ['log-content', selectedLog],
    queryFn: () => getLogContent(selectedLog!),
    enabled: !!selectedLog,
  });

  useEffect(() => {
    if (logContent && logContentRef.current) {
      logContentRef.current.scrollTop = logContentRef.current.scrollHeight;
    }
  }, [logContent]);

  const filtered = useMemo(() => {
    if (!logFiles) {
      return [];
    }
    if (!search) {
      return logFiles;
    }
    const q = search.toLowerCase();
    return logFiles.filter((f) => f.name.toLowerCase().includes(q));
  }, [logFiles, search]);

  const filteredLines = useMemo(() => {
    if (!logContent?.lines) {
      return [];
    }
    if (!logSearch) {
      return logContent.lines.map((line, i) => ({ line, num: i + 1 }));
    }
    const q = logSearch.toLowerCase();
    return logContent.lines
      .map((line, i) => ({ line, num: i + 1 }))
      .filter((entry) => entry.line.toLowerCase().includes(q));
  }, [logContent, logSearch]);

  const handleAnalyzeAll = useCallback(() => {
    if (!onAnalyzeWithAi || !logContent || !selectedLog) {
      return;
    }
    const lines = logContent.lines.slice(-200);
    onAnalyzeWithAi(
      'Analyze these log lines. Identify any errors, warnings, or issues. Suggest potential root causes and fixes.',
      lines,
      selectedLog,
    );
  }, [onAnalyzeWithAi, logContent, selectedLog]);

  const handleAskAboutError = useCallback(
    (lineNum: number, line: string) => {
      if (!onAnalyzeWithAi || !logContent || !selectedLog) {
        return;
      }
      // Grab context: 5 lines before and after the error
      const startIdx = Math.max(0, lineNum - 6);
      const endIdx = Math.min(logContent.lines.length, lineNum + 5);
      const contextLines = logContent.lines.slice(startIdx, endIdx);
      onAnalyzeWithAi(
        `Explain this error and suggest how to fix it:\n\n${line}`,
        contextLines,
        selectedLog,
      );
    },
    [onAnalyzeWithAi, logContent, selectedLog],
  );

  const getLogTypeColor = (name: string): string => {
    if (name.endsWith('.log')) {
      return 'blue';
    }
    if (name.endsWith('.out')) {
      return 'green';
    }
    if (name.endsWith('.gc')) {
      return 'orange';
    }
    return 'default';
  };

  const columns: ColumnsType<LogFile> = [
    {
      title: 'Log File',
      dataIndex: 'name',
      key: 'name',
      sorter: (a, b) => a.name.localeCompare(b.name),
      defaultSortOrder: 'ascend',
      render: (name: string) => (
        <Space>
          <FileTextOutlined />
          <Button
            type="link"
            size="small"
            style={{ padding: 0 }}
            onClick={() => setSelectedLog(name)}
          >
            {name}
          </Button>
          <Tag color={getLogTypeColor(name)} style={{ fontSize: 10 }}>
            {name.split('.').pop()?.toUpperCase()}
          </Tag>
        </Space>
      ),
    },
    {
      title: 'Size',
      dataIndex: 'size',
      key: 'size',
      width: 140,
      render: (size: string) => {
        const kb = parseFloat(size);
        const isLarge = kb > 10240;
        return (
          <Space size={4}>
            <Text type="secondary" style={{ fontSize: 13 }}>{size}</Text>
            {isLarge && (
              <Tooltip title="Large file — only the last lines are shown in the viewer. Use download for the full file.">
                <WarningOutlined style={{ color: 'var(--color-warning, #faad14)', fontSize: 12 }} />
              </Tooltip>
            )}
          </Space>
        );
      },
    },
    {
      title: 'Last Modified',
      dataIndex: 'lastModified',
      key: 'lastModified',
      width: 200,
      render: (date: string) => (
        <Text type="secondary" style={{ fontSize: 13 }}>{date}</Text>
      ),
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 120,
      render: (_, record) => (
        <Space>
          <Tooltip title="View log">
            <Button
              type="text"
              size="small"
              icon={<FileTextOutlined />}
              onClick={() => setSelectedLog(record.name)}
            />
          </Tooltip>
          <Tooltip title="Download full log">
            <Button
              type="text"
              size="small"
              icon={<DownloadOutlined />}
              href={getLogDownloadUrl(record.name)}
            />
          </Tooltip>
        </Space>
      ),
    },
  ];

  return (
    <>
      <div style={{ marginBottom: 16 }}>
        <Space>
          <Input
            placeholder="Filter log files..."
            prefix={<SearchOutlined />}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            allowClear
            style={{ width: 300 }}
          />
          <Button icon={<ReloadOutlined />} onClick={() => refetch()}>
            Refresh
          </Button>
        </Space>
      </div>

      {logFilesError ? (
        <Alert
          type="warning"
          showIcon
          icon={<WarningOutlined />}
          message="Unable to load log files"
          description="DRILL_LOG_DIR may not be configured. Set the environment variable and restart Drill to view logs."
          style={{ marginBottom: 16 }}
        />
      ) : isLoading ? (
        <div style={{ textAlign: 'center', padding: 60 }}>
          <Spin tip="Loading log files..." />
        </div>
      ) : (
        <Table
          dataSource={filtered}
          columns={columns}
          rowKey="name"
          size="small"
          pagination={false}
          locale={{ emptyText: <Empty description="No log files found" /> }}
        />
      )}

      <Drawer
        title={
          <Space>
            <FileTextOutlined />
            <span>{selectedLog}</span>
            {logContent && (
              <Tag color="blue" style={{ fontSize: 10 }}>
                last {logContent.maxLines.toLocaleString()} lines
              </Tag>
            )}
          </Space>
        }
        placement="right"
        open={!!selectedLog}
        onClose={() => {
          setSelectedLog(null);
          setLogSearch('');
        }}
        width="70%"
        extra={
          <Space>
            <Input
              placeholder="Search in log..."
              prefix={<SearchOutlined />}
              value={logSearch}
              onChange={(e) => setLogSearch(e.target.value)}
              allowClear
              size="small"
              style={{ width: 200 }}
            />
            {aiAvailable && (
              <Tooltip title="Analyze with Prospector AI">
                <Button
                  size="small"
                  icon={<RobotOutlined />}
                  onClick={handleAnalyzeAll}
                  type="primary"
                  ghost
                >
                  Analyze
                </Button>
              </Tooltip>
            )}
            <Tooltip title="Scroll to top">
              <Button size="small" icon={<ArrowUpOutlined />} onClick={() => {
                if (logContentRef.current) {
                  logContentRef.current.scrollTop = 0;
                }
              }} />
            </Tooltip>
            <Tooltip title="Scroll to bottom">
              <Button size="small" icon={<ArrowDownOutlined />} onClick={() => {
                if (logContentRef.current) {
                  logContentRef.current.scrollTop = logContentRef.current.scrollHeight;
                }
              }} />
            </Tooltip>
            {selectedLog && (
              <Button
                size="small"
                icon={<DownloadOutlined />}
                href={getLogDownloadUrl(selectedLog)}
              >
                Download
              </Button>
            )}
          </Space>
        }
      >
        {loadingContent ? (
          <div style={{ textAlign: 'center', padding: 60 }}>
            <Spin tip="Loading log content..." />
          </div>
        ) : logContent && filteredLines.length > 0 ? (
          <pre
            ref={logContentRef}
            style={{
              fontSize: 12,
              lineHeight: 1.6,
              fontFamily: "'SF Mono', 'Fira Code', 'Cascadia Code', monospace",
              background: 'var(--color-bg-elevated)',
              border: '1px solid var(--color-border)',
              borderRadius: 6,
              padding: 16,
              margin: 0,
              overflow: 'auto',
              height: 'calc(100vh - 160px)',
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-all',
            }}
          >
            {filteredLines.map((entry) => {
              const isError = /\bERROR\b/.test(entry.line);
              const isWarn = /\bWARN\b/.test(entry.line);
              const color = isError
                ? 'var(--color-error, #ff4d4f)'
                : isWarn
                  ? 'var(--color-warning, #faad14)'
                  : undefined;
              return (
                <div
                  key={entry.num}
                  style={{ color, cursor: isError && aiAvailable ? 'pointer' : undefined }}
                  onClick={isError && aiAvailable ? () => handleAskAboutError(entry.num, entry.line) : undefined}
                  title={isError && aiAvailable ? 'Click to ask Prospector about this error' : undefined}
                >
                  <span style={{ color: 'var(--color-text-quaternary)', userSelect: 'none', marginRight: 12, display: 'inline-block', width: 48, textAlign: 'right' }}>
                    {entry.num}
                  </span>
                  {entry.line}
                  {isError && aiAvailable && (
                    <RobotOutlined style={{ marginLeft: 8, fontSize: 11, opacity: 0.5 }} />
                  )}
                </div>
              );
            })}
          </pre>
        ) : (
          <Empty description={logSearch ? 'No matching lines' : 'Log is empty'} />
        )}
      </Drawer>
    </>
  );
}

function SqlQueryTab() {
  const [sql, setSql] = useState(EXAMPLE_QUERIES[0].sql);
  const [results, setResults] = useState<Record<string, unknown>[] | null>(null);
  const [resultColumns, setResultColumns] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const queryClient = useQueryClient();

  const { data: sqlStatus, isLoading: loadingStatus } = useQuery({
    queryKey: ['logs-sql-status'],
    queryFn: getLogSqlStatus,
  });

  const setupMutation = useMutation({
    mutationFn: setupLogSql,
    onSuccess: (data) => {
      if (data.success) {
        message.success(data.message);
        queryClient.invalidateQueries({ queryKey: ['logs-sql-status'] });
      } else {
        message.error(data.message);
      }
    },
    onError: (err: Error) => {
      message.error(`Setup failed: ${err.message}`);
    },
  });

  const [running, setRunning] = useState(false);

  const handleRun = useCallback(async () => {
    setRunning(true);
    setError(null);
    setResults(null);
    try {
      const result = await executeQuery({ query: sql, queryType: 'SQL' });
      setResultColumns(result.columns || []);
      setResults(result.rows || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Query failed');
    } finally {
      setRunning(false);
    }
  }, [sql]);

  if (loadingStatus) {
    return (
      <div style={{ textAlign: 'center', padding: 60 }}>
        <Spin tip="Checking log SQL configuration..." />
      </div>
    );
  }

  const isReady = sqlStatus?.ready;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {!isReady && (
        <Alert
          type={sqlStatus?.logDirConfigured ? 'info' : 'warning'}
          showIcon
          icon={<SettingOutlined />}
          message={
            !sqlStatus?.logDirConfigured
              ? 'DRILL_LOG_DIR is not set — cannot configure log SQL workspace'
              : 'Log SQL workspace needs to be configured'
          }
          description={
            sqlStatus?.logDirConfigured
              ? `Click Setup to create the dfs.logs workspace pointing to ${sqlStatus.logDir} and configure the log format parser.`
              : 'Set the DRILL_LOG_DIR environment variable and restart Drill to enable log querying.'
          }
          action={
            sqlStatus?.logDirConfigured ? (
              <Button
                type="primary"
                size="small"
                icon={<SettingOutlined />}
                onClick={() => setupMutation.mutate()}
                loading={setupMutation.isPending}
              >
                Setup
              </Button>
            ) : undefined
          }
        />
      )}

      {isReady && (
        <Alert
          type="success"
          showIcon
          icon={<CheckCircleOutlined />}
          message={`Log SQL workspace is ready — querying ${sqlStatus.logDir}`}
          closable
        />
      )}

      <div>
        <div style={{ marginBottom: 8 }}>
          <Space wrap>
            <Text strong style={{ fontSize: 13 }}>Example Queries:</Text>
            {EXAMPLE_QUERIES.map((eq) => (
              <Button
                key={eq.label}
                size="small"
                type={sql === eq.sql ? 'primary' : 'default'}
                onClick={() => setSql(eq.sql)}
              >
                {eq.label}
              </Button>
            ))}
          </Space>
        </div>

        <TextArea
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          rows={6}
          style={{
            fontFamily: "'SF Mono', 'Fira Code', 'Cascadia Code', monospace",
            fontSize: 13,
          }}
        />

        <div style={{ marginTop: 8 }}>
          <Button
            type="primary"
            icon={<PlayCircleOutlined />}
            onClick={handleRun}
            loading={running}
            disabled={!isReady || !sql.trim()}
          >
            Run Query
          </Button>
        </div>
      </div>

      {error && (
        <Alert type="error" message="Query Error" description={error} showIcon closable />
      )}

      {results && (
        <div>
          <Text type="secondary" style={{ fontSize: 12, marginBottom: 8, display: 'block' }}>
            {results.length} row{results.length !== 1 ? 's' : ''} returned
          </Text>
          <Table
            dataSource={results}
            columns={resultColumns.map((col) => ({
              title: col,
              dataIndex: col,
              key: col,
              ellipsis: true,
              render: (val: unknown) => (
                <Text style={{ fontSize: 12 }}>
                  {val === null ? <Text type="secondary" italic>null</Text> : String(val)}
                </Text>
              ),
            }))}
            rowKey={(_, i) => String(i)}
            size="small"
            scroll={{ x: 'max-content' }}
            pagination={{ pageSize: 50, showSizeChanger: true, pageSizeOptions: ['25', '50', '100'] }}
          />
        </div>
      )}
    </div>
  );
}

export default function LogsPage() {
  const [prospectorOpen, setProspectorOpen] = useState(false);
  const [aiAvailable, setAiAvailable] = useState(false);
  const [logContext, setLogContext] = useState<ChatContext>({ logAnalysisMode: true });

  const prospector = useProspector();

  useEffect(() => {
    getAiStatus()
      .then((status) => setAiAvailable(status.enabled))
      .catch(() => setAiAvailable(false));
  }, []);

  const handleAnalyzeWithAi = useCallback(
    (prompt: string, lines: string[], fileName: string) => {
      const ctx: ChatContext = {
        logAnalysisMode: true,
        logFileName: fileName,
        logLines: lines,
      };
      setLogContext(ctx);
      if (!prospectorOpen) {
        setProspectorOpen(true);
      }
      prospector.sendMessage(prompt, ctx);
    },
    [prospectorOpen, prospector],
  );

  const toggleProspector = useCallback(() => {
    setProspectorOpen((prev) => !prev);
  }, []);

  return (
    <div style={{ display: 'flex', height: '100%', overflow: 'hidden' }}>
      <div style={{ flex: 1, padding: 24, overflow: 'auto' }}>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Space>
            <Link to="/projects/system-drill-logs">
              <Button icon={<FolderOutlined />} size="small">
                Drill Logs Project
              </Button>
            </Link>
          </Space>
          {aiAvailable && (
            <Button
              icon={<RobotOutlined />}
              type={prospectorOpen ? 'primary' : 'default'}
              onClick={toggleProspector}
              size="small"
            >
              Prospector
            </Button>
          )}
        </div>

        <Tabs
          defaultActiveKey="files"
          items={[
            {
              key: 'files',
              label: (
                <span>
                  <FileTextOutlined />
                  {' Log Files'}
                </span>
              ),
              children: (
                <LogFilesTab
                  onAnalyzeWithAi={aiAvailable ? handleAnalyzeWithAi : undefined}
                  aiAvailable={aiAvailable}
                />
              ),
            },
            {
              key: 'sql',
              label: (
                <span>
                  <DatabaseOutlined />
                  {' SQL Query'}
                </span>
              ),
              children: <SqlQueryTab />,
            },
          ]}
        />
      </div>

      {aiAvailable && prospectorOpen && (
        <div
          style={{
            width: 380,
            borderLeft: '1px solid var(--color-border)',
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden',
          }}
        >
          <ProspectorPanel
            prospector={prospector}
            context={logContext}
          />
        </div>
      )}
    </div>
  );
}
