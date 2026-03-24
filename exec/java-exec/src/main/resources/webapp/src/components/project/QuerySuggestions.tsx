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
import { useState, useEffect, useRef } from 'react';
import { Card, Row, Col, Button, Typography, Spin, Empty, Tooltip } from 'antd';
import { BulbOutlined, ReloadOutlined, PlayCircleOutlined } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { format } from 'sql-formatter';
import { getAiStatus, streamChat } from '../../api/ai';
import { getSchemaTree } from '../../api/metadata';
import { aiObservability } from '../../services/aiObservability';
import type { DatasetRef, Project } from '../../types';
import type { ChatMessage, DeltaEvent } from '../../types/ai';

const { Text, Paragraph } = Typography;

interface QuerySuggestion {
  title: string;
  description: string;
  sql: string;
}

interface QuerySuggestionsProps {
  projectId: string;
  datasets: DatasetRef[];
  savedQueryCount: number;
  project?: Project;
  onSelectSql: (sql: string, title?: string) => void;
}

function getCacheKey(projectId: string): string {
  return `drill.querySuggestions.${projectId}`;
}

function loadCachedSuggestions(projectId: string): QuerySuggestion[] | null {
  try {
    const raw = localStorage.getItem(getCacheKey(projectId));
    if (raw) {
      return JSON.parse(raw) as QuerySuggestion[];
    }
  } catch {
    // Ignore corrupt cache
  }
  return null;
}

function saveCachedSuggestions(projectId: string, suggestions: QuerySuggestion[]): void {
  try {
    localStorage.setItem(getCacheKey(projectId), JSON.stringify(suggestions));
  } catch {
    // Ignore storage errors
  }
}

/**
 * Parse JSON from an AI response that may be wrapped in markdown code blocks.
 */
function parseJsonFromResponse(text: string): QuerySuggestion[] {
  if (!text || text.trim().length === 0) {
    throw new Error('Empty response from AI');
  }

  // Try to extract JSON from markdown code blocks first
  const codeBlockMatch = text.match(/```(?:json)?\s*\n?([\s\S]*?)\n?\s*```/);
  let jsonStr = codeBlockMatch ? codeBlockMatch[1].trim() : text.trim();

  // If no valid JSON array found in code block, try to find it in the text
  if (!jsonStr.startsWith('[')) {
    const arrayMatch = jsonStr.match(/\[\s*\{[\s\S]*?\}\s*\]/);
    if (arrayMatch) {
      jsonStr = arrayMatch[0];
    } else {
      throw new Error('No JSON array found in response');
    }
  }

  let parsed;
  try {
    parsed = JSON.parse(jsonStr);
  } catch (err) {
    // Try to fix common issues with incomplete JSON
    if (jsonStr.endsWith(',')) {
      jsonStr = jsonStr.slice(0, -1) + ']';
      parsed = JSON.parse(jsonStr);
    } else {
      throw new Error(`Invalid JSON: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  if (!Array.isArray(parsed)) {
    throw new Error('Expected a JSON array');
  }

  // Validate and clean up the parsed data
  const suggestions = parsed
    .map((item: Record<string, unknown>) => {
      const title = String(item.title || '').trim();
      const description = String(item.description || '').trim();
      const sql = String(item.sql || '').trim();

      // Skip invalid suggestions but don't fail
      if (!title || !description || !sql) {
        return null;
      }

      return { title, description, sql };
    })
    .filter((s) => s !== null);

  if (suggestions.length === 0) {
    throw new Error('AI could not generate valid suggestions. Try a different query or check your available tables.');
  }

  return suggestions;
}

interface SchemaTreeEntry {
  name: string;
  tables: Array<{
    name: string;
    columns: string[];
  }>;
}

function buildPrompt(
  datasets: DatasetRef[],
  savedQueryCount: number,
  schemaInfo?: SchemaTreeEntry[],
  project?: Project
): string {
  // Separate datasets by type
  const tables = datasets.filter(ds => ds.type === 'table');
  const schemas = datasets.filter(ds => ds.type === 'schema');
  const savedQueries = datasets.filter(ds => ds.type === 'saved_query');

  // Build list of ALL valid fully-qualified table names
  let allValidTables: string[] = [];
  let tableDescriptions = '';

  if (schemaInfo && schemaInfo.length > 0) {
    tableDescriptions = schemaInfo.map(schema => {
      const schemaTables = schema.tables.map(table => {
        const fullTableName = `${schema.name}.${table.name}`;
        allValidTables.push(fullTableName);
        const columnList = table.columns.slice(0, 15).join(', '); // Show first 15 columns
        const moreColsText = table.columns.length > 15 ? `, ... (${table.columns.length - 15} more)` : '';
        return `  - ${fullTableName} (columns: ${columnList}${moreColsText})`;
      }).join('\n');
      return schemaTables;
    }).join('\n');
  } else {
    tableDescriptions = tables.map((ds) => {
      const fullName = ds.schema ? `${ds.schema}.${ds.table || ds.label}` : (ds.table || ds.label);
      allValidTables.push(fullName);
      return `  - ${fullName}`;
    }).join('\n');
  }

  const schemaDescriptions = schemas.map((ds) => {
    return `  - ${ds.schema || ds.label}`;
  }).join('\n');

  const savedQueryDescriptions = savedQueries.map((ds) => {
    return `  - ${ds.label}`;
  }).join('\n');

  // Create a strict list of valid table references
  const validTablesSection = allValidTables.length > 0
    ? `\n\nVALID TABLE REFERENCES (COPY EXACTLY):\n${allValidTables.map(t => `  ${t}`).join('\n')}\n`
    : '';

  // Build project context section
  const projectContext = project
    ? `PROJECT CONTEXT:
Project Name: ${project.name}
${project.description ? `Project Description: ${project.description}\n` : ''}`
    : '';

  return `You are an Apache Drill SQL expert. Generate ONLY practical SQL queries using EXACTLY these available data sources.

${projectContext}

AVAILABLE TABLES AND COLUMNS:
${tableDescriptions}
${validTablesSection}
${schemas.length > 0 ? `AVAILABLE SCHEMAS:\n${schemaDescriptions}\n` : ''}${savedQueries.length > 0 ? `AVAILABLE SAVED QUERIES:\n${savedQueryDescriptions}\n` : ''}${savedQueryCount > 0 ? `Note: The project also has ${savedQueryCount} other saved queries available.\n\n` : ''}
========== ABSOLUTE REQUIREMENTS ==========
1. EVERY table name in your queries MUST be copied from the "VALID TABLE REFERENCES" section above
2. NEVER invent or guess table names
3. NEVER reference ANY tables not listed in "VALID TABLE REFERENCES"
4. NEVER reference tables, columns, or schemas not listed above
5. NEVER suggest "List All Tables", "Show Schemas", or metadata queries
6. Use the EXACT fully qualified table names: schema.table format
7. If a column has spaces or special characters, wrap it in backticks: \`column name\`
8. Each query must be valid Apache Drill SQL and executable
9. Generate queries that provide real business insights
10. INVALID: Making up table names like "sales", "customers", "products" - ONLY use tables from the list

TASK: Generate 3-5 diverse SQL queries analyzing the available data.

For each query provide:
- title: A clear, specific title (max 50 chars)
- description: What insight it provides (1-2 sentences)
- sql: Valid Apache Drill SQL using ONLY tables from "VALID TABLE REFERENCES"

OUTPUT REQUIREMENTS:
- Return ONLY a JSON array
- NO markdown code blocks
- NO explanations before or after the JSON
- NO line breaks in the JSON - all on one line
- Every table must be from the valid list

Example JSON (ONE LINE):
[{"title": "Revenue by Product", "description": "Shows total revenue by product.", "sql": "SELECT product_id, SUM(amount) as total_revenue FROM ${allValidTables.length > 0 ? allValidTables[0] : 'schema.table'} GROUP BY product_id"}]`;
}

export default function QuerySuggestions({
  projectId,
  datasets,
  savedQueryCount,
  project,
  onSelectSql,
}: QuerySuggestionsProps) {
  const [suggestions, setSuggestions] = useState<QuerySuggestion[] | null>(null);
  const [isGenerating, setIsGenerating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const { data: aiStatus } = useQuery({
    queryKey: ['aiStatus'],
    queryFn: getAiStatus,
    staleTime: 60_000,
  });

  // Load cached suggestions on mount
  useEffect(() => {
    const cached = loadCachedSuggestions(projectId);
    if (cached) {
      setSuggestions(cached);
    }
  }, [projectId]);

  // Cleanup abort controller on unmount
  useEffect(() => {
    return () => {
      if (abortRef.current) {
        abortRef.current.abort();
      }
    };
  }, []);

  const handleGenerate = async () => {
    if (abortRef.current) {
      abortRef.current.abort();
    }

    setIsGenerating(true);
    setError(null);
    setSuggestions(null);

    try {
      // Fetch schema tree to include actual table structures
      // Extract schema names from both 'schema' type datasets and 'table' type datasets
      const schemaNames = Array.from(new Set(
        datasets
          .filter(ds => (ds.type === 'schema') || (ds.type === 'table' && ds.schema))
          .map(ds => ds.type === 'schema' ? (ds.schema || ds.label) : ds.schema!)
          .filter(name => name && name.length > 0)
      ));

      console.log('Available schemas:', schemaNames);
      console.log('Total datasets:', datasets.length);
      console.log('Dataset types:', datasets.map(ds => `${ds.label} (${ds.type})`));

      let schemaInfo: SchemaTreeEntry[] | undefined;
      if (schemaNames.length > 0) {
        try {
          console.log('🔍 Calling getSchemaTree with schemas:', schemaNames);
          schemaInfo = await getSchemaTree(schemaNames);
          console.log('📊 Fetched schema tree:', schemaInfo);

          if (!schemaInfo || schemaInfo.length === 0) {
            console.warn('⚠️ Schema tree API returned empty! Endpoint may not be implemented.');
            console.warn('   Request: POST /api/v1/metadata/schema-tree with', { schemas: schemaNames });
            console.log('   ℹ️  To fix this, ensure the backend has the schema-tree endpoint implemented.');
            console.log('   ℹ️  Or add explicit table configurations to your project.');
          } else {
            console.log(`✅ Found ${schemaInfo.length} schemas with tables`);
            schemaInfo.forEach(s => {
              console.log(`   └─ ${s.name}: ${s.tables.length} tables`);
              s.tables.forEach(t => {
                console.log(`      • ${s.name}.${t.name} (${t.columns.length} columns)`);
              });
            });
          }
        } catch (err) {
          console.error('❌ Failed to fetch schema tree:', err);
          console.log('Tip: The /api/v1/metadata/schema-tree endpoint may not be implemented.');
        }
      } else {
        console.warn('⚠️ No schemas found in datasets');
      }

      // If schema tree is empty, add a warning to the prompt
      if (!schemaInfo || schemaInfo.length === 0) {
        console.error('');
        console.error('❌ CRITICAL ISSUE: Cannot generate valid query suggestions without table information!');
        console.error('');
        console.error('The schema tree API is not returning table data. This causes the AI to hallucinate columns.');
        console.error('');
        console.error('TO FIX THIS:');
        console.error('1. Check that your backend has the /api/v1/metadata/schema-tree endpoint');
        console.error('2. Verify it returns tables for the "mysql.store" schema');
        console.error('3. Or: Add explicit table definitions to your project configuration');
        console.error('');
      }

      const prompt = buildPrompt(datasets, savedQueryCount, schemaInfo, project);

      // Debug logging
      console.group('🔍 Query Suggestions - Prompt Details');
      console.log('Schema Info Available:', schemaInfo ? 'YES' : 'NO');
      if (schemaInfo) {
        console.log('Schema Count:', schemaInfo.length);
        schemaInfo.forEach((schema, idx) => {
          console.log(`  [${idx}] ${schema.name}: ${schema.tables.length} tables`);
          schema.tables.forEach(table => {
            console.log(`    - ${schema.name}.${table.name} (${table.columns.length} columns)`);
          });
        });
      }
      console.log('Project Context:', project ? `${project.name}` : 'NONE');
      console.log('Datasets Count:', datasets.length);
      console.log('Full Prompt (first 1000 chars):', prompt.substring(0, 1000));
      console.groupEnd();

      const messages: ChatMessage[] = [
        { role: 'user', content: prompt },
      ];

      let accumulated = '';
      const startTime = Date.now();

      abortRef.current = streamChat(
        { messages, tools: [], context: {} },
        (event: DeltaEvent) => {
          if (event.type === 'content') {
            accumulated += event.content;
          }
        },
        () => {
          // Done
          const duration = Date.now() - startTime;
          try {
            const parsed = parseJsonFromResponse(accumulated);
            setSuggestions(parsed);
            saveCachedSuggestions(projectId, parsed);
            setError(null);

            // Log successful AI call
            aiObservability.logAICall('query_suggestions', prompt, accumulated, duration);
          } catch (err) {
            const errorMsg = err instanceof Error ? err.message : 'Unknown error';
            console.error('Failed to parse AI response:', errorMsg);
            console.log('AI response was:', accumulated);
            setError(`Failed to parse suggestions: ${errorMsg}`);

            // Log failed AI call
            aiObservability.logAICall('query_suggestions', prompt, accumulated, duration, errorMsg);
          }
          setIsGenerating(false);
        },
        (err) => {
          const duration = Date.now() - startTime;
          setError(err.message || 'Failed to generate suggestions.');

          // Log error
          aiObservability.logAICall('query_suggestions', prompt, '', duration, err.message);
          setIsGenerating(false);
        },
      );
    } catch (err) {
      setError('Failed to generate suggestions.');
      setIsGenerating(false);
    }
  };

  // Don't render if AI is not enabled/configured
  if (!aiStatus?.enabled || !aiStatus?.configured) {
    return null;
  }

  const renderContent = () => {
    if (isGenerating) {
      return (
        <div style={{ textAlign: 'center', padding: '24px 0' }}>
          <Spin size="large" />
          <div style={{ marginTop: 12 }}>
            <Text type="secondary">Generating query suggestions...</Text>
          </div>
        </div>
      );
    }

    if (error) {
      return (
        <Empty
          description={error}
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        >
          <Button type="primary" onClick={handleGenerate}>
            Try Again
          </Button>
        </Empty>
      );
    }

    if (!suggestions) {
      return (
        <div style={{ textAlign: 'center', padding: '24px 0' }}>
          <Paragraph type="secondary">
            Let AI analyze your project datasets and suggest useful queries.
          </Paragraph>
          <Button
            type="primary"
            icon={<BulbOutlined />}
            onClick={handleGenerate}
          >
            Generate Suggestions
          </Button>
        </div>
      );
    }

    if (suggestions.length === 0) {
      return (
        <Empty description="No suggestions generated.">
          <Button onClick={handleGenerate} icon={<ReloadOutlined />}>
            Retry
          </Button>
        </Empty>
      );
    }

    return (
      <div>
        <div style={{ marginBottom: 12, textAlign: 'right' }}>
          <Tooltip title="Regenerate suggestions">
            <Button
              size="small"
              icon={<ReloadOutlined />}
              onClick={handleGenerate}
            >
              Refresh
            </Button>
          </Tooltip>
        </div>
        <Row gutter={[12, 12]}>
          {suggestions.map((suggestion, index) => {
            const formattedSql = format(suggestion.sql, { language: 'sql' });
            return (
              <Col key={index} xs={24} sm={12} lg={8}>
                <Card
                  size="small"
                  title={<Text strong>{suggestion.title}</Text>}
                  actions={[
                    <Button
                      key="use"
                      type="link"
                      icon={<PlayCircleOutlined />}
                      onClick={() => onSelectSql(formattedSql, suggestion.title)}
                    >
                      Use This Query
                    </Button>,
                  ]}
                >
                  <Paragraph
                    type="secondary"
                    ellipsis={{ rows: 2 }}
                    style={{ marginBottom: 8 }}
                  >
                    {suggestion.description}
                  </Paragraph>
                  <pre
                    style={{
                      fontSize: 11,
                      background: '#f5f5f5',
                      padding: 8,
                      borderRadius: 4,
                      maxHeight: 80,
                      overflow: 'hidden',
                      whiteSpace: 'pre-wrap',
                      wordBreak: 'break-all',
                      margin: 0,
                    }}
                  >
                    {formattedSql}
                  </pre>
                </Card>
              </Col>
            );
          })}
        </Row>
      </div>
    );
  };

  return (
    <div style={{ marginBottom: 12 }}>
      <div style={{ marginBottom: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
        <BulbOutlined style={{ fontSize: 16 }} />
        <Text strong style={{ fontSize: 16 }}>AI Query Suggestions</Text>
      </div>
      {renderContent()}
    </div>
  );
}
