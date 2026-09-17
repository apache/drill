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
import { describe, expect, it } from 'vitest';
import {
  looksLikeReport,
  reportTitle,
  queryAppendix,
  provenanceFooter,
  buildReportMarkdown,
} from './report';
import type { ChatMessage } from '../types/ai';

const REPORT = [
  '# Cyber Attack Analysis Report',
  '',
  '## Executive Summary',
  'A complete intrusion, start to finish.',
  '',
  '## Key Actors',
  '| Role | IP |',
  '| --- | --- |',
  '| Attacker | 98.114.205.102 |',
].join('\n');

describe('looksLikeReport', () => {
  it('accepts a headed document containing a table', () => {
    expect(looksLikeReport(REPORT)).toBe(true);
  });

  it('accepts a long headed document with no table', () => {
    const long = `# Findings\n\n## Detail\n\n${'word '.repeat(400)}`;
    expect(looksLikeReport(long)).toBe(true);
  });

  it('rejects a short answer with no headings', () => {
    expect(looksLikeReport('The table has 412 rows.')).toBe(false);
  });

  it('rejects a single-heading reply', () => {
    expect(looksLikeReport('# Result\n\nThree matching hosts.')).toBe(false);
  });

  it('rejects empty content', () => {
    expect(looksLikeReport('')).toBe(false);
  });
});

describe('reportTitle', () => {
  it('uses the first heading', () => {
    expect(reportTitle(REPORT)).toBe('Cyber Attack Analysis Report');
  });

  it('strips markdown emphasis from the heading', () => {
    expect(reportTitle('# **Q3** Review\n\ntext')).toBe('Q3 Review');
  });

  it('falls back to a timestamped title when there is no heading', () => {
    const now = new Date('2026-09-17T14:30:00Z');
    expect(reportTitle('just prose', now)).toBe('Prospector report 2026-09-17 14:30');
  });

  it('does not split an emoji at the length cap', () => {
    const title = reportTitle(`# ${'a'.repeat(119)}🚨`);
    expect([...title]).toHaveLength(120);
    expect(title.endsWith('🚨')).toBe(true);
  });
});

describe('queryAppendix', () => {
  const withSql = (sql: string): ChatMessage => ({
    role: 'assistant',
    content: null,
    toolCalls: [{ id: '1', name: 'execute_sql', arguments: JSON.stringify({ sql }) }],
  });

  it('collects executed SQL into a fenced appendix', () => {
    const out = queryAppendix([withSql('SELECT 1')]);
    expect(out).toContain('## Appendix: queries run');
    expect(out).toContain('```sql\nSELECT 1\n```');
  });

  it('de-duplicates repeated statements', () => {
    const out = queryAppendix([withSql('SELECT 1'), withSql('SELECT 1')]);
    expect(out.match(/SELECT 1/g)).toHaveLength(1);
  });

  it('returns nothing when no queries ran', () => {
    expect(queryAppendix([{ role: 'assistant', content: 'hello' }])).toBe('');
  });

  it('skips tool calls whose arguments are not valid JSON', () => {
    const broken: ChatMessage = {
      role: 'assistant',
      content: null,
      toolCalls: [{ id: '1', name: 'execute_sql', arguments: '{not json' }],
    };
    expect(queryAppendix([broken])).toBe('');
  });

  it('skips a tool call whose sql is not a string', () => {
    const bad: ChatMessage = {
      role: 'assistant',
      content: null,
      toolCalls: [{ id: '1', name: 'execute_sql', arguments: JSON.stringify({ sql: 123 }) }],
    };
    expect(queryAppendix([bad])).toBe('');
  });
});

describe('provenanceFooter', () => {
  it('names the model, provider and time', () => {
    const out = provenanceFooter({
      generatedAt: Date.UTC(2026, 8, 17, 14, 30, 0),
      model: 'claude-opus-5',
      provider: 'anthropic',
      conversationId: 'tab-7',
    });
    expect(out).toContain('anthropic / claude-opus-5');
    expect(out).toContain('2026-09-17 14:30:00');
    expect(out).toContain('tab-7');
  });

  it('still renders when the model is unknown', () => {
    const out = provenanceFooter({ generatedAt: Date.UTC(2026, 8, 17) });
    expect(out).toContain('an unspecified model');
  });

  it('degrades rather than throwing on an invalid timestamp', () => {
    expect(() => provenanceFooter({ generatedAt: NaN })).not.toThrow();
    expect(provenanceFooter({ generatedAt: NaN })).toContain('an unrecorded time');
  });
});

describe('buildReportMarkdown', () => {
  it('puts the appendix and the footer after the report body', () => {
    const msgs: ChatMessage[] = [{
      role: 'assistant',
      content: null,
      toolCalls: [{ id: '1', name: 'execute_sql', arguments: JSON.stringify({ sql: 'SELECT 1' }) }],
    }];
    const out = buildReportMarkdown(REPORT, msgs, { generatedAt: Date.UTC(2026, 8, 17) });
    expect(out.indexOf('Executive Summary')).toBeLessThan(out.indexOf('Appendix: queries run'));
    expect(out.indexOf('Appendix: queries run')).toBeLessThan(out.indexOf('Generated by Prospector'));
  });
});
