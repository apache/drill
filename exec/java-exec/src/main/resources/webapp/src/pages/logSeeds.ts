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
import type { ChatContext, ConversationSeed } from '../types/ai';

const ANALYZE_LINES = 200;
const ERROR_CONTEXT_LINES = 5;

function logContext(fileName: string, logLines: string[]): Partial<ChatContext> {
  return { feature: 'log_analysis', logAnalysisMode: true, logFileName: fileName, logLines };
}

/** Ask Prospector to look over the tail of a log file. */
export function analyzeSeed(fileName: string, lines: string[]): ConversationSeed {
  return {
    prompt: 'Analyze these log lines. Identify any errors, warnings, or issues. Suggest potential root causes and fixes.',
    context: logContext(fileName, lines.slice(-ANALYZE_LINES)),
  };
}

/** Ask Prospector about one line (1-based lineNum), with five lines of context either side. */
export function errorSeed(fileName: string, lines: string[], lineNum: number): ConversationSeed {
  const start = Math.max(0, lineNum - 1 - ERROR_CONTEXT_LINES);
  const end = Math.min(lines.length, lineNum + ERROR_CONTEXT_LINES);
  return {
    prompt: `Explain this error and suggest how to fix it:\n\n${lines[lineNum - 1]}`,
    context: logContext(fileName, lines.slice(start, end)),
  };
}
