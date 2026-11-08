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
import type { ChatMessage, ToolCall } from '../../types/ai';
import type { ArtifactKind } from './ArtifactCanvas';
import type { CanvasTarget } from './CanvasHost';

/** A durable artifact Prospector created, as the thread records it. */
export interface CreatedArtifact {
  kind: ArtifactKind;
  id: string;
  name: string;
}

/**
 * The tools whose result is a durable project artifact, and what kind it is. execute_sql is
 * deliberately absent: an exploratory query is not an artifact, and a card for every
 * intermediate one would bury the real ones. Its results still reach the canvas, through the
 * hook's onQueryResult. A report is a wiki page, so save_report is a wiki artifact.
 */
const ARTIFACT_TOOLS: Record<string, ArtifactKind> = {
  create_visualization: 'chart',
  create_dashboard: 'dashboard',
  save_query: 'query',
  save_report: 'wiki',
};

/**
 * The artifact a tool call created, read from its result in the thread, or null when the
 * call is not an artifact tool, has no result yet, failed, or returned no id. The tools
 * already return `id` and `name` (save_report returns `title`), so nothing is added to them.
 */
export function createdArtifact(call: ToolCall, result: ChatMessage | undefined): CreatedArtifact | null {
  const kind = ARTIFACT_TOOLS[call.name];
  if (!kind || !result?.content) {
    return null;
  }
  let body: unknown;
  try {
    body = JSON.parse(result.content);
  } catch {
    return null;
  }
  if (!body || typeof body !== 'object') {
    return null;
  }
  const { id, name, title, error } = body as Record<string, unknown>;
  if (error || typeof id !== 'string' || id === '') {
    return null;
  }
  const label = [name, title].find((v): v is string => typeof v === 'string' && v.trim() !== '');
  return { kind, id, name: label ?? id };
}

/**
 * What the latest turn created, in order: everything after the last user message. Read from
 * the thread rather than held as separate state, so it is right for a conversation loaded
 * from the server as much as for one streaming now.
 */
export function artifactsOfLastTurn(messages: ChatMessage[]): CreatedArtifact[] {
  let start = messages.length - 1;
  while (start >= 0 && messages[start].role !== 'user') {
    start -= 1;
  }
  const turn = messages.slice(start + 1);
  const created: CreatedArtifact[] = [];
  turn.forEach((m) => {
    m.toolCalls?.forEach((tc) => {
      const artifact = createdArtifact(tc, turn.find((r) => r.role === 'tool' && r.toolCallId === tc.id));
      if (artifact) {
        created.push(artifact);
      }
    });
  });
  return created;
}

/**
 * Where the canvas goes when a turn ends: the last artifact the turn created, so a turn that
 * builds several settles once on the last rather than stepping through each; or wherever it
 * already is when the turn created nothing. There is no pinned case here. A pin holds the
 * view in CanvasHost, which catches up to this target when it is released, the same as it
 * does for a query result that arrived while pinned.
 */
export function nextCanvasTarget(
  turnArtifacts: CanvasTarget[],
  current: CanvasTarget | null,
): CanvasTarget | null {
  return turnArtifacts.length === 0 ? current : turnArtifacts[turnArtifacts.length - 1];
}
