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

/** Everything the promotion decision depends on, gathered by the caller. */
export interface PromotionInput {
  /** A query has been run from this tab, whether it succeeded or failed. */
  hasExecuted: boolean;
  /** The tab is being closed right now. */
  isClosing: boolean;
  sql: string;
  /** Prospector messages belonging to this tab. */
  conversationLength: number;
  /** The tab already has a server-side record. */
  alreadyPromoted: boolean;
}

/**
 * Decides whether a tab should be written to the server.
 *
 * Tabs live in localStorage from the first keystroke; only promoted tabs reach the
 * server, and only promoted tabs appear in the project tree. The two triggers keep
 * this invariant true:
 *
 *   every tab is either currently open (visible in the tab strip)
 *   or in the project tree (recoverable)
 *
 * 1. **Executed** — running a query is the signal that a tab holds real work. A
 *    failed query counts: the user will want it back in order to fix it.
 * 2. **Closed holding content** — without this, typing SQL and closing without
 *    running would leave content in localStorage that the tree does not list and
 *    the user cannot reach.
 *
 * Content means SQL *or* a Prospector conversation. Checking only the editor would
 * silently discard a tab whose work happened entirely in the assistant panel.
 *
 * A tab closed with nothing in it promotes nothing, so a stray "New Query" click
 * does not leave a permanent `Query 7` in the tree.
 *
 * See docs/dev/TabPersistence.md.
 */
export function shouldPromote(input: PromotionInput): boolean {
  if (input.alreadyPromoted) {
    return false;
  }
  if (input.hasExecuted) {
    return true;
  }
  const hasContent = input.sql.trim().length > 0 || input.conversationLength > 0;
  return input.isClosing && hasContent;
}
