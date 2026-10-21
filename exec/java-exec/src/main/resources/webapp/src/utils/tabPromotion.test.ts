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
import { describe, it, expect } from 'vitest';
import { shouldPromote } from './tabPromotion';
import type { PromotionInput } from './tabPromotion';

const base: PromotionInput = {
  hasExecuted: false,
  isClosing: false,
  sql: '',
  conversationLength: 0,
  alreadyPromoted: false,
};

describe('shouldPromote', () => {
  it('promotes once a query has been executed', () => {
    expect(shouldPromote({ ...base, hasExecuted: true })).toBe(true);
  });

  // A failed query is still work the user will want back in order to fix it.
  it('does not care whether the execution succeeded', () => {
    expect(shouldPromote({ ...base, hasExecuted: true, sql: 'SELCT 1' })).toBe(true);
  });

  it('promotes an executed tab even with an empty editor', () => {
    expect(shouldPromote({ ...base, hasExecuted: true, sql: '' })).toBe(true);
  });

  it('does not promote an open, unexecuted tab', () => {
    expect(shouldPromote({ ...base, sql: 'SELECT 1' })).toBe(false);
  });

  it('promotes on close when SQL is present', () => {
    expect(shouldPromote({ ...base, isClosing: true, sql: 'SELECT 1' })).toBe(true);
  });

  // The editor is only half a tab's content; a conversation alone is worth keeping.
  it('promotes on close when only a conversation is present', () => {
    expect(shouldPromote({ ...base, isClosing: true, conversationLength: 4 })).toBe(true);
  });

  it('treats whitespace-only SQL as empty', () => {
    expect(shouldPromote({ ...base, isClosing: true, sql: '   \n  ' })).toBe(false);
  });

  // Keeps a stray "New Query" click from leaving a permanent Query 7 in the tree.
  it('promotes nothing for an empty tab closed with no content', () => {
    expect(shouldPromote({ ...base, isClosing: true })).toBe(false);
  });

  it('does not re-promote a tab already on the server', () => {
    expect(shouldPromote({ ...base, hasExecuted: true, alreadyPromoted: true })).toBe(false);
  });

  it('does not re-promote on close either', () => {
    expect(shouldPromote({
      ...base, isClosing: true, sql: 'SELECT 1', alreadyPromoted: true,
    })).toBe(false);
  });
});
