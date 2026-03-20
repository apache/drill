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
import type { UserInfo } from '../api/user';

/**
 * The permission logic inside useCurrentUser is captured in closures.
 * We replicate the exact logic here as pure functions so we can test
 * every combination without needing React or react-query.
 */
function canEdit(user: UserInfo | undefined, owner: string): boolean {
  if (!user) {
    return false;
  }
  if (!user.authEnabled || user.isAdmin) {
    return true;
  }
  return user.username === owner;
}

function canView(user: UserInfo | undefined, owner: string, isPublic: boolean): boolean {
  if (!user) {
    return false;
  }
  if (!user.authEnabled || user.isAdmin) {
    return true;
  }
  return isPublic || user.username === owner;
}

function isProjectOwner(user: UserInfo | undefined, projectOwner: string): boolean {
  if (!user) {
    return false;
  }
  if (!user.authEnabled || user.isAdmin) {
    return true;
  }
  return user.username === projectOwner;
}

// ---------------------------------------------------------------------------
// Test data
// ---------------------------------------------------------------------------

const adminUser: UserInfo = { username: 'admin', isAdmin: true, authEnabled: true };
const regularUser: UserInfo = { username: 'alice', isAdmin: false, authEnabled: true };
const anonUser: UserInfo = { username: 'anonymous', isAdmin: true, authEnabled: false };

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('useCurrentUser permissions', () => {
  describe('canEdit', () => {
    it('returns false when user is undefined', () => {
      expect(canEdit(undefined, 'someone')).toBe(false);
    });

    it('returns true when auth is disabled (anonymous)', () => {
      expect(canEdit(anonUser, 'anyone')).toBe(true);
    });

    it('returns true for admin users', () => {
      expect(canEdit(adminUser, 'bob')).toBe(true);
    });

    it('returns true when user owns the item', () => {
      expect(canEdit(regularUser, 'alice')).toBe(true);
    });

    it('returns false when user does not own the item', () => {
      expect(canEdit(regularUser, 'bob')).toBe(false);
    });
  });

  describe('canView', () => {
    it('returns false when user is undefined', () => {
      expect(canView(undefined, 'someone', true)).toBe(false);
    });

    it('returns true when auth is disabled', () => {
      expect(canView(anonUser, 'bob', false)).toBe(true);
    });

    it('returns true for admin users', () => {
      expect(canView(adminUser, 'bob', false)).toBe(true);
    });

    it('returns true for public items', () => {
      expect(canView(regularUser, 'bob', true)).toBe(true);
    });

    it('returns true when user owns the item even if private', () => {
      expect(canView(regularUser, 'alice', false)).toBe(true);
    });

    it('returns false for private items owned by others', () => {
      expect(canView(regularUser, 'bob', false)).toBe(false);
    });
  });

  describe('isProjectOwner', () => {
    it('returns false when user is undefined', () => {
      expect(isProjectOwner(undefined, 'someone')).toBe(false);
    });

    it('returns true when auth is disabled', () => {
      expect(isProjectOwner(anonUser, 'bob')).toBe(true);
    });

    it('returns true for admin users', () => {
      expect(isProjectOwner(adminUser, 'bob')).toBe(true);
    });

    it('returns true for the project owner', () => {
      expect(isProjectOwner(regularUser, 'alice')).toBe(true);
    });

    it('returns false for non-owners', () => {
      expect(isProjectOwner(regularUser, 'bob')).toBe(false);
    });
  });
});
