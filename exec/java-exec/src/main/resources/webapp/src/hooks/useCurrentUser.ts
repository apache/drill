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
import { useQuery } from '@tanstack/react-query';
import { getCurrentUser } from '../api/user';
import type { UserInfo } from '../api/user';

export interface UserPermissions {
  user: UserInfo | undefined;
  isLoading: boolean;
  /** True if this user can edit the given item (owns it, is admin, or auth is disabled) */
  canEdit: (owner: string) => boolean;
  /** True if this user can see the given item (owns it, it's public, is admin, or auth is disabled) */
  canView: (owner: string, isPublic: boolean) => boolean;
  /** True if this user can manage items in a project they own */
  isProjectOwner: (projectOwner: string) => boolean;
}

export function useCurrentUser(): UserPermissions {
  const { data: user, isLoading } = useQuery({
    queryKey: ['current-user'],
    queryFn: getCurrentUser,
    staleTime: 5 * 60 * 1000, // Cache for 5 minutes
  });

  const canEdit = (owner: string): boolean => {
    if (!user) {
      return false;
    }
    // Auth disabled (anonymous) = admin = can edit everything
    if (!user.authEnabled || user.isAdmin) {
      return true;
    }
    return user.username === owner;
  };

  const canView = (owner: string, isPublic: boolean): boolean => {
    if (!user) {
      return false;
    }
    if (!user.authEnabled || user.isAdmin) {
      return true;
    }
    return isPublic || user.username === owner;
  };

  const isProjectOwner = (projectOwner: string): boolean => {
    if (!user) {
      return false;
    }
    if (!user.authEnabled || user.isAdmin) {
      return true;
    }
    return user.username === projectOwner;
  };

  return { user, isLoading, canEdit, canView, isProjectOwner };
}
