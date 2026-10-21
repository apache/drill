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
package org.apache.drill.exec.server.rest.workflow;

import org.apache.drill.exec.server.rest.ProjectResources;

/** Who may see and change workflows. Pure functions; no I/O. */
public final class WorkflowPermissions {

  private WorkflowPermissions() { }

  /** Owner or explicitly shared member. Public visibility alone is not membership. */
  public static boolean isMember(ProjectResources.Project p, String user) {
    return p != null && user != null && (user.equals(p.getOwner()) || p.getSharedWith().contains(user));
  }

  /** Anyone who can read the project (including public projects); admins see everything. */
  public static boolean canView(ProjectResources.Project p, String user, boolean admin) {
    return admin || (p != null && ProjectResources.canRead(p, user));
  }

  /** Members only: public visibility does not let a stranger schedule work in someone's project. */
  public static boolean canCreate(ProjectResources.Project p, String user, boolean admin) {
    return admin || isMember(p, user);
  }

  /** Edits change what runs under the owner's identity, so only the owner (or an admin) may edit. */
  public static boolean canEdit(Workflow wf, String user, boolean admin) {
    return admin || (user != null && user.equals(wf.owner));
  }

  /** Pause, enable, delete, renew and run: the workflow owner, the project owner, or an admin. */
  public static boolean canManage(Workflow wf, ProjectResources.Project p, String user, boolean admin) {
    return canEdit(wf, user, admin) || (p != null && user != null && user.equals(p.getOwner()));
  }
}
