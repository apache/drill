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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.exec.server.rest.ProjectResources;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WorkflowPermissionsTest {

  private static ProjectResources.Project project(boolean isPublic) {
    return new ObjectMapper().convertValue(Map.of("id", "p", "name", "P", "owner", "olivia",
        "sharedWith", List.of("sam"), "isPublic", isPublic), ProjectResources.Project.class);
  }

  private static Workflow wf(String owner) {
    Workflow w = new Workflow();
    w.owner = owner;
    w.projectId = "p";
    return w;
  }

  @Test
  public void matrix() {
    ProjectResources.Project p = project(true);
    // view
    assertTrue(WorkflowPermissions.canView(p, "sam", false));
    assertTrue(WorkflowPermissions.canView(p, "stranger", false)); // public project
    assertFalse(WorkflowPermissions.canView(project(false), "stranger", false));
    assertTrue(WorkflowPermissions.canView(null, "admin", true));
    // create: members only, public is not enough
    assertTrue(WorkflowPermissions.canCreate(p, "sam", false));
    assertFalse(WorkflowPermissions.canCreate(p, "stranger", false));
    assertFalse(WorkflowPermissions.canCreate(null, "sam", false));
    // edit: own only (runs as owner), admin any
    assertTrue(WorkflowPermissions.canEdit(wf("sam"), "sam", false));
    assertFalse(WorkflowPermissions.canEdit(wf("sam"), "olivia", false));
    assertTrue(WorkflowPermissions.canEdit(wf("sam"), "root", true));
    // manage: own, project owner, admin
    assertTrue(WorkflowPermissions.canManage(wf("sam"), p, "olivia", false));
    assertTrue(WorkflowPermissions.canManage(wf("sam"), p, "sam", false));
    assertFalse(WorkflowPermissions.canManage(wf("olivia"), p, "sam", false));
  }
}
