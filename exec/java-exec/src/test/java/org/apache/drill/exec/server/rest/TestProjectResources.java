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
package org.apache.drill.exec.server.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.exec.server.rest.ProjectResources.Project;
import org.junit.Test;
import org.apache.drill.test.BaseTest;

/**
 * Unit tests for the project read and write access rules.
 */
public class TestProjectResources extends BaseTest {

  private static Project project(boolean isPublic) {
    Project p = new Project();
    p.setPublic(isPublic);
    p.setSharedWith(Arrays.asList("editor"));
    p.setViewers(Arrays.asList("viewer"));
    return withOwner(p, "owner");
  }

  private static Project withOwner(Project template, String owner) {
    return new Project("p1", "P", null, null, owner, template.isPublic(),
        template.getSharedWith(), template.getViewers(), null, null, null, null, null,
        false, 0L, 0L, null, null, 0L);
  }

  @Test
  public void testCanWriteAllowsOwnerAndSharedWith() {
    Project p = project(false);
    assertTrue(ProjectResources.canWrite(p, "owner"));
    assertTrue(ProjectResources.canWrite(p, "editor"));
  }

  @Test
  public void testCanWriteRejectsViewerAndStranger() {
    Project p = project(false);
    assertFalse(ProjectResources.canWrite(p, "viewer"));
    assertFalse(ProjectResources.canWrite(p, "stranger"));
  }

  @Test
  public void testCanWriteIgnoresIsPublic() {
    assertFalse(ProjectResources.canWrite(project(true), "stranger"));
  }

  @Test
  public void testCanReadAllowsViewer() {
    assertTrue(ProjectResources.canRead(project(false), "viewer"));
    assertFalse(ProjectResources.canRead(project(false), "stranger"));
  }

  @Test
  public void testViewersDefaultsToEmptyForStoredProject() throws Exception {
    String json = "{\"id\":\"p1\",\"name\":\"P\",\"owner\":\"owner\",\"sharedWith\":[\"a\"]}";
    Project p = new ObjectMapper().readValue(json, Project.class);
    assertNotNull(p.getViewers());
    assertEquals(Collections.emptyList(), p.getViewers());
  }

  @Test
  public void testOnlyOwnerCanAdminister() {
    Project p = project(false);
    assertTrue(ProjectResources.canAdminister(p, "owner"));
    assertFalse(ProjectResources.canAdminister(p, "editor"));
  }

  @Test
  public void testEditorCannotChangeSharing() {
    Project p = project(false);
    assertTrue(ProjectResources.changesSharing(p, true, null, null));
    assertTrue(ProjectResources.changesSharing(p, null, Arrays.asList("editor", "x"), null));
    assertTrue(ProjectResources.changesSharing(p, null, null, Collections.<String>emptyList()));
  }

  @Test
  public void testEditorCanChangeContentFields() {
    Project p = project(false);
    assertTrue(ProjectResources.canWrite(p, "editor"));
    assertFalse(ProjectResources.changesSharing(p, null, null, null));
    // Resending unchanged sharing values is not a change.
    assertFalse(ProjectResources.changesSharing(p, false, Arrays.asList("editor"), Arrays.asList("viewer")));
  }
}
