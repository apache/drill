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
package org.apache.ranger.authorization.drill.resource;

import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link DrillRangerAccessRequest} builder and
 * {@link DrillRangerAccessRequest#toRangerRequest()} field mapping.
 */
public class DrillRangerAccessRequestTest {

  private DrillAccessResource buildResource() {
    return new DrillAccessResource(
        "mysql",
        java.util.Optional.of("shf"),
        java.util.Optional.of("orders"));
  }

  @Test
  public void builder_setsAllFields() {
    Set<String> groups = new HashSet<>();
    groups.add("g1");
    DrillAccessResource resource = buildResource();

    DrillRangerAccessRequest request = DrillRangerAccessRequest.builder()
        .user("root")
        .groups(groups)
        .resource(resource)
        .accessType(DrillAccessType.SELECT)
        .action("select")
        .clientIPAddress("10.0.0.1")
        .clientType("drill-jdbc")
        .build();

    RangerAccessRequest rangerRequest = request.toRangerRequest();
    assertEquals("root", rangerRequest.getUser());
    assertTrue(rangerRequest.getUserGroups().contains("g1"));
    assertEquals(resource, rangerRequest.getResource());
    assertEquals("SELECT", rangerRequest.getAccessType());
    assertEquals("select", rangerRequest.getAction());
    assertEquals("10.0.0.1", rangerRequest.getClientIPAddress());
    assertEquals("drill-jdbc", rangerRequest.getClientType());
  }

  @Test
  public void builder_defaultScopeIsSelfOrDescendants() {
    DrillRangerAccessRequest request = DrillRangerAccessRequest.builder()
        .user("root")
        .resource(buildResource())
        .accessType(DrillAccessType.SELECT)
        .build();
    assertEquals(
        RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS,
        request.toRangerRequest().getResourceMatchingScope());
  }

  @Test
  public void builder_explicitScope_isApplied() {
    DrillRangerAccessRequest request = DrillRangerAccessRequest.builder()
        .user("root")
        .resource(buildResource())
        .accessType(DrillAccessType.SELECT)
        .resourceMatchingScope(RangerAccessRequest.ResourceMatchingScope.SELF)
        .build();
    assertEquals(
        RangerAccessRequest.ResourceMatchingScope.SELF,
        request.toRangerRequest().getResourceMatchingScope());
  }

  @Test
  public void builder_addGroup_accumulatesGroups() {
    DrillRangerAccessRequest request = DrillRangerAccessRequest.builder()
        .user("root")
        .resource(buildResource())
        .accessType(DrillAccessType.SELECT)
        .addGroup("g1")
        .addGroup("g2")
        .build();

    Set<String> groups = request.toRangerRequest().getUserGroups();
    assertTrue(groups.contains("g1"));
    assertTrue(groups.contains("g2"));
    assertEquals(2, groups.size());
  }

  @Test
  public void builder_nullGroups_createsEmptySet() {
    DrillRangerAccessRequest request = DrillRangerAccessRequest.builder()
        .user("root")
        .resource(buildResource())
        .accessType(DrillAccessType.SELECT)
        .groups(null)
        .build();

    Set<String> groups = request.toRangerRequest().getUserGroups();
    assertNotNull(groups);
    assertTrue(groups.isEmpty());
  }

  @Test
  public void toRangerRequest_accessTypeIsUppercase() {
    DrillRangerAccessRequest request = DrillRangerAccessRequest.builder()
        .user("root")
        .resource(buildResource())
        .accessType(DrillAccessType.SELECT)
        .build();
    assertEquals("SELECT", request.toRangerRequest().getAccessType());
  }

  @Test
  public void toRangerRequest_actionDefaultsToAccessTypeName() {
    DrillRangerAccessRequest request = DrillRangerAccessRequest.builder()
        .user("root")
        .resource(buildResource())
        .accessType(DrillAccessType.SELECT)
        .build();
    assertEquals("SELECT", request.toRangerRequest().getAction());
  }

  @Test
  public void toRangerRequest_setsResourceMatchingScope() {
    DrillRangerAccessRequest request = DrillRangerAccessRequest.builder()
        .user("root")
        .resource(buildResource())
        .accessType(DrillAccessType.SELECT)
        .resourceMatchingScope(RangerAccessRequest.ResourceMatchingScope.SELF)
        .build();
    assertEquals(
        RangerAccessRequest.ResourceMatchingScope.SELF,
        request.toRangerRequest().getResourceMatchingScope());
  }
}
