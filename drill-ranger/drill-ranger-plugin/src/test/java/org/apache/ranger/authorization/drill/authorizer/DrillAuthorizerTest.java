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
package org.apache.ranger.authorization.drill.authorizer;

import org.apache.ranger.authorization.drill.resource.DrillAccessType;
import org.apache.ranger.authorization.drill.resource.DrillResource;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mockStatic;

/**
 * Unit tests for {@link DrillAuthorizer}.
 * Covers resource validation, table-level and column-level access checks,
 * and per-column fail-fast behavior.
 */
public class DrillAuthorizerTest {

  /**
   * Creates a DrillAuthorizer with the singleton {@link RangerBaseAuthorizer}
   * mocked out, so the constructor does not actually contact Ranger Admin.
   */
  private DrillAuthorizer newAuthorizer(RangerBaseAuthorizer mockBase) {
    try (MockedStatic<RangerBaseAuthorizer> mocked = mockStatic(RangerBaseAuthorizer.class)) {
      mocked.when(RangerBaseAuthorizer::getInstance).thenReturn(mockBase);
      return new DrillAuthorizer("svc");
    }
  }

  private DrillResource buildValidResource() {
    DrillResource r = new DrillResource();
    r.setUser("root");
    r.setDataSource("mysql");
    r.setSchema("shf");
    r.setTable("orders");
    Set<String> columns = new LinkedHashSet<>();
    columns.add("amount");
    r.setColumns(columns);
    return r;
  }

  @Test
  public void validateResource_nullResource_returnsFalse() {
    DrillAuthorizer authorizer = newAuthorizer(mock(RangerBaseAuthorizer.class));
    assertFalse(authorizer.validateResource(null));
  }

  @Test
  public void validateResource_emptyUser_returnsFalse() {
    DrillAuthorizer authorizer = newAuthorizer(mock(RangerBaseAuthorizer.class));
    DrillResource r = buildValidResource();
    r.setUser(null);
    assertFalse(authorizer.validateResource(r));

    r.setUser("");
    assertFalse(authorizer.validateResource(r));
  }

  @Test
  public void validateResource_emptyDataSource_returnsFalse() {
    DrillAuthorizer authorizer = newAuthorizer(mock(RangerBaseAuthorizer.class));
    DrillResource r = buildValidResource();
    r.setDataSource(null);
    assertFalse(authorizer.validateResource(r));

    r.setDataSource("");
    assertFalse(authorizer.validateResource(r));
  }

  @Test
  public void validateResource_emptySchema_returnsFalse_atSchemaLevel() {
    DrillAuthorizer authorizer = newAuthorizer(mock(RangerBaseAuthorizer.class));
    DrillResource r = buildValidResource();
    r.setSchema(null);
    // COLUMN-level validate requires schema non-empty too
    assertFalse(authorizer.validateResource(r));

    r.setSchema("");
    assertFalse(authorizer.validateResource(r));
  }

  @Test
  public void validateResource_emptyTable_returnsFalse_atTableLevel() {
    DrillAuthorizer authorizer = newAuthorizer(mock(RangerBaseAuthorizer.class));
    DrillResource r = buildValidResource();
    r.setTable(null);
    assertFalse(authorizer.validateResource(r));

    r.setTable("");
    assertFalse(authorizer.validateResource(r));
  }

  @Test
  public void validateResource_emptyColumns_returnsFalse_atColumnLevel() {
    DrillAuthorizer authorizer = newAuthorizer(mock(RangerBaseAuthorizer.class));
    DrillResource r = buildValidResource();
    r.setColumns(null);
    assertFalse(authorizer.validateResource(r));

    r.setColumns(Collections.emptySet());
    assertFalse(authorizer.validateResource(r));
  }

  @Test
  public void validateResource_validResource_returnsTrue() {
    DrillAuthorizer authorizer = newAuthorizer(mock(RangerBaseAuthorizer.class));
    DrillResource r = buildValidResource();
    assertTrue(authorizer.validateResource(r));
  }

  @Test
  public void checkTableAccess_returnsFalse_whenValidationFails() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);

    DrillResource r = buildValidResource();
    r.setTable(null); // missing table -> validation fails at TABLE level
    assertFalse(authorizer.checkTableAccess(r, DrillAccessType.SELECT));
    verify(mockBase, never()).isAccessAllowed(any());
  }

  @Test
  public void checkTableAccess_delegatesToAuthorizer_withSelfOrDescendantsScope() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    when(mockBase.isAccessAllowed(any())).thenReturn(true);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);

    DrillResource r = buildValidResource();
    boolean result = authorizer.checkTableAccess(r, DrillAccessType.SELECT);

    assertTrue(result);
    ArgumentCaptor<RangerAccessRequest> captor = ArgumentCaptor.forClass(RangerAccessRequest.class);
    verify(mockBase).isAccessAllowed(captor.capture());
    RangerAccessRequest captured = captor.getValue();
    assertEquals(
        RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS,
        captured.getResourceMatchingScope());
  }

  @Test
  public void checkColumnAccess_returnsFalse_whenValidationFails() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);

    DrillResource r = buildValidResource();
    r.setColumns(Collections.emptySet()); // validation fails
    assertFalse(authorizer.checkColumnAccess(r, DrillAccessType.SELECT));
    verify(mockBase, never()).isAccessAllowed(any());
  }

  @Test
  public void checkColumnAccess_checksEachColumnIndividually_failFast() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    // First column allowed, second column denied — fail fast on second
    when(mockBase.isAccessAllowed(any())).thenReturn(true, false);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);

    DrillResource r = buildValidResource();
    // Use LinkedHashSet for deterministic iteration order
    Set<String> columns = new LinkedHashSet<>(Arrays.asList("c1", "c2", "c3"));
    r.setColumns(columns);

    boolean result = authorizer.checkColumnAccess(r, DrillAccessType.SELECT);
    assertFalse(result);
    // c1 (true) + c2 (false) -> 2 invocations; c3 should NOT be reached
    verify(mockBase, times(2)).isAccessAllowed(any());
  }

  @Test
  public void checkColumnAccess_allColumnsAllowed_returnsTrue() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    when(mockBase.isAccessAllowed(any())).thenReturn(true);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);

    DrillResource r = buildValidResource();
    Set<String> columns = new LinkedHashSet<>(Arrays.asList("c1", "c2", "c3"));
    r.setColumns(columns);

    boolean result = authorizer.checkColumnAccess(r, DrillAccessType.SELECT);
    assertTrue(result);
    verify(mockBase, times(3)).isAccessAllowed(any());
  }

  @Test
  public void checkColumnAccess_emptyColumnInSet_returnsFalse() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);

    DrillResource r = buildValidResource();
    Set<String> columns = new HashSet<>();
    columns.add("");
    r.setColumns(columns);

    assertFalse(authorizer.checkColumnAccess(r, DrillAccessType.SELECT));
    verify(mockBase, never()).isAccessAllowed(any());
  }
}
