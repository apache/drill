/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.drill.exec.security.spi.UserIdentity;
import org.apache.ranger.authorization.drill.resource.DrillAccessType;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DrillAuthorizer}.
 * Covers argument validation (fail-closed), request building (user, groups,
 * resource values, matching scope), and per-column fail-fast behavior.
 */
public class DrillAuthorizerTest {

  private static final String USER = "root";
  private static final String DS = "mysql";
  private static final String SCHEMA = "shf";
  private static final String TABLE = "orders";
  private static final Set<String> GROUPS =
      Collections.singleton("analysts");

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

  private UserIdentity identity() {
    return UserIdentity.builder().setUser(USER).setGroups(GROUPS).build();
  }

  // ========================================================================
  // Argument validation: fail-closed, no Ranger call
  // ========================================================================

  @Test
  public void checkTableAccess_returnsFalse_whenUserNull() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);
    assertFalse(authorizer.checkTableAccess(null, DS, SCHEMA, TABLE, DrillAccessType.SELECT));
    verify(mockBase, never()).isAccessAllowed(any());
  }

  @Test
  public void checkTableAccess_returnsFalse_whenArgumentsEmpty() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);
    UserIdentity user = identity();

    assertFalse(authorizer.checkTableAccess(user, null, SCHEMA, TABLE, DrillAccessType.SELECT));
    assertFalse(authorizer.checkTableAccess(user, "", SCHEMA, TABLE, DrillAccessType.SELECT));
    assertFalse(authorizer.checkTableAccess(user, DS, null, TABLE, DrillAccessType.SELECT));
    assertFalse(authorizer.checkTableAccess(user, DS, "  ", TABLE, DrillAccessType.SELECT));
    assertFalse(authorizer.checkTableAccess(user, DS, SCHEMA, null, DrillAccessType.SELECT));
    assertFalse(authorizer.checkTableAccess(user, DS, SCHEMA, "", DrillAccessType.SELECT));
    verify(mockBase, never()).isAccessAllowed(any());
  }

  @Test
  public void checkColumnAccess_returnsFalse_whenColumnsInvalid() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);
    UserIdentity user = identity();

    // null / empty / blank-element column sets all fail closed
    assertFalse(authorizer.checkColumnAccess(user, DS, SCHEMA, TABLE, null, DrillAccessType.SELECT));
    assertFalse(authorizer.checkColumnAccess(user, DS, SCHEMA, TABLE,
        Collections.emptySet(), DrillAccessType.SELECT));
    assertFalse(authorizer.checkColumnAccess(user, DS, SCHEMA, TABLE,
        Collections.singleton(""), DrillAccessType.SELECT));
    assertFalse(authorizer.checkColumnAccess(user, DS, SCHEMA, TABLE,
        Collections.singleton("  "), DrillAccessType.SELECT));
    verify(mockBase, never()).isAccessAllowed(any());
  }

  // ========================================================================
  // Request building: delegation with correct scope and values
  // ========================================================================

  @Test
  public void checkTableAccess_buildsRequest_withSelfOrDescendantsScope() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    when(mockBase.isAccessAllowed(any())).thenReturn(true);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);

    assertTrue(authorizer.checkTableAccess(identity(), DS, SCHEMA, TABLE, DrillAccessType.SELECT));

    ArgumentCaptor<RangerAccessRequest> captor = ArgumentCaptor.forClass(RangerAccessRequest.class);
    verify(mockBase).isAccessAllowed(captor.capture());
    RangerAccessRequest captured = captor.getValue();
    assertEquals(RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS,
        captured.getResourceMatchingScope());
    assertEquals(USER, captured.getUser());
    assertEquals(GROUPS, captured.getUserGroups());
    assertEquals("SELECT", captured.getAccessType());
  }

  @Test
  public void checkColumnAccess_buildsRequest_withSelfScope() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    when(mockBase.isAccessAllowed(any())).thenReturn(true);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);

    assertTrue(authorizer.checkColumnAccess(identity(), DS, SCHEMA, TABLE,
        Collections.singleton("amount"), DrillAccessType.SELECT));

    ArgumentCaptor<RangerAccessRequest> captor = ArgumentCaptor.forClass(RangerAccessRequest.class);
    verify(mockBase).isAccessAllowed(captor.capture());
    assertEquals(RangerAccessRequest.ResourceMatchingScope.SELF,
        captor.getValue().getResourceMatchingScope());
  }

  // ========================================================================
  // Per-column iteration: fail-fast
  // ========================================================================

  @Test
  public void checkColumnAccess_checksEachColumnIndividually_failFast() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    // First column allowed, second column denied — fail fast on second
    when(mockBase.isAccessAllowed(any())).thenReturn(true, false);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);

    // LinkedHashSet for deterministic iteration order
    Set<String> columns = new LinkedHashSet<>(Arrays.asList("c1", "c2", "c3"));
    assertFalse(authorizer.checkColumnAccess(identity(), DS, SCHEMA, TABLE, columns,
        DrillAccessType.SELECT));
    // c1 (true) + c2 (false) -> 2 invocations; c3 should NOT be reached
    verify(mockBase, times(2)).isAccessAllowed(any());
  }

  @Test
  public void checkColumnAccess_allColumnsAllowed_returnsTrue() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    when(mockBase.isAccessAllowed(any())).thenReturn(true);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);

    Set<String> columns = new LinkedHashSet<>(Arrays.asList("c1", "c2", "c3"));
    assertTrue(authorizer.checkColumnAccess(identity(), DS, SCHEMA, TABLE, columns,
        DrillAccessType.SELECT));
    verify(mockBase, times(3)).isAccessAllowed(any());
  }

  // ========================================================================
  // Shutdown lifecycle
  // ========================================================================

  @Test
  public void close_invokesBaseAuthorizerCleanUp() {
    RangerBaseAuthorizer mockBase = mock(RangerBaseAuthorizer.class);
    DrillAuthorizer authorizer = newAuthorizer(mockBase);

    authorizer.close();

    verify(mockBase).cleanUp();
  }
}
