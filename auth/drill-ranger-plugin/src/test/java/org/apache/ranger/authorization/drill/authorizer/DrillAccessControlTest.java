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

import org.apache.drill.exec.security.spi.AccessType;
import org.apache.drill.exec.security.spi.UserIdentity;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.drill.resource.DrillAccessType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the instance-based {@link DrillAccessControl} SPI
 * implementation. Covers system-schema bypass, fail-closed behavior
 * (exception / malformed schema), SPI-enum-to-DrillAccessType mapping,
 * delegation to {@link DrillAuthorizer} and user-group resolution
 * (engine-supplied groups take precedence, UGI is the fallback).
 */
public class DrillAccessControlTest {

  private static final String USER = "root";
  private static final String DS = "mysql";
  private static final String SCHEMA = "shf";
  private static final String TABLE = "orders";

  /**
   * Class-level mock of {@link UserGroupInformation} to prevent JNI-based group
   * lookup ({@code JniBasedUnixGroupsMapping}) which fails on Windows /
   * non-Unix environments and pollutes test logs with IOException stacks.
   * Opened in {@link #setUp()} and closed in {@link #tearDown()} so that every
   * test method — including those that indirectly call {@code getUserGroups}
   * via the group-resolution fallback — gets a deterministic empty group set
   * without touching the OS.
   */
  private MockedStatic<UserGroupInformation> ugiMock;
  private UserGroupInformation mockUgi;

  private DrillAuthorizer mockAuthorizer;
  private DrillAccessControl accessControl;

  @BeforeEach
  public void setUp() {
    // Stub UGI for any user: createRemoteUser returns a mock whose
    // getGroupNames() returns an empty array by default. Individual tests
    // (e.g. getUserGroups_returnsNonNullForValidUser) can re-stub mockUgi
    // to return specific groups or throw exceptions.
    ugiMock = mockStatic(UserGroupInformation.class);
    mockUgi = mock(UserGroupInformation.class);
    ugiMock.when(() -> UserGroupInformation.createRemoteUser(anyString()))
        .thenReturn(mockUgi);
    when(mockUgi.getGroupNames()).thenReturn(new String[0]);

    mockAuthorizer = mock(DrillAuthorizer.class);
    accessControl = new DrillAccessControl(mockAuthorizer);
  }

  @AfterEach
  public void tearDown() {
    if (ugiMock != null) {
      ugiMock.close();
      ugiMock = null;
    }
  }

  // ========================================================================
  // System-schema bypass
  // ========================================================================

  @Test
  public void checkTableAccess_bypassesSystemSchema_informationSchema() {
    assertTrue(accessControl.checkTableAccess(
        UserIdentity.of(USER), "dfs", "INFORMATION_SCHEMA", "TABLES", AccessType.SELECT));
    verify(mockAuthorizer, never()).checkTableAccess(any(), any(), any(), any(), any());
  }

  @Test
  public void checkTableAccess_bypassesSystemSchema_sys_caseInsensitive() {
    for (String schema : new String[] {"sys", "Sys", "SYS"}) {
      assertTrue(accessControl.checkTableAccess(
          UserIdentity.of(USER), "dfs", schema, "DRILLBITS", AccessType.SELECT),
          "schema=" + schema + " should bypass authorization");
    }
    verify(mockAuthorizer, never()).checkTableAccess(any(), any(), any(), any(), any());
  }

  @Test
  public void checkTableAccess_bypassesSystemSchema_compoundPath() {
    // Top-level segment "information_schema" should match, even with compound path
    assertTrue(accessControl.checkTableAccess(
        UserIdentity.of(USER), "dfs", "information_schema.tables", "COLUMNS", AccessType.SELECT));
    verify(mockAuthorizer, never()).checkTableAccess(any(), any(), any(), any(), any());
  }

  @Test
  public void checkColumnAccess_bypassesSystemSchema() {
    Set<String> columns = new HashSet<>(Collections.singletonList("TABLE_NAME"));
    assertTrue(accessControl.checkColumnAccess(
        UserIdentity.of(USER), "dfs", "INFORMATION_SCHEMA", "TABLES", columns, AccessType.SELECT));
    verify(mockAuthorizer, never()).checkColumnAccess(any(), any(), any(), any(), any(), any());
  }

  // ========================================================================
  // Malformed schema and unknown access type: fail-closed
  // ========================================================================

  @Test
  public void checkTableAccess_doesNotBypass_nullSchema_denied() {
    // A null schema is not a system schema; it must fail closed (denied) rather
    // than being silently treated as one and bypassing authorization.
    assertFalse(accessControl.checkTableAccess(
        UserIdentity.of(USER), "dfs", null, TABLE, AccessType.SELECT),
        "null schema must not bypass authorization");
    verify(mockAuthorizer, never()).checkTableAccess(any(), any(), any(), any(), any());
  }

  @Test
  public void checkTableAccess_doesNotBypass_emptySchema_denied() {
    // Empty/whitespace schemas are not system schemas; they must fail closed
    // (denied) rather than silently bypassing authorization.
    for (String schema : new String[] {"", "   "}) {
      assertFalse(accessControl.checkTableAccess(
          UserIdentity.of(USER), "dfs", schema, TABLE, AccessType.SELECT),
          "empty/whitespace schema must not bypass authorization, schema='" + schema + "'");
    }
    verify(mockAuthorizer, never()).checkTableAccess(any(), any(), any(), any(), any());
  }

  // ========================================================================
  // Delegation to DrillAuthorizer
  // ========================================================================

  @Test
  public void checkTableAccess_delegatesToAuthorizer() {
    when(mockAuthorizer.checkTableAccess(any(), anyString(), anyString(), anyString(),
        eq(DrillAccessType.SELECT))).thenReturn(true);

    assertTrue(accessControl.checkTableAccess(
        UserIdentity.of(USER), DS, SCHEMA, TABLE, AccessType.SELECT));

    ArgumentCaptor<UserIdentity> captor = ArgumentCaptor.forClass(UserIdentity.class);
    verify(mockAuthorizer).checkTableAccess(captor.capture(), eq(DS), eq(SCHEMA),
        eq(TABLE), eq(DrillAccessType.SELECT));
    assertEquals(USER, captor.getValue().getUser());
  }

  @Test
  public void checkColumnAccess_delegatesToAuthorizer() {
    when(mockAuthorizer.checkColumnAccess(any(), anyString(), anyString(), anyString(),
        any(), eq(DrillAccessType.SELECT))).thenReturn(false);

    Set<String> columns = new HashSet<>(Arrays.asList("id", "amount"));
    assertFalse(accessControl.checkColumnAccess(
        UserIdentity.of(USER), DS, SCHEMA, TABLE, columns, AccessType.SELECT));

    ArgumentCaptor<UserIdentity> identityCaptor = ArgumentCaptor.forClass(UserIdentity.class);
    ArgumentCaptor<Set<String>> columnsCaptor = ArgumentCaptor.forClass(Set.class);
    verify(mockAuthorizer).checkColumnAccess(identityCaptor.capture(), eq(DS), eq(SCHEMA),
        eq(TABLE), columnsCaptor.capture(), eq(DrillAccessType.SELECT));
    assertEquals(USER, identityCaptor.getValue().getUser());
    assertEquals(columns, columnsCaptor.getValue());
  }

  @Test
  public void checkTableAccess_mapsSpiEnumToDrillAccessType_byName() {
    // Pins the name-based mapping: every SPI AccessType the engine can emit
    // must have a DrillAccessType with the same name, otherwise the check
    // fails closed at run time.
    when(mockAuthorizer.checkTableAccess(any(), anyString(), anyString(), anyString(), any()))
        .thenReturn(true);

    for (AccessType type : AccessType.values()) {
      assertTrue(accessControl.checkTableAccess(
          UserIdentity.of(USER), DS, SCHEMA, TABLE, type),
          "SPI AccessType." + type.name() + " must map to a DrillAccessType");
      verify(mockAuthorizer).checkTableAccess(any(), eq(DS), eq(SCHEMA), eq(TABLE),
          eq(DrillAccessType.valueOf(type.name())));
    }
  }

  @Test
  public void checkTableAccess_returnsFalse_whenAuthorizerThrows() {
    when(mockAuthorizer.checkTableAccess(any(), anyString(), anyString(), anyString(), any()))
        .thenThrow(new RuntimeException("boom"));

    assertFalse(accessControl.checkTableAccess(
        UserIdentity.of(USER), DS, SCHEMA, TABLE, AccessType.SELECT));
  }

  @Test
  public void checkColumnAccess_returnsFalse_whenAuthorizerThrows() {
    when(mockAuthorizer.checkColumnAccess(any(), anyString(), anyString(), anyString(), any(), any()))
        .thenThrow(new RuntimeException("boom"));

    Set<String> columns = new HashSet<>(Collections.singletonList("amount"));
    assertFalse(accessControl.checkColumnAccess(
        UserIdentity.of(USER), DS, SCHEMA, TABLE, columns, AccessType.SELECT));
  }

  // ========================================================================
  // Group resolution: engine-supplied groups win, UGI is the fallback
  // ========================================================================

  @Test
  public void checkTableAccess_usesEngineSuppliedGroups() {
    Set<String> engineGroups = new HashSet<>(Arrays.asList("analysts", "etl"));
    UserIdentity identity = UserIdentity.builder()
        .setUser(USER)
        .setGroups(engineGroups)
        .build();

    accessControl.checkTableAccess(identity, DS, SCHEMA, TABLE, AccessType.SELECT);

    // The identity is passed through as-is (engine groups already present)
    ArgumentCaptor<UserIdentity> captor = ArgumentCaptor.forClass(UserIdentity.class);
    verify(mockAuthorizer).checkTableAccess(captor.capture(), eq(DS), eq(SCHEMA),
        eq(TABLE), eq(DrillAccessType.SELECT));
    assertEquals(engineGroups, captor.getValue().getGroups());
  }

  @Test
  public void checkTableAccess_fallsBackToUgiGroups_whenIdentityHasNone() {
    // UserIdentity.of() carries no groups → resolveIdentity falls back to UGI.
    // Re-stub mockUgi to return specific groups and verify they reach the authorizer.
    when(mockUgi.getGroupNames()).thenReturn(new String[] {"root", "wheel"});

    accessControl.checkTableAccess(UserIdentity.of(USER), DS, SCHEMA, TABLE, AccessType.SELECT);

    ArgumentCaptor<UserIdentity> captor = ArgumentCaptor.forClass(UserIdentity.class);
    verify(mockAuthorizer).checkTableAccess(captor.capture(), eq(DS), eq(SCHEMA),
        eq(TABLE), eq(DrillAccessType.SELECT));
    assertEquals(new HashSet<>(Arrays.asList("root", "wheel")), captor.getValue().getGroups());
  }

  // ========================================================================
  // getUserGroups (UGI resolution helper)
  // ========================================================================

  @Test
  public void getUserGroups_returnsEmptyForNullUser() {
    assertEquals(Collections.emptySet(), DrillAccessControl.getUserGroups(null));
  }

  @Test
  public void getUserGroups_returnsEmptyForEmptyUser() {
    assertEquals(Collections.emptySet(), DrillAccessControl.getUserGroups(""));
    assertEquals(Collections.emptySet(), DrillAccessControl.getUserGroups("   "));
  }

  @Test
  public void getUserGroups_returnsNonNullForValidUser() {
    // Re-stub the class-level mockUgi to return specific groups, then verify
    // getUserGroups propagates them correctly.
    when(mockUgi.getGroupNames()).thenReturn(new String[] {"root", "wheel"});

    Set<String> groups = DrillAccessControl.getUserGroups(USER);
    assertNotNull(groups);
    assertEquals(new HashSet<>(Arrays.asList("root", "wheel")), groups);
  }

  @Test
  public void getUserGroups_returnsEmptySet_whenUgiThrows() {
    // Verifies the catch-block fallback: when UGI lookup throws, the method
    // returns an empty set instead of propagating the exception.
    when(mockUgi.getGroupNames()).thenThrow(new RuntimeException("ugi boom"));

    Set<String> groups = DrillAccessControl.getUserGroups(USER);
    assertNotNull(groups);
    assertTrue(groups.isEmpty());
  }

  // ========================================================================
  // Shutdown lifecycle
  // ========================================================================

  @Test
  public void close_releasesDelegateAuthorizer() {
    DrillAccessControl accessControl = new DrillAccessControl(mockAuthorizer);

    accessControl.close();

    verify(mockAuthorizer).close();
  }
}
