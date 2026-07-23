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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.drill.resource.DrillAccessType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DrillAccessControl} static facade.
 * Covers fail-open (disabled), system-schema bypass, fail-closed (exception)
 * and user-group resolution behavior.
 */
public class DrillAccessControlTest {

  /**
   * Class-level mock of {@link UserGroupInformation} to prevent JNI-based group
   * lookup ({@code JniBasedUnixGroupsMapping}) which fails on Windows /
   * non-Unix environments and pollutes test logs with IOException stacks.
   * Opened in {@link #setUp()} and closed in {@link #tearDown()} so that every
   * test method — including those that indirectly call {@code getUserGroups}
   * via {@code checkTableAccess}/{@code checkColumnAccess} — gets a deterministic
   * empty group set without touching the OS.
   */
  private MockedStatic<UserGroupInformation> ugiMock;
  private UserGroupInformation mockUgi;

  @BeforeEach
  public void setUp() throws Exception {
    setStaticField(DrillAccessControl.class, "enabled", false);
    setStaticField(DrillAccessControl.class, "authorizer", null);

    // Stub UGI for any user: createRemoteUser returns a mock whose
    // getGroupNames() returns an empty array by default. Individual tests
    // (e.g. getUserGroups_returnsNonNullForValidUser) can re-stub mockUgi
    // to return specific groups or throw exceptions.
    ugiMock = mockStatic(UserGroupInformation.class);
    mockUgi = mock(UserGroupInformation.class);
    ugiMock.when(() -> UserGroupInformation.createRemoteUser(
        org.mockito.ArgumentMatchers.anyString())).thenReturn(mockUgi);
    when(mockUgi.getGroupNames()).thenReturn(new String[0]);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (ugiMock != null) {
      ugiMock.close();
      ugiMock = null;
    }
    setStaticField(DrillAccessControl.class, "enabled", false);
    setStaticField(DrillAccessControl.class, "authorizer", null);
  }

  @Test
  public void isEnabled_returnsFalse_beforeInit() {
    assertFalse(DrillAccessControl.isEnabled());
  }

  @Test
  public void checkTableAccess_returnsTrue_whenNotEnabled() {
    assertTrue(DrillAccessControl.checkTableAccess(
        "root", "mysql", "shf", "orders", DrillAccessType.SELECT));
  }

  @Test
  public void checkColumnAccess_returnsTrue_whenNotEnabled() {
    Set<String> columns = new HashSet<>();
    columns.add("amount");
    assertTrue(DrillAccessControl.checkColumnAccess(
        "root", "mysql", "shf", "orders", columns, DrillAccessType.SELECT));
  }

  @Test
  public void checkTableAccess_bypassesSystemSchema_informationSchema() throws Exception {
    DrillAuthorizer mockAuthorizer = mock(DrillAuthorizer.class);
    setStaticField(DrillAccessControl.class, "enabled", true);
    setStaticField(DrillAccessControl.class, "authorizer", mockAuthorizer);

    assertTrue(DrillAccessControl.checkTableAccess(
        "root", "dfs", "INFORMATION_SCHEMA", "TABLES", DrillAccessType.SELECT));
    verify(mockAuthorizer, never()).checkTableAccess(
        org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void checkTableAccess_bypassesSystemSchema_sys_caseInsensitive() throws Exception {
    DrillAuthorizer mockAuthorizer = mock(DrillAuthorizer.class);
    setStaticField(DrillAccessControl.class, "enabled", true);
    setStaticField(DrillAccessControl.class, "authorizer", mockAuthorizer);

    for (String schema : new String[] {"sys", "Sys", "SYS"}) {
      assertTrue(DrillAccessControl.checkTableAccess(
          "root", "dfs", schema, "DRILLBITS", DrillAccessType.SELECT),
          "schema=" + schema + " should bypass authorization");
    }
    verify(mockAuthorizer, never()).checkTableAccess(
        org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void checkTableAccess_bypassesSystemSchema_compoundPath() throws Exception {
    DrillAuthorizer mockAuthorizer = mock(DrillAuthorizer.class);
    setStaticField(DrillAccessControl.class, "enabled", true);
    setStaticField(DrillAccessControl.class, "authorizer", mockAuthorizer);

    // Top-level segment "information_schema" should match, even with compound path
    assertTrue(DrillAccessControl.checkTableAccess(
        "root", "dfs", "information_schema.tables", "COLUMNS", DrillAccessType.SELECT));
    verify(mockAuthorizer, never()).checkTableAccess(
        org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void checkTableAccess_bypassesSystemSchema_nullSchema() throws Exception {
    DrillAuthorizer mockAuthorizer = mock(DrillAuthorizer.class);
    setStaticField(DrillAccessControl.class, "enabled", true);
    setStaticField(DrillAccessControl.class, "authorizer", mockAuthorizer);

    assertTrue(DrillAccessControl.checkTableAccess(
        "root", "dfs", null, "orders", DrillAccessType.SELECT));
  }

  @Test
  public void checkTableAccess_bypassesSystemSchema_emptySchema() throws Exception {
    DrillAuthorizer mockAuthorizer = mock(DrillAuthorizer.class);
    setStaticField(DrillAccessControl.class, "enabled", true);
    setStaticField(DrillAccessControl.class, "authorizer", mockAuthorizer);

    assertTrue(DrillAccessControl.checkTableAccess(
        "root", "dfs", "", "orders", DrillAccessType.SELECT));
    assertTrue(DrillAccessControl.checkTableAccess(
        "root", "dfs", "   ", "orders", DrillAccessType.SELECT));
  }

  @Test
  public void checkTableAccess_delegatesToAuthorizer_whenEnabled() throws Exception {
    DrillAuthorizer mockAuthorizer = mock(DrillAuthorizer.class);
    when(mockAuthorizer.checkTableAccess(
        org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.eq(DrillAccessType.SELECT))).thenReturn(true);
    setStaticField(DrillAccessControl.class, "enabled", true);
    setStaticField(DrillAccessControl.class, "authorizer", mockAuthorizer);

    boolean result = DrillAccessControl.checkTableAccess(
        "root", "mysql", "shf", "orders", DrillAccessType.SELECT);
    assertTrue(result);
    verify(mockAuthorizer).checkTableAccess(
        org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.eq(DrillAccessType.SELECT));
  }

  @Test
  public void checkTableAccess_returnsFalse_whenAuthorizerThrows() throws Exception {
    DrillAuthorizer mockAuthorizer = mock(DrillAuthorizer.class);
    when(mockAuthorizer.checkTableAccess(
        org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.any())).thenThrow(new RuntimeException("boom"));
    setStaticField(DrillAccessControl.class, "enabled", true);
    setStaticField(DrillAccessControl.class, "authorizer", mockAuthorizer);

    boolean result = DrillAccessControl.checkTableAccess(
        "root", "mysql", "shf", "orders", DrillAccessType.SELECT);
    assertFalse(result);
  }

  @Test
  public void checkColumnAccess_returnsFalse_whenAuthorizerThrows() throws Exception {
    DrillAuthorizer mockAuthorizer = mock(DrillAuthorizer.class);
    when(mockAuthorizer.checkColumnAccess(
        org.mockito.ArgumentMatchers.any(),
        org.mockito.ArgumentMatchers.any())).thenThrow(new RuntimeException("boom"));
    setStaticField(DrillAccessControl.class, "enabled", true);
    setStaticField(DrillAccessControl.class, "authorizer", mockAuthorizer);

    Set<String> columns = new HashSet<>();
    columns.add("amount");
    boolean result = DrillAccessControl.checkColumnAccess(
        "root", "mysql", "shf", "orders", columns, DrillAccessType.SELECT);
    assertFalse(result);
  }

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

    Set<String> groups = DrillAccessControl.getUserGroups("root");
    assertNotNull(groups);
    assertEquals(new HashSet<>(Arrays.asList("root", "wheel")), groups);
  }

  @Test
  public void getUserGroups_returnsEmptySet_whenUgiThrows() {
    // Verifies the catch-block fallback: when UGI lookup throws, the method
    // returns an empty set instead of propagating the exception.
    when(mockUgi.getGroupNames()).thenThrow(new RuntimeException("ugi boom"));

    Set<String> groups = DrillAccessControl.getUserGroups("root");
    assertNotNull(groups);
    assertTrue(groups.isEmpty());
  }

  // ========================================================================
  // Reflection helpers
  // ========================================================================

  private static void setStaticField(Class<?> clazz, String fieldName, Object value) throws Exception {
    Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(null, value);
  }
}
