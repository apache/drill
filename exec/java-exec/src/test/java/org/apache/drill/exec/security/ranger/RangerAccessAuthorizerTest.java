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
package org.apache.drill.exec.security.ranger;

import org.apache.drill.test.BaseTest;
import org.apache.ranger.authorization.drill.authorizer.DrillAccessControl;
import org.apache.ranger.authorization.drill.resource.DrillAccessType;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

/**
 * Unit tests for {@link RangerAccessAuthorizer}.
 * Verifies that all SPI methods delegate to {@link DrillAccessControl} with
 * the correct {@link DrillAccessType} (uppercase) and that the private
 * {@code toAccessType} helper maps null/unknown access types to {@code SELECT}.
 */
public class RangerAccessAuthorizerTest extends BaseTest {

  private static final String USER = "alice";
  private static final String DS = "mysql";
  private static final String SCHEMA = "shf";
  private static final String TABLE = "orders";

  @Test
  public void init_delegatesToDrillAccessControl() {
    try (MockedStatic<DrillAccessControl> mocked = mockStatic(DrillAccessControl.class)) {
      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer();
      authorizer.init("mySvc");
      mocked.verify(() -> DrillAccessControl.init("mySvc"));
    }
  }

  @Test
  public void isEnabled_delegatesToDrillAccessControl() {
    try (MockedStatic<DrillAccessControl> mocked = mockStatic(DrillAccessControl.class)) {
      mocked.when(DrillAccessControl::isEnabled).thenReturn(true);
      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer();
      assertTrue(authorizer.isEnabled());

      mocked.when(DrillAccessControl::isEnabled).thenReturn(false);
      assertFalse(authorizer.isEnabled());
    }
  }

  @Test
  public void checkTableAccess_delegatesWithUppercaseSelect() {
    try (MockedStatic<DrillAccessControl> mocked = mockStatic(DrillAccessControl.class)) {
      mocked.when(() -> DrillAccessControl.checkTableAccess(
          anyString(), anyString(), anyString(), anyString(), any()))
          .thenReturn(true);
      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer();
      boolean result = authorizer.checkTableAccess(USER, DS, SCHEMA, TABLE, "select");
      assertTrue(result);
      // accessType is normalized to upper-case enum value
      mocked.verify(() -> DrillAccessControl.checkTableAccess(
          eq(USER), eq(DS), eq(SCHEMA), eq(TABLE), eq(DrillAccessType.SELECT)));
    }
  }

  @Test
  public void checkTableAccess_delegatesWithNullAccessType_defaultsToSelect() {
    try (MockedStatic<DrillAccessControl> mocked = mockStatic(DrillAccessControl.class)) {
      mocked.when(() -> DrillAccessControl.checkTableAccess(
          anyString(), anyString(), anyString(), anyString(), any()))
          .thenReturn(true);
      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer();
      boolean result = authorizer.checkTableAccess(USER, DS, SCHEMA, TABLE, null);
      assertTrue(result);
      mocked.verify(() -> DrillAccessControl.checkTableAccess(
          eq(USER), eq(DS), eq(SCHEMA), eq(TABLE), eq(DrillAccessType.SELECT)));
    }
  }

  @Test
  public void checkTableAccess_delegatesWithUnknownAccessType_defaultsToSelect() {
    try (MockedStatic<DrillAccessControl> mocked = mockStatic(DrillAccessControl.class)) {
      mocked.when(() -> DrillAccessControl.checkTableAccess(
          anyString(), anyString(), anyString(), anyString(), any()))
          .thenReturn(true);
      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer();
      // "FOO" is not a valid DrillAccessType — must fall back to SELECT
      boolean result = authorizer.checkTableAccess(USER, DS, SCHEMA, TABLE, "FOO");
      assertTrue(result);
      mocked.verify(() -> DrillAccessControl.checkTableAccess(
          eq(USER), eq(DS), eq(SCHEMA), eq(TABLE), eq(DrillAccessType.SELECT)));
    }
  }

  @Test
  public void checkTableAccess_delegatesWithCreateAccessType() {
    try (MockedStatic<DrillAccessControl> mocked = mockStatic(DrillAccessControl.class)) {
      mocked.when(() -> DrillAccessControl.checkTableAccess(
          anyString(), anyString(), anyString(), anyString(), any()))
          .thenReturn(true);
      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer();
      boolean result = authorizer.checkTableAccess(USER, DS, SCHEMA, TABLE, "create");
      assertTrue(result);
      mocked.verify(() -> DrillAccessControl.checkTableAccess(
          eq(USER), eq(DS), eq(SCHEMA), eq(TABLE), eq(DrillAccessType.CREATE)));
    }
  }

  @Test
  public void checkColumnAccess_delegatesPerColumn() {
    try (MockedStatic<DrillAccessControl> mocked = mockStatic(DrillAccessControl.class)) {
      mocked.when(() -> DrillAccessControl.checkColumnAccess(
          anyString(), anyString(), anyString(), anyString(), any(), any()))
          .thenReturn(true);
      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer();
      Set<String> columns = new HashSet<>(Arrays.asList("id", "amount"));
      boolean result = authorizer.checkColumnAccess(
          USER, DS, SCHEMA, TABLE, columns, "select");
      assertTrue(result);
      mocked.verify(() -> DrillAccessControl.checkColumnAccess(
          eq(USER), eq(DS), eq(SCHEMA), eq(TABLE),
          eq(columns), eq(DrillAccessType.SELECT)));
    }
  }

  @Test
  public void checkColumnAccess_withNullAccessType_defaultsToSelect() {
    try (MockedStatic<DrillAccessControl> mocked = mockStatic(DrillAccessControl.class)) {
      mocked.when(() -> DrillAccessControl.checkColumnAccess(
          anyString(), anyString(), anyString(), anyString(), any(), any()))
          .thenReturn(true);
      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer();
      Set<String> columns = new HashSet<>(Arrays.asList("id"));
      boolean result = authorizer.checkColumnAccess(
          USER, DS, SCHEMA, TABLE, columns, null);
      assertTrue(result);
      mocked.verify(() -> DrillAccessControl.checkColumnAccess(
          eq(USER), eq(DS), eq(SCHEMA), eq(TABLE),
          eq(columns), eq(DrillAccessType.SELECT)));
    }
  }
}
