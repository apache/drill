/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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
package org.apache.drill.exec.security.ranger;

import org.apache.drill.test.BaseTest;
import org.apache.ranger.authorization.drill.authorizer.DrillAccessControl;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RangerAccessAuthorizer}.
 *
 * <p>{@code RangerAccessAuthorizer} delegates to {@code DrillAccessControl}
 * (in {@code drill-ranger-plugin}) via reflection through an isolated
 * {@link RangerPluginClassLoader}. These tests verify the delegation by:</p>
 * <ol>
 *   <li>Injecting a mock {@link RangerPluginClassLoader} via the package-private
 *       constructor {@link RangerAccessAuthorizer#RangerAccessAuthorizer(RangerPluginClassLoader)}.
 *       This is necessary because Mockito refuses to mock static methods of
 *       {@link ClassLoader} subclasses (to avoid class-loading infinite loops),
 *       so {@code RangerPluginClassLoader.getInstance()} cannot be stubbed. The
 *       mock classloader's {@code loadClass(String)} delegates to the test
 *       classloader, making {@code Class.forName(name, true, mockCl)} inside
 *       {@code init()} resolve to the test stub class ({@link DrillAccessControl})
 *       that lives in the test source tree.</li>
 *   <li>Mocking {@code DrillAccessControl} static methods via {@code mockStatic}
 *       to verify that the reflection-based {@code Method.invoke()} calls are
 *       correctly delegated.</li>
 * </ol>
 *
 * <p>{@code RangerAccessAuthorizer} delegates to the generic
 * {@code checkTableAccess(..., String)} /
 * {@code checkColumnAccess(..., Set, String)} methods on
 * {@code DrillAccessControl}. The {@code accessType} string (e.g.
 * {@link AccessTypes#SELECT}) is forwarded as-is; {@code DrillAccessControl}
 * converts it to {@code DrillAccessType} internally. These tests verify
 * dispatch to those generic String-operator overloads — no
 * {@code DrillAccessType} enum is visible on the main classpath.</p>
 *
 * <p>Fail-open / fail-closed behavior is verified without any classloader mock
 * by simply NOT calling {@code init()} (leaving {@code drillAccessControlClass}
 * as {@code null}).</p>
 */
public class RangerAccessAuthorizerTest extends BaseTest {

  private static final String USER = "alice";
  private static final String DS = "mysql";
  private static final String SCHEMA = "shf";
  private static final String TABLE = "orders";

  /**
   * Builds a mock {@link RangerPluginClassLoader} whose {@code loadClass(String)}
   * delegates to the test classloader. {@code Class.forName(name, true, mockCl)}
   * in {@link RangerAccessAuthorizer#init} calls {@code mockCl.loadClass(name)},
   * which resolves the test stub class (DrillAccessControl) from the test classpath.
   *
   * <p>{@code activate()} and {@code deactivate()} are no-ops on the mock (Mockito
   * default behavior), which is exactly what we want — no TCCL switching during
   * tests.</p>
   */
  private RangerPluginClassLoader mockPluginClassLoader() {
    RangerPluginClassLoader mockCl = mock(RangerPluginClassLoader.class);
    ClassLoader testCl = RangerAccessAuthorizerTest.class.getClassLoader();
    try {
      when(mockCl.loadClass(anyString())).thenAnswer(inv -> {
        String name = inv.getArgument(0);
        return Class.forName(name, false, testCl);
      });
    } catch (ClassNotFoundException e) {
      // mockCl.loadClass() on a Mockito mock never actually throws; this
      // catch is only to satisfy the compiler's checked-exception analysis.
      throw new RuntimeException(e);
    }
    return mockCl;
  }

  @Test
  public void isEnabled_returnsFalse_whenNotInitialized() {
    RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer();
    assertFalse(authorizer.isEnabled());
  }

  @Test
  public void checkTableAccess_failOpen_whenNotInitialized() {
    RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer();
    assertTrue(authorizer.checkTableAccess(USER, DS, SCHEMA, TABLE, AccessTypes.SELECT));
  }

  @Test
  public void checkColumnAccess_failOpen_whenNotInitialized() {
    RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer();
    Set<String> columns = new HashSet<>(Arrays.asList("id", "amount"));
    assertTrue(authorizer.checkColumnAccess(USER, DS, SCHEMA, TABLE, columns, AccessTypes.SELECT));
  }

  @Test
  public void init_delegatesToDrillAccessControl() {
    RangerPluginClassLoader mockCl = mockPluginClassLoader();
    try (MockedStatic<DrillAccessControl> dacMock = mockStatic(DrillAccessControl.class)) {
      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer(mockCl);
      authorizer.init("mySvc");

      dacMock.verify(() -> DrillAccessControl.init("mySvc"));
    }
  }

  @Test
  public void init_throwsRuntimeException_whenClassLoadingFails() {
    // Simulate loadClass() failure (exercises the same error-handling path in
    // init() as a getInstance() failure: both are wrapped in RuntimeException).
    RangerPluginClassLoader mockCl = mock(RangerPluginClassLoader.class);
    try {
      when(mockCl.loadClass(anyString()))
          .thenThrow(new ClassNotFoundException("class not found boom"));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer(mockCl);
    RuntimeException ex = assertThrows(
        RuntimeException.class, () -> authorizer.init("mySvc"));
    assertTrue(ex.getMessage().contains("Failed to initialize RangerAccessAuthorizer"));
  }

  @Test
  public void isEnabled_delegatesToDrillAccessControl() {
    RangerPluginClassLoader mockCl = mockPluginClassLoader();
    try (MockedStatic<DrillAccessControl> dacMock = mockStatic(DrillAccessControl.class)) {
      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer(mockCl);
      authorizer.init("mySvc");

      dacMock.when(DrillAccessControl::isEnabled).thenReturn(true);
      assertTrue(authorizer.isEnabled());

      dacMock.when(DrillAccessControl::isEnabled).thenReturn(false);
      assertFalse(authorizer.isEnabled());
    }
  }

  @Test
  public void checkTableAccess_delegatesToDrillAccessControl() {
    RangerPluginClassLoader mockCl = mockPluginClassLoader();
    try (MockedStatic<DrillAccessControl> dacMock = mockStatic(DrillAccessControl.class)) {
      dacMock.when(() -> DrillAccessControl.checkTableAccess(
          anyString(), anyString(), anyString(), anyString(), anyString()))
          .thenReturn(true);

      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer(mockCl);
      authorizer.init("mySvc");
      assertTrue(authorizer.checkTableAccess(USER, DS, SCHEMA, TABLE, AccessTypes.SELECT));

      dacMock.verify(() -> DrillAccessControl.checkTableAccess(
          eq(USER), eq(DS), eq(SCHEMA), eq(TABLE), eq(AccessTypes.SELECT)));
    }
  }

  @Test
  public void checkTableAccess_returnsFalse_whenInvocationThrows() {
    RangerPluginClassLoader mockCl = mockPluginClassLoader();
    try (MockedStatic<DrillAccessControl> dacMock = mockStatic(DrillAccessControl.class)) {
      dacMock.when(() -> DrillAccessControl.checkTableAccess(
          anyString(), anyString(), anyString(), anyString(), anyString()))
          .thenThrow(new RuntimeException("check boom"));

      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer(mockCl);
      authorizer.init("mySvc");
      // fail-closed on error
      assertFalse(authorizer.checkTableAccess(USER, DS, SCHEMA, TABLE, AccessTypes.SELECT));
    }
  }

  @Test
  public void checkColumnAccess_delegatesToDrillAccessControl() {
    RangerPluginClassLoader mockCl = mockPluginClassLoader();
    try (MockedStatic<DrillAccessControl> dacMock = mockStatic(DrillAccessControl.class)) {
      dacMock.when(() -> DrillAccessControl.checkColumnAccess(
          anyString(), anyString(), anyString(), anyString(), any(), anyString()))
          .thenReturn(true);

      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer(mockCl);
      authorizer.init("mySvc");
      Set<String> columns = new HashSet<>(Arrays.asList("id", "amount"));
      assertTrue(authorizer.checkColumnAccess(USER, DS, SCHEMA, TABLE, columns, AccessTypes.SELECT));

      dacMock.verify(() -> DrillAccessControl.checkColumnAccess(
          eq(USER), eq(DS), eq(SCHEMA), eq(TABLE), eq(columns), eq(AccessTypes.SELECT)));
    }
  }

  @Test
  public void checkColumnAccess_returnsFalse_whenInvocationThrows() {
    RangerPluginClassLoader mockCl = mockPluginClassLoader();
    try (MockedStatic<DrillAccessControl> dacMock = mockStatic(DrillAccessControl.class)) {
      dacMock.when(() -> DrillAccessControl.checkColumnAccess(
          anyString(), anyString(), anyString(), anyString(), any(), anyString()))
          .thenThrow(new RuntimeException("column check boom"));

      RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer(mockCl);
      authorizer.init("mySvc");
      Set<String> columns = new HashSet<>(Arrays.asList("id"));
      // fail-closed on error
      assertFalse(authorizer.checkColumnAccess(USER, DS, SCHEMA, TABLE, columns, AccessTypes.SELECT));
    }
  }
}
