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
package org.apache.drill.exec.security.ranger;

import org.apache.drill.exec.security.spi.AccessType;
import org.apache.drill.exec.security.spi.UserIdentity;
import org.apache.ranger.authorization.drill.authorizer.DrillAccessControl;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RangerAccessAuthorizer}.
 *
 * <p>{@code RangerAccessAuthorizer} delegates to {@code DrillAccessControl}
 * (in {@code drill-ranger-plugin}) through the {@link AccessAuthorizer}
 * SPI interface. These tests verify the delegation by:</p>
 * <ol>
 *   <li>Injecting a mock {@link RangerPluginClassLoader} via the package-private
 *       constructor {@link RangerAccessAuthorizer#RangerAccessAuthorizer(RangerPluginClassLoader, String)}.
 *       This is necessary because Mockito refuses to mock static methods of
 *       {@link ClassLoader} subclasses (to avoid class-loading infinite loops),
 *       so the production classloader holder cannot be stubbed. The mock
 *       classloader's {@code loadClass(String)} delegates to the test
 *       classloader, so the reflective class lookup resolves to the test stub
 *       class that lives in the test source tree (same FQCN as the real
 *       plugin class).</li>
 *   <li>Asserting against the stub's captured arguments and control knobs
 *       (see the stub {@link DrillAccessControl} for the available knobs).
 *       This exercises the real production path: reflective
 *       {@code getConstructor(String).newInstance(...)} + cast to
 *       {@link AccessAuthorizer} + direct virtual calls.</li>
 * </ol>
 *
 * <p>Initialization happens in the constructor (mirroring Presto's
 * RangerSystemAccessControl shim), so every test constructs the shim with a
 * service name; the service name reaches the stub's constructor through the
 * same reflective path used in production.</p>
 */
public class RangerAccessAuthorizerTest {

  private static final String USER = "alice";
  private static final String DS = "mysql";
  private static final String SCHEMA = "shf";
  private static final String TABLE = "orders";

  @Before
  public void resetStub() {
    DrillAccessControl.reset();
  }

  /**
   * Builds a mock {@link RangerPluginClassLoader} whose {@code loadClass(String)}
   * delegates to the test classloader. The reflective class lookup inside the
   * shim constructor resolves the test stub class (DrillAccessControl) from
   * the test classpath.
   *
   * <p>{@code activate()} and {@code deactivate()} are no-ops on the mock
   * (Mockito default behavior), which is exactly what we want — no TCCL
   * switching during tests.</p>
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
  public void constructor_passesServiceNameToDrillAccessControl() {
    RangerPluginClassLoader mockCl = mockPluginClassLoader();
    new RangerAccessAuthorizer(mockCl, "mySvc");
    assertEquals("mySvc", DrillAccessControl.lastServiceName);
  }

  @Test
  public void constructor_throwsRuntimeException_whenClassLoadingFails() {
    // Simulate loadClass() failure — the constructor wraps it in a
    // RuntimeException (fail-closed) instead of leaking checked exceptions.
    RangerPluginClassLoader mockCl = mock(RangerPluginClassLoader.class);
    try {
      when(mockCl.loadClass(anyString()))
          .thenThrow(new ClassNotFoundException("class not found boom"));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    RuntimeException ex = assertThrows(
        RuntimeException.class, () -> new RangerAccessAuthorizer(mockCl, "mySvc"));
    assertTrue(ex.getMessage().contains("Failed to initialize RangerAccessAuthorizer"));
  }

  @Test
  public void constructor_throwsRuntimeException_whenDelegateConstructionFails() {
    // Simulate DrillAccessControl initialization failure (e.g. Ranger Admin
    // unreachable): the shim must surface a RuntimeException (fail-closed).
    DrillAccessControl.constructFails = true;
    RangerPluginClassLoader mockCl = mockPluginClassLoader();

    RuntimeException ex = assertThrows(
        RuntimeException.class, () -> new RangerAccessAuthorizer(mockCl, "mySvc"));
    assertTrue(ex.getMessage().contains("Failed to initialize RangerAccessAuthorizer"));
  }

  @Test
  public void checkTableAccess_delegatesToDrillAccessControl() {
    RangerPluginClassLoader mockCl = mockPluginClassLoader();
    DrillAccessControl.result = true;
    RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer(mockCl, "mySvc");

    assertTrue(authorizer.checkTableAccess(
        UserIdentity.of(USER), DS, SCHEMA, TABLE, AccessType.SELECT));

    assertEquals(USER, DrillAccessControl.lastUser.getUser());
    assertEquals(DS, DrillAccessControl.lastDataSource);
    assertEquals(SCHEMA, DrillAccessControl.lastSchema);
    assertEquals(TABLE, DrillAccessControl.lastTable);
    assertEquals(AccessType.SELECT, DrillAccessControl.lastAccessType);
  }

  @Test
  public void checkTableAccess_returnsFalse_whenInvocationThrows() {
    RangerPluginClassLoader mockCl = mockPluginClassLoader();
    DrillAccessControl.checkFailure = new RuntimeException("check boom");
    RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer(mockCl, "mySvc");

    // fail-closed on error
    assertFalse(authorizer.checkTableAccess(
        UserIdentity.of(USER), DS, SCHEMA, TABLE, AccessType.SELECT));
  }

  @Test
  public void checkColumnAccess_delegatesToDrillAccessControl() {
    RangerPluginClassLoader mockCl = mockPluginClassLoader();
    DrillAccessControl.result = false;
    RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer(mockCl, "mySvc");

    Set<String> columns = new HashSet<>(Arrays.asList("id", "amount"));
    assertFalse(authorizer.checkColumnAccess(
        UserIdentity.of(USER), DS, SCHEMA, TABLE, columns, AccessType.SELECT));

    assertEquals(USER, DrillAccessControl.lastUser.getUser());
    assertEquals(DS, DrillAccessControl.lastDataSource);
    assertEquals(SCHEMA, DrillAccessControl.lastSchema);
    assertEquals(TABLE, DrillAccessControl.lastTable);
    assertEquals(columns, DrillAccessControl.lastColumns);
    assertEquals(AccessType.SELECT, DrillAccessControl.lastAccessType);
  }

  @Test
  public void checkColumnAccess_returnsFalse_whenInvocationThrows() {
    RangerPluginClassLoader mockCl = mockPluginClassLoader();
    DrillAccessControl.checkFailure = new RuntimeException("column check boom");
    RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer(mockCl, "mySvc");

    Set<String> columns = new HashSet<>(Arrays.asList("id"));
    // fail-closed on error
    assertFalse(authorizer.checkColumnAccess(
        UserIdentity.of(USER), DS, SCHEMA, TABLE, columns, AccessType.SELECT));
  }

  // ========================================================================
  // Shutdown lifecycle
  // ========================================================================

  @Test
  public void close_delegatesToDrillAccessControl() {
    RangerPluginClassLoader mockCl = mockPluginClassLoader();
    RangerAccessAuthorizer authorizer = new RangerAccessAuthorizer(mockCl, "mySvc");

    authorizer.close();

    assertEquals("close() must be forwarded to the delegate", 1,
        DrillAccessControl.closeCount);
  }
}
