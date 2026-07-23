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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.BaseTest;
import org.apache.ranger.authorization.drill.authorizer.DrillAccessControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

/**
 * Unit tests for {@link AccessAuthorizerFactory}.
 * Covers the configuration-driven reflective loading of the {@link AccessAuthorizer}
 * SPI, including default fallbacks, caching, and failure modes.
 */
public class AccessAuthorizerFactoryTest extends BaseTest {

  /**
   * Resets the {@code instance} singleton before each test so that the
   * double-checked locking in {@link AccessAuthorizerFactory#getAuthorizer}
   * re-runs the initialization path. The field is {@code private static volatile}
   * and must be cleared via reflection.
   */
  @Before
  @After
  public void resetFactoryInstance() throws Exception {
    Field f = AccessAuthorizerFactory.class.getDeclaredField("instance");
    f.setAccessible(true);
    f.set(null, null);
  }

  @Test
  public void getAuthorizer_returnsNoOp_whenRangerDisabled() {
    // No RANGER_AUTH_ENABLED property at all → treated as disabled
    DrillConfig config = DrillConfig.forClient();
    AccessAuthorizer authorizer = AccessAuthorizerFactory.getAuthorizer(config);
    assertTrue("Expected NoOpAccessAuthorizer when ranger disabled",
        authorizer instanceof NoOpAccessAuthorizer);
    assertFalse(authorizer.isEnabled());
  }

  @Test
  public void getAuthorizer_returnsNoOp_whenRangerEnabledFalse() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.RANGER_AUTH_ENABLED, "false");
    DrillConfig config = DrillConfig.create(props);
    AccessAuthorizer authorizer = AccessAuthorizerFactory.getAuthorizer(config);
    assertTrue("Expected NoOpAccessAuthorizer when ranger.enabled=false",
        authorizer instanceof NoOpAccessAuthorizer);
    assertFalse(authorizer.isEnabled());
  }

  @Test
  public void getAuthorizer_returnsCachedInstance() {
    DrillConfig config = DrillConfig.forClient();
    AccessAuthorizer first = AccessAuthorizerFactory.getAuthorizer(config);
    AccessAuthorizer second = AccessAuthorizerFactory.getAuthorizer(config);
    assertSame("Singleton must cache the same instance", first, second);
  }

  @Test
  public void getAuthorizer_usesDefaultImpl_whenImplKeyAbsent() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.RANGER_AUTH_ENABLED, "true");
    DrillConfig config = DrillConfig.create(props);

    // Mock DrillAccessControl so RangerAccessAuthorizer.init() does not attempt
    // to actually initialize the Ranger plugin (which would fail without a
    // service-def / policy cache on the classpath).
    try (MockedStatic<DrillAccessControl> mocked = mockStatic(DrillAccessControl.class)) {
      mocked.when(() -> DrillAccessControl.isEnabled()).thenReturn(true);
      AccessAuthorizer authorizer = AccessAuthorizerFactory.getAuthorizer(config);
      assertTrue("Expected default RangerAccessAuthorizer impl",
          authorizer instanceof RangerAccessAuthorizer);
      assertTrue(authorizer.isEnabled());
      // init("drill") should have been invoked by the factory
      mocked.verify(() -> DrillAccessControl.init("drill"));
    }
  }

  @Test
  public void getAuthorizer_usesDefaultServiceName_whenServiceNameAbsent() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.RANGER_AUTH_ENABLED, "true");
    // impl explicitly set; service name left to default ("drill")
    props.setProperty(ExecConstants.RANGER_AUTHORIZER_IMPL,
        "org.apache.drill.exec.security.ranger.RangerAccessAuthorizer");
    DrillConfig config = DrillConfig.create(props);

    try (MockedStatic<DrillAccessControl> mocked = mockStatic(DrillAccessControl.class)) {
      mocked.when(() -> DrillAccessControl.isEnabled()).thenReturn(true);
      AccessAuthorizer authorizer = AccessAuthorizerFactory.getAuthorizer(config);
      assertTrue(authorizer instanceof RangerAccessAuthorizer);
      mocked.verify(() -> DrillAccessControl.init("drill"));
    }
  }

  @Test
  public void getAuthorizer_usesCustomImpl_whenConfigured() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.RANGER_AUTH_ENABLED, "true");
    props.setProperty(ExecConstants.RANGER_AUTHORIZER_IMPL,
        "org.apache.drill.exec.security.ranger.RangerAccessAuthorizer");
    props.setProperty(ExecConstants.RANGER_SERVICE_NAME, "myDrillSvc");
    DrillConfig config = DrillConfig.create(props);

    try (MockedStatic<DrillAccessControl> mocked = mockStatic(DrillAccessControl.class)) {
      mocked.when(() -> DrillAccessControl.isEnabled()).thenReturn(true);
      AccessAuthorizer authorizer = AccessAuthorizerFactory.getAuthorizer(config);
      assertTrue(authorizer instanceof RangerAccessAuthorizer);
      mocked.verify(() -> DrillAccessControl.init("myDrillSvc"));
    }
  }

  @Test
  public void getAuthorizer_throwsRuntimeException_whenImplClassNotFound() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.RANGER_AUTH_ENABLED, "true");
    props.setProperty(ExecConstants.RANGER_AUTHORIZER_IMPL,
        "org.example.nonexistent.Authorizer");
    DrillConfig config = DrillConfig.create(props);

    RuntimeException ex = assertThrows(RuntimeException.class,
        () -> AccessAuthorizerFactory.getAuthorizer(config));
    // The factory wraps ClassNotFoundException in RuntimeException
    assertTrue(ex.getMessage().contains("Failed to initialize Ranger authorizer"));
  }

  @Test
  public void getAuthorizer_throwsRuntimeException_whenInitFails() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.RANGER_AUTH_ENABLED, "true");
    // Use default impl (RangerAccessAuthorizer); force init() to throw via mock
    DrillConfig config = DrillConfig.create(props);

    try (MockedStatic<DrillAccessControl> mocked = mockStatic(DrillAccessControl.class)) {
      mocked.when(() -> DrillAccessControl.init(anyString()))
          .thenThrow(new RuntimeException("init boom"));
      RuntimeException ex = assertThrows(RuntimeException.class,
          () -> AccessAuthorizerFactory.getAuthorizer(config));
      assertTrue(ex.getMessage().contains("Failed to initialize Ranger authorizer"));
    }
  }

  /**
   * Sanity check that distinct configs (disabled vs enabled) produce non-identical
   * instances after a reset. This guards against the singleton cache leaking
   * across tests when @Before/@After reset ismisconfigured.
   */
  @Test
  public void getAuthorizer_factoryReinitializesAfterReset() throws Exception {
    DrillConfig disabledConfig = DrillConfig.forClient();
    AccessAuthorizer first = AccessAuthorizerFactory.getAuthorizer(disabledConfig);
    assertTrue(first instanceof NoOpAccessAuthorizer);

    // Reset and ask for an enabled config — must NOT return the cached NoOp
    resetFactoryInstance();
    Properties props = new Properties();
    props.setProperty(ExecConstants.RANGER_AUTH_ENABLED, "true");
    DrillConfig enabledConfig = DrillConfig.create(props);

    try (MockedStatic<DrillAccessControl> mocked = mockStatic(DrillAccessControl.class)) {
      mocked.when(() -> DrillAccessControl.isEnabled()).thenReturn(true);
      AccessAuthorizer second = AccessAuthorizerFactory.getAuthorizer(enabledConfig);
      assertNotSame(first, second);
      assertTrue(second instanceof RangerAccessAuthorizer);
    }
  }
}
