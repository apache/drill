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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.BaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AccessAuthorizerFactory}.
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
    TestAccessAuthorizer.reset();
  }

  // ========================================================================
  // Disabled → NoOp
  // ========================================================================

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

  /**
   * Verifies that when {@code RANGER_AUTH_ENABLED=true} and no impl class is
   * configured, the factory selects {@link RangerAccessAuthorizer} as the
   * default impl.
   *
   * <p>Tests the selection logic via the package-private
   * {@link AccessAuthorizerFactory#getImplClassName(DrillConfig)} method rather
   * than calling {@code getAuthorizer()} directly. This avoids triggering
   * {@code RangerAccessAuthorizer.init()}, which calls
   * {@code RangerPluginClassLoader.getInstance()} — Mockito cannot mock this
   * static (it is a {@link ClassLoader} subclass) and the real call crashes
   * the JVM in the test environment. Full delegation testing of
   * {@link RangerAccessAuthorizer} is covered by
   * {@link RangerAccessAuthorizerTest} using constructor injection.</p>
   */
  @Test
  public void getImplClassName_returnsDefault_whenNotConfigured() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.RANGER_AUTH_ENABLED, "true");
    DrillConfig config = DrillConfig.create(props);

    assertEquals(RangerAccessAuthorizer.class.getName(),
        AccessAuthorizerFactory.getImplClassName(config));
  }

  @Test
  public void getImplClassName_returnsConfigured_whenSet() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.RANGER_AUTH_ENABLED, "true");
    props.setProperty(ExecConstants.RANGER_AUTHORIZER_IMPL,
        "com.example.MyAuthorizer");
    DrillConfig config = DrillConfig.create(props);

    assertEquals("com.example.MyAuthorizer",
        AccessAuthorizerFactory.getImplClassName(config));
  }

  @Test
  public void getServiceName_returnsDefault_whenNotConfigured() {
    DrillConfig config = DrillConfig.forClient();
    assertEquals(AccessAuthorizerFactory.DEFAULT_SERVICE_NAME,
        AccessAuthorizerFactory.getServiceName(config));
  }

  @Test
  public void getServiceName_returnsConfigured_whenSet() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.RANGER_SERVICE_NAME, "myDrillSvc");
    DrillConfig config = DrillConfig.create(props);

    assertEquals("myDrillSvc",
        AccessAuthorizerFactory.getServiceName(config));
  }

  // ========================================================================
  // Default service name — uses TestAccessAuthorizer (no classloader mock)
  // ========================================================================

  @Test
  public void getAuthorizer_usesDefaultServiceName_whenServiceNameAbsent() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.RANGER_AUTH_ENABLED, "true");
    // impl explicitly set to the test stub; service name left to default ("drill")
    props.setProperty(ExecConstants.RANGER_AUTHORIZER_IMPL,
        TestAccessAuthorizer.class.getName());
    DrillConfig config = DrillConfig.create(props);

    AccessAuthorizer authorizer = AccessAuthorizerFactory.getAuthorizer(config);
    assertTrue(authorizer instanceof TestAccessAuthorizer);
    // Factory must pass the default service name "drill" to init()
    assertEquals("drill", TestAccessAuthorizer.getLastInitServiceName());
  }

  @Test
  public void getAuthorizer_usesCustomImpl_whenConfigured() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.RANGER_AUTH_ENABLED, "true");
    props.setProperty(ExecConstants.RANGER_AUTHORIZER_IMPL,
        TestAccessAuthorizer.class.getName());
    props.setProperty(ExecConstants.RANGER_SERVICE_NAME, "myDrillSvc");
    DrillConfig config = DrillConfig.create(props);

    AccessAuthorizer authorizer = AccessAuthorizerFactory.getAuthorizer(config);
    assertTrue(authorizer instanceof TestAccessAuthorizer);
    assertEquals("myDrillSvc", TestAccessAuthorizer.getLastInitServiceName());
  }

  // ========================================================================
  // Failure modes
  // ========================================================================

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
    props.setProperty(ExecConstants.RANGER_AUTHORIZER_IMPL,
        TestAccessAuthorizer.class.getName());
    DrillConfig config = DrillConfig.create(props);

    TestAccessAuthorizer.setShouldThrow(true);
    RuntimeException ex = assertThrows(RuntimeException.class,
        () -> AccessAuthorizerFactory.getAuthorizer(config));
    assertTrue(ex.getMessage().contains("Failed to initialize Ranger authorizer"));
    // init() must have been attempted (and recorded the service name) before throwing
    assertEquals("drill", TestAccessAuthorizer.getLastInitServiceName());
  }

  /**
   * Sanity check that distinct configs (disabled vs enabled) produce non-identical
   * instances after a reset. This guards against the singleton cache leaking
   * across tests when @Before/@After reset is misconfigured.
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
    props.setProperty(ExecConstants.RANGER_AUTHORIZER_IMPL,
        TestAccessAuthorizer.class.getName());
    DrillConfig enabledConfig = DrillConfig.create(props);

    AccessAuthorizer second = AccessAuthorizerFactory.getAuthorizer(enabledConfig);
    assertNotSame(first, second);
    assertTrue(second instanceof TestAccessAuthorizer);
  }
}
