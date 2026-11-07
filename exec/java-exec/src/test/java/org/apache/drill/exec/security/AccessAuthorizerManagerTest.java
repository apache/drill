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
package org.apache.drill.exec.security;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.security.spi.AccessAuthorizer;
import org.apache.drill.test.BaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AccessAuthorizerManager}.
 *
 * <p>Uses the {@link TestAccessAuthorizerFactory} (name "test", registered via
 * the test META-INF/services file) to verify ServiceLoader-based factory
 * discovery and config flattening without requiring the production Ranger
 * shim's {@code RangerPluginClassLoader} infrastructure.</p>
 */
public class AccessAuthorizerManagerTest extends BaseTest {

  /**
   * Resets the {@code instance} singleton before/after each test so that the
   * double-checked locking in {@link AccessAuthorizerManager#getAuthorizer}
   * re-runs the initialization path. {@code reset()} is package-private;
   * this test lives in the same package.
   */
  @Before
  @After
  public void resetManagerInstance() {
    AccessAuthorizerManager.reset();
    TestAccessAuthorizer.reset();
  }

  // ========================================================================
  // Disabled → AllowAll (fail-open)
  // ========================================================================

  @Test
  public void getAuthorizer_returnsAllowAll_whenAuthorizerConfigAbsent() {
    // No drill.exec.security.authorizer property at all → treated as disabled
    DrillConfig config = DrillConfig.forClient();
    AccessAuthorizer authorizer = AccessAuthorizerManager.getAuthorizer(config);
    assertTrue("Expected AllowAllAccessAuthorizer when authorizer config absent",
        authorizer instanceof AllowAllAccessAuthorizer);
  }

  @Test
  public void getAuthorizer_returnsAllowAll_whenEnabledFalse() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.AUTHORIZER_ENABLED, "false");
    DrillConfig config = DrillConfig.create(props);
    AccessAuthorizer authorizer = AccessAuthorizerManager.getAuthorizer(config);
    assertTrue("Expected AllowAllAccessAuthorizer when enabled=false",
        authorizer instanceof AllowAllAccessAuthorizer);
  }

  @Test
  public void getAuthorizer_returnsCachedInstance() {
    DrillConfig config = DrillConfig.forClient();
    AccessAuthorizer first = AccessAuthorizerManager.getAuthorizer(config);
    AccessAuthorizer second = AccessAuthorizerManager.getAuthorizer(config);
    assertSame("Singleton must cache the same instance", first, second);
  }

  // ========================================================================
  // Factory discovery via ServiceLoader (test factory, name="test")
  // ========================================================================

  @Test
  public void getAuthorizer_selectsFactoryByConfiguredName() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.AUTHORIZER_ENABLED, "true");
    props.setProperty(ExecConstants.AUTHORIZER_NAME, "test");
    DrillConfig config = DrillConfig.create(props);

    AccessAuthorizer authorizer = AccessAuthorizerManager.getAuthorizer(config);
    assertTrue(authorizer instanceof TestAccessAuthorizer);
  }

  /**
   * The flattened config passed to the factory must NOT contain the
   * engine-managed selection keys ("enabled", "name") — only the
   * plugin-specific remainder of the subtree.
   */
  @Test
  public void getAuthorizer_passesFlattenedConfigWithoutSelectionKeys() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.AUTHORIZER_ENABLED, "true");
    props.setProperty(ExecConstants.AUTHORIZER_NAME, "test");
    props.setProperty("drill.exec.security.authorizer.service.name", "myDrillSvc");
    props.setProperty("drill.exec.security.authorizer.custom.key", "customValue");
    DrillConfig config = DrillConfig.create(props);

    AccessAuthorizerManager.getAuthorizer(config);

    assertEquals("myDrillSvc", TestAccessAuthorizer.getLastConfig().get("service.name"));
    assertEquals("customValue", TestAccessAuthorizer.getLastConfig().get("custom.key"));
    assertFalse("enabled must not leak into factory config",
        TestAccessAuthorizer.getLastConfig().containsKey("enabled"));
    assertFalse("name must not leak into factory config",
        TestAccessAuthorizer.getLastConfig().containsKey("name"));
  }

  @Test
  public void getAuthorizer_usesDefaultServiceName_whenServiceNameAbsent() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.AUTHORIZER_ENABLED, "true");
    props.setProperty(ExecConstants.AUTHORIZER_NAME, "test");
    DrillConfig config = DrillConfig.create(props);

    AccessAuthorizerManager.getAuthorizer(config);
    // The test factory applies the same default as the Ranger shim: "drill"
    assertEquals("drill", TestAccessAuthorizer.getLastServiceName());
  }

  @Test
  public void getAuthorizer_forwardsServiceName_whenConfigured() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.AUTHORIZER_ENABLED, "true");
    props.setProperty(ExecConstants.AUTHORIZER_NAME, "test");
    props.setProperty("drill.exec.security.authorizer.service.name", "myDrillSvc");
    DrillConfig config = DrillConfig.create(props);

    AccessAuthorizerManager.getAuthorizer(config);
    assertEquals("myDrillSvc", TestAccessAuthorizer.getLastServiceName());
  }

  // ========================================================================
  // Failure modes (fail-closed)
  // ========================================================================

  @Test
  public void getAuthorizer_throws_whenNoFactoryMatchesName() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.AUTHORIZER_ENABLED, "true");
    props.setProperty(ExecConstants.AUTHORIZER_NAME, "nonexistent");
    DrillConfig config = DrillConfig.create(props);

    RuntimeException ex = assertThrows(RuntimeException.class,
        () -> AccessAuthorizerManager.getAuthorizer(config));
    assertTrue(ex.getMessage().contains("nonexistent"));
  }

  @Test
  public void getAuthorizer_throws_whenFactoryCreateFails() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.AUTHORIZER_ENABLED, "true");
    props.setProperty(ExecConstants.AUTHORIZER_NAME, "test");
    DrillConfig config = DrillConfig.create(props);

    TestAccessAuthorizer.setShouldThrow(true);
    RuntimeException ex = assertThrows(RuntimeException.class,
        () -> AccessAuthorizerManager.getAuthorizer(config));
    assertTrue(ex.getMessage().contains("create boom"));
  }

  /**
   * Sanity check that distinct configs (disabled vs enabled) produce
   * non-identical instances after a reset. This guards against the singleton
   * cache leaking across tests when @Before/@After reset is misconfigured.
   */
  @Test
  public void getAuthorizer_reinitializesAfterReset() {
    DrillConfig disabledConfig = DrillConfig.forClient();
    AccessAuthorizer first = AccessAuthorizerManager.getAuthorizer(disabledConfig);
    assertTrue(first instanceof AllowAllAccessAuthorizer);

    // Reset and ask for an enabled config — must NOT return the cached allow-all
    AccessAuthorizerManager.reset();
    TestAccessAuthorizer.reset();
    Properties props = new Properties();
    props.setProperty(ExecConstants.AUTHORIZER_ENABLED, "true");
    props.setProperty(ExecConstants.AUTHORIZER_NAME, "test");
    DrillConfig enabledConfig = DrillConfig.create(props);

    AccessAuthorizer second = AccessAuthorizerManager.getAuthorizer(enabledConfig);
    assertNotSame(first, second);
    assertTrue(second instanceof TestAccessAuthorizer);
  }

  // ========================================================================
  // Shutdown / close lifecycle
  // ========================================================================

  @Test
  public void close_withoutInit_isSafe() {
    // No authorizer cached yet — close() must be a safe no-op.
    AccessAuthorizerManager.close();
  }

  @Test
  public void close_closesCachedAuthorizer_andClearsSingleton() {
    Properties props = new Properties();
    props.setProperty(ExecConstants.AUTHORIZER_ENABLED, "true");
    props.setProperty(ExecConstants.AUTHORIZER_NAME, "test");
    DrillConfig config = DrillConfig.create(props);

    AccessAuthorizer first = AccessAuthorizerManager.getAuthorizer(config);
    assertTrue(first instanceof TestAccessAuthorizer);

    AccessAuthorizerManager.close();

    assertEquals("close() must be forwarded to the cached authorizer", 1,
        TestAccessAuthorizer.getCloseCount());
    AccessAuthorizer second = AccessAuthorizerManager.getAuthorizer(config);
    assertNotSame("After close() the singleton must be re-creatable", first, second);
  }

  @Test
  public void close_isIdempotent_andReinitializesAfterRestart() {
    // Simulates a Drillbit shutdown + restart inside the same JVM: after
    // close() (even called twice), a new getAuthorizer() must build a fresh,
    // working instance — the old one dropped its plugin resources on close.
    Properties props = new Properties();
    props.setProperty(ExecConstants.AUTHORIZER_ENABLED, "true");
    props.setProperty(ExecConstants.AUTHORIZER_NAME, "test");
    DrillConfig config = DrillConfig.create(props);

    AccessAuthorizer first = AccessAuthorizerManager.getAuthorizer(config);
    AccessAuthorizerManager.close();
    AccessAuthorizerManager.close(); // singleton already cleared: safe no-op
    AccessAuthorizer second = AccessAuthorizerManager.getAuthorizer(config);

    assertEquals("close() must be forwarded exactly once", 1,
        TestAccessAuthorizer.getCloseCount());
    assertNotSame(first, second);
  }
}
