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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the lifecycle of {@link RangerBaseAuthorizer}, the singleton
 * wrapper around {@link RangerDrillPlugin}.
 *
 * <p>The tests operate on the real singleton and reset its plugin field before
 * (and after) each test so state never leaks across cases. No Ranger Admin or
 * network is involved: the plugin is either a Mockito mock injected via
 * reflection, or a mock produced by {@link MockedConstruction} for the
 * {@code init()} path.</p>
 */
public class RangerBaseAuthorizerTest {

  private final RangerBaseAuthorizer authorizer = RangerBaseAuthorizer.getInstance();

  @BeforeEach
  @AfterEach
  public void resetPluginField() throws Exception {
    setPluginField(null);
  }

  private void setPluginField(RangerDrillPlugin plugin) throws Exception {
    Field f = RangerBaseAuthorizer.class.getDeclaredField("plugin");
    f.setAccessible(true);
    f.set(authorizer, plugin);
  }

  private RangerDrillPlugin getPluginField() throws Exception {
    Field f = RangerBaseAuthorizer.class.getDeclaredField("plugin");
    f.setAccessible(true);
    return (RangerDrillPlugin) f.get(authorizer);
  }

  @Test
  public void cleanUp_releasesPluginAndClearsField() throws Exception {
    RangerDrillPlugin plugin = mock(RangerDrillPlugin.class);
    setPluginField(plugin);

    authorizer.cleanUp();

    verify(plugin).cleanup();
    assertNull(getPluginField(),
        "cleanUp() must drop the plugin reference so a later init() can re-create it");
  }

  @Test
  public void cleanUp_isIdempotent() throws Exception {
    RangerDrillPlugin plugin = mock(RangerDrillPlugin.class);
    setPluginField(plugin);

    authorizer.cleanUp();
    authorizer.cleanUp();

    verify(plugin, times(1)).cleanup();
    assertNull(getPluginField());
  }

  @Test
  public void cleanUp_withoutInit_isSafe() {
    // Plugin field is null (never initialized, or already cleaned up):
    // cleanUp() must be a safe no-op.
    authorizer.cleanUp();
  }

  /**
   * After a cleanUp() the plugin field is null, so the next init() must
   * construct a fresh plugin instead of early-returning. Verified with a
   * {@link MockedConstruction} so no real Ranger Admin connection is
   * attempted.
   */
  @Test
  public void initAfterCleanUp_createsFreshPlugin() {
    try (MockedConstruction<RangerDrillPlugin> mocked =
             mockConstruction(RangerDrillPlugin.class)) {
      authorizer.init("svc-after-cleanup");

      RangerDrillPlugin constructed = mocked.constructed().get(0);
      assertNotNull(getPluginField());
      verify(constructed).init();
    } catch (Exception e) {
      throw new AssertionError("init() must succeed after cleanUp()", e);
    }
  }

  /**
   * While a plugin is still present (not yet cleaned up), a second init()
   * must early-return and NOT construct another plugin.
   */
  @Test
  public void init_withActivePlugin_isNoOp() {
    try (MockedConstruction<RangerDrillPlugin> mocked =
             mockConstruction(RangerDrillPlugin.class)) {
      authorizer.init("svc-1");
      authorizer.init("svc-2");

      // Only one plugin constructed; the second init() early-returned.
      assertEquals(1, mocked.constructed().size());
    } catch (Exception e) {
      throw new AssertionError("init() must not fail", e);
    }
  }
}