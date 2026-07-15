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

import java.util.Set;

/**
 * Test-only {@link AccessAuthorizer} implementation used by
 * {@link AccessAuthorizerFactoryTest} to verify the factory's reflective
 * loading, init-delegation, and caching logic WITHOUT requiring the
 * {@code RangerPluginClassLoader} / {@code drill-ranger-plugin} infrastructure.
 */
public class TestAccessAuthorizer implements AccessAuthorizer {

  private static volatile String lastInitServiceName;
  private static volatile boolean enabled;
  private static volatile boolean shouldThrow;

  public static void reset() {
    lastInitServiceName = null;
    enabled = false;
    shouldThrow = false;
  }

  public static String getLastInitServiceName() {
    return lastInitServiceName;
  }

  public static void setEnabled(boolean value) {
    enabled = value;
  }

  public static void setShouldThrow(boolean value) {
    shouldThrow = value;
  }

  @Override
  public void init(String serviceName) {
    lastInitServiceName = serviceName;
    if (shouldThrow) {
      throw new RuntimeException("init boom");
    }
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public boolean checkTableAccess(String user, String dataSource, String schema,
                                  String table, String accessType) {
    return true;
  }

  @Override
  public boolean checkColumnAccess(String user, String dataSource, String schema,
                                   String table, Set<String> columns, String accessType) {
    return true;
  }
}
