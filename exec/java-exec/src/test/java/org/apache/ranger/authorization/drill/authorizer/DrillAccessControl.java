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
package org.apache.ranger.authorization.drill.authorizer;

import java.util.Set;

/**
 * Test stub for the real {@code DrillAccessControl} class that lives in the
 * {@code drill-ranger-plugin} module (loaded by the isolated
 * {@code RangerPluginClassLoader} at runtime).
 */
public class DrillAccessControl {

  private static boolean enabled;

  private DrillAccessControl() {
  }

  public static synchronized void init(String serviceName) {
    enabled = true;
  }

  public static boolean isEnabled() {
    return enabled;
  }

  public static boolean checkTableAccess(String user, String dataSource, String schema,
      String table, String operator) {
    return true; // fail-open default
  }

  public static boolean checkColumnAccess(String user, String dataSource, String schema,
      String table, Set<String> columns, String operator) {
    return true; // fail-open default
  }
}
