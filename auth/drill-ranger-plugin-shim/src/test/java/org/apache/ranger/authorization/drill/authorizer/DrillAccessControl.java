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

import org.apache.drill.exec.security.spi.AccessAuthorizer;
import org.apache.drill.exec.security.spi.AccessType;
import org.apache.drill.exec.security.spi.UserIdentity;

import java.util.Set;

/**
 * Test stub for the real {@code DrillAccessControl} class that lives in the
 * {@code drill-ranger-plugin} module (loaded by the isolated
 * {@code RangerPluginClassLoader} at runtime).
 *
 * <p>Mirrors the real class's instance-based SPI contract: constructor
 * {@code DrillAccessControl(String serviceName)} completes initialization,
 * then all checks arrive as {@link AccessAuthorizer} virtual calls. The
 * static fields below let tests control construction failure, check results
 * and check failures, and inspect the arguments the shim forwarded.</p>
 */
public class DrillAccessControl implements AccessAuthorizer {

  /** Service name passed to the last constructor invocation. */
  public static String lastServiceName;

  /** When {@code true}, the constructor throws (simulates init failure). */
  public static boolean constructFails;

  /** Value returned by the check methods. */
  public static boolean result = true;

  /** When non-null, thrown by the check methods (fail-closed path). */
  public static RuntimeException checkFailure;

  /** Number of close() invocations (shutdown lifecycle). */
  public static int closeCount;

  // Arguments captured from the last check calls
  public static UserIdentity lastUser;
  public static String lastDataSource;
  public static String lastSchema;
  public static String lastTable;
  public static AccessType lastAccessType;
  public static Set<String> lastColumns;

  public DrillAccessControl(String serviceName) {
    lastServiceName = serviceName;
    if (constructFails) {
      throw new RuntimeException("construct boom");
    }
  }

  @Override
  public boolean checkTableAccess(UserIdentity user, String dataSource, String schema,
      String table, AccessType accessType) {
    lastUser = user;
    lastDataSource = dataSource;
    lastSchema = schema;
    lastTable = table;
    lastAccessType = accessType;
    if (checkFailure != null) {
      throw checkFailure;
    }
    return result;
  }

  @Override
  public boolean checkColumnAccess(UserIdentity user, String dataSource, String schema,
      String table, Set<String> columns, AccessType accessType) {
    lastUser = user;
    lastDataSource = dataSource;
    lastSchema = schema;
    lastTable = table;
    lastColumns = columns;
    lastAccessType = accessType;
    if (checkFailure != null) {
      throw checkFailure;
    }
    return result;
  }

  @Override
  public void close() {
    closeCount++;
  }

  /** Resets all control knobs and captured arguments. */
  public static void reset() {
    lastServiceName = null;
    constructFails = false;
    result = true;
    checkFailure = null;
    closeCount = 0;
    lastUser = null;
    lastDataSource = null;
    lastSchema = null;
    lastTable = null;
    lastAccessType = null;
    lastColumns = null;
  }
}
