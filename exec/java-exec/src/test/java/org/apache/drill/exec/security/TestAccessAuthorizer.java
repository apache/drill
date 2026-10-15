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

import org.apache.drill.exec.security.spi.AccessAuthorizer;
import org.apache.drill.exec.security.spi.AccessType;
import org.apache.drill.exec.security.spi.UserIdentity;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Test-only {@link AccessAuthorizer} created by
 * {@link TestAccessAuthorizerFactory}. Records the flattened config map it
 * was created with so tests can assert the manager's config handling.
 *
 * <p>Also provides test knobs for authorization checks:
 * <ul>
 *   <li>{@code setDeniedAccessTypes(...)} — denies exactly the given access
 *       types (default: deny nothing, i.e. allow all);</li>
 *   <li>{@code getLastTableCheck()} / {@code getLastColumnCheck()} — records
 *       the most recent check arguments so tests can assert resource mapping
 *       (datasource/schema/table and the requesting user).</li>
 * </ul>
 * All state is static so a single instance created via the manager serves a
 * whole embedded Drillbit; call {@link #reset()} between tests.</p>
 */
public class TestAccessAuthorizer implements AccessAuthorizer {

  /** Arguments of the most recent checkTableAccess call. */
  public static class TableCheck {
    public final String user;
    public final String dataSource;
    public final String schema;
    public final String table;
    public final AccessType accessType;

    TableCheck(String user, String dataSource, String schema, String table, AccessType accessType) {
      this.user = user;
      this.dataSource = dataSource;
      this.schema = schema;
      this.table = table;
      this.accessType = accessType;
    }
  }

  /** Arguments of the most recent checkColumnAccess call. */
  public static class ColumnCheck {
    public final String user;
    public final String dataSource;
    public final String schema;
    public final String table;
    public final Set<String> columns;
    public final AccessType accessType;

    ColumnCheck(String user, String dataSource, String schema, String table,
        Set<String> columns, AccessType accessType) {
      this.user = user;
      this.dataSource = dataSource;
      this.schema = schema;
      this.table = table;
      this.columns = columns;
      this.accessType = accessType;
    }
  }

  private static volatile boolean shouldThrow;
  private static volatile String lastServiceName;
  private static volatile Map<String, String> lastConfig;
  private static volatile Set<AccessType> deniedAccessTypes = Collections.emptySet();
  private static volatile TableCheck lastTableCheck;
  private static volatile ColumnCheck lastColumnCheck;
  private static volatile int closeCount;

  public static void reset() {
    shouldThrow = false;
    lastServiceName = null;
    lastConfig = null;
    deniedAccessTypes = Collections.emptySet();
    lastTableCheck = null;
    lastColumnCheck = null;
    closeCount = 0;
  }

  public static String getLastServiceName() {
    return lastServiceName;
  }

  public static Map<String, String> getLastConfig() {
    return lastConfig;
  }

  public static void setShouldThrow(boolean value) {
    shouldThrow = value;
  }

  /**
   * Sets the access types that will be denied. Any access type not in the set
   * is allowed. Default (after {@link #reset()}) is an empty set: allow all.
   */
  public static void setDeniedAccessTypes(Set<AccessType> types) {
    deniedAccessTypes = types == null ? Collections.emptySet() : types;
  }

  /** Returns the most recent checkTableAccess call, or {@code null} if none. */
  public static TableCheck getLastTableCheck() {
    return lastTableCheck;
  }

  /** Returns the most recent checkColumnAccess call, or {@code null} if none. */
  public static ColumnCheck getLastColumnCheck() {
    return lastColumnCheck;
  }

  /** Returns how many times close() has been invoked (shutdown lifecycle). */
  public static int getCloseCount() {
    return closeCount;
  }

  TestAccessAuthorizer(Map<String, String> config) {
    lastConfig = config;
    lastServiceName = config.getOrDefault("service.name", "drill");
    if (shouldThrow) {
      throw new RuntimeException("create boom");
    }
  }

  @Override
  public boolean checkTableAccess(UserIdentity user, String dataSource, String schema,
                                   String table, AccessType accessType) {
    lastTableCheck = new TableCheck(user.getUser(), dataSource, schema, table, accessType);
    return !deniedAccessTypes.contains(accessType);
  }

  @Override
  public boolean checkColumnAccess(UserIdentity user, String dataSource, String schema,
                                    String table, Set<String> columns, AccessType accessType) {
    lastColumnCheck = new ColumnCheck(user.getUser(), dataSource, schema, table, columns, accessType);
    return !deniedAccessTypes.contains(accessType);
  }

  @Override
  public void close() {
    closeCount++;
  }
}
